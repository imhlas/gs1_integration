# file: sharepoint_upload.py
from typing import Optional, Sequence, Dict, Tuple
from urllib.parse import urlparse, quote
import os
import re

import msal
import requests

from image_extractor import ImageExtractor, ItemImage
from delta_images import get_image_rows


# ---------------- PIKKUAPURI: normalisointi sarakenimille ----------------

def _norm(s: Optional[str]) -> str:
    """
    Normalisoi sarakenimen vertailua varten:
    - lower-case
    - trimmaa whitespace (myös non-breaking space)
    - korvaa kaikki yleiset viivat yhdenmukaiseksi '-'
    """
    if not s:
        return ""
    x = s.replace("\u00A0", " ")   # non-breaking space -> space
    for ch in ["\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212", "\u00AD"]:
        x = x.replace(ch, "-")
    x = re.sub(r"\s+", " ", x).strip().lower()
    return x


# ---------------- GRAPH HELPERS ----------------

def _acquire_graph_token(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    scope: Sequence[str],
) -> str:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(scopes=list(scope))
    if "access_token" not in result:
        raise RuntimeError(f"MSAL token error: {result.get('error')} - {result.get('error_description')}")
    return result["access_token"]

def _graph_get(url: str, token: str):
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if not r.ok:
        raise RuntimeError(f"GET {url} -> {r.status_code}: {r.text}")
    return r.json()

def _graph_post(url: str, token: str, json_body: dict):
    r = requests.post(url, json=json_body, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    if not r.ok:
        raise RuntimeError(f"POST {url} -> {r.status_code}: {r.text}")
    return r.json()

def _graph_patch(url: str, token: str, json_body: dict):
    r = requests.patch(url, json=json_body, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    if not r.ok:
        raise RuntimeError(f"PATCH {url} -> {r.status_code}: {r.text}")
    return r.json() if r.text else {}

def _graph_put_bytes(url: str, token: str, content: bytes):
    r = requests.put(url, data=content, headers={"Authorization": f"Bearer {token}"})
    if not r.ok:
        raise RuntimeError(f"PUT {url} -> {r.status_code}: {r.text}")
    return r.json()

def _get_site(token: str, site_url: str, graph_base: str, hostname: str) -> dict:
    path_part = site_url.split("://", 1)[-1].split("/", 1)[-1]
    url = f"{graph_base}/sites/{hostname}:/{path_part}"
    return _graph_get(url, token)

def _list_site_drives(token: str, site_id: str, graph_base: str) -> list:
    url = f"{graph_base}/sites/{site_id}/drives"
    data = _graph_get(url, token)
    return data.get("value", [])

def _get_drive_by_name(token: str, site_id: str, library_name: str, graph_base: str) -> dict:
    drives = _list_site_drives(token, site_id, graph_base)
    for d in drives:
        if _norm(d.get("name")) == _norm(library_name):
            return d
    raise RuntimeError(f"Drive (kirjasto) nimellä '{library_name}' ei löytynyt sivulta {site_id}")

def _ensure_folder_path(token: str, drive_id: str, folder_path: str, graph_base: str):
    if not folder_path or not folder_path.strip():
        return

    parts = [p for p in folder_path.strip("/").split("/") if p]
    cur = ""
    for p in parts:
        cur = f"{cur}/{p}" if cur else p
        check_url = f"{graph_base}/drives/{drive_id}/root:/{quote(cur)}"
        resp = requests.get(check_url, headers={"Authorization": f"Bearer {token}"})
        if resp.status_code == 404:
            parent = "/".join(cur.split("/")[:-1])
            parent_enc = quote(parent) if parent else ""
            create_url = (
                f"{graph_base}/drives/{drive_id}/root:/{parent_enc}:/children"
                if parent_enc else
                f"{graph_base}/drives/{drive_id}/root/children"
            )
            _graph_post(create_url, token, {
                "name": p,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "fail",
            })
        elif not resp.ok:
            raise RuntimeError(f"Check folder {cur} -> {resp.status_code}: {resp.text}")

def _upload_file(token: str, drive_id: str, target_subfolder: str, filename: str, content: bytes, graph_base: str) -> dict:
    full_path = f"{target_subfolder.rstrip('/')}/{filename}" if target_subfolder and target_subfolder.strip() else filename
    url = f"{graph_base}/drives/{drive_id}/root:/{quote(full_path)}:/content"
    return _graph_put_bytes(url, token, content)

def _get_list_for_drive(token: str, drive_id: str, graph_base: str) -> dict:
    url = f"{graph_base}/drives/{drive_id}/list"
    return _graph_get(url, token)

def _get_columns_for_list(token: str, site_id: str, list_id: str, graph_base: str) -> Dict[str, Tuple[str, str]]:
    """
    Palauttaa: displayName -> (id, internalName)
    """
    url = f"{graph_base}/sites/{site_id}/lists/{list_id}/columns"
    data = _graph_get(url, token)
    cols = {}
    for col in data.get("value", []):
        display = col.get("displayName")
        internal = col.get("name")
        cid = col.get("id")
        if display and internal:
            cols[display] = (cid, internal)
    return cols

def _find_internal_by_display(cols: Dict[str, Tuple[str, str]], target_display: str) -> Optional[Tuple[str, str]]:
    if target_display in cols:
        return cols[target_display]
    norm_target = _norm(target_display)
    for disp, pair in cols.items():
        if _norm(disp) == norm_target:
            return pair
    return None

def _update_item_metadata_generic(
    token: str,
    drive_id: str,
    item_id: str,
    fields_body: Dict[str, str],
    graph_base: str,
):
    """
    Päivittää ladatun tiedoston metatiedot (ListItem/fields).
    fields_body: { internalName: value, ... }
    """
    if not fields_body:
        return
    url = f"{graph_base}/drives/{drive_id}/items/{item_id}/listItem/fields"
    _graph_patch(url, token, fields_body)

def _filename_from_url(url: str) -> Optional[str]:
    try:
        path = urlparse(url).path
        name = os.path.basename(path)
        return name if name else None
    except Exception:
        return None

# ---------------- BATCH-AJO ----------------

def process_batch(
    spark,
    curated_items_path: str,
    *,
    limit: int,
    site_url: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scope: Sequence[str],
    graph_base: str,
    hostname: str,
    target_library_name: str,
    target_subfolder: str,
    ean_display_name: str,
    gpc1_display_name: str,
    gpc2_display_name: str,
    brand_display_name: str,
    # UUTTA: Kesko-kategoriat (SharePointin sarakkeiden näyttönimet)
    kesko1_display_name: str = "Kesko-kategoria1",
    kesko2_display_name: str = "Kesko-kategoria2",
    kesko3_display_name: str = "Kesko-kategoria3",
    product_display_name: str = "Tuote",
) -> None:
    """
    Lukee Delta-polusta max 'limit' riviä, hakee kuvat ja lataa ne SharePointin
    dokumenttikirjastoon. Päivittää metatiedot:
      - EAN                 ← GTIN
      - GS1-kategoria1      ← GpcFamilyCode
      - GS1-kategoria2      ← GpcClassCode
      - BRAND               ← BrandName
      - Kesko-kategoria1..3 ← PRODUCT_HIERARCHY_LEVEL_2/3/4
    """
    # 1) Graph + kirjasto
    token = _acquire_graph_token(client_id, client_secret, tenant_id, scope)
    site  = _get_site(token, site_url, graph_base, hostname)
    drive = _get_drive_by_name(token, site["id"], target_library_name, graph_base)

    # 2) Alikansion varmistus (jos annettu)
    _ensure_folder_path(token, drive["id"], target_subfolder, graph_base)

    # 3) Listan sarakkeet → poimi sisäiset nimet
    lst   = _get_list_for_drive(token, drive["id"], graph_base)
    cols  = _get_columns_for_list(token, site["id"], lst["id"], graph_base)

    ean_pair    = _find_internal_by_display(cols, ean_display_name)
    gpc1_pair   = _find_internal_by_display(cols, gpc1_display_name)
    gpc2_pair   = _find_internal_by_display(cols, gpc2_display_name)
    brand_pair  = _find_internal_by_display(cols, brand_display_name)
    kesko1_pair = _find_internal_by_display(cols, kesko1_display_name)
    kesko2_pair = _find_internal_by_display(cols, kesko2_display_name)
    kesko3_pair = _find_internal_by_display(cols, kesko3_display_name)
    product_pair = _find_internal_by_display(cols, product_display_name)

    missing = []
    if not ean_pair:    missing.append(f"'{ean_display_name}'")
    if not gpc1_pair:   missing.append(f"'{gpc1_display_name}'")
    if not gpc2_pair:   missing.append(f"'{gpc2_display_name}'")
    if not brand_pair:  missing.append(f"'{brand_display_name}'")
    if not kesko1_pair: missing.append(f"'{kesko1_display_name}'")
    if not kesko2_pair: missing.append(f"'{kesko2_display_name}'")
    if not kesko3_pair: missing.append(f"'{kesko3_display_name}'")
    if not product_pair: missing.append(f"'{product_display_name}'")
    if missing:
        available = ", ".join(sorted(cols.keys()))
        raise RuntimeError(
            "Metatietokenttiä ei löytynyt: "
            + " ja ".join(missing)
            + f". Saatavilla olevat (displayName): {available}"
        )

    _, ean_internal    = ean_pair
    _, gpc1_internal   = gpc1_pair
    _, gpc2_internal   = gpc2_pair
    _, brand_internal  = brand_pair
    _, kesko1_internal = kesko1_pair
    _, kesko2_internal = kesko2_pair
    _, kesko3_internal = kesko3_pair
    _, product_internal = product_pair

    # 4) Lue rivit Deltasta
    rows = get_image_rows(spark, curated_items_path, limit=limit)
    extractor = ImageExtractor(timeout_sec=30)

    ok, fail = 0, 0

    # 5) Käy rivit läpi
    for r in rows:
        url    = (r.get("PrimaryImageUrl") or "").strip()
        name   = (r.get("PrimaryImageFileName") or "").strip()
        gpc1   = (str(r.get("GpcFamilyCode") or "").strip())
        gpc2   = (str(r.get("GpcClassCode") or "").strip())
        brand  = (r.get("BrandName") or "").strip()
        gtin   = (str(r.get("GTIN") or "").strip())

        # Kesko-tasot Deltasta
        kesko1 = (r.get("PRODUCT_HIERARCHY_LEVEL_2") or "").strip()
        kesko2 = (r.get("PRODUCT_HIERARCHY_LEVEL_3") or "").strip()
        kesko3 = (r.get("PRODUCT_HIERARCHY_LEVEL_4") or "").strip()
        product = (r.get("TradeItemDescription_fi") or "").strip()

        if not url:
            print("⚠️ Ohitetaan: tyhjä URL")
            continue

        # EAN/GTIN ensisijaisesti Deltasta
        base_gtin = gtin

        # Fallback, jos GTIN puuttuu
        if not base_gtin:
            if name:
                base_gtin = re.sub(r"\.[^.]+$", "", name).strip()
            if not base_gtin:
                base_gtin = gpc1 or "unknown"

        # Jos tiedostonimi puuttuu, yritä tulkita se URL:ista
        if not name:
            url_name = _filename_from_url(url)
            name = url_name if url_name else f"{base_gtin}.jpg"

        try:
            # Lataa kuva
            item = ItemImage(gpc_family_code=gpc1, gtin=base_gtin, url=url)
            blob = extractor.fetch(item)

            # Varmista tiedostopääte
            if not re.search(r"\.(jpg|jpeg|png|webp|gif|bmp|tif|tiff)$", name, flags=re.IGNORECASE):
                name = re.sub(r"\.[^.]+$", "", name) + blob.extension

            # Upload
            drive_item = _upload_file(token, drive["id"], target_subfolder, name, blob.content, graph_base)
            item_id = drive_item.get("id")
            if not item_id:
                raise RuntimeError("Upload onnistui, mutta item-id puuttuu vastauksesta.")

            # Metatiedot (päivitä vain ei-tyhjät)
            fields = {}
            if base_gtin: fields[ean_internal] = base_gtin
            if gpc1:      fields[gpc1_internal] = gpc1
            if gpc2:      fields[gpc2_internal] = gpc2
            if brand:     fields[brand_internal] = brand
            if kesko1:    fields[kesko1_internal] = kesko1   # ✅ Kesko-kategoria1 (L2)
            if kesko2:    fields[kesko2_internal] = kesko2   # ✅ Kesko-kategoria2 (L3)
            if kesko3:    fields[kesko3_internal] = kesko3   # ✅ Kesko-kategoria3 (L4)
            if product:   fields[product_internal] = product

            _update_item_metadata_generic(
                token=token,
                drive_id=drive["id"],
                item_id=item_id,
                fields_body=fields,
                graph_base=graph_base,
            )

            ok += 1
            print(f"✅ Tiedosto tallennettu ja metatiedot päivitetty: {name}")

        except Exception as e:
            print(f"❌ Epäonnistui: {name or '(no name)'} ({url}) -> {e}")
            fail += 1

    # 6) Yhteenveto
    print(f"Valmis. Onnistui: {ok}, epäonnistui: {fail}, yhteensä: {ok + fail}")
