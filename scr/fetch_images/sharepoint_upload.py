# sharepoint_upload.py
from typing import Optional, Sequence, Dict
from urllib.parse import quote, urlparse
from collections import defaultdict
import os
import re

import msal
import requests

from image_extractor import ImageExtractor, ItemImage
from delta_images import get_image_rows


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

def _graph_put_bytes(url: str, token: str, content: bytes):
    r = requests.put(url, data=content, headers={"Authorization": f"Bearer {token}"})
    if not r.ok:
        raise RuntimeError(f"PUT {url} -> {r.status_code}: {r.text}")
    return r.json()

def _get_site(token: str, site_url: str, graph_base: str, hostname: str) -> dict:
    # SITE_URL: https://{hostname}/sites/Insights  -> "sites/Insights"
    path_part = site_url.split("://", 1)[-1].split("/", 1)[-1]
    url = f"{graph_base}/sites/{hostname}:/{path_part}"
    return _graph_get(url, token)

def _get_drive(token: str, site_id: str, graph_base: str) -> dict:
    url = f"{graph_base}/sites/{site_id}/drive"
    return _graph_get(url, token)

def _ensure_folder_path(token: str, drive_id: str, folder_path: str, graph_base: str):
    # Luo puuttuvan polun segmentti kerrallaan
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

def _upload_file(token: str, drive_id: str, target_path: str, filename: str, content: bytes, graph_base: str):
    full_path = f"{target_path.rstrip('/')}/{filename}"
    url = f"{graph_base}/drives/{drive_id}/root:/{quote(full_path)}:/content"
    _graph_put_bytes(url, token, content)

def _list_children(token: str, drive_id: str, parent_path: str, graph_base: str) -> Dict[str, dict]:
    """
    Palauttaa dictin: nimi -> item-objekti, parent_pathin suorat lapset.
    """
    if parent_path:
        url = f"{graph_base}/drives/{drive_id}/root:/{quote(parent_path)}:/children"
    else:
        url = f"{graph_base}/drives/{drive_id}/root/children"
    data = _graph_get(url, token)
    items = data.get("value", [])
    return {it["name"]: it for it in items}

# ---------------- JULKINEN UPLOAD-API ----------------

def upload_to_sharepoint_via_graph(
    filename: str,
    content: bytes,
    *,
    site_url: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scope: Sequence[str],
    graph_base: str,
    hostname: str,
    target_path: str,
) -> None:
    token = _acquire_graph_token(client_id, client_secret, tenant_id, scope)
    site  = _get_site(token, site_url, graph_base, hostname)
    drive = _get_drive(token, site["id"], graph_base)
    _ensure_folder_path(token, drive["id"], target_path, graph_base)
    _upload_file(token, drive["id"], target_path, filename, content, graph_base)
    print(f"✅ Kuva tallennettu: {target_path}/{filename}")

# ---------------- BATCH-AJO ----------------

def _filename_from_url(url: str) -> Optional[str]:
    try:
        path = urlparse(url).path
        name = os.path.basename(path)
        return name if name else None
    except Exception:
        return None

def _build_code_to_folder_map(
    token: str,
    drive_id: str,
    base_path: str,
    graph_base: str,
) -> Dict[str, str]:
    """
    Lukee base_pathin suorat alikansiot ja muodostaa mapin:
      '^\d+' (prefix-numero) -> 'kansion koko nimi'
    """
    children = _list_children(token, drive_id, base_path, graph_base)
    code_map: Dict[str, str] = {}
    for folder_name, item in children.items():
        # Kiinnostaa vain kansiot
        if "folder" not in item:
            continue
        m = re.match(r"^(\d+)", folder_name)
        if m:
            code_map[m.group(1)] = folder_name
    return code_map

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
    target_path: str,
    unmatched_folder_name: str,
) -> None:
    """
    Lukee Delta-polusta max 'limit' riviä, hakee kuvat ja lataa SharePointiin
    GpcFamilyCode-pohjaiseen alikansioon. Jos vastaavaa kansiota ei löydy,
    viedään kansioon 'unmatched_folder_name'. Tulostaa yhteenvedon mihin ja
    montako kuvaa meni.
    """
    # 1) Token + drive + varmista base-kansio
    token = _acquire_graph_token(client_id, client_secret, tenant_id, scope)
    site  = _get_site(token, site_url, graph_base, hostname)
    drive = _get_drive(token, site["id"], graph_base)
    _ensure_folder_path(token, drive["id"], target_path, graph_base)

    # 2) Rakenna koodikartta SharePointin alikansioista (vain kerran)
    code_to_folder = _build_code_to_folder_map(token, drive["id"], target_path, graph_base)

    # 3) Varmista että fallback-kansio on olemassa
    fallback_path = f"{target_path.rstrip('/')}/{unmatched_folder_name}"
    _ensure_folder_path(token, drive["id"], fallback_path, graph_base)

    # 4) Lue rivit Delasta
    rows = get_image_rows(spark, curated_items_path, limit=limit)
    extractor = ImageExtractor(timeout_sec=30)

    # 5) Laskurit
    per_folder_counts = defaultdict(int)
    ok, fail = 0, 0

    for r in rows:
        url  = (r.get("PrimaryImageUrl") or "").strip()
        name = (r.get("PrimaryImageFileName") or "").strip()
        code = (str(r.get("GpcFamilyCode") or "").strip())

        if not url:
            print("⚠️ Ohitetaan: tyhjä URL")
            continue

        # Päätä kohde-alikansio SharePointissa
        subfolder = code_to_folder.get(code, unmatched_folder_name)
        target_subpath = f"{target_path.rstrip('/')}/{subfolder}"

        # ItemImage tarvitsee gtin & gpc_family_code (ei kriittisiä uploadiin)
        item = ItemImage(
            gpc_family_code=code,
            gtin=(os.path.splitext(name)[0] or code or ""),
            url=url,
        )

        try:
            blob = extractor.fetch(item)

            # Fallback tiedostonimelle jos puuttuu
            if not name:
                url_name = _filename_from_url(url)
                name = url_name if url_name else f"{(item.gtin or 'unknown')}{blob.extension}"

            # Lataa kuvan oikeaan alikansioon
            _upload_file(token, drive["id"], target_subpath, name, blob.content, graph_base)

            ok += 1
            per_folder_counts[subfolder] += 1
        except Exception as e:
            print(f"❌ Epäonnistui: {name or '(no name)'} ({url}) -> {e}")
            fail += 1

    # 6) Yhteenveto
    print(f"Valmis. Onnistui: {ok}, epäonnistui: {fail}, yhteensä: {ok + fail}")
    if per_folder_counts:
        print("Kansiokohtainen yhteenveto (vain kansiot joihin meni kuvia):")
        for folder_name, cnt in per_folder_counts.items():
            print(f" - {folder_name}: {cnt} kpl")

