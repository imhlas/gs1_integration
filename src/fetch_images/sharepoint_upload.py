# src/fetch_images/sharepoint_upload.py
# Nopeutettu rinnakkaisajolla, tokenin automaattisella uusinnalla ja retryillä.

from typing import Optional, Sequence, Dict, Tuple, Callable, Any
from urllib.parse import urlparse, quote
from src.config import *
import os
import re
import time
import threading
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import msal
import requests

from src.fetch_images.image_extractor import ImageExtractor, ItemImage
from src.fetch_images.delta_images import get_image_rows_iter


# ---------------- YLEISAPURI: normalisointi sarakenimille ----------------

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    x = s.replace("\u00A0", " ")
    for ch in ["\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212", "\u00AD"]:
        x = x.replace(ch, "-")
    return re.sub(r"\s+", " ", x).strip().lower()


# ---------------- TOKEN-MANAGER ----------------
class TokenManager:
    """
    Vastaa Graph-tokenista:
    - hakee uuden
    - muistaa vanhenemisajan
    - uusii automaattisesti, jos aikaa < refresh_margin_sec
    """
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        scope: Sequence[str],
        refresh_margin_sec: int = 300,  # 5 min
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.scope = list(scope)
        self.refresh_margin_sec = refresh_margin_sec
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._exp_ts: float = 0.0  # epoch seconds

    def _acquire(self) -> None:
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret,
        )
        result = app.acquire_token_for_client(scopes=self.scope)
        if "access_token" not in result:
            raise RuntimeError(f"MSAL token error: {result.get('error')} - {result.get('error_description')}")
        self._token = result["access_token"]
        # expires_in on sekunteina; lasketaan vanhenemisaika
        expires_in = result.get("expires_in", 3600)
        self._exp_ts = time.time() + max(0, int(expires_in)) - 10  # pieni varmuusvähennys

    def get_token(self) -> str:
        with self._lock:
            if not self._token or (time.time() > self._exp_ts - self.refresh_margin_sec):
                self._acquire()
            return self._token  # type: ignore[return-value]

    def force_refresh(self) -> str:
        with self._lock:
            self._acquire()
            return self._token  # type: ignore[return-value]


# ---------------- GRAPH-CLIENT (retry + token refresh) ----------------
class GraphClient:
    """
    Yksinkertainen Graph-asiakas:
    - käyttää TokenManageria
    - pysyvä Session (connection pooling)
    - automaattinen retry 429/5xx
    - 401 → uusii tokenin ja yrittää kerran uudelleen
    """
    def __init__(self, token_mgr: TokenManager, base: str, timeout: int = 25, pool_max: int = 64):
        self.token_mgr = token_mgr
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=pool_max, pool_maxsize=pool_max)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token_mgr.get_token()}"}

    def _request(self, method: str, url: str, *, json: Any = None, data: Any = None, extra_headers: Dict[str, str] = None, retry: int = 3) -> requests.Response:
        headers = self._headers()
        if extra_headers:
            headers.update(extra_headers)
        for attempt in range(retry + 1):
            r = self.session.request(method, url, json=json, data=data, headers=headers, timeout=self.timeout)
            if r.status_code == 401 and attempt == 0:
                # token todennäköisesti vanhentunut → uusi ja yritä uudestaan kerran
                self.token_mgr.force_refresh()
                headers = self._headers()
                if extra_headers:
                    headers.update(extra_headers)
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                # lue Retry-After jos annettu
                if attempt < retry:
                    ra = r.headers.get("Retry-After")
                    wait = int(ra) if ra and ra.isdigit() else (0.5 * (2 ** attempt))  # 0.5,1,2
                    time.sleep(wait)
                    continue
            return r
        return r

    # Convenience wrappers
    def get(self, path: str, **kw) -> dict:
        url = f"{self.base}{path}"
        r = self._request("GET", url, **kw)
        if not r.ok:
            raise RuntimeError(f"GET {url} -> {r.status_code}: {r.text}")
        return r.json()

    def post(self, path: str, json_body: dict, **kw) -> dict:
        url = f"{self.base}{path}"
        r = self._request("POST", url, json=json_body, **kw)
        if not r.ok:
            raise RuntimeError(f"POST {url} -> {r.status_code}: {r.text}")
        return r.json()

    def patch(self, path: str, json_body: dict, **kw) -> dict:
        url = f"{self.base}{path}"
        r = self._request("PATCH", url, json=json_body, **kw)
        if not r.ok:
            raise RuntimeError(f"PATCH {url} -> {r.status_code}: {r.text}")
        return r.json() if r.text else {}

    def put_bytes(self, path: str, content: bytes, **kw) -> dict:
        url = f"{self.base}{path}"
        r = self._request("PUT", url, data=content, **kw)
        if not r.ok:
            raise RuntimeError(f"PUT {url} -> {r.status_code}: {r.text}")
        return r.json()

    def delete(self, path: str, **kw) -> None:
        url = f"{self.base}{path}"
        r = self._request("DELETE", url, **kw)
        if r.status_code not in (200, 202, 204):
            raise RuntimeError(f"DELETE {url} -> {r.status_code}: {r.text}")


# ---------------- GRAPH HELPERS ----------------

def _get_site(gc: GraphClient, site_url: str, hostname: str) -> dict:
    path_part = site_url.split("://", 1)[-1].split("/", 1)[-1]
    return gc.get(f"/sites/{hostname}:/{path_part}")

def _list_site_drives(gc: GraphClient, site_id: str) -> list:
    data = gc.get(f"/sites/{site_id}/drives")
    return data.get("value", [])

def _get_drive_by_name(gc: GraphClient, site_id: str, library_name: str) -> dict:
    drives = _list_site_drives(gc, site_id)
    for d in drives:
        if _norm(d.get("name")) == _norm(library_name):
            return d
    raise RuntimeError(f"Drive (kirjasto) nimellä '{library_name}' ei löytynyt sivulta {site_id}")

def _ensure_folder_path(gc: GraphClient, drive_id: str, folder_path: str):
    if not folder_path or not folder_path.strip():
        return
    parts = [p for p in folder_path.strip("/").split("/") if p]
    cur = ""
    for p in parts:
        cur = f"{cur}/{p}" if cur else p
        check = gc.session.get(
            f"{gc.base}/drives/{drive_id}/root:/{quote(cur)}",
            headers=gc._headers(),
            timeout=gc.timeout,
        )
        if check.status_code == 404:
            parent = "/".join(cur.split("/")[:-1])
            parent_enc = quote(parent) if parent else ""
            create_path = f"/drives/{drive_id}/root:/{parent_enc}:/children" if parent_enc else f"/drives/{drive_id}/root/children"
            gc.post(create_path, {
                "name": p,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "fail",
            })
        elif not check.ok:
            raise RuntimeError(f"Check folder {cur} -> {check.status_code}: {check.text}")

def _get_list_for_drive(gc: GraphClient, drive_id: str) -> dict:
    return gc.get(f"/drives/{drive_id}/list")

def _get_columns_for_list(gc: GraphClient, site_id: str, list_id: str) -> Dict[str, Tuple[str, str]]:
    data = gc.get(f"/sites/{site_id}/lists/{list_id}/columns")
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

def _update_item_metadata(gc: GraphClient, drive_id: str, item_id: str, fields_body: Dict[str, str]):
    if not fields_body:
        return
    gc.patch(f"/drives/{drive_id}/items/{item_id}/listItem/fields", fields_body)


# ---------------- NIMEÄMIS- JA POLKUAPURIT ----------------

def _filename_for_ean(ean: str, ext: str) -> str:
    ext = ext if ext.startswith(".") else f".{ext}"
    return f"{ean}{ext}"

def _full_path(target_subfolder: str, filename: str) -> str:
    return f"{target_subfolder.rstrip('/')}/{filename}" if target_subfolder and target_subfolder.strip() else filename


# ---------------- RIVIN KÄSITTELY (yhden tuotteen pipeline) ----------------

def process_row(
    row: Dict[str, str],
    *,
    image_extractor: ImageExtractor,
    gc: GraphClient,
    drive_id: str,
    list_ids: Dict[str, str],  # sisäiset nimet: ean/gpc1/gpc2/brand/kesko1/kesko2/kesko3/product
    target_subfolder: str,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    Yksi tuote: hae kuva → PUT polkuun (ylikirjoitus) → PATCH metatiedot.
    Palauttaa True jos onnistui, False jos epäonnistui.
    """
    url     = (row.get("PrimaryImageUrl") or "").strip()
    name    = (row.get("PrimaryImageFileName") or "").strip()
    gpc1    = (str(row.get("GpcFamilyCode") or "").strip())
    gpc2    = (str(row.get("GpcClassCode") or "").strip())
    brand   = (row.get("BrandName") or "").strip()
    gtin    = (str(row.get("GTIN_NO_LEADING_ZEROS") or "").strip())
    kesko1  = (row.get("PRODUCT_HIERARCHY_LEVEL_2") or "").strip()
    kesko2  = (row.get("PRODUCT_HIERARCHY_LEVEL_3") or "").strip()
    kesko3  = (row.get("PRODUCT_HIERARCHY_LEVEL_4") or "").strip()
    product = (row.get("TradeItemDescription_fi") or "").strip()

    if not url:
        if progress_cb: progress_cb("skip: empty url")
        return False

    # EAN / fallback
    base_gtin = gtin or (re.sub(r"\.[^.]+$", "", name).strip() if name else (gpc1 or "unknown"))

    try:
        # 1) Kuva
        blob = image_extractor.fetch(ItemImage(gpc_family_code=gpc1, gtin=base_gtin, url=url))

        # 2) Nimi aina {EAN}.{ext} (idempotentti, estää duplikaatit)
        final_name = _filename_for_ean(base_gtin, blob.extension)
        full_path  = _full_path(target_subfolder, final_name)

        # 3) Upload polkuun → ylikirjoitus tai uusi
        drive_item = gc.put_bytes(f"/drives/{drive_id}/root:/{quote(full_path)}:/content", blob.content)
        item_id = drive_item.get("id") or drive_item.get("id")  # yleensä mukana

        if not item_id:
            # varmistus: hae juuri ladattu polulla
            got = gc.get(f"/drives/{drive_id}/root:/{quote(full_path)}")
            item_id = got.get("id")
            if not item_id:
                raise RuntimeError("Upload onnistui, mutta item-id puuttuu.")

        # 4) Metatiedot
        fields = {}
        if base_gtin: fields[list_ids["ean"]] = base_gtin
        if gpc1:      fields[list_ids["gpc1"]] = gpc1
        if gpc2:      fields[list_ids["gpc2"]] = gpc2
        if brand:     fields[list_ids["brand"]] = brand
        if kesko1:    fields[list_ids["kesko1"]] = kesko1
        if kesko2:    fields[list_ids["kesko2"]] = kesko2
        if kesko3:    fields[list_ids["kesko3"]] = kesko3
        if product:   fields[list_ids["product"]] = product

        _update_item_metadata(gc, drive_id, item_id, fields)

        if progress_cb: progress_cb(f"ok {final_name}")
        return True

    except Exception as e:
        if progress_cb: progress_cb(f"fail {base_gtin}: {e}")
        return False


# ---------------- PÄÄAJOFUNKTIO (RINNAKKAISUUS) ----------------

def process_batch_parallel(
    spark,
    curated_items_path: str,
    *,
    limit: Optional[int],
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
    kesko1_display_name: str = "Kesko-kategoria1",
    kesko2_display_name: str = "Kesko-kategoria2",
    kesko3_display_name: str = "Kesko-kategoria3",
    product_display_name: str = "Tuote",
    max_workers: int = 12,
    image_timeout_sec: int = 12,
    graph_timeout_sec: int = 25,
    progress_every: int = 500,   # tulosta väliraportti joka N rivi
) -> None:
    """
    Rinnakkaisajo:
      - streamaa rivit Deltasta (ei collect)
      - käsittelee säiepoolissa (kuva→PUT→PATCH)
      - ylikirjoittaa aina (idempotentti nimi {EAN}.{ext})
      - automaattinen tokenin uusinta + retry 429/5xx
    """

    # 0) Rakennetaan asiakkaat
    token_mgr = TokenManager(client_id, client_secret, tenant_id, scope)
    gc = GraphClient(token_mgr, graph_base, timeout=graph_timeout_sec, pool_max=max(32, 2*max_workers))

    # 1) Site + Drive + Folder
    site  = _get_site(gc, site_url, hostname)
    drive = _get_drive_by_name(gc, site["id"], target_library_name)
    _ensure_folder_path(gc, drive["id"], target_subfolder)

    # 2) Listan sarakkeiden sisäiset nimet
    lst   = _get_list_for_drive(gc, drive["id"])
    cols  = _get_columns_for_list(gc, site["id"], lst["id"])

    ean_pair     = _find_internal_by_display(cols, ean_display_name)
    gpc1_pair    = _find_internal_by_display(cols, gpc1_display_name)
    gpc2_pair    = _find_internal_by_display(cols, gpc2_display_name)
    brand_pair   = _find_internal_by_display(cols, brand_display_name)
    kesko1_pair  = _find_internal_by_display(cols, kesko1_display_name)
    kesko2_pair  = _find_internal_by_display(cols, kesko2_display_name)
    kesko3_pair  = _find_internal_by_display(cols, kesko3_display_name)
    product_pair = _find_internal_by_display(cols, product_display_name)

    missing = []
    if not ean_pair:     missing.append(f"'{ean_display_name}'")
    if not gpc1_pair:    missing.append(f"'{gpc1_display_name}'")
    if not gpc2_pair:    missing.append(f"'{gpc2_display_name}'")
    if not brand_pair:   missing.append(f"'{brand_display_name}'")
    if not kesko1_pair:  missing.append(f"'{kesko1_display_name}'")
    if not kesko2_pair:  missing.append(f"'{kesko2_display_name}'")
    if not kesko3_pair:  missing.append(f"'{kesko3_display_name}'")
    if not product_pair: missing.append(f"'{product_display_name}'")
    if missing:
        available = ", ".join(sorted(cols.keys()))
        raise RuntimeError(
            "Metatietokenttiä ei löytynyt: "
            + " ja ".join(missing)
            + f". Saatavilla (displayName): {available}"
        )

    list_ids = {
        "ean":     ean_pair[1],
        "gpc1":    gpc1_pair[1],
        "gpc2":    gpc2_pair[1],
        "brand":   brand_pair[1],
        "kesko1":  kesko1_pair[1],
        "kesko2":  kesko2_pair[1],
        "kesko3":  kesko3_pair[1],
        "product": product_pair[1],
    }

    # 3) Työkalut
    extractor = ImageExtractor(timeout_sec=image_timeout_sec)

    # 4) Laskurit (thread-safe)
    counters = {
        "seen": 0,
        "ok": 0,
        "fail": 0,
        "skipped": 0
    }
    lock = threading.Lock()
    start_ts = time.time()

    def progress_cb(msg: str):
        # kevyt debug viesti tarvittaessa
        pass

    def wrap_row(row: Dict[str, str]) -> bool:
        ok = process_row(
            row,
            image_extractor=extractor,
            gc=gc,
            drive_id=drive["id"],
            list_ids=list_ids,
            target_subfolder=target_subfolder,
            progress_cb=None,
        )
        with lock:
            if ok:
                counters["ok"] += 1
            else:
                counters["fail"] += 1
        return ok

    # 5) Streamaa rivit ja työnnä säiepooliin
    print(f"Start: parallel upload with max_workers={max_workers}, limit={limit or 'ALL'}")
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for row in get_image_rows_iter(spark, curated_items_path, limit=limit):
            with lock:
                counters["seen"] += 1
                seen = counters["seen"]
            futures.append(pool.submit(wrap_row, row))

        # odota valmistuminen
        for f in as_completed(futures):
            _ = f.result()  # nostaa mahdolliset poikkeukset rivikohtaisesti jo laskureihin

    # 6) Yhteenveto
    elapsed = time.time() - start_ts
    rate = counters["ok"] / elapsed if elapsed > 0 else 0.0
    print("-" * 60)
    print(f"Rivejä käsitelty (Delta): {counters['seen']}")
    print(f"Onnistui:               {counters['ok']}")
    print(f"Epäonnistui:            {counters['fail']}")
    print(f"Kokonaisaika:           {elapsed:.1f} s")
    print(f"Keskinopeus:            {rate:.2f} ok/s  (~{rate*3600:.0f} / h)")

    # Palautetaan statsit kutsujalle
    return {
        "seen": counters["seen"],   # rivit jotka yritettiin käsitellä
        "ok": counters["ok"],       # kuvat jotka päätyivät SharePointiin
        "fail": counters["fail"],   # rivit joissa lataus / metadata epäonnistui
        "elapsed_sec": elapsed,
        "rate_ok_per_sec": rate,
    }