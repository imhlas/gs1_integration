# fetch_images/clean_sharepoint_library.py
from typing import Optional, Sequence, Tuple, Dict
from urllib.parse import quote
import msal
import requests


# ------------ MSAL / GRAPH PERUSAPURIT ------------

def _acquire_graph_token(client_id: str, client_secret: str, tenant_id: str, scope: Sequence[str]) -> str:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(scopes=list(scope))
    if "access_token" not in result:
        raise RuntimeError(f"MSAL token error: {result.get('error')} - {result.get('error_description')}")
    return result["access_token"]

def _graph_get(url: str, token: str) -> dict:
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if not r.ok:
        raise RuntimeError(f"GET {url} -> {r.status_code}: {r.text}")
    return r.json()

def _graph_delete(url: str, token: str) -> None:
    r = requests.delete(url, headers={"Authorization": f"Bearer {token}"})
    if r.status_code not in (200, 202, 204):
        raise RuntimeError(f"DELETE {url} -> {r.status_code}: {r.text}")

def _get_site(token: str, site_url: str, graph_base: str, hostname: str) -> dict:
    # esim. site_url = "https://lejosfi.sharepoint.com/sites/Data"
    path_part = site_url.split("://", 1)[-1].split("/", 1)[-1]
    url = f"{graph_base}/sites/{hostname}:/{path_part}"
    return _graph_get(url, token)

def _list_site_drives(token: str, site_id: str, graph_base: str) -> list:
    url = f"{graph_base}/sites/{site_id}/drives"
    data = _graph_get(url, token)
    return data.get("value", [])

def _get_drive_by_name(token: str, site_id: str, library_name: str, graph_base: str) -> dict:
    for d in _list_site_drives(token, site_id, graph_base):
        if (d.get("name") or "").strip().lower() == library_name.strip().lower():
            return d
    raise RuntimeError(f"Kirjastoa nimellä '{library_name}' ei löytynyt (site_id={site_id}).")

def _resolve_item_by_path(token: str, drive_id: str, path: str, graph_base: str) -> Optional[dict]:
    """
    Palauttaa driveItemin annetusta polusta.
    path = "" tai None => juuritaso
    """
    if not path or not path.strip():
        # root
        url = f"{graph_base}/drives/{drive_id}/root"
    else:
        clean = path.strip("/")
        url = f"{graph_base}/drives/{drive_id}/root:/{quote(clean)}"
    try:
        return _graph_get(url, token)
    except Exception:
        return None

def _list_children_paginated(token: str, drive_id: str, item_id: Optional[str], graph_base: str):
    """
    Iteroi kaikki lapset (juuri tai kansion alla) sivutettuna.
    item_id = None => root/children
    """
    if item_id:
        url = f"{graph_base}/drives/{drive_id}/items/{item_id}/children"
    else:
        url = f"{graph_base}/drives/{drive_id}/root/children"

    while True:
        data = _graph_get(url, token)
        for it in data.get("value", []):
            yield it
        next_link = data.get("@odata.nextLink")
        if not next_link:
            break
        url = next_link

def _delete_item_recursive(token: str, drive_id: str, item_id: str, graph_base: str, dry_run: bool) -> None:
    """
    Yksinkertainen: Graphin DELETE /items/{id} poistaa myös kansion sisällön (siirtyy roskakoriin).
    """
    url = f"{graph_base}/drives/{drive_id}/items/{item_id}"
    if dry_run:
        # ei tehdä oikeaa poistoa dry runissa
        return
    _graph_delete(url, token)


# ------------ JULKINEN API: KIRJASTON TYYHJENNYS ------------

def wipe_library(
    *,
    site_url: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scope: Sequence[str],
    graph_base: str,
    hostname: str,
    library_name: str,
    target_subfolder: str = "",     # jos annettu, poistaa vain tämän alikansion
    dry_run: bool = True            # True = näyttää mitä poistettaisiin, False = oikeasti poistaa
) -> Dict[str, int]:
    """
    Tyhjentää annetun SharePoint-kirjaston sisällön TAI vain nimetyn alikansion.
    Palauttaa laskurit: {'folders_deleted': x, 'files_deleted': y, 'errors': z}
    """
    token = _acquire_graph_token(client_id, client_secret, tenant_id, scope)
    site  = _get_site(token, site_url, graph_base, hostname)
    drive = _get_drive_by_name(token, site["id"], library_name, graph_base)

    # Jos target_subfolder annettu → poistetaan koko kansio yhdellä käskyllä.
    # Muuten poistetaan kaikki juuren lapset (yksi kerrallaan).
    folders_deleted = 0
    files_deleted   = 0
    errors          = 0

    if target_subfolder and target_subfolder.strip():
        # Poista vain tämä alikansio
        sub = _resolve_item_by_path(token, drive["id"], target_subfolder, graph_base)
        if not sub or not sub.get("id"):
            print(f"⚠️ Alikansiota ei löytynyt: '{target_subfolder}'. Ei poistettavaa.")
            return {"folders_deleted": 0, "files_deleted": 0, "errors": 0}

        # Poista kansio (sisältöineen)
        print(f"{'[DRY-RUN] ' if dry_run else ''}Poistetaan alikansio: {target_subfolder} (id={sub['id']})")
        try:
            _delete_item_recursive(token, drive["id"], sub["id"], graph_base, dry_run)
            folders_deleted += 1
        except Exception as e:
            print(f"❌ Virhe poistossa: {e}")
            errors += 1

    else:
        # Tyhjennä koko kirjaston juuren lapset (mutta ei itse kirjastoa)
        print(f"{'[DRY-RUN] ' if dry_run else ''}Tyhjennetään kirjasto: '{library_name}' (driveId={drive['id']})")

        for child in _list_children_paginated(token, drive["id"], None, graph_base):
            cid  = child.get("id")
            name = child.get("name")
            ftype = child.get("folder")  # jos on dict → kansio

            if not cid:
                continue

            prefix = "[DRY-RUN] " if dry_run else ""
            if ftype:
                print(f"{prefix}Poistetaan kansio: {name} (id={cid})")
                try:
                    _delete_item_recursive(token, drive["id"], cid, graph_base, dry_run)
                    folders_deleted += 1
                except Exception as e:
                    print(f"❌ Virhe poistossa (kansio {name}): {e}")
                    errors += 1
            else:
                try:
                    _delete_item_recursive(token, drive["id"], cid, graph_base, dry_run)
                    files_deleted += 1
                except Exception as e:
                    print(f"❌ Virhe poistossa (tiedosto {name}): {e}")
                    errors += 1

    # Yhteenveto
    print("-" * 60)
    print(f"Folders deleted: {folders_deleted}")
    print(f"Files deleted:   {files_deleted}")
    print(f"Errors:          {errors}")
    if dry_run:
        print("Kuiva-ajo (dry_run=True): mitään ei poistettu oikeasti.")

    return {"folders_deleted": folders_deleted, "files_deleted": files_deleted, "errors": errors}


# ------------ ESIMERKKI KÄYTTÖ (valinnainen) ------------

if __name__ == "__main__":
    # Täytä omilla arvoilla tai kutsu Databricksista parametrisoiden
    SITE_URL      = "https://lejosfi.sharepoint.com/sites/REPLACE_ME"
    TENANT_ID     = "003bf88f-5447-4afe-907b-8c4ca7f0d200"
    CLIENT_ID     = "REPLACE_ME"
    CLIENT_SECRET = "REPLACE_ME"
    SCOPE         = ["https://graph.microsoft.com/.default"]
    GRAPH_BASE    = "https://graph.microsoft.com/v1.0"
    HOSTNAME      = "lejosfi.sharepoint.com"

    LIBRARY_NAME      = "GS1 Tuotekuvat"
    TARGET_SUBFOLDER  = ""     # esim. "Tuotteet/2025" tai "" koko kirjaston tyhjennys
    DRY_RUN           = True   # vaihda False kun haluat oikeasti poistaa

    wipe_library(
        site_url=SITE_URL,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scope=SCOPE,
        graph_base=GRAPH_BASE,
        hostname=HOSTNAME,
        library_name=LIBRARY_NAME,
        target_subfolder=TARGET_SUBFOLDER,
        dry_run=DRY_RUN,
    )
