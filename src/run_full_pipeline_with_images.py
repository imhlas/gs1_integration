#src/run_full_with_images.py

import time
import datetime

from src.pipelines import run_full_pipeline
from utils.webhook import send_gs1_pipeline_webhook
from src.fetch_images.sharepoint_upload import process_batch_parallel
from src.fetch_images.clean_sharepoint_library import wipe_library
from src.config import *

def _fmt_duration(seconds: float) -> str:
    """Palauta kiva kesto-teksti, esim. '5 min 32 s'."""
    seconds = int(round(seconds))
    mins, secs = divmod(seconds, 60)
    hours, mins = divmod(mins, 60)
    parts = []
    if hours:
        parts.append(f"{hours} h")
    if mins:
        parts.append(f"{mins} min")
    parts.append(f"{secs} s")
    return " ".join(parts)


def run_full_pipeline_with_images(spark, dbutils):

    access_key = dbutils.secrets.get("gs1-kv", "storage-access-key")
    spark.conf.set(f"fs.azure.account.key.gs1datalake.dfs.core.windows.net", access_key)

    """
    Ajaa:
      1) FULL-tuotepipelinen (All -> Silver -> Curated -> SQL + Kesko)
      2) Kuvapipelinen kaikille tuotteille SharePointiin

    Mittaa erikseen:
      - product_duration_sec   = tuotepipelinen kesto
      - images_duration_sec    = kuvapipelinen kesto

    Palauttaa dictin, jota voi käyttää esim. sähköpostissa.
    """
    overall_start = datetime.datetime.utcnow().isoformat()
    print(f">>> FULL + IMAGES -ajo käynnissä (UTC start: {overall_start})")

    # ---------- 1) Tuotepipeline (FULL) ---------- #
    t0 = time.perf_counter()
    product_stats = run_full_pipeline(spark, dbutils)
    t1 = time.perf_counter()
    product_duration_sec = t1 - t0

    print(f">>> Tuotepipelinen kesto: {_fmt_duration(product_duration_sec)}")

    # ---------- 2) Kuvapipeline (kaikki tuotteet) ---------- #
    print(">>> Aloitetaan SharePoint-kuvapipeline (snapshot kaikista tuotteista)")

    SITE_URL      = dbutils.secrets.get("gs1-kv", "sharepoint-site-url")
    CLIENT_ID     = dbutils.secrets.get("gs1-kv", "sharepoint-client-id")
    CLIENT_SECRET = dbutils.secrets.get("gs1-kv", "sharepoint-client-secret")

    TENANT_ID  = SP_TENANT_ID
    GRAPH_BASE = SP_GRAPH_BASE
    HOSTNAME   = SP_HOSTNAME
    SCOPE      = SP_SCOPE

    TARGET_LIBRARY_NAME = SP_TARGET_LIBRARY_NAME
    TARGET_SUBFOLDER    = SP_TARGET_SUBFOLDER

    t2 = time.perf_counter()

    # 🧹 2a) Tyhjennä SharePoint-kirjasto ennen kuvien latausta
    print(f">>> Tyhjennetään SharePoint-kirjasto '{TARGET_LIBRARY_NAME}' (alikansio: '{TARGET_SUBFOLDER or '/'}')")
    wipe_stats = wipe_library(
        site_url=SITE_URL,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scope=SCOPE,
        graph_base=GRAPH_BASE,
        hostname=HOSTNAME,
        library_name=TARGET_LIBRARY_NAME,
        target_subfolder=TARGET_SUBFOLDER,   # "" = koko kirjasto juuresta
        dry_run=False,                       # 👈 oikea tyhjennys
    )
    print(
        f">>> Kirjasto tyhjennetty. Folders deleted: {wipe_stats['folders_deleted']}, "
        f"files deleted: {wipe_stats['files_deleted']}, errors: {wipe_stats['errors']}"
    )

    stats = process_batch_parallel(
        spark,
        curated_items_path=CURATED_ITEMS_WITH_KESKO,
        limit=None,              # -> kaikki rivit
        site_url=SITE_URL,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scope=SCOPE,
        graph_base=GRAPH_BASE,
        hostname=HOSTNAME,
        target_library_name=TARGET_LIBRARY_NAME,
        target_subfolder=TARGET_SUBFOLDER,
        ean_display_name="EAN",
        gpc1_display_name="GS1-kategoria1",
        gpc2_display_name="GS1-kategoria2",
        brand_display_name="BRAND",
        kesko1_display_name="Kesko-kategoria1",
        kesko2_display_name="Kesko-kategoria2",
        kesko3_display_name="Kesko-kategoria3",
        product_display_name="Tuote",
        max_workers=12,
        image_timeout_sec=12,
        graph_timeout_sec=25,
        progress_every=200,
    )
    t3 = time.perf_counter()
    images_duration_sec = t3 - t2

    product_duration_human = _fmt_duration(product_duration_sec)
    images_duration_human = _fmt_duration(images_duration_sec)

    print(f">>> Kuvapipelinen kesto: {images_duration_human}")
    print(f"Päivitettyjä rivejä (seen): {stats['seen']} kpl")
    print(f"Onnistuneet kuvalataukset: {stats['ok']} kpl")
    print(f"Epäonnistuneet kuvalataukset: {stats['fail']} kpl")

    overall_end = datetime.datetime.utcnow().isoformat()
    print(f">>> FULL + IMAGES -ajo valmis (UTC end: {overall_end})")

    # Kootaan KAIKKI tarvittava info yhteen dictiin
    result = {
        "started_at_utc": overall_start,
        "finished_at_utc": overall_end,
        "product_duration_human": product_duration_human,
        "images_duration_human": images_duration_human,
        "images_stats": stats,          # sisältää seen/ok/fail
        "all_keys": product_stats["all_keys"],
        "kesko_stats": product_stats["kesko_stats"],
    }

    # Lähetä webhook (kaikki webhook-logiikka utils/webhook.py:ssä)
    send_gs1_pipeline_webhook(dbutils, result)

    return result


if __name__ == "__main__":
    # Databricksissa spark ja dbutils tulevat ympäristöstä
    result = run_full_pipeline_with_images(spark, dbutils)
    # Halutessasi voit printata dictin tässä debugia varten:
    print("Yhteenveto:", result)
