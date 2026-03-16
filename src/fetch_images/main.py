# fetch_images/main.py
from sharepoint_upload import process_batch_parallel
from clean_sharepoint_library import wipe_library
from src.config import *

def main():
    # 🔐 Salaisuudet ja kovakoodit
    SITE_URL       = dbutils.secrets.get("gs1-kv", "sharepoint-site-url")
    CLIENT_ID      = dbutils.secrets.get("gs1-kv", "sharepoint-client-id")
    CLIENT_SECRET  = dbutils.secrets.get("gs1-kv", "sharepoint-client-secret")

    TENANT_ID  = SP_TENANT_ID
    GRAPH_BASE = SP_GRAPH_BASE
    HOSTNAME   = SP_HOSTNAME
    SCOPE      = SP_SCOPE

    TARGET_LIBRARY_NAME = SP_TARGET_LIBRARY_NAME
    TARGET_SUBFOLDER    = SP_TARGET_SUBFOLDER

    access_key = dbutils.secrets.get("gs1-kv", "storage-access-key")
    spark.conf.set(f"fs.azure.account.key.{ACCOUNT}.dfs.core.windows.net", access_key)

    # 🧹 1) Tyhjennetään SharePoint-kirjasto ennen kuvapipelinea
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
        target_subfolder=TARGET_SUBFOLDER,  # "" = koko kirjasto juuresta
        dry_run=False,                      # 👈 oikea tyhjennys, ei kuiva-ajoa
    )
    print(f">>> Kirjasto tyhjennetty. Folders deleted: {wipe_stats['folders_deleted']}, "
          f"files deleted: {wipe_stats['files_deleted']}, errors: {wipe_stats['errors']}")

    # 📸 2) Kuvapipeline – uudet kuvat sisään
    stats = process_batch_parallel(
        spark,
        curated_items_path=CURATED_ITEMS_WITH_KESKO,
        limit=200000,  # tai None = kaikki
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
        progress_every=500,
    )

    print(f"Kuvia ladattu SharePointiin (onnistuneet uploadit): {stats['ok']} kpl")


if __name__ == "__main__":
    main()
