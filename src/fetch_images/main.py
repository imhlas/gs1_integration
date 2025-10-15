# file: main.py
from sharepoint_upload import process_batch
from src.config import *

def main():
    # 🔐 Salaisuudet ja kovakoodit
    SITE_URL       = dbutils.secrets.get("gs1-kv", "sharepoint-site-url")
    CLIENT_ID      = dbutils.secrets.get("gs1-kv", "sharepoint-client-id")
    CLIENT_SECRET  = dbutils.secrets.get("gs1-kv", "sharepoint-client-secret")

    # 🔒 Graph/SharePoint peruskonfiguraatio
    TENANT_ID   = "003bf88f-5447-4afe-907b-8c4ca7f0d200"
    GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
    HOSTNAME    = "lejosfi.sharepoint.com"
    SCOPE       = ["https://graph.microsoft.com/.default"]

    # 🗂️ Kohdekirjasto ja (valinnainen) alikansio
    TARGET_LIBRARY_NAME = "GS1 Tuotekuvat"
    TARGET_SUBFOLDER    = ""  # esim. "Tuotteet/2025" tai "" kirjaston juureen

    access_key = dbutils.secrets.get("gs1-kv", "storage-access-key")
    spark.conf.set(f"fs.azure.account.key.{ACCOUNT}.dfs.core.windows.net", access_key)

    # ▶️ Aja pieni erä testausta varten
    process_batch(
        spark,
        curated_items_path=CURATED_ITEMS_WITH_KESKO,
        limit=1000,
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
    )


if __name__ == "__main__":
    main()
