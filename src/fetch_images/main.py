# file: main.py
from sharepoint_upload import process_batch

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

    # 🔎 Delta-polku
    account    = "gs1datalake"
    container  = "datalake"
    access_key = "5kEehpTCdCfzcmwOzmq+w4iaiFeMGnfV2OCaxQlGut2kb65ItOD4QDWapkmcT/NI4t8sLaOFAbjL+AStaIorWg=="

    spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", access_key)
    CURATED_ITEMS = f"abfss://{container}@{account}.dfs.core.windows.net/gs1/curated/items_selected_fields"

    # ▶️ Aja pieni erä testausta varten
    process_batch(
        spark,
        CURATED_ITEMS,
        limit=20,
        site_url=SITE_URL,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scope=SCOPE,
        graph_base=GRAPH_BASE,
        hostname=HOSTNAME,
        target_library_name=TARGET_LIBRARY_NAME,
        target_subfolder=TARGET_SUBFOLDER,  # "" = kirjaston juuri
        ean_display_name="EAN",
        gpc1_display_name="GS1-kategoria1",
        gpc2_display_name="GS1-kategoria2",
        brand_display_name="BRAND",
    )

if __name__ == "__main__":
    main()
