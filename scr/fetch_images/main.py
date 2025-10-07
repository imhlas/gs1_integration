# main.py
from sharepoint_upload import process_batch

def main():
    # 🔐 Salaisuudet (pidetään kaikki kovakoodit/konfigit tässä mainissa)
    SITE_URL      = dbutils.secrets.get("gs1-kv", "sharepoint-site-url")        # esim. https://lejosfi.sharepoint.com/sites/Insights
    CLIENT_ID     = dbutils.secrets.get("gs1-kv", "sharepoint-client-id")
    CLIENT_SECRET = dbutils.secrets.get("gs1-kv", "sharepoint-client-secret")

    # 🔒 Graph/SharePoint konfiguraatio
    TENANT_ID  = "003bf88f-5447-4afe-907b-8c4ca7f0d200"
    GRAPH_BASE = "https://graph.microsoft.com/v1.0"
    HOSTNAME   = "lejosfi.sharepoint.com"
    TARGET_PATH = "General/GS1 Tuotekuvat"
    SCOPE = ["https://graph.microsoft.com/.default"]

    # Fallback-kansion nimi (jos koodilla alkavaa alikansiota ei löydy)
    UNMATCHED_FOLDER_NAME = "Kohdistamattomat"

    # 🔎 Delta-polku 
    account    = "gs1datalake"   # storage accountin nimi
    container  = "datalake"      # containerin nimi
    access_key = "5kEehpTCdCfzcmwOzmq+w4iaiFeMGnfV2OCaxQlGut2kb65ItOD4QDWapkmcT/NI4t8sLaOFAbjL+AStaIorWg=="

    # Aseta avain ennen lukua
    spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", access_key)

    # Delta-polku muodostetaan tämän jälkeen
    CURATED_ITEMS = f"abfss://{container}@{account}.dfs.core.windows.net/gs1/curated/items_selected_fields"


    # ▶️ Aja pieni erä testausta varten
    process_batch(
        spark,
        CURATED_ITEMS,
        limit=10,
        site_url=SITE_URL,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scope=SCOPE,
        graph_base=GRAPH_BASE,
        hostname=HOSTNAME,
        target_path=TARGET_PATH,
        unmatched_folder_name=UNMATCHED_FOLDER_NAME,
    )

if __name__ == "__main__":
    main()
