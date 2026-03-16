"""
Minimal config for run_all.
- Ei salaisuuksia täällä (ne haetaan Key Vaultista).
- Yksi totuus poluista, ajotavasta ja SQL-taulusta.
"""

# Storage (ei salaisuuksia)
ACCOUNT = "gs1datalake"
CONTAINER = "datalake"

def ABFSS(path: str) -> str:
    """Muodosta abfss-URI annetulle polulle."""
    return f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/{path.strip('/')}"

# Delta-polut
DELTA_KEYS     = ABFSS("gs1/bronze/public_item_sync")
DELTA_PRODUCTS = ABFSS("gs1/silver/catalogue_items")
CURATED_ITEMS  = ABFSS("gs1/curated/items_selected_fields")
CURATED_ITEMS_WITH_KESKO = ABFSS("gs1/gold/items_with_kesko_levels")

# Ajoasetukset
RUN_MODE = "full"  # "full" tai "changes"
SINCE_ISO = "2025-09-02T07:03:26.1655687Z"
ONLY_GPC_SEGMENT_CODE = "50000000"  # tai None jos ei suodatusta
RATE_LIMIT_PER_MIN = 15

# SQL-kohdetaulu (pidä sama nimi kirjoituksessa ja luvussa)
SQL_TABLE_CURATED_ITEMS = "dbo.Test_Curated_Items2"
SQL_TABLE_KESKO_CATEGORIES = "dbo.product_data_with_kesko"

# (valinnainen) Spark-lippu jonka voit lukea ja asettaa run_allissa
SPARK_DELTA_AUTOMERGE = True

# 🔒 Graph/SharePoint peruskonfiguraatio
SP_TENANT_ID   = "003bf88f-5447-4afe-907b-8c4ca7f0d200"
SP_GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
SP_HOSTNAME    = "lejosfi.sharepoint.com"
SP_SCOPE       = ["https://graph.microsoft.com/.default"]

# 🗂️ Kohdekirjasto ja (valinnainen) alikansio
SP_TARGET_LIBRARY_NAME = "GS1 Tuotekuvat"
SP_TARGET_SUBFOLDER    = ""  # esim. "Tuotteet/2025" tai "" kirjaston juureen
