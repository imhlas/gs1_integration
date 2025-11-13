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
RUN_MODE = "changes"  # "full" tai "changes"
SINCE_ISO = "2025-11-11T07:03:26.1655687Z"
ONLY_GPC_SEGMENT_CODE = "50000000"  # tai None jos ei suodatusta
RATE_LIMIT_PER_MIN = 15

# SQL-kohdetaulu (pidä sama nimi kirjoituksessa ja luvussa)
SQL_TABLE_CURATED_ITEMS = "dbo.Test_Curated_Items2"
SQL_TABLE_KESKO_CATEGORIES = "dbo.product_data_with_kesko2"

# (valinnainen) Spark-lippu jonka voit lukea ja asettaa run_allissa
SPARK_DELTA_AUTOMERGE = True
