# file: delta_images.py
# Vastaa datan (urlien) hausta Deltasta

from typing import List, Dict, Iterator, Optional

def _base_df(spark, curated_items_path: str):
    return (
        spark.read.format("delta").load(curated_items_path)
        .where("TradeItemUnitDescriptor = 'BASE_UNIT_OR_EACH'")
        .where("PrimaryImageUrl IS NOT NULL AND length(trim(PrimaryImageUrl)) > 0")
        .where("PRODUCT_HIERARCHY_LEVEL_2 IS NOT NULL AND length(trim(PRODUCT_HIERARCHY_LEVEL_2)) > 0")
        .select(
            "PrimaryImageUrl",
            "TradeItemDescription_fi",
            "PrimaryImageFileName",
            "GpcFamilyCode",
            "GpcClassCode",
            "BrandName",
            "GTIN",
            "PRODUCT_HIERARCHY_LEVEL_2",
            "PRODUCT_HIERARCHY_LEVEL_3",
            "PRODUCT_HIERARCHY_LEVEL_4"
        )
    )

def get_image_rows(spark, curated_items_path: str, limit: int = 10) -> List[Dict]:
    """
    VANHA: lukee enintään 'limit' riviä ja palauttaa listan (nostaa muistiin).
    """
    df = _base_df(spark, curated_items_path).limit(limit)
    return [r.asDict(recursive=True) for r in df.collect()]

def get_image_rows_iter(spark, curated_items_path: str, limit: Optional[int] = None) -> Iterator[Dict]:
    """
    UUSI: palauttaa rivit 'streaminä' driverille (ei kerää kaikkia muistiin).
    - Jos limit annetaan, rajaa siihen; muuten käy läpi kaikki.
    - Tuottaa sanakirjoja kuten vanha funktio.
    """
    df = _base_df(spark, curated_items_path)
    if limit:
        df = df.limit(limit)
    for r in df.toLocalIterator():
        yield r.asDict(recursive=True)

