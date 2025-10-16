# file: delta_images.py
# Vastaa datan (urlien) hausta Deltasta

from typing import List, Dict

def get_image_rows(spark, curated_items_path: str, limit: int = 10) -> List[Dict]:
    """
    Lukee curated-items -datan annetusta Delta-polusta, suodattaa
    BASE_UNIT_OR_EACH + ei-tyhjä PrimaryImageUrl + ei-tyhjä Kesko L2,
    ja palauttaa listan rivejä.
    """
    df = (
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
        .limit(limit)
    )
    return [r.asDict(recursive=True) for r in df.collect()]
