# file: delta_images.py
# Vastaa datan (urlien) hausta Deltasta

from typing import List, Dict

def get_image_rows(spark, curated_items_path: str, limit: int = 10) -> List[Dict]:
    """
    Lukee curated-items -datan annetusta Delta-polusta, suodattaa
    BASE_UNIT_OR_EACH + ei-NULL/ei-tyhjät URLit, ja palauttaa listan rivejä.
    Palauttaa kentät:
      PrimaryImageUrl, PrimaryImageFileName,
      GpcFamilyCode, GpcClassCode, BrandName, GTIN, PRODUCT_HIERARCHY_LEVEL_2,
      PRODUCT_HIERARCHY_LEVEL_3, PRODUCT_HIERARCHY_LEVEL_4
    """
    df = (
        spark.read.format("delta").load(curated_items_path)
        .where("TradeItemUnitDescriptor = 'BASE_UNIT_OR_EACH'")
        .where("PrimaryImageUrl IS NOT NULL AND PrimaryImageUrl <> ''")
        .select(
            "PrimaryImageUrl",
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
