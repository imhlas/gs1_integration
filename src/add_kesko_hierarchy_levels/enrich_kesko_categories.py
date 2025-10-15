# src/add_kesko_hierarchy_levels/enrich_kesko_categories.py

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from utils import azuresqlserver as sqlutil
from src.config import SQL_TABLE_CURATED_ITEMS, CURATED_ITEMS_WITH_KESKO  # käyttää keskitettyä konfigia


def _strip_leading_zeros_col(col: F.Column) -> F.Column:
    """
    Poista vasemman reunan nollat merkkijonosta.
    Jos tulos on tyhjä, palautetaan NULL (helpottaa joinia).
    """
    no_zeros = F.regexp_replace(F.coalesce(col.cast(StringType()), F.lit("")), r"^0+", "")
    return F.when(no_zeros == "", F.lit(None).cast(StringType())).otherwise(no_zeros)


def enrich_curated_with_kesko_categories(
    spark,
    dbutils,
    output_path: str | None = None,                 # <-- Täysi ABFSS-URI configista (kuten CURATED_ITEMS_WITH_KESKO)
    curated_table: str | None = None,               # jos None -> SQL_TABLE_CURATED_ITEMS
    kesko_levels_table: str = "dbo.KESKO_00_PRODUCT_HIERARCHY_LEVELS",
    write_mode: str = "overwrite",
    overwrite_schema: bool = True,
) -> dict:
    """
    Lue SQL:stä curated-tuoterivit ja Kesko-hierarkiat, liitä L2/L3/L4 GTIN-matchillä:
      1) suora GTIN == GTIN
      2) fallback: curated GTIN ilman etunollia == GTIN
    Kirjoita lopputulos Deltaan polkuun, joka tulee suoraan configista (kuten curate-funktiossa).

    Palauttaa: {"rows_written": int, "rows_categorized": int}
    """
    curated_table = curated_table or SQL_TABLE_CURATED_ITEMS
    output_path = output_path or CURATED_ITEMS_WITH_KESKO

    # --- 1) Lue SQL:stä ---
    curated_df = sqlutil.read_table(
        spark, table=curated_table, dbutils=dbutils,
        columns=None, top_n=None, order_by=None, to_pandas=False
    )

    kesko_df = sqlutil.read_table(
        spark, table=kesko_levels_table, dbutils=dbutils,
        columns=["GTIN", "PRODUCT_HIERARCHY_LEVEL_2", "PRODUCT_HIERARCHY_LEVEL_3", "PRODUCT_HIERARCHY_LEVEL_4"],
        top_n=None, order_by=None, to_pandas=False
    )

    # Tyypit & deduplikointi (Kesko-puolella yksi rivi / GTIN)
    curated = curated_df.withColumn("GTIN", F.col("GTIN").cast(StringType()))
    kesko = (
        kesko_df.select(
            F.col("GTIN").cast(StringType()).alias("GTIN"),
            F.col("PRODUCT_HIERARCHY_LEVEL_2").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_2"),
            F.col("PRODUCT_HIERARCHY_LEVEL_3").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_3"),
            F.col("PRODUCT_HIERARCHY_LEVEL_4").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_4"),
        )
        .dropDuplicates(["GTIN"])
    )

    # Fallback-avain: GTIN ilman etunollia
    curated = curated.withColumn("GTIN_NO_LEADING_ZEROS", _strip_leading_zeros_col(F.col("GTIN")))

    # --- 2) L1: suora GTIN-join ---
    l1 = curated.alias("c").join(
        kesko.alias("k1"),
        on=F.col("c.GTIN") == F.col("k1.GTIN"),
        how="left",
    ).select(
        F.col("c.*"),
        F.col("k1.PRODUCT_HIERARCHY_LEVEL_2").alias("KESKO_L2_L1"),
        F.col("k1.PRODUCT_HIERARCHY_LEVEL_3").alias("KESKO_L3_L1"),
        F.col("k1.PRODUCT_HIERARCHY_LEVEL_4").alias("KESKO_L4_L1"),
    )

    # --- 3) L2: fallback niille joilla ei osumaa ---
    no_match = l1.filter(
        F.col("KESKO_L2_L1").isNull() & F.col("KESKO_L3_L1").isNull() & F.col("KESKO_L4_L1").isNull()
    ).drop("KESKO_L2_L1", "KESKO_L3_L1", "KESKO_L4_L1")

    l2 = no_match.alias("c").join(
        kesko.alias("k2"),
        on=F.col("c.GTIN_NO_LEADING_ZEROS") == F.col("k2.GTIN"),
        how="left",
    ).select(
        F.col("c.*"),
        F.col("k2.PRODUCT_HIERARCHY_LEVEL_2").alias("KESKO_L2_L2"),
        F.col("k2.PRODUCT_HIERARCHY_LEVEL_3").alias("KESKO_L3_L2"),
        F.col("k2.PRODUCT_HIERARCHY_LEVEL_4").alias("KESKO_L4_L2"),
    )

    # --- 4) Yhdistä: coalesce L1 → L2 ---
    join_key_cols = ["Id"] if "Id" in l1.columns and "Id" in l2.columns else ["GTIN"]

    enriched = (
        l1.alias("a")
        .join(l2.alias("b"), on=[F.col(f"a.{k}") == F.col(f"b.{k}") for k in join_key_cols], how="left")
        .select(
            F.col("a.*"),
            F.coalesce(F.col("a.KESKO_L2_L1"), F.col("b.KESKO_L2_L2")).alias("PRODUCT_HIERARCHY_LEVEL_2"),
            F.coalesce(F.col("a.KESKO_L3_L1"), F.col("b.KESKO_L3_L2")).alias("PRODUCT_HIERARCHY_LEVEL_3"),
            F.coalesce(F.col("a.KESKO_L4_L1"), F.col("b.KESKO_L4_L2")).alias("PRODUCT_HIERARCHY_LEVEL_4"),
        )
        .drop("KESKO_L2_L1", "KESKO_L3_L1", "KESKO_L4_L1", "KESKO_L2_L2", "KESKO_L3_L2", "KESKO_L4_L2")
    )

    # --- 5) Kirjoita Deltaan konfiguroituun polkuun ---
    writer = enriched.write.format("delta").mode(write_mode)
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    writer.save(output_path)

    # --- 6) Raportti + 5 esimerkkiriviä ---
    rows_written = enriched.count()
    rows_categorized = enriched.filter(
        F.col("PRODUCT_HIERARCHY_LEVEL_2").isNotNull()
        | F.col("PRODUCT_HIERARCHY_LEVEL_3").isNotNull()
        | F.col("PRODUCT_HIERARCHY_LEVEL_4").isNotNull()
    ).count()

    print(f"Deltaan kirjoitettu rivejä: {rows_written:,}")
    print(f"Kesko-kategorioilla nimettyjä rivejä: {rows_categorized:,} ({rows_categorized/rows_written*100:.1f} %)")
    print("\nEsimerkkirivejä (5 kpl):")
    (spark.read.format("delta").load(output_path)
         .select("Id", "GTIN", "BrandName", "TradeItemDescription_fi", "PRODUCT_HIERARCHY_LEVEL_2", "PRODUCT_HIERARCHY_LEVEL_3", "PRODUCT_HIERARCHY_LEVEL_4")
         .limit(5)
         .show(truncate=False))

    return {"rows_written": rows_written, "rows_categorized": rows_categorized}

