# src/add_case_hierarchy/enrich_case_dimensions.py

from pyspark.sql import functions as F, Window
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType

from src.config import CURATED_ITEMS_WITH_KESKO


CHILD_TRADE_ITEM_SCHEMA = ArrayType(StructType([
    StructField("Gtin", StringType(), True),
    StructField("QuantityOfNextLowerLevelTradeItem", LongType(), True),
]))


def enrich_curated_with_case_dimensions(
    spark,
    input_path: str | None = None,
    output_path: str | None = None,
    write_mode: str = "overwrite",
) -> dict:
    """
    Lue Gold-Delta (Kesko-rikastettu) ja liitä BASE_UNIT-riveille sen myyntierän (CASE
    + IsDespatchUnit=true) mitat uusina sarakkeina.

    Strategia 1:N (sama BASE_UNIT useassa CASE:ssa): valitaan myyntierä, jossa pienin
    TotalQuantityOfNextLowerLevelTradeItem (= lähin myyntierä BASE_UNIT:lle).

    Jos BASE_UNIT-rivi on itse despatch unit (`IsDespatchUnit=true`) eikä CASE-emoa löydy
    joinilla, kopioidaan rivin omat mitat Case_*-sarakkeisiin (Case_GTIN=GTIN, UnitsPerCase=1).
    Tämä kattaa tuotteet jotka toimitetaan suoraan ilman erillistä CASE-pakkausta.

    Uudet sarakkeet BASE_UNIT-riveille:
        Case_GTIN, Case_Depth_mm, Case_Width_mm, Case_Height_mm, Case_GrossWeight_g,
        UnitsPerCase

    Muiden tasojen riveille (CASE, PALLET) uudet sarakkeet ovat NULL — kuluttaja filtteröi
    BASE_UNIT-rivit ja katsoo niiden Case_*-sarakkeita.

    Palauttaa: {"rows_written": int, "rows_with_case_dims": int}
    """
    input_path = input_path or CURATED_ITEMS_WITH_KESKO
    output_path = output_path or CURATED_ITEMS_WITH_KESKO

    src = spark.read.format("delta").load(input_path)

    # Idempotenssi: jos edellinen ajo on jo lisännyt Case_*-sarakkeet samaan polkuun,
    # dropataan ne ennen kuin lisätään uudet versiot. Muuten join joinin alla
    # tuottaa AMBIGUOUS_REFERENCE -virheen.
    stale_cols = [c for c in ["Case_GTIN", "Case_Depth_mm", "Case_Width_mm",
                              "Case_Height_mm", "Case_GrossWeight_g", "UnitsPerCase"]
                  if c in src.columns]
    if stale_cols:
        src = src.drop(*stale_cols)

    base_rows = src.where("TradeItemUnitDescriptor = 'BASE_UNIT_OR_EACH'")
    other_rows = src.where("TradeItemUnitDescriptor IS NULL OR TradeItemUnitDescriptor <> 'BASE_UNIT_OR_EACH'")

    despatch_cases = src.where(
        "TradeItemUnitDescriptor = 'CASE' AND IsDespatchUnit = true "
        "AND ChildTradeItemJson IS NOT NULL"
    ).select(
        F.col("GTIN").alias("Case_GTIN"),
        F.col("Depth_mm").alias("Case_Depth_mm"),
        F.col("Width_mm").alias("Case_Width_mm"),
        F.col("Height_mm").alias("Case_Height_mm"),
        F.col("GrossWeight_g").alias("Case_GrossWeight_g"),
        F.col("TotalQuantityOfNextLowerLevelTradeItem").cast(LongType()).alias("UnitsPerCase"),
        F.from_json(F.col("ChildTradeItemJson"), CHILD_TRADE_ITEM_SCHEMA).alias("ChildArr"),
    )

    case_to_base = (despatch_cases
        .withColumn("Child", F.explode("ChildArr"))
        .select(
            F.col("Child.Gtin").alias("BaseGTIN"),
            "Case_GTIN",
            "Case_Depth_mm",
            "Case_Width_mm",
            "Case_Height_mm",
            "Case_GrossWeight_g",
            "UnitsPerCase",
        )
        .where("BaseGTIN IS NOT NULL"))

    pick_smallest = Window.partitionBy("BaseGTIN").orderBy(F.asc_nulls_last("UnitsPerCase"))
    case_pick = (case_to_base
        .withColumn("_rn", F.row_number().over(pick_smallest))
        .where("_rn = 1")
        .drop("_rn"))

    joined = (base_rows.alias("b")
        .join(case_pick.alias("c"), F.col("b.GTIN") == F.col("c.BaseGTIN"), "left")
        .drop("BaseGTIN"))

    is_self_despatch = F.col("IsDespatchUnit") == True

    enriched_base = (joined
        .withColumn("Case_GTIN",
            F.coalesce(F.col("Case_GTIN"),
                       F.when(is_self_despatch, F.col("GTIN"))))
        .withColumn("Case_Depth_mm",
            F.coalesce(F.col("Case_Depth_mm"),
                       F.when(is_self_despatch, F.col("Depth_mm"))))
        .withColumn("Case_Width_mm",
            F.coalesce(F.col("Case_Width_mm"),
                       F.when(is_self_despatch, F.col("Width_mm"))))
        .withColumn("Case_Height_mm",
            F.coalesce(F.col("Case_Height_mm"),
                       F.when(is_self_despatch, F.col("Height_mm"))))
        .withColumn("Case_GrossWeight_g",
            F.coalesce(F.col("Case_GrossWeight_g"),
                       F.when(is_self_despatch, F.col("GrossWeight_g"))))
        .withColumn("UnitsPerCase",
            F.coalesce(F.col("UnitsPerCase"),
                       F.when(is_self_despatch, F.lit(1).cast(LongType())))))

    null_case_cols = [
        F.lit(None).cast(StringType()).alias("Case_GTIN"),
        F.lit(None).cast(StringType()).alias("Case_Depth_mm"),
        F.lit(None).cast(StringType()).alias("Case_Width_mm"),
        F.lit(None).cast(StringType()).alias("Case_Height_mm"),
        F.lit(None).cast(StringType()).alias("Case_GrossWeight_g"),
        F.lit(None).cast(LongType()).alias("UnitsPerCase"),
    ]
    other_with_nulls = other_rows.select("*", *null_case_cols)

    result = enriched_base.unionByName(other_with_nulls)

    (result.write.format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .save(output_path))

    rows_written = result.count()
    rows_with_case_dims = result.filter(F.col("Case_GTIN").isNotNull()).count()

    print(f"Case-rikastettuja rivejä yhteensä: {rows_written:,}")
    print(f"BASE_UNIT-rivit, joille saatiin myyntierämitat: {rows_with_case_dims:,}")

    return {"rows_written": rows_written, "rows_with_case_dims": rows_with_case_dims}
