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

    Uudet sarakkeet BASE_UNIT-riveille:
        Case_GTIN, Case_Depth_mm, Case_Width_mm, Case_Height_mm, Case_GrossWeight_g,
        UnitsPerCase

    Muiden tasojen riveille (CASE, PALLET, consumer-monipakkaukset) uudet sarakkeet ovat NULL.

    Palauttaa: {"rows_written": int, "rows_with_case_dims": int}
    """
    input_path = input_path or CURATED_ITEMS_WITH_KESKO
    output_path = output_path or CURATED_ITEMS_WITH_KESKO

    src = spark.read.format("delta").load(input_path)

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

    new_case_cols = ["Case_GTIN", "Case_Depth_mm", "Case_Width_mm",
                     "Case_Height_mm", "Case_GrossWeight_g", "UnitsPerCase"]

    enriched_base = (base_rows.alias("b")
        .join(case_pick.alias("c"), F.col("b.GTIN") == F.col("c.BaseGTIN"), "left")
        .select("b.*", *[F.col(f"c.{c}") for c in new_case_cols]))

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
