# src/add_kesko_hierarchy_levels/enrich_kesko_categories.py

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from utils import azuresqlserver as sqlutil
from src.config import SQL_TABLE_CURATED_ITEMS, CURATED_ITEMS_WITH_KESKO


def _strip_leading_zeros_col(col: F.Column) -> F.Column:
    """
    Poista vasemman reunan nollat merkkijonosta.
    Jos tulos on tyhjä, palautetaan NULL (helpottaa joinia).
    """
    no_zeros = F.regexp_replace(F.coalesce(col.cast(StringType()), F.lit("")), r"^0+", "")
    return F.when(no_zeros == "", F.lit(None).cast(StringType())).otherwise(no_zeros)


# --- Rajausmäärittelyt ---
EXCLUDE_L2 = [
    '11 - Kala',
    '12 - Lihajaloste',
    '14 - Leipä',
    '26 - Jäätelöt',
    '33 - Mehut',
    '39 - Mureat leivät & korput',
    '40 - Keitot, kastikkeet ja kuivaruoka-ainekse',
]

EXCLUDE_L3_BY_L2 = {
    '41 - Maustaminen ja säilöntä': [
        '410 - Mausteet',
        '411 - Leivontatarvikkeet ja koristelu',
        '415 - Suolat',
    ],
}


def _filter_kesko_categories(kesko_df):
    """Rajaa pois ei-halutut L2-kategoriat ja L2+L3-yhdistelmät."""
    filtered = kesko_df.filter(~F.col("PRODUCT_HIERARCHY_LEVEL_2").isin(EXCLUDE_L2))

    for l2_val, l3_list in EXCLUDE_L3_BY_L2.items():
        filtered = filtered.filter(
            ~(
                (F.col("PRODUCT_HIERARCHY_LEVEL_2") == l2_val)
                & F.col("PRODUCT_HIERARCHY_LEVEL_3").isin(l3_list)
            )
        )
    return filtered


def _load_realtime_kesko_lookup_from_kesko_02(spark, dbutils):
    """
    Hae omien tuotteiden EAN-arvot KESKO_02_weekly_sales -taulusta (Lejosin myynnit
    Keskon myymälöissä viimeisen 4kk ajalta) ja yhdistä ne KESKO_00:n L2/L3/L4-nimiin
    TuoteryhmäID:n perusteella (L4-prefix LIKE-haku).

    KESKO_02 päivittyy viikoittain → omat tuotteet saavat kategorian käytännössä
    reaaliaikaisesti, ei 4kk viiveellä kuten suoraan KESKO_00:sta.

    KESKO_02-taulu on heap (ei indeksiä) ja noin 3.6M riviä — kysely vie ~70-80s.
    Tulosjoukko on pieni (n. 300-400 riviä) jotta sen voi broadcastata join-vaiheessa.
    """
    query = (
        "(SELECT DISTINCT k02.EAN, "
        "        k00.PRODUCT_HIERARCHY_LEVEL_2, "
        "        k00.PRODUCT_HIERARCHY_LEVEL_3, "
        "        k00.PRODUCT_HIERARCHY_LEVEL_4 "
        " FROM (SELECT DISTINCT EAN, [Tuoteryhmäid] "
        "       FROM dbo.KESKO_02_weekly_sales "
        "       WHERE (Vuosi * 100 + Viikko) >= "
        "             (YEAR(DATEADD(MONTH, -4, GETDATE())) * 100 "
        "              + DATEPART(WEEK, DATEADD(MONTH, -4, GETDATE())))) k02 "
        " LEFT JOIN dbo.KESKO_00_PRODUCT_HIERARCHY_LEVELS k00 "
        "   ON k00.PRODUCT_HIERARCHY_LEVEL_4 LIKE k02.[Tuoteryhmäid] + ' - %' "
        " WHERE k00.PRODUCT_HIERARCHY_LEVEL_2 IS NOT NULL) AS rt"
    )
    return sqlutil.read_table(spark, table=query, dbutils=dbutils)


def enrich_curated_with_kesko_categories(
    spark,
    dbutils,
    output_path: str | None = None,
    curated_table: str | None = None,
    kesko_levels_table: str = "dbo.KESKO_00_PRODUCT_HIERARCHY_LEVELS",
    write_mode: str = "overwrite",
    overwrite_schema: bool = True,
) -> dict:
    """
    Lue SQL:stä curated-tuoterivit ja Kesko-hierarkiat, liitä L2/L3/L4 kolmessa vaiheessa:
      0) realtime: KESKO_02_weekly_sales (omat tuotteet, n. 4kk ajalta) → KESKO_00.L4
         prefix-matchilla. Voittaa muut, jotta omien tuotteiden kategoria päivittyy
         ilman 4kk viivettä.
      1) viivästetty: suora curated.GTIN == KESKO_00.GTIN
      2) fallback: curated GTIN ilman etunollia == KESKO_00.GTIN

    Coalesce-järjestys L0 → L1 → L2. Kirjoita lopputulos Deltaan polkuun, joka tulee
    suoraan configista.

    Palauttaa: {"rows_written": int, "rows_categorized": int, "rows_via_realtime": int}
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

    # --- Rajaa ei-halutut kategoriat pois ---
    kesko = _filter_kesko_categories(kesko)

    # Fallback-avain: GTIN ilman etunollia
    curated = curated.withColumn("GTIN_NO_LEADING_ZEROS", _strip_leading_zeros_col(F.col("GTIN")))

    # --- 2) L0: realtime-join KESKO_02:n EAN-listalla ---
    print(">>> Haetaan realtime-kategoriat KESKO_02_weekly_sales -taulusta (n. 70-80s)")
    realtime_df = _load_realtime_kesko_lookup_from_kesko_02(spark, dbutils)
    realtime = (
        realtime_df
        .select(
            F.col("EAN").cast(StringType()).alias("EAN"),
            F.col("PRODUCT_HIERARCHY_LEVEL_2").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_2"),
            F.col("PRODUCT_HIERARCHY_LEVEL_3").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_3"),
            F.col("PRODUCT_HIERARCHY_LEVEL_4").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_4"),
        )
        .dropDuplicates(["EAN"])  # jos sama EAN matchaa useaan L4-prefiksiin, ota ensimmäinen
    )
    realtime = _filter_kesko_categories(realtime)

    l0 = curated.alias("c").join(
        F.broadcast(realtime).alias("k0"),
        on=F.col("c.GTIN_NO_LEADING_ZEROS") == F.col("k0.EAN"),
        how="left",
    ).select(
        F.col("c.*"),
        F.col("k0.PRODUCT_HIERARCHY_LEVEL_2").alias("KESKO_L2_L0"),
        F.col("k0.PRODUCT_HIERARCHY_LEVEL_3").alias("KESKO_L3_L0"),
        F.col("k0.PRODUCT_HIERARCHY_LEVEL_4").alias("KESKO_L4_L0"),
    )

    # --- 3) L1: suora GTIN-join (viivästetty, KESKO_00) ---
    l1 = l0.alias("c").join(
        kesko.alias("k1"),
        on=F.col("c.GTIN") == F.col("k1.GTIN"),
        how="left",
    ).select(
        F.col("c.*"),
        F.col("k1.PRODUCT_HIERARCHY_LEVEL_2").alias("KESKO_L2_L1"),
        F.col("k1.PRODUCT_HIERARCHY_LEVEL_3").alias("KESKO_L3_L1"),
        F.col("k1.PRODUCT_HIERARCHY_LEVEL_4").alias("KESKO_L4_L1"),
    )

    # --- 4) L2: fallback niille joilla ei osumaa L0:ssa eikä L1:ssä ---
    no_match = l1.filter(
        F.col("KESKO_L2_L0").isNull() & F.col("KESKO_L3_L0").isNull() & F.col("KESKO_L4_L0").isNull()
        & F.col("KESKO_L2_L1").isNull() & F.col("KESKO_L3_L1").isNull() & F.col("KESKO_L4_L1").isNull()
    ).drop("KESKO_L2_L0", "KESKO_L3_L0", "KESKO_L4_L0",
           "KESKO_L2_L1", "KESKO_L3_L1", "KESKO_L4_L1")

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

    # --- 5) Yhdistä: coalesce L0 → L1 → L2 ---
    join_key_cols = ["Id"] if "Id" in l1.columns and "Id" in l2.columns else ["GTIN"]

    enriched = (
        l1.alias("a")
        .join(l2.alias("b"), on=[F.col(f"a.{k}") == F.col(f"b.{k}") for k in join_key_cols], how="left")
        .select(
            F.col("a.*"),
            F.coalesce(F.col("a.KESKO_L2_L0"), F.col("a.KESKO_L2_L1"), F.col("b.KESKO_L2_L2")).alias("PRODUCT_HIERARCHY_LEVEL_2"),
            F.coalesce(F.col("a.KESKO_L3_L0"), F.col("a.KESKO_L3_L1"), F.col("b.KESKO_L3_L2")).alias("PRODUCT_HIERARCHY_LEVEL_3"),
            F.coalesce(F.col("a.KESKO_L4_L0"), F.col("a.KESKO_L4_L1"), F.col("b.KESKO_L4_L2")).alias("PRODUCT_HIERARCHY_LEVEL_4"),
            F.when(F.col("a.KESKO_L2_L0").isNotNull(), F.lit("realtime"))
             .when(F.col("a.KESKO_L2_L1").isNotNull(), F.lit("kesko00_direct"))
             .when(F.col("b.KESKO_L2_L2").isNotNull(), F.lit("kesko00_no_leading_zeros"))
             .otherwise(F.lit(None).cast(StringType()))
             .alias("KESKO_CATEGORY_SOURCE"),
        )
        .drop("KESKO_L2_L0", "KESKO_L3_L0", "KESKO_L4_L0",
              "KESKO_L2_L1", "KESKO_L3_L1", "KESKO_L4_L1",
              "KESKO_L2_L2", "KESKO_L3_L2", "KESKO_L4_L2")
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
    rows_via_realtime = enriched.filter(F.col("KESKO_CATEGORY_SOURCE") == "realtime").count()

    print(f"Deltaan kirjoitettu rivejä: {rows_written:,}")
    print(f"Kesko-kategorioilla nimettyjä rivejä: {rows_categorized:,} ({rows_categorized/rows_written*100:.1f} %)")
    print(f"  niistä realtime-lähteestä (KESKO_02): {rows_via_realtime:,}")

    return {
        "rows_written": rows_written,
        "rows_categorized": rows_categorized,
        "rows_via_realtime": rows_via_realtime,
    }
