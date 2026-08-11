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

# --- Omien tuotteiden placeholder-kategoria ---
# Omat tuotteet tunnistetaan GS1:n InformationProviderOfTradeItem.PartyName -kentästä
# (curated-sarake InfoProviderName).
#
# Jos omalle tuotteelle ei löydy Kesko-kategoriaa miltään tasolta (L0/L1/L2), sille
# kirjoitetaan placeholder KAIKKIIN kolmeen hierarkiasarakkeeseen. Näin kuvapipeline
# (delta_images._base_df vaatii ei-tyhjän PRODUCT_HIERARCHY_LEVEL_2) lataa tuotteen
# kuvan ja se näkyy SharePointissa omana näkymänään.
#
# Placeholder korvautuu oikealla kategorialla automaattisesti seuraavassa ajossa heti
# kun tuotteelle kertyy Kesko-myyntiä (L0/realtime) tai KESKO_00 päivittyy (L1/L2) —
# coalesce-järjestys L0 → L1 → L2 → placeholder hoitaa korvautumisen.
OWN_INFO_PROVIDER_NAME = "Lejos Oy"
OWN_PLACEHOLDER_CATEGORY = "99 - Luokittelematon (oma tuote)"


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

    Coalesce-järjestys L0 → L1 → L2. Jos omalta tuotteelta (InfoProviderName ==
    OWN_INFO_PROVIDER_NAME) puuttuu kategoria kaikilta tasoilta, kaikkiin kolmeen
    hierarkiasarakkeeseen kirjoitetaan OWN_PLACEHOLDER_CATEGORY, jotta kuvapipeline
    lataa tuotteen kuvan. Placeholder korvautuu automaattisesti heti kun oikea
    kategoria löytyy.

    Placeholderia EI anneta tuotteelle joka löytyy suodattamattomasta Kesko-datasta
    (KESKO_00 tai KESKO_02): sellaisen kategoria on rajattu pois tarkoituksella
    EXCLUDE_L2/EXCLUDE_L3_BY_L2 -listoilla, eikä kyseessä ole luokittelematon uutuus.

    Kirjoita lopputulos Deltaan polkuun, joka tulee suoraan configista.

    Palauttaa: {"rows_written": int, "rows_categorized": int, "rows_via_realtime": int,
                "rows_via_own_placeholder": int, "rows_own_suppressed": int}
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
    kesko_unfiltered = (
        kesko_df.select(
            F.col("GTIN").cast(StringType()).alias("GTIN"),
            F.col("PRODUCT_HIERARCHY_LEVEL_2").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_2"),
            F.col("PRODUCT_HIERARCHY_LEVEL_3").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_3"),
            F.col("PRODUCT_HIERARCHY_LEVEL_4").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_4"),
        )
        .dropDuplicates(["GTIN"])
    )

    # --- Rajaa ei-halutut kategoriat pois ---
    kesko = _filter_kesko_categories(kesko_unfiltered)

    # Fallback-avain: GTIN ilman etunollia
    curated = curated.withColumn("GTIN_NO_LEADING_ZEROS", _strip_leading_zeros_col(F.col("GTIN")))

    # --- 2) L0: realtime-join KESKO_02:n EAN-listalla ---
    print(">>> Haetaan realtime-kategoriat KESKO_02_weekly_sales -taulusta (n. 70-80s)")
    realtime_df = _load_realtime_kesko_lookup_from_kesko_02(spark, dbutils)
    realtime_unfiltered = (
        realtime_df
        .select(
            F.col("EAN").cast(StringType()).alias("EAN"),
            F.col("PRODUCT_HIERARCHY_LEVEL_2").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_2"),
            F.col("PRODUCT_HIERARCHY_LEVEL_3").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_3"),
            F.col("PRODUCT_HIERARCHY_LEVEL_4").cast(StringType()).alias("PRODUCT_HIERARCHY_LEVEL_4"),
        )
        .dropDuplicates(["EAN"])  # jos sama EAN matchaa useaan L4-prefiksiin, ota ensimmäinen
    )
    realtime = _filter_kesko_categories(realtime_unfiltered)

    # --- Kesko-tuntemus SUODATTAMATTOMASTA datasta ---
    # Tuote joka löytyy KESKO_00:sta tai KESKO_02:sta ei ole "luokittelematon uutuus",
    # vaikka sen kategoria olisi suodattunut pois EXCLUDE_L2/EXCLUDE_L3_BY_L2 -listoilla.
    # Näiltä placeholder estetään, jotta tietoinen kategoriarajaus ei kierry ohi
    # omien tuotteiden kohdalla. Kun tällaisen tuotteen kategoria myöhemmin ilmestyy
    # KESKO_00:aan, se pysyy edelleen rajauksen piirissä.
    known_keys = (
        kesko_unfiltered.select(F.col("GTIN").alias("KNOWN_KEY"))
        .unionByName(realtime_unfiltered.select(F.col("EAN").alias("KNOWN_KEY")))
        .where(F.col("KNOWN_KEY").isNotNull())
        .dropDuplicates(["KNOWN_KEY"])
        .withColumn("_KNOWN", F.lit(True))
    )

    curated = (
        curated.alias("cu")
        .join(known_keys.alias("ka"), F.col("cu.GTIN") == F.col("ka.KNOWN_KEY"), "left")
        .select(F.col("cu.*"), F.col("ka._KNOWN").alias("_KNOWN_BY_GTIN"))
    )
    curated = (
        curated.alias("cu")
        .join(known_keys.alias("kb"), F.col("cu.GTIN_NO_LEADING_ZEROS") == F.col("kb.KNOWN_KEY"), "left")
        .select(F.col("cu.*"), F.col("kb._KNOWN").alias("_KNOWN_BY_NLZ"))
    )
    curated = (
        curated
        .withColumn("KESKO_KNOWN",
                    F.coalesce(F.col("_KNOWN_BY_GTIN"), F.col("_KNOWN_BY_NLZ"), F.lit(False)))
        .drop("_KNOWN_BY_GTIN", "_KNOWN_BY_NLZ")
    )

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

    # --- 5) Yhdistä: coalesce L0 → L1 → L2 → placeholder (omat tuotteet) ---
    join_key_cols = ["Id"] if "Id" in l1.columns and "Id" in l2.columns else ["GTIN"]

    joined = l1.alias("a").join(
        l2.alias("b"),
        on=[F.col(f"a.{k}") == F.col(f"b.{k}") for k in join_key_cols],
        how="left",
    )

    # Kesko-kategoria tasoittain: L0 (realtime) → L1 (suora) → L2 (fallback)
    kesko_l2 = F.coalesce(F.col("a.KESKO_L2_L0"), F.col("a.KESKO_L2_L1"), F.col("b.KESKO_L2_L2"))
    kesko_l3 = F.coalesce(F.col("a.KESKO_L3_L0"), F.col("a.KESKO_L3_L1"), F.col("b.KESKO_L3_L2"))
    kesko_l4 = F.coalesce(F.col("a.KESKO_L4_L0"), F.col("a.KESKO_L4_L1"), F.col("b.KESKO_L4_L2"))

    # Placeholder annetaan vain omille tuotteille joilta puuttuu kategoria kaikilta
    # tasoilta. Ehto on rivikohtainen (ei sarakekohtainen), jotta kaikki kolme saraketta
    # täyttyvät aina yhdessä eikä synny puoliksi placeholder -rivejä.
    # trim+upper suojaa InfoProviderName-kentän kirjoitusasun vaihtelulta.
    is_own_product = (
        F.upper(F.trim(F.col("a.InfoProviderName"))) == F.lit(OWN_INFO_PROVIDER_NAME.upper())
    )
    # KESKO_KNOWN = tuote löytyy suodattamattomasta Kesko-datasta → sen kategoria on
    # rajattu pois tarkoituksella, joten se ei saa placeholderia.
    is_unknown_to_kesko = F.col("a.KESKO_KNOWN") == F.lit(False)
    use_placeholder = is_own_product & kesko_l2.isNull() & is_unknown_to_kesko

    # KESKO_KNOWN säilytetään vielä tässä välivaiheessa, jotta kaikki raportoitavat
    # luvut saadaan laskettua yhdellä läpikäynnillä. Se pudotetaan ennen kirjoitusta,
    # joten Gold-Deltan ja SQL-taulun skeema pysyy ennallaan.
    enriched_full = (
        joined
        .select(
            F.col("a.*"),
            F.when(use_placeholder, F.lit(OWN_PLACEHOLDER_CATEGORY)).otherwise(kesko_l2).alias("PRODUCT_HIERARCHY_LEVEL_2"),
            F.when(use_placeholder, F.lit(OWN_PLACEHOLDER_CATEGORY)).otherwise(kesko_l3).alias("PRODUCT_HIERARCHY_LEVEL_3"),
            F.when(use_placeholder, F.lit(OWN_PLACEHOLDER_CATEGORY)).otherwise(kesko_l4).alias("PRODUCT_HIERARCHY_LEVEL_4"),
            F.when(F.col("a.KESKO_L2_L0").isNotNull(), F.lit("realtime"))
             .when(F.col("a.KESKO_L2_L1").isNotNull(), F.lit("kesko00_direct"))
             .when(F.col("b.KESKO_L2_L2").isNotNull(), F.lit("kesko00_no_leading_zeros"))
             .when(use_placeholder, F.lit("own_placeholder"))
             .otherwise(F.lit(None).cast(StringType()))
             .alias("KESKO_CATEGORY_SOURCE"),
        )
        .drop("KESKO_L2_L0", "KESKO_L3_L0", "KESKO_L4_L0",
              "KESKO_L2_L1", "KESKO_L3_L1", "KESKO_L4_L1",
              "KESKO_L2_L2", "KESKO_L3_L2", "KESKO_L4_L2")
    )

    # Spark on laiska: jokainen action ajaisi koko ketjun (2 JDBC-lukua + KESKO_02:n
    # 70-80 s kysely + joinit) uudelleen. Cachetetaan kerran, jotta raportti ja
    # kirjoitus jakavat saman lasketun tuloksen.
    enriched_full = enriched_full.cache()

    enriched = enriched_full.drop("KESKO_KNOWN")

    # --- 5) Kirjoita Deltaan konfiguroituun polkuun ---
    writer = enriched.write.format("delta").mode(write_mode)
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    writer.save(output_path)

    # --- 6) Raportti: kaikki luvut YHDELLÄ läpikäynnillä ---
    # Aiemmin nämä olivat erillisiä count()-kutsuja, joista jokainen ajoi koko
    # laskentaketjun uudelleen. Nyt sama tulos yhdellä aggregaatilla cachetetusta
    # välituloksesta.
    _src = F.col("KESKO_CATEGORY_SOURCE")

    # rows_categorized lasketaan vain OIKEISTA Kesko-kategorioista — placeholder
    # jätetään ulkopuolelle, jotta mittari ei näytä paremmalta kuin tilanne on.
    _real_category = (
        (
            F.col("PRODUCT_HIERARCHY_LEVEL_2").isNotNull()
            | F.col("PRODUCT_HIERARCHY_LEVEL_3").isNotNull()
            | F.col("PRODUCT_HIERARCHY_LEVEL_4").isNotNull()
        )
        & ~F.coalesce(_src == F.lit("own_placeholder"), F.lit(False))
    )
    # Estetyt: oma tuote, ei kategoriaa (source NULL), mutta Kesko tuntee tuotteen.
    _suppressed = (
        (F.upper(F.trim(F.col("InfoProviderName"))) == F.lit(OWN_INFO_PROVIDER_NAME.upper()))
        & _src.isNull()
        & (F.col("KESKO_KNOWN") == F.lit(True))
    )

    def _cnt(cond):
        return F.sum(F.when(cond, F.lit(1)).otherwise(F.lit(0)))

    m = enriched_full.agg(
        F.count(F.lit(1)).alias("rows_written"),
        _cnt(_real_category).alias("rows_categorized"),
        _cnt(_src == F.lit("realtime")).alias("rows_via_realtime"),
        _cnt(_src == F.lit("own_placeholder")).alias("rows_via_own_placeholder"),
        _cnt(_suppressed).alias("rows_own_suppressed"),
    ).collect()[0]

    rows_written = int(m["rows_written"] or 0)
    rows_categorized = int(m["rows_categorized"] or 0)
    rows_via_realtime = int(m["rows_via_realtime"] or 0)
    rows_via_own_placeholder = int(m["rows_via_own_placeholder"] or 0)
    rows_own_suppressed = int(m["rows_own_suppressed"] or 0)

    enriched_full.unpersist()

    _pct = (rows_categorized / rows_written * 100) if rows_written else 0.0
    print(f"Deltaan kirjoitettu rivejä: {rows_written:,}")
    print(f"Kesko-kategorioilla nimettyjä rivejä: {rows_categorized:,} ({_pct:.1f} %)")
    print(f"  niistä realtime-lähteestä (KESKO_02): {rows_via_realtime:,}")
    print(f"Omia tuotteita placeholder-kategorialla '{OWN_PLACEHOLDER_CATEGORY}': {rows_via_own_placeholder:,}")
    print(f"  placeholder estetty (Kesko tuntee, kategoria rajattu pois): {rows_own_suppressed:,}")

    return {
        "rows_written": rows_written,
        "rows_categorized": rows_categorized,
        "rows_via_realtime": rows_via_realtime,
        "rows_via_own_placeholder": rows_via_own_placeholder,
        "rows_own_suppressed": rows_own_suppressed,
    }
