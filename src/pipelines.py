# src/pipelines.py

import time
import datetime
from src.connection import APIClient
from src.endpoints import list_keys_all, list_keys_changes, next_offset
from src.loaders import fetch_all_keys_to_bronze, fetch_items_to_silver_json
from src.add_kesko_hierarchy_levels.enrich_kesko_categories import enrich_curated_with_kesko_categories
from src.add_case_hierarchy.enrich_case_dimensions import enrich_curated_with_case_dimensions
from src.fetch_images.sharepoint_upload import process_batch_parallel
from src.curate_items import kuratoi_ja_talleta_deltaan_like_batch
from utils.azuresqlserver import write_overwrite  
from src.config import *


# ------------- Yhteiset asetukset / apufunktiot ----------------- #

KEY_BATCH_SIZE = 90000
ITEM_BATCH = 1000
BASE = "https://productapi-synkka.gs1.fi"


def _setup_spark_and_storage(spark, dbutils):
    """Aseta storage key ja Delta-automerge."""
    access_key = dbutils.secrets.get("gs1-kv", "storage-access-key")
    spark.conf.set(f"fs.azure.account.key.{ACCOUNT}.dfs.core.windows.net", access_key)

    if SPARK_DELTA_AUTOMERGE:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


def _create_gs1_client(dbutils):
    """Luo APIClient ja autentikoi."""
    email = dbutils.secrets.get("gs1-kv", "email")
    password = dbutils.secrets.get("gs1-kv", "password")
    gln = dbutils.secrets.get("gs1-kv", "gln")

    client = APIClient(BASE, email, password, gln)
    client.authenticate(verbose=True)
    return client


# ------------- FULL-ajo: koko katalogi alusta asti ------------- #

def run_full_pipeline(spark, dbutils):
    """
    FULL-ajo:
    - Hakee kaikki avaimet GS1:stä (All → Bronze)
    - Hakee kaikki tuotteet (Bronze → Silver)
    - Kuratoi (Silver → Curated, duplikaattien poisto + Lejos_UpdatedAt)
    - Kirjoittaa Curatedin SQL-tauluun (overwrite)
    - Rikastaa Kesko-kategorioilla JA kirjoittaa myös ne SQL:ään (overwrite snapshot)
    """
    print(">>> FULL-ajo käynnissä")

    _setup_spark_and_storage(spark, dbutils)
    client = _create_gs1_client(dbutils)

    # 1) All → Bronze
    all_keys = fetch_all_keys_to_bronze(
        spark,
        client,
        bronze_path=DELTA_KEYS,
        batch_size=KEY_BATCH_SIZE,
    )
    print(f"Avaimien (All) nouto valmis, avaimia talletettu: {all_keys}")

    # 2) Bronze → Silver (kaikki Id:t)
    n_fetched = fetch_items_to_silver_json(
        spark,
        client,
        silver_path=DELTA_PRODUCTS,
        bronze_path=DELTA_KEYS,
        batch_size=ITEM_BATCH,
        first_write_mode="overwrite",
        rate_limit_per_minute=RATE_LIMIT_PER_MIN,
        verbose=True,
        only_gpc_segment_code=ONLY_GPC_SEGMENT_CODE,
    )
    print(f"Koko kannan tuotteet haettu ja tallennettu Silveriin. Rivimäärä: {n_fetched}")

    # 3) Silver → Curated (sis. duplikaattien poisto GTIN + StartAvailabilityDateTime)
    print(">>> Kuratoidaan tuotteet (Silver → Curated)")
    rows_curated = kuratoi_ja_talleta_deltaan_like_batch(
        spark,
        silver_path=DELTA_PRODUCTS,
        curated_path=CURATED_ITEMS,
        write_mode="overwrite",
        sample_rows=5,
    )
    print(f"Kuratoituja rivejä kirjoitettu Curated-Deltaan: {rows_curated}")

    # 4) Curated → SQL (yli kirjoittaen: taulu = aina uusin snapshot)
    print(">>> Kirjoitetaan Curated SQL-tauluun (overwrite)")
    curated_df = spark.read.format("delta").load(CURATED_ITEMS)
    write_overwrite(curated_df, SQL_TABLE_CURATED_ITEMS, dbutils, truncate=True)


    print(">>> Lisätään Kesko-kategoriat")
    kesko_stats = enrich_curated_with_kesko_categories(spark, dbutils)

    print(">>> Liitetään myyntierämitat BASE_UNIT-riveille (CASE + IsDespatchUnit=true)")
    case_stats = enrich_curated_with_case_dimensions(spark)

    print(">>> Kirjoitetaan Kesko + Case-rikastettu data SQL-tauluun (overwrite)")
    kesko_df = spark.read.format("delta").load(CURATED_ITEMS_WITH_KESKO)
    write_overwrite(kesko_df, SQL_TABLE_KESKO_CATEGORIES, dbutils, truncate=True)

    print(">>> FULL-ajo valmis.")

    # 🔙 Palautetaan kaikki olennaiset luvut
    return {
        "all_keys": all_keys,
        "silver_rows": n_fetched,
        "curated_rows": rows_curated,
        "kesko_stats": kesko_stats,  # tämä otetaan enrich-funktiosta
        "case_stats": case_stats,
    }


# ------------- CHANGES-ajo: vain muuttuneet tuotteet ----------- #

def run_changes_pipeline(spark, dbutils, since_iso):
    """
    CHANGES-ajo:
    - Hakee GS1:stä vain muuttuneet avaimet (Changes)
    - Hakee näiden tuotteiden datan Silveriin
    - Kuratoi koko Silver-datan (sis. duplikaattien poiston)
    - Kirjoittaa Curatedin SQL-tauluun (overwrite)

    """
    print(f">>> CHANGES-ajo käynnissä alkaen {since_iso.split('T', 1)[0]}")

    # Otetaan talteen ajon aloitusaika
    run_start_ts = datetime.datetime.utcnow().isoformat()

    _setup_spark_and_storage(spark, dbutils)
    client = _create_gs1_client(dbutils)

    # 1) Changes → Id-lista
    chg_ids = []
    offset = None

    while True:
        resp_chg = list_keys_changes(
            client,
            batch_size=KEY_BATCH_SIZE,
            since=since_iso,
            offset=offset,
        )
        items = resp_chg.get("Items") or []
        got = [it.get("Id") for it in items if it.get("Id")]
        chg_ids.extend(got)

        print(f"Haettu sivu, uusia ID:itä: {len(got)}, yhteensä: {len(chg_ids)}")

        offset = next_offset(resp_chg)
        if not offset:
            break
        time.sleep(1.5)

    print(f"Avaimien (Changes) nouto valmis, avaimia haettu yhteensä: {len(chg_ids)}")

    if not chg_ids:
        print("Ei muutoksia haettavaksi.")
        return

    # 2) Id-lista → Silver (vain nämä Id:t)
    n_fetched = fetch_items_to_silver_json(
        spark,
        client,
        silver_path=DELTA_PRODUCTS,
        ids=chg_ids,
        batch_size=ITEM_BATCH,
        first_write_mode="append", 
        rate_limit_per_minute=RATE_LIMIT_PER_MIN,
        verbose=True,
        only_gpc_segment_code=ONLY_GPC_SEGMENT_CODE,
    )
    print(f"Muuttuneiden tuotteiden tiedot noudettu ja tallennettu Silveriin. Rivimäärä: {n_fetched}")

    # ---- Poimi tämän ajon aikana päivittyneet GTINit Silveristä ----
    # Luetaan Silver-taulu
    silver_df = spark.read.format("delta").load(DELTA_PRODUCTS)

    # Rajataan vain rivit, joiden ingest_ts on tämän ajon aloitusajan jälkeen
    new_rows = silver_df.filter(f"ingest_ts >= '{run_start_ts}'")

    # Poimitaan GTIN JSON:sta käyttäen SQL-funktioita tekstinä
    gtin_df = (
        new_rows
        .selectExpr(
            "coalesce(get_json_object(raw_json, '$.TradeItem.Gtin'), "
            "         get_json_object(raw_json, '$.GTIN')) as GTIN"
        )
        .where("GTIN IS NOT NULL")
        .distinct()
    )

    updated_gtins = [row["GTIN"] for row in gtin_df.collect()]
    print(f"Päivittyneitä GTIN-koodeja tässä ajossa: {len(updated_gtins)}")

    # 3) Silver → Curated (sis. duplikaattien poisto)
    print(">>> Kuratoidaan tuotteet (Silver → Curated)")
    rows_curated = kuratoi_ja_talleta_deltaan_like_batch(
        spark,
        silver_path=DELTA_PRODUCTS,
        curated_path=CURATED_ITEMS,
        write_mode="overwrite",
        sample_rows=5,
    )
    print(f"Kuratoituja rivejä kirjoitettu Curated-Deltaan: {rows_curated}")

    # 4) Curated → SQL (overwrite)
    print(">>> Kirjoitetaan Curated SQL-tauluun (overwrite)")
    curated_df = spark.read.format("delta").load(CURATED_ITEMS)
    write_overwrite(curated_df, SQL_TABLE_CURATED_ITEMS, dbutils, truncate=True)


    print(">>> Lisätään Kesko-kategoriat")
    enrich_curated_with_kesko_categories(spark, dbutils)

    print(">>> Liitetään myyntierämitat BASE_UNIT-riveille (CASE + IsDespatchUnit=true)")
    enrich_curated_with_case_dimensions(spark)

    print(">>> CHANGES-ajo valmis.")

    return updated_gtins

def run_changes_pipeline_with_images(spark, dbutils, since_iso: str):
    updated_gtins = run_changes_pipeline(spark, dbutils, since_iso=since_iso)

    if not updated_gtins:
        print("Ei päivittyneitä GTINeitä – ei tarvitse päivittää kuvia.")
        return

    print(f"Päivitettyjä GTINeitä {len(updated_gtins)} kpl")

    SITE_URL      = dbutils.secrets.get("gs1-kv", "sharepoint-site-url")
    CLIENT_ID     = dbutils.secrets.get("gs1-kv", "sharepoint-client-id")
    CLIENT_SECRET = dbutils.secrets.get("gs1-kv", "sharepoint-client-secret")

    TENANT_ID  = SP_TENANT_ID
    GRAPH_BASE = SP_GRAPH_BASE
    HOSTNAME   = SP_HOSTNAME
    SCOPE      = SP_SCOPE

    TARGET_LIBRARY_NAME = SP_TARGET_LIBRARY_NAME
    TARGET_SUBFOLDER    = SP_TARGET_SUBFOLDER

    stats = process_batch_parallel(
        spark,
        curated_items_path=CURATED_ITEMS_WITH_KESKO,
        updated_gtins=updated_gtins,
        limit=None,
        site_url=SITE_URL,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scope=SCOPE,
        graph_base=GRAPH_BASE,
        hostname=HOSTNAME,
        target_library_name=TARGET_LIBRARY_NAME,
        target_subfolder=TARGET_SUBFOLDER,
        ean_display_name="EAN",
        gpc1_display_name="GS1-kategoria1",
        gpc2_display_name="GS1-kategoria2",
        brand_display_name="BRAND",
        kesko1_display_name="Kesko-kategoria1",
        kesko2_display_name="Kesko-kategoria2",
        kesko3_display_name="Kesko-kategoria3",
        product_display_name="Tuote",
        max_workers=12,
        image_timeout_sec=12,
        graph_timeout_sec=25,
        progress_every=200,
    )

    print(f"Päivitettyjä tuotetietoja (GTIN): {len(updated_gtins)} kpl")
    print(f"Kuvakäsittelyn rivimäärä (seen):   {stats['seen']} kpl")
    print(f"Onnistuneet kuvalataukset (ok):    {stats['ok']} kpl")
