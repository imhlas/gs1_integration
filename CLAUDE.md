# CLAUDE.md

## 1. Projektin tarkoitus

Tämä repositorio toteuttaa data-pipelinen, joka hakee tuotetietoja GS1 Synkka -rajapinnasta,
tallettaa ne vaiheittain Azure Databricksin Delta Lakeen (Bronze → Silver → Curated → Gold),
rikastaa Kesko-kategoriahierarkialla, kirjoittaa lopputuloksen Azure SQL -tauluun ja vie
tuotekuvat SharePoint-kirjastoon Microsoft Graphin kautta. Pipeline tukee sekä koko katalogin
täysajoa (`full`) että muuttuneiden tuotteiden inkrementaalista ajoa (`changes`).

## 2. Arkkitehtuuri & moduulit

Päätason orkestrointi tapahtuu Databricks-notebookista [run_all.ipynb](run_all.ipynb), joka
kutsuu `src/`-paketin pipeline-funktioita.

**Konfiguraatio ja yhteydet**
- [src/config.py](src/config.py) — kaikki polut (ABFSS Delta-URI:t), ajoasetukset (`RUN_MODE`, `SINCE_ISO`, `ONLY_GPC_SEGMENT_CODE`, `RATE_LIMIT_PER_MIN`), SQL-taulujen nimet ja SharePoint/Graph-asetukset. Ei salaisuuksia — ne haetaan Key Vaultista.
- [src/connection.py](src/connection.py) — `APIClient` GS1-rajapinnalle: autentikointi (`/Account/Token`) ja yleiskäyttöinen `call()`-metodi tokenilla varustettuun sessioniin.
- [src/endpoints.py](src/endpoints.py) — ohuet wrapperit GS1-endpointeille: `list_keys_all`, `list_keys_changes`, `items_many`, `next_offset` (käsittelee NextOffset/NextofSet/NextOfSet-kirjoitusasut).

**Lataus- ja kuratointivaihe**
- [src/loaders.py](src/loaders.py) — `fetch_all_keys_to_bronze` sivuttaa kaikki Id:t Bronzeen; `fetch_items_to_silver_json` hakee `/Many`-endpointilla (max 1000/erä) ja kirjoittaa `Id + raw_json + ingest_ts` Silveriin. Tukee GPC-segmenttisuodatusta sekä rate-limittiä (oletus 15 kutsua/min).
- [src/curate_items.py](src/curate_items.py) — `kuratoi_ja_talleta_deltaan_like_batch` parsii Silverin `raw_json`:n, poimii nimetyt kentät, pisteyttää kuva-URLit (`SUFFIX_PRIORITY` × `EXT_PRIORITY`), valitsee Primary/Secondary-kuvan ja deduplikoi GTIN:llä (uusin `StartAvailabilityDateTime` voittaa).
- [src/pipelines.py](src/pipelines.py) — orkestrointi: `run_full_pipeline`, `run_changes_pipeline`, `run_changes_pipeline_with_images`. Kovakoodattu `BASE = "https://productapi-synkka.gs1.fi"`, `KEY_BATCH_SIZE = 90000`, `ITEM_BATCH = 1000`.
- [src/run_full_pipeline_with_images.py](src/run_full_pipeline_with_images.py) — ylätason "täysajo + kuvat": ajaa `run_full_pipeline`, tyhjentää SharePoint-kirjaston ja lataa kaikki kuvat. Mittaa kestot ja lähettää webhookin.

**Rikastus ja sivulataukset**
- [src/add_kesko_hierarchy_levels/enrich_kesko_categories.py](src/add_kesko_hierarchy_levels/enrich_kesko_categories.py) — joinaa Curated-tuotteet `dbo.KESKO_00_PRODUCT_HIERARCHY_LEVELS`-tauluun GTIN:llä; fallback: GTIN ilman etunollia. Suodattaa pois Kesko-tason `EXCLUDE_L2` ja `EXCLUDE_L3_BY_L2` -kategoriat ennen joinia. Kirjoittaa Gold-tason `CURATED_ITEMS_WITH_KESKO`-poluun.
- [src/fetch_images/](src/fetch_images/) — kuvapipeline:
  - [image_extractor.py](src/fetch_images/image_extractor.py) — `ImageExtractor.fetch()` hakee kuvan ja päättelee tiedostopäätteen Content-Type/URL-päätteestä.
  - [delta_images.py](src/fetch_images/delta_images.py) — streamaa kuvarivit Curated/Gold-Deltasta (`BASE_UNIT_OR_EACH`, ei tyhjä URL, ei tyhjä L2-kategoria); `get_image_rows_iter` käyttää `toLocalIterator()`-streamia.
  - [sharepoint_upload.py](src/fetch_images/sharepoint_upload.py) — `TokenManager` (MSAL client credentials + auto-refresh), `GraphClient` (retry 429/5xx, 401 → force-refresh), `process_batch_parallel` (`ThreadPoolExecutor`, `max_workers=12`). PUT kuvan polkuun → PATCH metatiedot (EAN, GS1/Kesko-kategoriat, BRAND, Tuote).
  - [clean_sharepoint_library.py](src/fetch_images/clean_sharepoint_library.py) — `wipe_library` tyhjentää kirjaston (tai alikansion) Graphin DELETE-kutsuilla.

**Apukirjastot**
- [utils/azuresqlserver.py](utils/azuresqlserver.py) — JDBC-pohjaiset `write_append`, `write_overwrite` (TRUNCATE-tilassa) ja `read_table` (sarake/TOP/ORDER-tuella). Yhteystiedot Key Vaultista (`gs1-kv`).
- [utils/gpcutils.py](utils/gpcutils.py) — diagnostiikkatyökalu: kerää uniikit `GpcCategoryCode`-arvot Silveristä ja valitsee "parhaan" nimen (suosii ääkkösellisiä, sitten pisintä).
- [utils/webhook.py](utils/webhook.py) — POST yhteenveto-payload Azure Logic App -webhookiin (URL kovakoodattu).

## 3. Data-skeemat

**GS1 Synkka -lähde** — `/PublicCatalogueItemSync/All` ja `/Changes` palauttavat Id-listan ja `NextOffset`-sivutuksen; `/PublicCatalogueItem/Many` palauttaa täydet item-JSONit (max 1000 ID/kutsu). Kriittiset polut item-JSONista:
- `TradeItem.TradeItemDescriptionModule.TradeItemDescriptionInformation.BrandNameInformation.BrandName`
- `TradeItem.TradeItemDescriptionModule.TradeItemDescriptionInformation.TradeItemDescription` (kielikohtainen lista, suositaan `LanguageCode == "fi"`)
- `TradeItem.GdsnTradeItemClassification.{GpcCategoryCode, GpcCategoryName, GpcSegmentCode, GpcFamilyCode, GpcClassCode}`
- `TradeItem.TradeItemMeasurementsModule.TradeItemMeasurements.{Depth, Width, Height}.Value` ja `TradeItemWeight.GrossWeight.Value`
- `TradeItem.TradeItemUnitDescriptorCode.Value` (suodatetaan `BASE_UNIT_OR_EACH` kuvapipelinessä)
- `TradeItem.Gtin`, `CatalogueItemInfo.{Id, LastUpdatedDateTime, Deleted}`
- `TradeItem.DeliveryPurchasingInformationModule.DeliveryPurchasingInformation.StartAvailabilityDateTime`
- Kuva-URLit: poimitaan rekursiivisesti `URL_KEYS` / `FILENAME_KEYS` / `MEDIA_ID_KEYS` -avaimista; pisteytys `_XXXX.<ext>`-suffiksin (esim. `C1C1`, `C1N1`) ja tiedostopäätteen mukaan.

**Delta-tasot**
- **Bronze** `gs1/bronze/public_item_sync` — `Id`, `load_time`.
- **Silver** `gs1/silver/catalogue_items` — `Id`, `raw_json` (alkuperäinen JSON-merkkijono), `ingest_ts`.
- **Curated** `gs1/curated/items_selected_fields` — litistetty taulukko: `StartAvailabilityDateTime`, `BrandName`, `TradeItemDescription_fi`, `GpcCategoryCode/Name`, `GpcSegmentCode/FamilyCode/ClassCode`, `Depth_mm/Width_mm/Height_mm/GrossWeight_g` (kaikki StringType), `TradeItemUnitDescriptor`, `InfoProviderName`, `GTIN`, `Id`, `LastUpdatedDateTime`, `Deleted` (Boolean), `PrimaryImageUrl/FileName/MediaItemId`, `SecondaryImageUrl`, `Lejos_UpdatedAt`. Deduplikoitu GTIN:llä.
- **Gold** `gs1/gold/items_with_kesko_levels` — Curated + `PRODUCT_HIERARCHY_LEVEL_2/3/4` + `GTIN_NO_LEADING_ZEROS`.

**Azure SQL -kohteet**
- `dbo.Test_Curated_Items2` (`SQL_TABLE_CURATED_ITEMS`) — Curated-tason snapshot (overwrite + truncate).
- `dbo.product_data_with_kesko` (`SQL_TABLE_KESKO_CATEGORIES`) — Gold-tason snapshot (overwrite + truncate).
- `dbo.KESKO_00_PRODUCT_HIERARCHY_LEVELS` (luetaan) — Keskon hierarkia, sarakkeet `GTIN`, `PRODUCT_HIERARCHY_LEVEL_2/3/4`.

## 4. Ajoympäristö

- **Alusta**: Azure Databricks; `spark` ja `dbutils` injektoidaan notebook-runtimesta.
- **Storage**: Azure Data Lake Storage Gen2 (`abfss://datalake@gs1datalake.dfs.core.windows.net/...`). Storage-avain haetaan `dbutils.secrets.get("gs1-kv", "storage-access-key")` ja asetetaan `spark.conf`-asetukseen.
- **Salaisuudet**: kaikki Databricks-secret scope `gs1-kv` (Azure Key Vault -backed): `email`, `password`, `gln`, `azuresql-server/database/username/password`, `sharepoint-site-url/client-id/client-secret`, `storage-access-key`.
- **Riippuvuudet**: PySpark + Delta Lake, `requests`, `pandas`, `msal` (Microsoft Graph -token), `mimetypes` (kuvapääte). Ei `requirements.txt` — paketit oletetaan Databricks-clusterissa.
- **Spark-asetukset**: `spark.databricks.delta.schema.autoMerge.enabled = true` (lipun `SPARK_DELTA_AUTOMERGE` perusteella).
- **Webhook**: Azure Logic App -URL kovakoodattu `utils/webhook.py`:ssa.

## 5. Konventiot

- **Kieli**: tunnisteet ja kommentit suomeksi; logiviestit suomenkielisiä, sisältävät emojeja (✅, 🔒, 📤, ⚠️). Funktioiden nimet englanniksi paitsi `kuratoi_ja_talleta_deltaan_like_batch`.
- **Tiedostotyyli**: kommenttirivi `# src/...` jokaisen moduulin alussa (joskus typolla, esim. `# scr/curate_items.py`).
- **Konfiguraatio**: importataan koko config wildcardilla — `from src.config import *`. Kaikki polut muodostetaan `ABFSS(path)`-funktiolla.
- **Spark I/O**: kirjoitukset Delta-muodossa, ensikirjoitus `overwrite` + `overwriteSchema=true`, jatkokirjoitukset `append` + `mergeSchema=true`. SQL-overwrite käyttää `truncate=true`-optiota säilyttäen skeeman.
- **Datatyypit Curatedissa**: kaikki mitat (`Depth_mm`, jne.) talletetaan `StringType`-muodossa varmistaen yhteensopivuus.
- **Rate-limit**: `time.sleep(60 / RATE_LIMIT_PER_MIN * 1.05)` `/Many`-kutsujen välissä; `time.sleep(1.2–1.5 s)` avain-sivutuksen välissä.
- **Idempotenssi kuvissa**: SharePoint-tiedoston nimi on aina `{GTIN}.{ext}` → PUT ylikirjoittaa duplikaatit. Metatiedot päivitetään PATCH-kutsulla `displayName`-pohjaiseen sarakemappiin.
- **Rinnakkaisuus**: kuva-upload `ThreadPoolExecutor`, `max_workers=12`; `TokenManager` thread-safe lockilla; `GraphClient`-retry 429/5xx (exp. backoff) ja 401-tokenin force-refresh.
- **Virheenkäsittely**: yksittäisen rivin/kuvan epäonnistuminen ei keskeytä ajoa — pyydetään `try/except` ja kasvatetaan `fail`-laskuria.

## 6. Yleiset komennot

Repositoriossa ei ole `package.json`, `requirements.txt`, pytest-konfiguraatiota tai CI:tä — kaikki ajetaan Databricksissä notebookista.

**Täysajo (kaikki tuotteet + kuvat + Kesko-rikastus + webhook)** — solu [run_all.ipynb](run_all.ipynb):
```python
from src.run_full_pipeline_with_images import run_full_pipeline_with_images
run_full_pipeline_with_images(spark, dbutils)
```

**Inkrementaalinen ajo (vain muuttuneet + kuvat niille)**:
```python
from src.pipelines import run_changes_pipeline_with_images
from src.config import SINCE_ISO
run_changes_pipeline_with_images(spark, dbutils, since_iso=SINCE_ISO)
```

**Pelkkä tuotepipeline ilman kuvia**:
```python
from src.pipelines import run_full_pipeline
run_full_pipeline(spark, dbutils)
```

**Diagnostiikka — uniikit GPC-koodit Silveristä**:
```python
from utils.gpcutils import print_unique_gpc_codes_and_names
from src.config import DELTA_PRODUCTS
print_unique_gpc_codes_and_names(spark, DELTA_PRODUCTS)
```

**Ajotavan/suodattimen muuttaminen**: muokkaa [src/config.py](src/config.py) — `RUN_MODE`, `SINCE_ISO`, `ONLY_GPC_SEGMENT_CODE` (esim. `"50000000"` rajaa elintarvikkeisiin), `RATE_LIMIT_PER_MIN`.

**Git**: pääbranch `main`; ei pull request -workflow’ta näkyvissä — committaa suoraan kun muutos on testattu Databricksissä.
