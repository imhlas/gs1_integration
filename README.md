# GS1-tuotetietojen lataus GS1 Synkka → Delta → SQL → SharePoint / Kesko

Tämän repositorion tarkoitus on:

- hakea GS1 Synkka -rajapinnasta vähittäiskaupan tuotetietoja

- tallettaa data vaiheittain Azure Databricks / Delta Lakeen (Bronze → Silver → Curated)

- viedä kuratoidut tuotetiedot Azure SQL -tietokantaan

- päivittää tuotetietoihin Keskon kategoria

- ladata tuotekuvat urlien avulla Sharepointiin

## 1. Kokonaiskuva datavirrasta

###  GS1 Synkka -rajapinta

- haetaan tuotteiden avaimet (Id:t)

- haetaan tuotteiden täydet JSON-kuvaukset /Many-endpointilla

### Bronze-taso (Delta)

- tallennetaan vain avaimet: Id + load_time

### Silver-taso (Delta)

- tallennetaan tuotteiden raakadata:

  - Id

  - raw_json (koko GS1:n item JSON)

  - ingest_ts (milloin haettu)

### Curated-taso (Delta)

- parsitaan raw_json ja poimitaan vain tärkeimmät kentät, esim:

  - BrandName, TradeItemDescription_fi (suomenkielinen nimi)

  - GPC-koodit (segment, perhe, luokka)

  - GTIN, Id

  - mitat (korkeus, leveys, syvyys, paino)

  - PrimaryImageUrl, SecondaryImageUrl ja kuvien metat

  - tallennetaan selkeään tauluun CURATED_ITEMS

### Azure SQL -taulu

- Curated-Delta luetaan Sparkilla

- viedään Azure SQL -tauluun dbo.Test_Curated_Items append-tilassa

## 2.  Ajo: GS1 → Delta → SQL (run_all-notebook)

### 4.1. Ajotavan valinta (full vs changes)

Vaihtoehdot:

- "full"

  -hakee koko GS1-katalogin

  - kirjoittaa Bronzeen ja Silveriin koko setin (overwrite)

- "changes"

  - hakee vain muuttuneet tuotteet tietystä ajankohdasta (SINCE_ISO src/config.py:ssä)

  - käyttää GS1:n /PublicCatalogueItemSync/Changes -endpointia

## 3. Moduulit (mistä mikäkin vastaa)
**src/config.py**

- Kaikki tärkeät polut ja asetukset yhdessä paikassa:

  - Delta-polut (Bronze, Silver, Curated, Gold)

  - GS1-ajon asetukset (RUN_MODE, SINCE_ISO, ONLY_GPC_SEGMENT_CODE, RATE_LIMIT_PER_MIN)

  - SQL-taulujen nimet (SQL_TABLE_CURATED_ITEMS, SQL_TABLE_KESKO_CATEGORIES)

  - lippu SPARK_DELTA_AUTOMERGE

**src/connection.py**

- Hoitaa yhteyden GS1 API:in:

  - autentikointi (authenticate)

  - kutsujen teko (call)

**src/endpoints.py**

- Selkeästi nimetyt funktiot GS1-pään rajapinnoille:

  ```list_keys_all```

  ```list_keys_changes```

  ```items_many```

  ```next_offset```

**src/loaders.py**

- Varsinainen “latauslogiikka” GS1 → Bronze/Silver:

```fetch_all_keys_to_bronze```

```read_ids_from_bronze```

```fetch_items_to_silver_json``` (mukaan lukien GPC-segmenttisuodatus)

**src/curate_items.py**

- Muuttaa Silverin raw_json-datan selkeäksi taulukkomuodoksi:

  - poimii brändit, nimet, koodit, mitat, GTIN, Id

  - etsii ja pisteyttää kuva-URLit

  - päättää, mikä kuva on PrimaryImageUrl (ja mahdollinen SecondaryImageUrl)

  - kirjoittaa Curated-Deltaan

**src/enrich_kesko_categories**

- Rikastaa tuotteet Kesko-kategorioilla EAN/GTIN:n perusteella.

**src/fetch_images**

- Lataa kuvat ja vie ne SharePoint-kirjastoon

**utils/azuresqlserver.py**

- Yksinkertaiset apufunktiot Azure SQL -yhteyksiin Sparkilla:

  - write_append → lisää rivejä tauluun

  - write_overwrite → tyhjentää ja täyttää taulun

  - read_table → hakee dataa SQL:stä Spark/Pandas-muotoon

**utils/gpc_utils.py**

GPC-koodien ja -nimien koonti Silveristä.

Tulostaa yhteenvedon:

uniikit GpcCategoryCode-arvot

“paras” nimi per koodi (painottaen ääkkösiä sisältäviä nimiä)
