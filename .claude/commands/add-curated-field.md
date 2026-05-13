---
description: Lisää uusi kenttä GS1 Synkka raw_jsonista Curated-pipelineen
---

# /add-curated-field — lisää uusi kenttä GS1-pipelineen

Käytä tätä kun käyttäjä haluaa rikastaa Curated/SQL-tauluja uudella GS1 Synkasta tulevalla
kentällä (esim. allergeenit, säilyvyysajat, myyntierämitat). Idea: data on jo Silver-tason
`raw_json`:ssa — uusia API-kutsuja ei tarvita.

**HUOM:** tämä komento ei korvaa ajattelua. Eri kentät vaativat eri lähestymistapoja
(katso Vaihe 0 alta). Käy vaiheet järjestyksessä ja kirjaa havainnot Tutkimuspäiväkirjaan.

## Vaihe 0 — selvitä kentän rakenteellinen scope ENNEN parse_one-muokkausta

Älä oleta että uusi kenttä on "sama rivi, uusi sarake". GS1:ssä data voi olla:

- **A) Samassa TradeItem-rivissä jonka jo käsittelet** → puhdas `parse_one()`-lisäys, pieni muutos.
- **B) Eri TradeItem-rivissä (oma GTIN)** kuten `CASE`/`PALLET` myyntierä/lava. Curated-Delta deduppaa GTIN:llä, joten näistä syntyy omia rivejä — kenttä ei "ilmesty" BASE_UNIT-riville ilman joinia. Mahdolliset polut:
  - B1: Hyväksy oma rivi per GTIN. SQL-kuluttaja filtteröi `TradeItemUnitDescriptor`-sarakkeella.
  - B2: Liitä `NextLowerLevelTradeItemInformation` / `TradeItemHierarchyModule` -viittauksen kautta BASE_UNIT-riville uusina sarakkeina (`Case_Depth_mm`, jne.). Vaatii uuden Spark-vaiheen `curate_items.py`:hen tai erillisen modulin.
- **C) Nested list samassa rivissä** (esim. allergeenit, ravintoarvot) → tarvitsee valintastrategian (FI ensin? max? collect_set?). Älä yliyleistä yhden kentän perusteella.

**Pyydä käyttäjältä päätös B1 vs. B2 ennen koodin koskemista**, jos kenttä on tyyppiä B. Tämä muuttaa featuren scope:a merkittävästi.

## Vaihe 1 — selvitä mistä JSON-polusta kenttä löytyy

Aja näitä Silver-sondi-soluja (suoraan notebookkiin, ei erillistä py-tiedostoa). **Muista storage-avain ensin** (ks. Sudenkuopat).

```python
from pyspark.sql import functions as F
import json
from src.config import DELTA_PRODUCTS

silver = spark.read.format("delta").load(DELTA_PRODUCTS)

# a) UnitDescriptor-jakauma → kertoo onko data BASE_UNIT/CASE/PALLET-tasolla
(silver.select(F.get_json_object("raw_json",
        "$.TradeItem.TradeItemUnitDescriptorCode.Value").alias("u"))
 .groupBy("u").count().orderBy(F.desc("count")).show(truncate=False))

# b) Sample-rivin TradeItem-tason avaimet (rakenteen kartoitus)
row = silver.select("raw_json").limit(1).collect()[0]
ti = json.loads(row["raw_json"]).get("TradeItem", {})
print(sorted(ti.keys()))

# c) Pretty-print sample-rivi koko raw_json (rajaa tarvittavaan moduliin)
print(json.dumps(json.loads(row["raw_json"]), indent=2, ensure_ascii=False)[:8000])
```

Yleisiä löytöpaikkoja (tarkista aina sample-rivistä, älä luota muistiin):
- Mitat/paino: `TradeItem.TradeItemMeasurementsModule.TradeItemMeasurements.{Depth,Width,Height}.Value`, `...TradeItemWeight.GrossWeight.Value`
- Pakkaushierarkia: `TradeItem.NextLowerLevelTradeItemInformation`, `TradeItemHierarchyModule`, `PackagingInformationModule`
- Allergeenit/ravinto: `TradeItem.FoodAndBeverageIngredientModule`, `NutritionalInformationModule`
- Päiväykset: `TradeItem.TradeItemLifespanModule`, `TradeItemSynchronisationDates`
- Kieliriippuvat listat (FI): `curate_items.py`:n `first_fi_value`-apuri

## Vaihe 2 — muokkaa `src/curate_items.py`

**Tyyppi A (yksi rivi, uusi sarake):**
- `parse_one()`:hen uusi lukukohta `get(...)`-apurilla.
- `StructType`-skeemaan vastaava `StructField`. Pidä järjestys yhtenäisenä.
- Numeerinen arvo → `str(x) if x is not None else None` (Curated on kaikki `StringType`).

**Tyyppi B2 (join eri TradeItem-rivistä):**
- Tarvitset joko erillisen funktion `curate_items.py`:hen joka rakentaa lookup-mapin CASE→BASE_UNIT (`NextLowerLevelTradeItemInformation` kertoo lower-level GTIN:n), tai erillisen Spark-joinin Curated-Delta-vaiheen perään.
- Älä yritä ratkaista B2:ta `parse_one()`:n sisällä — se käsittelee yhden rivin kerrallaan.

**Tyyppi C (nested list):**
- Valintastrategia näkyväksi. Älä piilota sitä funktion sisään ilman kommenttia.

## Vaihe 3 — aja Curated uudelleen ja varmista

```python
from src.curate_items import kuratoi_ja_talleta_deltaan_like_batch
from src.config import DELTA_PRODUCTS, CURATED_ITEMS
kuratoi_ja_talleta_deltaan_like_batch(spark, DELTA_PRODUCTS, CURATED_ITEMS, write_mode="overwrite")

# Sanity: uusi sarake EI saa olla NULL kaikilla
(spark.read.format("delta").load(CURATED_ITEMS)
 .select("GTIN", "TradeItemUnitDescriptor", "UusiKentta")
 .where("UusiKentta IS NOT NULL").show(10, truncate=False))
```

## Vaihe 4 — Azure SQL -skeema (⚠ TODENNÄKÖINEN "miksei tämä toimi" -hetki)

[utils/azuresqlserver.py](../../utils/azuresqlserver.py)`:n write_overwrite(..., truncate=True)` käyttää JDBC-optiota `truncate=true`, joka **säilyttää nykyisen taulun skeeman**. Uusi sarake Curated-Deltassa **ei automaattisesti** päädy `dbo.Test_Curated_Items2`-tauluun. Vaihtoehdot:

1. **Aja kerran SQL:ssä:** `ALTER TABLE dbo.Test_Curated_Items2 ADD UusiKentta NVARCHAR(...)` ja säilytä `truncate=True`. Suositeltava jos downstream-järjestelmiä (raportit, Power BI) on jo kytketty.
2. **Vaihda `truncate=False`** → Spark dropaa ja luo taulun uudelleen. Yksinkertaisempi, mutta downstream-yhteydet (FK:t, viewit, indeksit) menevät.

Sama koskee `dbo.product_data_with_kesko`-taulua jos kenttä virtaa Kesko-rikastuksen läpi.

## Vaihe 5 — päivitä [CLAUDE.md](../../CLAUDE.md)

Kohta "3. Data-skeemat" → Curated/Gold-sarakelistat. Lisää uusi sarake nimettynä ja kuvattuna.

## Vaihe 6 — Tutkimuspäiväkirja

Kirjaa tähän alle **rehellisesti** jokaisen featuren tutkimisen jäljet. Älä kaunistele jälkikäteen. Tämä on slash-commandin paras oppimispaikka tuleville lisäyksille.

Per feature: päivämäärä, mitä Silver-soluja ajettiin (SELECT/JSON-polut talteen), mitkä polut olivat umpikujia, missä kohtaa Claude joutui kysymään käyttäjältä (= näihin tarvitaan ohje/oletus), mitä virheitä tuli ensimmäisellä `parse_one()`-yrityksellä, tuliko SQL-skeemaongelmia.

### 2026-05-13 — myyntierämitat (CASE-tason mitat)
- **Silver-jakauma `TradeItemUnitDescriptorCode.Value`**: BASE_UNIT_OR_EACH 59 829, CASE 49 916, PALLET 37 324 (yht. 147 069 riviä).
- **Löytö 1:** myyntierä = oma TradeItem-rivi omalla GTIN:llä, ei upotettu BASE_UNIT-riviin → **tyyppi B**, ei pelkkä `parse_one()`-lisäys.
- **Löytö 2 (käytännössä ratkaiseva):** Curated-Deltassa CASE 49 392, PALLET 36 990, BASE_UNIT 59 100 — **myyntierärivit ovat siellä jo nyt** ja `parse_one()` poimii niiden mitat samasta polusta `TradeItemMeasurementsModule.TradeItemMeasurements.{Depth,Width,Height}.Value` ja `...TradeItemWeight.GrossWeight.Value`. Eli "puuttuva feature" oli **olemassa mutta löytämätön** — käyttäjä ei välttämättä tiennyt että SQL-taulussa rivit jo ovat.
- **`NextLowerLevelTradeItemInformation`-rakenne:** `ChildTradeItem[]` (lista), kullakin `Gtin` (lower-level GTIN, BASE_UNIT) ja `QuantityOfNextLowerLevelTradeItem` (montako kpl per myyntierä). Yläkentät: `QuantityOfChildren`, `TotalQuantityOfNextLowerLevelTradeItem`. → Käyttökelpoiset sarakkeet jos B2 valitaan: `UnitsPerCase` (`TotalQuantityOfNextLowerLevelTradeItem`), `ChildBaseUnitGTIN` (`ChildTradeItem[0].Gtin`).
- **Edge case (ei vielä todennettu samplella):** voiko yksi BASE_UNIT-GTIN olla useassa CASE:ssa (esim. eri pakkauskoot)? Jos kyllä, B2-join 1:N → täytyy valita strategia (ensimmäinen / lista / aggregaatti).
- **Sudenkuopat tähän mennessä:**
  - Tutkimussolu ei toiminut ilman storage-avaimen asetusta (ks. Sudenkuopat).
  - Curated-Delta ei filtteröi `TradeItemUnitDescriptor`-arvolla → kaikki tasot säilyvät. Mutta `delta_images.py` filtteröi BASE_UNIT-rivit kuvapipelineen.
  - Käyttäjän alkuoletus ("myyntieräkohtaisia mittoja ei ole tietokannassa") osoittautui vääräksi: SQL-taulu sisälsi jo CASE 49 392 ja PALLET 36 990 -rivit. → Ennen koodin koskemista pitää **aina** tarkistaa onko data jo Curatedissa/SQL:ssä.
  - **Terminologia ei vastaa GS1-koodeja:** käyttäjän "myyntierä" ≠ GS1-koodi `CASE` välttämättä. `CASE` Synkassa sisältää sekä kuluttajalle myytäviä monipakkauksia (juomien 6-pack) että tukkutoimituseriä (jauhojen laatikko). Erottelu tehdään `IsTradeItemAConsumerUnit` / `IsTradeItemADespatchUnit` -boolean-kenttien avulla, ei UnitDescriptor-koodilla. PackagingInformationModule.PackagingTypeCode voi antaa lisätarkennusta (TRAY/BOX/CASE).
  - **1:N -tilanne**: 5 205 BASE_UNIT-GTINiä (~8.8 %) esiintyy useassa CASE:ssa (eri pakkauskoot). Min/max-strategia tai lista pakollinen — ei voi olettaa 1:1.
  - **Sekoituspakkaukset**: 580 CASE:ssa eri BASE_UNIT-GTINejä. Pieni edge case mutta huomioitava.
  - **Boolean-jakauma vahvistaa terminologian:** ~96 % CASE-riveistä on `IsDespatchUnit=true, IsConsumerUnit=false` (varsinainen tukkutoimituserä = käyttäjän "myyntierä"). ~1 800 CASE-riviä on `IsConsumerUnit=true` (juomien 6-packit yms.). → join-suodatin pitäisi olla `TradeItemUnitDescriptor='CASE' AND IsTradeItemADespatchUnit=true`, ei pelkkä `CASE`. Tämä eliminoi häiriön kuluttajamonipakkauksista.
  - **PackagingTypeCode** käyttää UN/ECE Rec 21 -koodeja (`CS`=Case, `AA`=Intermediate bulk container) — fyysinen pakkaustyyppi, ei business-rooli. Älä luota tähän roolin tunnistuksessa, käytä boolean-kenttiä.
  - **Käyttäjän hypoteesi "myyntierämitat ovat BASE_UNIT-jsonissa" osoittautui datalla vääräksi.** Tutkittiin 3 BASE_UNIT-tuotetta (juoma, lihajaloste, jauhopussi) ja kaikki TradeItem-tason moduulit (`SalesInformationModule`, `VariableTradeItemInformationModule`, `TradeItemMeasurementsModule`, `PackagingInformationModule`). Yksikään ei sisällä myyntierämittoja. **GS1 GDSN -arkkitehtuuri on yksisuuntainen ylhäältä alas**: CASE-rivi linkkaa BASE_UNIT:iin `NextLowerLevelTradeItemInformation.ChildTradeItem[].Gtin`-kautta, mutta BASE_UNIT-rivissä ei ole vastaavaa ylätason linkkiä eikä myyntierämittoja. Alkuperäinen CASE-rikastus-suunnitelma oli oikea — vain cache-bugi (alla) esti sen toimimisen. **Oppi:** kun käyttäjä esittää oletuksen rakenteesta, vahvista se datalla pretty-print-solulla ennen koodimuutoksia, mutta älä luovu omasta hypoteesista jos data sen kumoaa.
  - **Cache-bugi (Python sys.modules):** `inspect.getsource(module)` lukee tiedostosta ja näyttää uutta koodia, mutta **runtime-funktioobjekti pysyy modulin ensilatauksen versiona** kunnes notebook-istunto käynnistetään uudelleen. Diagnostiikka voi siis valehdella. Aina kun `*.py`-tiedostoa muutetaan ja Pull tehdään Reposissa, **`dbutils.library.restartPython()` on PAKOLLINEN ennen uudelleenajoa**, tai vanha funktio kirjoittaa Curated-Deltan vanhalla skeemalla ja uudet sarakkeet ovat NULL. Tämä oli session suurin aikasyöppö.
- **Lopullinen tulos (toteutuksen jälkeen):** 41 121 BASE_UNIT-tuotetta (~70 % kaikista BASE_UNITeista) sai myyntierämitat. Loput ~18 000 ovat tuotteita joiden CASE-emolla `IsDespatchUnit≠true` tai joiden toimittaja ei ole julkaissut CASE-tason GTINiä Synkkaan.

## Sudenkuopat ja toistuvat kysymykset

### Storage-avain pitää asettaa ennen ABFSS-lukua
Tutkimussolut Silveriin epäonnistuvat virheellä `Invalid configuration value detected for fs.azure.account.key`, jos Spark-konffiin ei ole asetettu storage-avainta. Tuotantopipelinessä `_setup_spark_and_storage()` ([src/pipelines.py](../../src/pipelines.py)) hoitaa tämän, mutta tutkimusnotebookissa pitää tehdä sama käsin **ennen** ensimmäistä `spark.read.format("delta").load(...)`-kutsua:

```python
from src.config import ACCOUNT
access_key = dbutils.secrets.get("gs1-kv", "storage-access-key")
spark.conf.set(f"fs.azure.account.key.{ACCOUNT}.dfs.core.windows.net", access_key)
```

### Silver sisältää useita tasoja (BASE_UNIT, CASE, PALLET) — älä oleta yhtä riviä per tuote
Curated deduppaa GTIN:llä, joten eri tasot säilyvät omina riveinään. Tarkista `TradeItemUnitDescriptorCode.Value`-jakauma aina ensimmäisessä sondisolussa. Tämä ratkaisee onko kysymyksessä tyyppi A (sama rivi) vai tyyppi B (eri rivi → join). Katso Vaihe 0.

### Käyttäjän terminologia ei välttämättä vastaa GS1-koodeja
Käyttäjä puhuu liiketoiminta-termein ("myyntierä", "varastoyksikkö", "tilausyksikkö"), mutta `TradeItemUnitDescriptorCode.Value` antaa vain teknisiä koodeja (`BASE_UNIT_OR_EACH`, `CASE`, `PALLET`, jne.). Sama koodi voi sisältää eri liiketoimintatarkoituksia. Tarkista:
- `IsTradeItemAConsumerUnit`, `IsTradeItemADespatchUnit`, `IsTradeItemAnOrderableUnit`, `IsTradeItemAnInvoiceUnit` — boolean-kentät kertovat mihin rooliin rivi kuuluu
- `PackagingInformationModule.PackagingTypeCode` — fyysinen pakkaustyyppi (TRAY, BOX, BOTTLE, …)

Kysy käyttäjältä **mihin rooliin** kenttä liittyy, älä oleta UnitDescriptor-koodi → liiketoiminta-termi -vastaavuutta.

### Tarkista AINA ensin onko data jo Curated/SQL:ssä
Ennen Silver-tutkimusta ja parse_one-muokkausta: aja `spark.read.format("delta").load(CURATED_ITEMS).columns` ja katso onko etsitty kenttä jo siellä. Tarkista myös SQL-taulun jakauma `TradeItemUnitDescriptor`-sarakkeella. Saattaa olla että feature on jo olemassa eikä koodimuutosta tarvita — vain dokumentointi.

### Python sys.modules cache pitää vanhaa funktioobjektia
**Tämä on session suurin sudenkuoppa Databricks-notebookeissa.** Kun `*.py`-tiedosto muutetaan ja Git-pull tehdään Repos-näkymästä:
- `inspect.getsource(module)` lukee tiedostosta → näyttää **uutta** koodia
- `module.__file__` osoittaa **uuteen** tiedostoon
- MUTTA `module.funktio` on **vanha funktio-objekti**, luotu modulin ensilatauksen yhteydessä — Python ei reload:aa importteja kun tiedosto muuttuu

Oire: pipeline ajaa onnistuneesti, kirjoittaa Curated-Deltaan, mutta uudet sarakkeet/kentät ovat NULL kaikilla riveillä. `groupBy("UusiKentta")` toimii (sarake on Deltassa) mutta arvot puuttuvat.

**Korjaus:** `dbutils.library.restartPython()` omassa solussaan ennen kuin ajat funktion joka on muuttunut. Tämä nollaa kernelin → seuraavat importit lataavat moduulit tuoreina. Pidä tämä mielessä **JOKA KERTA** kun teet `git pull` Databricks Repos -näkymässä `*.py`-muutosten jälkeen.

### Uudet tiedostot/hakemistot eivät automaattisesti näy Databricksissa
Paikallisten editor-muutosten ja Databricks-ajon välissä on **Git-vaihe**: cluster lukee koodin Databricks Repos -integraation kautta GitHubista. Aina kun lisätään uusi moduli tai hakemisto:

1. `git add` + `git commit` + `git push origin main` paikallisesti.
2. Databricks-UI:ssa Repos-näkymästä **Pull**.
3. Vasta sen jälkeen `from src.uusi_moduli import ...` toimii notebook-solussa.

Oire: `ModuleNotFoundError: No module named 'src.xxx'` vaikka tiedosto on olemassa paikallisesti. Muista mainita käyttäjälle tämä **ennen** kuin pyydät häntä ajamaan importteja sisältäviä soluja.

### `write_overwrite(truncate=True)` ei lisää uusia sarakkeita SQL-tauluun
Katso Vaihe 4. Tämä on ensimmäinen "miksei kenttä näy SQL:ssä, vaikka Curated-Deltassa se on" -hetki. Päätä ALTER TABLE vs. `truncate=False` käyttäjän kanssa.

<!-- Lisää uusia oppeja tähän kun törmätään niihin -->
