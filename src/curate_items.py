# scr/curate_items.py

import json, re
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# --- Prioriteetit (annettuna) ---
SUFFIX_PRIORITY = {
    "C1C1": 120, "C1N1": 115, "A1C1": 110, "H1N1": 105, "A1N1": 102,
    "C1L1": 100, "C1R1": 98,  "C1N0": 96,  "A1C0": 94,  "C1C0": 92,
    "H1C1": 90,  "A1L1": 88,  "C3C1": 86,  "D1NE": 84,  "A1N0": 82,
}
EXT_PRIORITY = {"jpg": 10, "jpeg": 10, "png": 8, "webp": 6, "tif": 5, "tiff": 5, "gif": 2}

# --- Regex suffiksin poimintaan: ..._XXXX.<ext> ---
RX_SUFFIX = re.compile(r"_([A-Za-z0-9]{4})\.", re.IGNORECASE)

# --- Avainjoukot urlien/metadatan löytämiseen ---
URL_KEYS = {
    "url", "uri", "uniformresourceidentifier",
    "fileurl", "httpurl", "httpsurl",
    "referencedfileuniformresourceidentifier"
}
FILENAME_KEYS = {"filename", "originalfilename", "originalfileName", "file", "name"}
MEDIA_ID_KEYS = {"mediaitemid", "mediaid"}

def kuratoi_ja_talleta_deltaan_like_batch(
    spark,
    silver_path: str,
    curated_path: str,
    write_mode: str = "overwrite",
    sample_rows: int = 5
) -> int:
    print(">>> Vaihe: Kuratoi tuotteet ja tallenna Deltaan")

    # Lue kaikki Silver-rivit (vain raw_json)
    src_df = (spark.read.format("delta").load(silver_path)
              .select("raw_json")
              .filter("raw_json IS NOT NULL"))

    if src_df.rdd.isEmpty():
        print("Silverissä ei ole rivejä. Ei mitään kuratoitavaa.")
        return 0

    # --- Pienet apurit ---
    def get(d, *path, default=None):
        cur = d
        for k in path:
            if not isinstance(cur, dict) or k not in cur:
                return default
            cur = cur[k]
        return cur

    def first_fi_value(lst):
        """Palauta listasta (LanguageCode/Value) fi-kielinen Value, tai 1. arvo."""
        if not isinstance(lst, list):
            return None
        for it in lst:
            if isinstance(it, dict) and it.get("LanguageCode") == "fi":
                return it.get("Value")
        it = lst[0] if lst else None
        return it.get("Value") if isinstance(it, dict) else None

    def lower_ext_from(s):
        if not isinstance(s, str):
            return None
        s = s.strip().lower()
        if "." in s:
            return s.rsplit(".", 1)[-1]
        return None

    def extract_suffix(url: str) -> str:
        if not isinstance(url, str):
            return "NONE"
        m = RX_SUFFIX.search(url)
        return m.group(1).upper() if m else "NONE"

    def score_url(url: str) -> int:
        """Pisteet = suffiksi + tiedostopääte. Isompi parempi."""
        suff = extract_suffix(url)
        ext  = lower_ext_from(url) or ""
        return SUFFIX_PRIORITY.get(suff, 0) + EXT_PRIORITY.get(ext, 0)

    def find_all_image_urls_and_meta(obj):
        """
        Kerää (url, filename, media_id) rekursiivisesti.
        Palauttaa listan dict: {url, filename, media_id}
        """
        results = []

        def maybe_add(node):
            if not isinstance(node, dict):
                return
            kl = {k.lower(): k for k in node.keys()}

            # URL
            url_val = None
            for k in URL_KEYS:
                if k in kl:
                    v = node[kl[k]]
                    if isinstance(v, str) and v.strip():
                        url_val = v.strip()
                        break
            if not url_val:
                return  # ilman urlia ei pisteytetä

            # Filename
            fname_val = None
            for k in FILENAME_KEYS:
                if k in kl:
                    v = node[kl[k]]
                    if isinstance(v, str) and v.strip():
                        fname_val = v.strip()
                        break

            # Media ID
            media_val = None
            for k in MEDIA_ID_KEYS:
                if k in kl:
                    v = node[kl[k]]
                    if isinstance(v, (str, int)) and str(v).strip():
                        media_val = str(v).strip()
                        break

            results.append({"url": url_val, "filename": fname_val, "media_id": media_val})

        def walk(x):
            if isinstance(x, dict):
                maybe_add(x)
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)

        walk(obj)
        return results

    def pick_top2_urls(obj):
        """Palauta (primary_url, secondary_url, primary_filename, primary_mediaid) suffiksi/ext -pisteytyksellä."""
        items = find_all_image_urls_and_meta(obj)
        if not items:
            return None, None, None, None

        # pisteytä
        scored = []
        seen = set()
        for it in items:
            u = it["url"]
            if not u or u in seen:
                continue
            seen.add(u)
            scored.append((score_url(u), it))
        if not scored:
            return None, None, None, None

        # järjestä pisteiden mukaan, suurin ensin
        scored.sort(key=lambda t: t[0], reverse=True)
        primary = scored[0][1]
        secondary = scored[1][1] if len(scored) > 1 else None

        return primary["url"], (secondary["url"] if secondary else None), primary["filename"], primary["media_id"]

    def parse_one(raw_json_str):
        try:
            data = json.loads(raw_json_str) if isinstance(raw_json_str, str) else {}
        except Exception:
            data = {}

        ti = data.get("TradeItem") or {}
        ci = data.get("CatalogueItemInfo") or {}

        # --- StartAvailabilityDateTime ---
        start_avail = get(ti, "DeliveryPurchasingInformationModule", "DeliveryPurchasingInformation", "StartAvailabilityDateTime")

        # --- Brändi & NIMI (FI) ---
        desc_block = get(ti, "TradeItemDescriptionModule", "TradeItemDescriptionInformation", default={}) or {}
        brand = get(desc_block, "BrandNameInformation", "BrandName")
        desc_fi = first_fi_value(desc_block.get("TradeItemDescription")) \
                  or first_fi_value(desc_block.get("DescriptionShort"))

        # --- GPC ---
        gpc = get(ti, "GdsnTradeItemClassification", default={}) or {}
        gpc_cat   = gpc.get("GpcCategoryCode")
        gpc_name  = gpc.get("GpcCategoryName")
        gpc_seg   = gpc.get("GpcSegmentCode")
        gpc_fam   = gpc.get("GpcFamilyCode")
        gpc_class = gpc.get("GpcClassCode")

        # --- Mitat & paino ---
        meas = get(ti, "TradeItemMeasurementsModule", "TradeItemMeasurements", default={}) or {}
        depth  = get(meas, "Depth",  "Value")
        width  = get(meas, "Width",  "Value")
        height = get(meas, "Height", "Value")
        grossw = get(get(meas, "TradeItemWeight", default={}) or {}, "GrossWeight", "Value")

        # --- Yksikkö & toimittaja ---
        unit_descr = get(ti, "TradeItemUnitDescriptorCode", "Value")
        provider   = get(ti, "InformationProviderOfTradeItem", "PartyName") or get(ti, "InformationProvider", "Name")

        # --- Tunnisteet & status ---
        gtin   = ti.get("Gtin") or data.get("GTIN")
        _id    = ci.get("Id") or ti.get("Id") or gtin
        lastup = ci.get("LastUpdatedDateTime") or data.get("LastUpdatedDateTime") or get(ti, "TradeItemSynchronisationDates", "LastChangeDateTime")
        deleted= ci.get("Deleted") if ci.get("Deleted") is not None else data.get("Deleted")

        # --- Kuvat: suffiksi/ext -pisteytys → Primary & Secondary URL ---
        primary_url, secondary_url, primary_filename, primary_media_id = pick_top2_urls(data)

        return {
            "StartAvailabilityDateTime": start_avail,
            "BrandName":                 brand,
            "TradeItemDescription_fi":   desc_fi,

            "GpcCategoryCode":           gpc_cat,
            "GpcCategoryName":           gpc_name,

            "GpcSegmentCode":            gpc_seg,
            "GpcFamilyCode":             gpc_fam,
            "GpcClassCode":              gpc_class,

            "Depth_mm":                  str(depth)  if depth  is not None else None,
            "Width_mm":                  str(width)  if width  is not None else None,
            "Height_mm":                 str(height) if height is not None else None,
            "GrossWeight_g":             str(grossw) if grossw is not None else None,

            "TradeItemUnitDescriptor":   unit_descr,
            "InfoProviderName":          provider,

            "GTIN":                      gtin,
            "Id":                        _id,
            "LastUpdatedDateTime":       lastup,
            "Deleted":                   True if deleted is True else (False if deleted is False else None),

            "PrimaryImageUrl":           primary_url,
            "PrimaryImageFileName":      primary_filename,
            "PrimaryImageMediaItemId":   primary_media_id,

            "SecondaryImageUrl":         secondary_url,
        }

    curated_rows_rdd = src_df.rdd.map(lambda r: Row(**parse_one(r["raw_json"])))

    schema = StructType([
        StructField("StartAvailabilityDateTime", StringType(), True),
        StructField("BrandName",                 StringType(), True),
        StructField("TradeItemDescription_fi",   StringType(), True),

        StructField("GpcCategoryCode",           StringType(), True),
        StructField("GpcCategoryName",           StringType(), True),

        StructField("GpcSegmentCode",            StringType(), True),
        StructField("GpcFamilyCode",             StringType(), True),
        StructField("GpcClassCode",              StringType(), True),

        StructField("Depth_mm",                  StringType(), True),
        StructField("Width_mm",                  StringType(), True),
        StructField("Height_mm",                 StringType(), True),
        StructField("GrossWeight_g",             StringType(), True),

        StructField("TradeItemUnitDescriptor",   StringType(), True),
        StructField("InfoProviderName",          StringType(), True),

        StructField("GTIN",                      StringType(), True),
        StructField("Id",                        StringType(), True),
        StructField("LastUpdatedDateTime",       StringType(), True),
        StructField("Deleted",                   BooleanType(), True),

        StructField("PrimaryImageUrl",           StringType(), True),
        StructField("PrimaryImageFileName",      StringType(), True),
        StructField("PrimaryImageMediaItemId",   StringType(), True),

        StructField("SecondaryImageUrl",         StringType(), True),
    ])

    curated_df = spark.createDataFrame(curated_rows_rdd, schema=schema)

    (curated_df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .save(curated_path))

    total = curated_df.count()
    print(f"Kuratoituja rivejä kirjoitettu: {total}")

    print("Esimerkkituotteita curated-taulusta:")
    (spark.read.format("delta").load(curated_path)
         .select(
             "Id",
             "BrandName",
             "TradeItemDescription_fi",
             "GTIN",
             "GpcSegmentCode",
             "GpcFamilyCode",
             "GpcClassCode",
             "PrimaryImageUrl",
             "SecondaryImageUrl"
         )
         .limit(sample_rows)
         .show(truncate=False))

    return total

