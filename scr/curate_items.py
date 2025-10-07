# scr/curate_items.py

import json
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

def kuratoi_ja_talleta_deltaan_like_batch(
    spark,
    silver_path: str,
    curated_path: str,
    write_mode: str = "overwrite",
    sample_rows: int = 5
) -> int:
    print(">>> Vaihe: Kuratoi tuotteet ja tallenna Deltaan")

    src_df = spark.read.format("delta").load(silver_path).select("raw_json")
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
        """Palauta listasta (jossa LanguageCode/Value) fi-kielinen Value, tai None. Varalla 1. arvo."""
        if not isinstance(lst, list):
            return None
        for it in lst:
            if isinstance(it, dict) and it.get("LanguageCode") == "fi":
                return it.get("Value")
        it = lst[0] if lst else None
        return it.get("Value") if isinstance(it, dict) else None

    def extract_url(d):
        return (d or {}).get("Url") or (d or {}).get("URL") or (d or {}).get("UniformResourceIdentifier")

    def extract_filename(d):
        return (d or {}).get("FileName") or (d or {}).get("Filename") or (d or {}).get("OriginalFileName")

    def lower_ext_from(s):
        if not isinstance(s, str):
            return None
        s = s.strip().lower()
        if "." in s:
            return s.rsplit(".", 1)[-1]
        return None

    def canonical_mime(d, url):
        """Yritä lukea MIME-tyyppi objektista; jollei löydy, päättele URL-päätteestä."""
        cand = None
        for k in ("MimeType", "ContentType", "MediaType", "FileContentType", "FileMimeType"):
            v = (d or {}).get(k)
            if isinstance(v, str) and v:
                cand = v.strip().lower()
                break
        if not cand:
            ext = lower_ext_from(url or "") or lower_ext_from(extract_filename(d) or "")
            if ext in ("jpg", "jpeg"):
                cand = "image/jpeg"
            elif ext == "png":
                cand = "image/png"
            elif ext in ("tif", "tiff"):
                cand = "image/tiff"
        return cand

    def is_image_like(d):
        """Heuristiikka: näyttääkö 'kuvalta'."""
        if not isinstance(d, dict):
            return False
        url = extract_url(d)
        fname = extract_filename(d)
        mime = canonical_mime(d, url)
        if mime and mime.startswith("image/"):
            return True
        if url or fname:
            ext = lower_ext_from(url or fname or "")
            if ext in ("jpg", "jpeg", "png", "tif", "tiff", "gif", "webp"):
                return True
        if d.get("AssetTypeCode") in ("PRODUCT_IMAGE", "PRIMARY_IMAGE"):
            return True
        return False

    def find_image_candidates(obj):
        """Kerää kaikki kuvan kaltaiset objektit rekursiivisesti."""
        found = []
        def walk(x):
            if isinstance(x, dict):
                if is_image_like(x):
                    found.append(x)
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)
        walk(obj)
        return found

    def is_primary_flag(d):
        return True if (d or {}).get("IsPrimary") is True or (d or {}).get("Primary") is True or (d or {}).get("PrimaryFlag") is True else False

    def image_type_bucket(d):
        """Palauta 0=web-ystävällinen (jpg/png), 1=tiff, 2=muu/tuntematon."""
        url = extract_url(d)
        mime = canonical_mime(d, url) or ""
        ext = (lower_ext_from(url or "") or "").lower()
        # web-friendly ensin
        if mime in ("image/jpeg", "image/png") or ext in ("jpg", "jpeg", "png"):
            return 0
        # tiff toisena
        if mime == "image/tiff" or ext in ("tif", "tiff"):
            return 1
        # muu viimeisenä
        return 2

    def pick_best_image(images):
        """
        Valitse kuva seuraavalla tärkeysjärjestyksellä:
        1) jpg/png (web), 2) tiff, 3) muu. Kunkin ryhmän sisällä IsPrimary=True ensin.
        """
        if not isinstance(images, list) or not images:
            return None
        scored = []
        for img in images:
            if not isinstance(img, dict):
                continue
            bucket = image_type_bucket(img)
            primary_rank = 0 if is_primary_flag(img) else 1
            scored.append((bucket, primary_rank, img))
        if not scored:
            return None
        scored.sort(key=lambda t: (t[0], t[1]))
        return scored[0][2]

    def parse_one(raw_json_str):
        try:
            data = json.loads(raw_json_str) if isinstance(raw_json_str, str) else {}
        except Exception:
            data = {}

        ti   = data.get("TradeItem") or {}
        ci   = data.get("CatalogueItemInfo") or {}

        # --- StartAvailabilityDateTime ---
        start_avail = get(ti, "DeliveryPurchasingInformationModule", "DeliveryPurchasingInformation", "StartAvailabilityDateTime")

        # --- Brändi & NIMI (FI) ---
        desc_block = get(ti, "TradeItemDescriptionModule", "TradeItemDescriptionInformation", default={}) or {}
        brand = get(desc_block, "BrandNameInformation", "BrandName")
        # ENSISIJAISESTI TradeItemDescription (fi), varalla DescriptionShort (fi)
        desc_fi = first_fi_value(desc_block.get("TradeItemDescription")) \
                  or first_fi_value(desc_block.get("DescriptionShort"))

        # --- GPC ---
        gpc = get(ti, "GdsnTradeItemClassification", default={}) or {}
        gpc_cat   = gpc.get("GpcCategoryCode")
        gpc_name  = gpc.get("GpcCategoryName")
        gpc_seg   = gpc.get("GpcSegmentCode")
        gpc_fam   = gpc.get("GpcFamilyCode")
        gpc_class = gpc.get("GpcClassCode")

        # --- Mitat & paino (mm, g) ---
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

        # --- Kuvat: etsitään rekursiivisesti koko objektista ---
        imgs = find_image_candidates(data)
        if not imgs and isinstance(ti.get("Images"), list):
            imgs = ti["Images"]

        chosen = pick_best_image(imgs)

        url = extract_url(chosen) if chosen else None
        file_name   = extract_filename(chosen) if chosen else None
        media_id    = (chosen or {}).get("MediaItemId") or (chosen or {}).get("MediaId")
        is_primary  = is_primary_flag(chosen)
        mime_type   = canonical_mime(chosen, url)

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
            "PrimaryImageUrl":           url,
            "PrimaryImageFileName":      file_name,
            "PrimaryImageMediaItemId":   str(media_id) if media_id is not None else None,
            "PrimaryIsPrimaryFlag":      True if is_primary is True else (False if is_primary is False else None),
            "PrimaryImageMimeType":      mime_type,
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
        StructField("PrimaryIsPrimaryFlag",      BooleanType(), True),
        StructField("PrimaryImageMimeType",      StringType(), True),
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
         .select("Id", "BrandName", "TradeItemDescription_fi", "GTIN", "PrimaryImageUrl", "PrimaryImageMimeType")
         .limit(sample_rows)
         .show(truncate=False))

    return total

