import time, datetime, pandas as pd
import json
from src.endpoints import list_keys_all, list_keys_changes, items_many, next_offset

def fetch_all_keys_to_bronze(spark, client, bronze_path: str, batch_size: int) -> int:
    """
    Hakee KAIKKI avaimet /PublicCatalogueItemSync/All -endpointista,
    sivuttaa NextOffsetilla ja kirjoittaa Bronze-Deltaan MINIMIN (Id + load_time).
    Ensimmäinen erä overwrite, loput append.
    Palauttaa ja tulostaa kirjoitettujen Id:iden kokonaismäärän.
    """
    offset = None
    first_write = True
    total = 0

    while True:
        resp = list_keys_all(client, batch_size=batch_size, offset=offset)
        items = resp.get("Items") or []
        ids   = [it.get("Id") for it in items if it.get("Id")]
        if not ids:
            break

        load_time = datetime.datetime.utcnow().isoformat()
        pdf = pd.DataFrame({"Id": ids, "load_time": load_time})

        writer = spark.createDataFrame(pdf).write.format("delta")
        if first_write:
            writer.mode("overwrite").option("overwriteSchema", "true").save(bronze_path)
            first_write = False
        else:
            writer.mode("append").save(bronze_path)

        total += len(ids)
        offset = next_offset(resp)
        if not offset:
            break

        time.sleep(1.2)  # API ~1 pyyntö/s

    return total

def read_ids_from_bronze(spark, bronze_path: str):
    """
    Lukee kaikki Id:t annetusta Bronze-Delta-polusta Pandas-DF:ään.
    Palauttaa listan Id:itä.
    """
    df = spark.read.format("delta").load(bronze_path).select("Id").distinct()
    return [row["Id"] for row in df.collect()]
    
def fetch_items_to_silver_json(
    spark,
    client,
    silver_path: str,
    bronze_path: str = None,
    ids: list = None,
    batch_size: int = 1000,
    first_write_mode: str = "overwrite",
    rate_limit_per_minute: int = 15,
    verbose: bool = True,
    only_gpc_segment_code: str = None,   # <-- UUSI: suodata tällä (esim. "50000000")
) -> int:
    """
    Hakee tuotteet /Many-endpointilla ja tallettaa Silveriin (Id, raw_json, ingest_ts).
    Jos only_gpc_segment_code on annettu, Silveriin viedään vain ne rivit,
    joiden GpcSegmentCode vastaa annettua arvoa.
    """

    # --- ID-lähde ---
    if ids is None:
        if not bronze_path:
            raise ValueError("Anna joko ids-lista tai bronze_path.")
        ids = read_ids_from_bronze(spark, bronze_path)

    if not ids:
        if verbose:
            print("Ei haettavia Id:itä.")
        return 0

    # --- Eräkoko max 1000 ---
    eff_batch = max(1, min(int(batch_size), 1000))

    # --- Odotus Many-kutsujen väliin (rate limit) ---
    try:
        delay_s = max(60.0 / float(rate_limit_per_minute), 0.0) * 1.05
    except Exception:
        delay_s = 4.2

    total_written = 0
    first_write = True

    if verbose:
        flt = f", suodatin GpcSegmentCode={only_gpc_segment_code}" if only_gpc_segment_code else ""
        print(f"Haetaan {len(ids)} tuotetta erissä (koko {eff_batch}), viive {delay_s:.1f}s / Many-kutsu{flt}")

    # Pieni apuri polun lukemiseen
    def _get(d, *path, default=None):
        cur = d
        for k in path:
            if not isinstance(cur, dict) or k not in cur:
                return default
            cur = cur[k]
        return cur

    # --- Eräsilmukka ---
    for start in range(0, len(ids), eff_batch):
        batch_ids = ids[start:start + eff_batch]

        # API-kutsu
        resp = items_many(client, batch_ids)
        items = resp.get("Items") or []
        if verbose:
            print(f"Erä {start // eff_batch + 1}: palautui {len(items)} itemiä")

        if not items:
            time.sleep(delay_s)
            continue

        # Muodostetaan rivit: Id + raw_json + ingest_ts (vain suodatetut)
        ingest_ts = datetime.datetime.utcnow().isoformat()
        rows = []
        skipped = 0

        for it in items:
            try:
                # Lue GpcSegmentCode turvallisesti
                ti  = it.get("TradeItem", {}) or {}
                gpc = ti.get("GdsnTradeItemClassification", {}) or {}
                seg_code = gpc.get("GpcSegmentCode") or it.get("GpcSegmentCode")  # varapolku

                # Suodatus (jos asetettu)
                if only_gpc_segment_code is not None:
                    if str(seg_code) != str(only_gpc_segment_code):
                        skipped += 1
                        continue

                trade = it.get("TradeItem", {}) or {}
                info  = it.get("CatalogueItemInfo", {}) or {}
                item_id = info.get("Id") or trade.get("Id") or trade.get("Gtin")

                rows.append({
                    "Id": item_id,
                    "raw_json": json.dumps(it, ensure_ascii=False),
                    "ingest_ts": ingest_ts,
                })
            except Exception:
                # Jos yksittäinen rivi hajoaa, ohitetaan se
                continue

        if verbose and only_gpc_segment_code is not None:
            print(f"  -> Suodatettu pois {skipped} kpl (ei GpcSegmentCode={only_gpc_segment_code})")

        if not rows:
            time.sleep(delay_s)
            continue

        # Pandas → Spark → Delta
        pdf = pd.DataFrame(rows)
        writer = spark.createDataFrame(pdf).write.format("delta").option("mergeSchema", "true")

        if first_write:
            if first_write_mode == "overwrite":
                writer.mode("overwrite").option("overwriteSchema", "true").save(silver_path)
            else:
                writer.mode("append").save(silver_path)
            first_write = False
        else:
            writer.mode("append").save(silver_path)

        total_written += len(rows)
        time.sleep(delay_s)  # kunnioita Many-API:n rajoitusta

    if verbose:
        print(f"Valmista: Silveriin kirjoitettu {total_written} riviä")
    return total_written
