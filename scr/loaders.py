import time, datetime, pandas as pd
import json
from scr.endpoints import list_keys_all, list_keys_changes, items_many, next_offset

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
    batch_size: int = 1000
) -> int:
    """
    Hakee tuotteiden raakadatat /Many-endpointilla ja tallettaa ne Silveriin JSON-stringeinä.
    - Jos ids-lista annettu → käytetään sitä
    - Muuten luetaan Id:t annetusta bronze_pathista
    Silveriin tallennetaan vain kaksi saraketta:
        - Id
        - raw_json (koko item objektina JSON-merkkijonona)
    Palauttaa kirjoitettujen itemien kokonaismäärän.
    """

    # Jos ids ei annettu, haetaan ne bronzesta
    if ids is None:
        if not bronze_path:
            raise ValueError("Anna joko ids-lista tai bronze_path.")
        ids = read_ids_from_bronze(spark, bronze_path)

    total = 0
    first_write = True

    # Käydään id:t läpi 1000 kpl erissä
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i+batch_size]

        # Haetaan erä tuotteita API:sta
        resp = items_many(client, batch_ids)
        items = resp.get("Items") or []
        if not items:
            continue

        # Muodostetaan yksinkertainen lista: Id + raw_json
        rows = []
        for it in items:
            try:
                rows.append({
                    "Id": it.get("Id"),
                    "raw_json": json.dumps(it, ensure_ascii=False)
                })
            except Exception:
                continue

        if not rows:
            continue

        # Pandas → Spark → Delta
        pdf = pd.DataFrame(rows)
        writer = spark.createDataFrame(pdf).write.format("delta")

        if first_write:
            # Ensimmäinen erä overwrite
            writer.mode("overwrite").option("overwriteSchema", "true").save(silver_path)
            first_write = False
        else:
            # Seuraavat append
            writer.mode("append").save(silver_path)

        total += len(rows)
        time.sleep(1.2)

    return total