import time, datetime, pandas as pd
from scr.endpoints import list_keys_all, list_keys_changes, next_offset

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