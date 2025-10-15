def list_keys_all(client, batch_size, offset=None, path="/PublicCatalogueItemSync/All"):
    """
    Kutsuu /PublicCatalogueItemSync/All.
    Palauttaa palvelimen JSON-vastauksen (dict) sellaisenaan.
    """
    payload = {"BatchSize": int(batch_size)}
    if offset:
        payload["Offset"] = offset
    return client.call(path, payload=payload)

def list_keys_changes(client, batch_size, since, offset=None, path="/PublicCatalogueItemSync/Changes"):
    """
    Kutsuu /PublicCatalogueItemSync/Changes.
    'since' on ISO-aikaleima (esim. '2025-09-22T07:03:26.1655687Z').
    Palauttaa palvelimen JSON-vastauksen (dict) sellaisenaan.
    """
    payload = {"BatchSize": int(batch_size), "Since": since}
    if offset:
        payload["Offset"] = offset
    return client.call(path, payload=payload)

def items_many(client, ids, path="/PublicCatalogueItem/Many"):
    """
    Kutsuu /...CatalogueItem/Many (max 1000 id:tä/kutsu).
    Palauttaa palvelimen JSON-vastauksen (dict) sellaisenaan.
    """
    return client.call(path, payload={"Ids": [str(x) for x in ids]})

def next_offset(resp_dict):
    """
    Lukee NextOffset-kentän eri kirjoitusasuilla.
    Ei tee muuta.
    """
    return (resp_dict or {}).get("NextOffset") \
        or (resp_dict or {}).get("NextofSet") \
        or (resp_dict or {}).get("NextOfSet")