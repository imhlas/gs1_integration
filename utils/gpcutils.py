# utils/gpc_utils.py
# Kerää Silveristä uniikit GpcCategoryCodet ja niille "paras" nimi (prefer FI).
# Tulostaa yhteenvedon ja listan: Code → SelectedName (sekä muut löydetyt nimet)

import re
from collections import Counter
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# JSON-poiminta on tehokkain get_json_object:illa,
# jotta ei tarvitse parseta koko raw_jsonia Pythonissa.

def _is_finnishish(name: str) -> bool:
    """Heuristiikka: sisältää ääkkösiä (å/ä/ö) → todennäköisesti suomi/ruotsi; meille ok.
    Jos haluat tiukemman, lisää sanalistoja tai kielentunnistus myöhemmin."""
    if not isinstance(name, str):
        return False
    return bool(re.search(r"[åäöÅÄÖ]", name))

def _pick_best_name(names: list[str]) -> str:
    """Valitse paras nimi: useimmin esiintyvä FI-tyylinen → muuten useimmin esiintyvä.
    Tasatilanteessa valitse pisin merkkijono (deskriptiivisempi)."""
    names = [n for n in names if isinstance(n, str) and n.strip()]
    if not names:
        return None
    c = Counter(names)
    # 1) yritä FI-tyyliset
    fi_candidates = [n for n in c if _is_finnishish(n)]
    if fi_candidates:
        # valitse useimmin esiintyvä FI-kandidaatti
        max_count = max(c[n] for n in fi_candidates)
        tied = [n for n in fi_candidates if c[n] == max_count]
    else:
        # ei fi-kandidaatteja → yleisin overall
        max_count = max(c.values())
        tied = [n for n, k in c.items() if k == max_count]
    # tasatilanteessa valitse pisin
    tied.sort(key=lambda s: (len(s), s))  # pituus, sitten aakkoset
    return tied[-1]  # pisin

def print_unique_gpc_codes_and_names(spark, silver_path: str) -> None:
    """
    Lue Silver-Delta (raw_json), poimi GpcCategoryCode & GpcCategoryName,
    ja tulosta uniikit koodit sekä valittu 'paras' nimi per koodi.
    """
    # Poimi kentät ilman täyttä json-parsintaa
    src = (spark.read.format("delta").load(silver_path)
           .select(
               F.get_json_object(F.col("raw_json"), "$.TradeItem.GdsnTradeItemClassification.GpcCategoryCode")
                   .alias("GpcCategoryCode"),
               F.get_json_object(F.col("raw_json"), "$.TradeItem.GdsnTradeItemClassification.GpcCategoryName")
                   .alias("GpcCategoryName"),
           )
           .filter(F.col("GpcCategoryCode").isNotNull())
          )

    # Kerää kaikki nimet per koodi
    agg = (src.groupBy("GpcCategoryCode")
              .agg(F.collect_list("GpcCategoryName").alias("names")))

    # UDF: valitse paras
    pick_udf = F.udf(_pick_best_name, StringType())

    res = (agg
           .withColumn("SelectedName", pick_udf(F.col("names")))
           .withColumn("DistinctNames", F.array_distinct(F.col("names")))
           .select("GpcCategoryCode", "SelectedName", "DistinctNames")
           .orderBy(F.col("GpcCategoryCode").asc())
          )

    total_codes = res.count()
    print(f"\n=== GPC-kategoriat Silveristä ===")
    print(f"Uniikkeja GpcCategoryCodeja: {total_codes}\n")
    # Tulosta kaikki (voit muuttaa .show(n) jos haluat rajata)
    res.show(total_codes, truncate=False)

    # Lisäksi yhteenveto epäyhtenäisistä nimistä
    multi_name_cnt = res.filter(F.size("DistinctNames") > 1).count()
    if multi_name_cnt > 0:
        print(f"\nHuom: {multi_name_cnt} koodilla on useampi eri nimi datassa (näkyvät DistinctNames-kentässä).")
    else:
        print("\nKaikilla koodeilla yksi nimi datassa.")
