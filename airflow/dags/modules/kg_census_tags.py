"""
kg_census_tags.py — tag frequency + restricted co-occurrence census (the ontology component)
==================================================================================================
Read-only, no Airflow imports. Tags are a folksonomy with a much larger
vocabulary than entry_type, so co-occurrence is restricted to the --top-k
most frequent tags (default 300) — a full pairwise matrix over the whole
tag vocabulary would be both statistically noisy (singleton tags carry no
signal) and impractically large to page back for review. Full frequency
counts are still reported for every distinct tag, uncapped — that part is
cheap regardless of vocabulary size.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m modules.kg_census_tags --top-k 300 --min-pair-count 5 \
        --output /opt/airflow/dags/kg_census_tags.json

Connection settings mirror parse_store.py's env vars:
    MONGODB_URI                 (default: mongodb://localhost:27017)
    MONGODB_DB                  (default: memes)
    MONGODB_ENTRIES_COLLECTION  (default: entries)
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
from collections import Counter


def _get_collection():
    from pymongo import MongoClient
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGODB_DB", "memes")
    coll_name = os.getenv("MONGODB_ENTRIES_COLLECTION", "entries")
    client = MongoClient(uri)
    return client[db_name][coll_name]


def run_census(top_k: int = 300, min_pair_count: int = 5) -> dict:
    entries = _get_collection()

    corpus_size = 0
    with_tags = 0
    tag_counts: Counter[str] = Counter()
    tags_per_entry: Counter[int] = Counter()
    all_docs: list[list[str]] = []

    cursor = entries.find({}, {"_id": 0, "tags": 1})
    for doc in cursor:
        corpus_size += 1
        tags = sorted({t.strip().lower() for t in (doc.get("tags") or []) if t.strip()})
        if not tags:
            tags_per_entry[0] += 1
            continue
        with_tags += 1
        tags_per_entry[len(tags)] += 1
        tag_counts.update(tags)
        all_docs.append(tags)

    top_tags = {t for t, _ in tag_counts.most_common(top_k)}

    pair_counts: Counter[tuple[str, str]] = Counter()
    for tags in all_docs:
        restricted = sorted(t for t in tags if t in top_tags)
        for a, b in itertools.combinations(restricted, 2):
            pair_counts[(a, b)] += 1

    pairs = [{"a": a, "b": b, "count": c}
             for (a, b), c in pair_counts.items() if c >= min_pair_count]
    pairs.sort(key=lambda r: r["count"], reverse=True)

    return {
        "corpus_size": corpus_size,
        "entries_with_tags": with_tags,
        "distinct_tags_total": len(tag_counts),
        "top_k_used_for_cooccurrence": top_k,
        "top_tag_counts": dict(tag_counts.most_common(top_k)),
        "tags_per_entry_distribution": dict(sorted(tags_per_entry.items())),
        "pair_cooccurrence": pairs,
        "pair_count_min_filter": min_pair_count,
        "total_pairs_returned": len(pairs),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--top-k", type=int, default=300,
                     help="Restrict co-occurrence computation to this many most-frequent tags.")
    ap.add_argument("--min-pair-count", type=int, default=5,
                     help="Only include tag pairs co-occurring at least this many times.")
    ap.add_argument("--output", type=str, default=None)
    args = ap.parse_args()

    result = run_census(top_k=args.top_k, min_pair_count=args.min_pair_count)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Wrote full census to {args.output}")
    print(f"corpus_size={result['corpus_size']} "
          f"entries_with_tags={result['entries_with_tags']} "
          f"distinct_tags_total={result['distinct_tags_total']} "
          f"total_pairs_returned={result['total_pairs_returned']}")


if __name__ == "__main__":
    main()