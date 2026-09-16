"""
kg_census.py — entry_type frequency + co-occurrence census for the KG taxonomy build
=======================================================================================
Read-only, no Airflow imports. Streams `entries.entry_type` from MongoDB and
emits the raw counts needed to hand-derive an entry-type hierarchy: per-type
frequency, the per-entry type-count distribution, and pairwise co-occurrence
counts. Does NOT compute containment/PMI itself — those derivations happen
from this output, by hand, so every threshold choice stays visible and
reviewable instead of hidden in a script default.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m modules.kg_census --min-pair-count 3 --output /opt/airflow/dags/kg_census_entry_type.json

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


def run_census(min_pair_count: int = 3) -> dict:
    entries = _get_collection()

    corpus_size = 0
    with_types = 0
    type_counts: Counter[str] = Counter()
    types_per_entry: Counter[int] = Counter()
    pair_counts: Counter[tuple[str, str]] = Counter()

    cursor = entries.find({}, {"_id": 0, "entry_type": 1})
    for doc in cursor:
        corpus_size += 1
        types = sorted(set(doc.get("entry_type") or []))
        if not types:
            types_per_entry[0] += 1
            continue
        with_types += 1
        types_per_entry[len(types)] += 1
        type_counts.update(types)
        for a, b in itertools.combinations(types, 2):
            pair_counts[(a, b)] += 1

    pairs = [
        {"a": a, "b": b, "count": c}
        for (a, b), c in pair_counts.items()
        if c >= min_pair_count
    ]
    pairs.sort(key=lambda r: r["count"], reverse=True)

    return {
        "corpus_size": corpus_size,
        "entries_with_entry_type": with_types,
        "distinct_types": len(type_counts),
        "type_counts": dict(type_counts.most_common()),
        "types_per_entry_distribution": dict(sorted(types_per_entry.items())),
        "pair_cooccurrence": pairs,
        "pair_count_min_filter": min_pair_count,
        "total_pairs_seen": len(pair_counts),
        "total_pairs_returned": len(pairs),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--min-pair-count", type=int, default=3,
                     help="Only include type pairs co-occurring at least this many times (default 3).")
    ap.add_argument("--output", type=str, default=None,
                     help="If set, write full JSON here in addition to a summary on stdout.")
    args = ap.parse_args()

    result = run_census(min_pair_count=args.min_pair_count)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Wrote full census to {args.output}")
        print(f"corpus_size={result['corpus_size']} "
              f"entries_with_entry_type={result['entries_with_entry_type']} "
              f"distinct_types={result['distinct_types']} "
              f"total_pairs_seen={result['total_pairs_seen']} "
              f"total_pairs_returned={result['total_pairs_returned']}")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()