"""
kg_store.py — MongoDB I/O for the KG build stage (owns kg_nodes/kg_edges)
============================================================================
Reads:  `entries`              (owned by the parse stage, read-only here)
Writes: `kg_nodes`, `kg_edges` (owned by this module)

Node docs dedupe by _id = node id (frame url / "type:<slug>" /
"tag:<label>"); a real frame doc must never be downgraded back to a
frame_stub by a later write that only saw the stub side of an edge.

Fully rebuildable from `entries` at any time — drop kg_nodes/kg_edges and
rerun if the shape needs to change; nothing here is hand-edited state.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m modules.kg_store --limit 0 --sample-neighborhood https://knowyourmeme.com/memes/doge
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Iterable


def _get_db():
    from pymongo import MongoClient
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGODB_DB", "memes")
    client = MongoClient(uri)
    return client[db_name]


def iter_entries():
    db = _get_db()
    yield from db.entries.find({}, {"_id": 0})


def upsert_nodes_and_edges(nodes: Iterable[dict], edges: Iterable[dict]) -> dict:
    db = _get_db()
    n_nodes = n_edges = 0
    for n in nodes:
        existing = db.kg_nodes.find_one({"_id": n["id"]}, {"kind": 1})
        if existing is not None and existing.get("kind") == "frame" and n["kind"] == "frame_stub":
            continue  # never downgrade a real frame back to a stub
        db.kg_nodes.replace_one({"_id": n["id"]}, {**n, "_id": n["id"]}, upsert=True)
        n_nodes += 1
    for e in edges:
        eid = f'{e["src"]}|{e["type"]}|{e["dst"]}'
        db.kg_edges.replace_one({"_id": eid}, {**e, "_id": eid}, upsert=True)
        n_edges += 1
    return {"nodes_written": n_nodes, "edges_written": n_edges}


def main():
    from modules.helpers import kg_build

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=0, help="Only process this many entries (0 = all)")
    ap.add_argument("--sample-neighborhood", type=str, default=None,
                     help="After building, print the full node/edge neighborhood for this entry URL")
    args = ap.parse_args()

    node_map: dict[str, dict] = {}
    all_edges: list[dict] = []
    n = 0
    for entry in iter_entries():
        nodes, edges = kg_build.build_nodes_and_edges(entry)
        for nd in nodes:
            existing = node_map.get(nd["id"])
            if existing is None or (existing.get("kind") == "frame_stub" and nd["kind"] == "frame"):
                node_map[nd["id"]] = nd
        all_edges.extend(edges)
        n += 1
        if args.limit and n >= args.limit:
            break

    stats = upsert_nodes_and_edges(node_map.values(), all_edges)
    print(f"Processed {n} entries -> {stats}")

    if args.sample_neighborhood:
        url = args.sample_neighborhood
        nb_edges = [e for e in all_edges if e["src"] == url or e["dst"] == url]
        ids = {url} | {e["dst"] for e in nb_edges if e["src"] == url} \
                    | {e["src"] for e in nb_edges if e["dst"] == url}
        nb_nodes = [node_map[i] for i in ids if i in node_map]
        print(json.dumps({"nodes": nb_nodes, "edges": nb_edges}, indent=2))


if __name__ == "__main__":
    main()