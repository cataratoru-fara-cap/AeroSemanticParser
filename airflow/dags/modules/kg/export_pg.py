"""
kg/export_pg.py — dump kg_nodes/kg_edges to CSV for visualization
================================================================
Zero-dependency export (csv module only) producing two files that both
Cosmograph (drag & drop in the browser) and Gephi (File > Import
Spreadsheet) accept directly:

    kg_view_nodes.csv   id,label,kind,category,status
    kg_view_edges.csv   source,target,type

Filters (composable):
    --exclude-kinds tag_concept,frame_stub   drop whole node kinds
    --top-tags 500                           keep only the 500 highest-degree
                                             tag nodes (ignored if
                                             tag_concept is excluded)
Edges touching an excluded node are dropped with it.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m modules.kg.export_pg --exclude-kinds frame_stub --top-tags 500 \
        --out-dir /opt/airflow/data

Connection settings mirror parse_store.py's env vars (MONGODB_URI, MONGODB_DB).
"""
from __future__ import annotations

import argparse
import csv
import os
from collections import Counter


def _get_db():
    from pymongo import MongoClient
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGODB_DB", "memes")
    client = MongoClient(uri)
    return client[db_name]


def export(out_dir: str, exclude_kinds: set[str], top_tags: int) -> dict:
    db = _get_db()

    kept_ids: set[str] = set()
    kind_counts: Counter[str] = Counter()

    tag_allowed: set[str] | None = None
    if top_tags and "tag_concept" not in exclude_kinds:
        deg: Counter[str] = Counter()
        for e in db.kg_edges.find({"type": "hasTag"}, {"_id": 0, "dst": 1}):
            deg[e["dst"]] += 1
        tag_allowed = {t for t, _ in deg.most_common(top_tags)}

    nodes_path = os.path.join(out_dir, "kg_view_nodes.csv")
    edges_path = os.path.join(out_dir, "kg_view_edges.csv")

    with open(nodes_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "label", "kind", "category", "status"])
        for n in db.kg_nodes.find({}, {"_id": 0}):
            kind = n.get("kind")
            if kind in exclude_kinds:
                continue
            if kind == "tag_concept" and tag_allowed is not None \
                    and n["id"] not in tag_allowed:
                continue
            kept_ids.add(n["id"])
            kind_counts[kind] += 1
            w.writerow([n["id"], n.get("label") or "", kind,
                        n.get("category") or "", n.get("status") or ""])

    n_edges = 0
    edge_type_counts: Counter[str] = Counter()
    with open(edges_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["source", "target", "type"])
        for e in db.kg_edges.find({}, {"_id": 0}):
            if e["src"] in kept_ids and e["dst"] in kept_ids:
                w.writerow([e["src"], e["dst"], e["type"]])
                n_edges += 1
                edge_type_counts[e["type"]] += 1

    return {"nodes_written": sum(kind_counts.values()),
            "by_kind": dict(kind_counts),
            "edges_written": n_edges,
            "by_edge_type": dict(edge_type_counts),
            "files": [nodes_path, edges_path]}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--exclude-kinds", type=str, default="",
                     help="Comma-separated node kinds to drop entirely.")
    ap.add_argument("--top-tags", type=int, default=0,
                     help="Keep only the N highest-degree tag nodes (0 = all).")
    ap.add_argument("--out-dir", type=str, default=".")
    args = ap.parse_args()

    exclude = {k.strip() for k in args.exclude_kinds.split(",") if k.strip()}
    stats = export(args.out_dir, exclude, args.top_tags)
    for k, v in stats.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()