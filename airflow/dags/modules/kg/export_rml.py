"""
kg/export_rml.py — entries collection -> clean CSVs for the RML/YARRRML mapping
==================================================================================
Reads `entries` directly (not kg_nodes/kg_edges) — the raw arrays there are
already clean (no "type:"/"tag:" id prefixes to strip), which keeps every
mapping rule in kg_mapping.yarrrml.yml a straight column-to-triple
substitution. relatesToMeme/citesExternal aren't in raw entries (they're
derived), so those two reuse kg_build.py's own link-classification helpers
rather than re-implementing the logic — single source of truth.

Writes eight homogeneous CSVs, one per RML mapping rule:
    frames.csv            url,title,category,status
    types.csv              slug,label                (distinct entry_type slugs seen)
    entry_type_edges.csv   url,slug
    broader_edges.csv      narrower,broader            (curated, hardcoded below —
                                                          keep in sync with
                                                          dags/kg_config/entry_type_taxonomy.yaml)
    tag_edges.csv          url,tag
    series_edges.csv       url,parent_url
    relates_edges.csv      url,target_url
    cites_edges.csv        url,target_url

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m modules.kg.export_rml --out-dir /opt/airflow/dags/rml_data
"""
from __future__ import annotations

import argparse
import csv
import os

# Curated narrower/broader pairs, resolved and reviewed — see
# dags/kg_config/entry_type_taxonomy.yaml `broader_confirmed` + `broader_semantic_only`
# (minus anything still in `contested`). Keep this list in sync by hand;
# it's small and deliberately not auto-derived from the census.
CURATED_BROADER_EDGES: tuple[tuple[str, str], ...] = (
    ("streamer", "creator"),
    ("fan-art", "fan-labor"),
    ("vlogger", "creator"),
    ("generator", "application"),
    ("ai-influencer", "influencer"),
    ("company", "organization"),
    ("song", "music"),
    ("album", "music"),
    ("flash-mob", "performance"),
    ("blockchain", "technology"),
    ("snowclone", "catchphrase"),
    ("creepypasta", "copypasta"),
    ("model", "influencer"),
)


def _get_db():
    from pymongo import MongoClient
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGODB_DB", "memes")
    client = MongoClient(uri)
    return client[db_name]


def export(out_dir: str) -> dict:
    from modules.kg.build import _iter_link_urls, _is_kym_url  # single source of truth

    os.makedirs(out_dir, exist_ok=True)
    db = _get_db()

    seen_types: set[str] = set()
    counts = {k: 0 for k in
              ["frames", "entry_type_edges", "tag_edges", "series_edges",
               "relates_edges", "cites_edges"]}

    f_frames = open(os.path.join(out_dir, "frames.csv"), "w", newline="")
    f_type_edges = open(os.path.join(out_dir, "entry_type_edges.csv"), "w", newline="")
    f_tag_edges = open(os.path.join(out_dir, "tag_edges.csv"), "w", newline="")
    f_series_edges = open(os.path.join(out_dir, "series_edges.csv"), "w", newline="")
    f_relates = open(os.path.join(out_dir, "relates_edges.csv"), "w", newline="")
    f_cites = open(os.path.join(out_dir, "cites_edges.csv"), "w", newline="")

    w_frames = csv.writer(f_frames); w_frames.writerow(["url", "title", "category", "status"])
    w_types_edges = csv.writer(f_type_edges); w_types_edges.writerow(["url", "slug"])
    w_tags = csv.writer(f_tag_edges); w_tags.writerow(["url", "tag"])
    w_series = csv.writer(f_series_edges); w_series.writerow(["url", "parent_url"])
    w_relates = csv.writer(f_relates); w_relates.writerow(["url", "target_url"])
    w_cites = csv.writer(f_cites); w_cites.writerow(["url", "target_url"])

    for entry in db.entries.find({}, {"_id": 0}):
        url = entry.get("url")
        if not url:
            continue

        w_frames.writerow([url, entry.get("title") or "",
                            entry.get("category") or "", entry.get("status") or ""])
        counts["frames"] += 1

        for slug in entry.get("entry_type") or []:
            seen_types.add(slug)
            w_types_edges.writerow([url, slug])
            counts["entry_type_edges"] += 1

        for tag in entry.get("tags") or []:
            tag_norm = tag.strip().lower()
            if tag_norm:
                w_tags.writerow([url, tag_norm])
                counts["tag_edges"] += 1

        sp = entry.get("series_parent")
        if sp:
            w_series.writerow([url, sp])
            counts["series_edges"] += 1

        seen_internal, seen_external = set(), set()
        for link_url in _iter_link_urls(entry):
            if link_url == url:
                continue
            if _is_kym_url(link_url):
                if link_url not in seen_internal:
                    seen_internal.add(link_url)
                    w_relates.writerow([url, link_url])
                    counts["relates_edges"] += 1
            else:
                if link_url not in seen_external:
                    seen_external.add(link_url)
                    w_cites.writerow([url, link_url])
                    counts["cites_edges"] += 1

    for f in (f_frames, f_type_edges, f_tag_edges, f_series_edges, f_relates, f_cites):
        f.close()

    with open(os.path.join(out_dir, "types.csv"), "w", newline="") as f:
        w = csv.writer(f); w.writerow(["slug", "label"])
        for slug in sorted(seen_types):
            w.writerow([slug, slug.replace("-", " ")])

    with open(os.path.join(out_dir, "broader_edges.csv"), "w", newline="") as f:
        w = csv.writer(f); w.writerow(["narrower", "broader"])
        skipped = 0
        for narrower, broader in CURATED_BROADER_EDGES:
            if narrower in seen_types and broader in seen_types:
                w.writerow([narrower, broader])
            else:
                skipped += 1
                print(f"  !! skipping broader edge {narrower}->{broader}: "
                      f"slug not seen in corpus (typo, or corpus has moved on)")

    counts["distinct_types"] = len(seen_types)
    counts["broader_edges_written"] = len(CURATED_BROADER_EDGES) - skipped
    return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out-dir", type=str, default=".")
    args = ap.parse_args()
    counts = export(args.out_dir)
    for k, v in counts.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
