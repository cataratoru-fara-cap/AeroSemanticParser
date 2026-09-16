"""
kg/serialize.py — one build, every on-disk representation, one code path
===========================================================================
Pure: no Mongo, no Airflow. Given the nodes and edges of one build, writes
into a build directory:

    graph.nt                 the RDF graph (kg/rdf.py)           — the product
    rml_data/*.csv           the eight CSVs the YARRRML mapping reads — the
                             input to the morph-kgc VALIDATION path
    kg_view_nodes.csv        the property graph for Cosmograph / Gephi
    kg_view_edges.csv
    manifest.json            build_id, stamps, per-file sha256 and row counts

Why one module
--------------
This replaces two: ``kg_export.py`` (property-graph CSVs from kg_nodes/
kg_edges) and ``kg_export_rml.py`` (RML CSVs, re-derived DIRECTLY from
`entries` by a second copy of the link-classification loop). Two loops, one
copy-paste apart, and they drifted: the RML copy forgot to seed its
per-entry ``seen`` set with ``series_parent``, so the published RDF carried
14,563 ``mk:relatesToMeme`` triples the property graph did not. Every file
below is now a projection of the SAME node/edge stream from kg/build.py, so
the representations cannot disagree about which edges exist — only about
how each chooses to render them, and that is what the vocabulary test and
the diff gate pin down.

Inputs are factories, not iterables
-----------------------------------
``nodes`` and ``edges`` are zero-argument callables returning a fresh
iterable each time. Each output family walks the stream once more rather
than the whole graph being buffered: 348k nodes + 713k edges is ~500 MB in
Python objects, and a Mongo cursor is cheap to reopen. Peak memory stays
near one row per open file.

Nothing is half-written
-----------------------
Every file is written as ``<name>.tmp`` and ``os.replace``d into place when
complete, and the manifest is written last. The previous exporter opened six
handles bare with no try/finally; an exception mid-run left six truncated
CSVs that looked complete to morph-kgc. Here a crash leaves ``.tmp`` files
and no manifest — visibly unfinished, never plausibly finished.

Value mapping to the RML CSVs mirrors ``kg_mapping.yarrrml.yml`` exactly: the
property graph's ``type:<slug>`` and ``tag:<label>`` ids are stripped back
to the raw slug/label, because the mapping mints the IRI or literal itself.
"""
from __future__ import annotations

import contextlib
import csv
import hashlib
import json
import os
from collections import Counter
from typing import Any, Callable, Iterable

from modules.kg import rdf
from modules.kg.build import EDGE_TYPES, NODE_KINDS
from modules.kg.taxonomy import CONCEPT_EDGE_TYPES

__all__ = [
    "EDGE_TYPE_TO_RML_FILE", "RML_NODE_FILES", "PG_NODES_HEADER",
    "PG_EDGES_HEADER", "RML_DIR", "write_build", "load_manifest",
]

NodeSource = Callable[[], Iterable[dict]]
EdgeSource = Callable[[], Iterable[dict]]

RML_DIR = "rml_data"

# edge type -> (RML csv name, header). tests/test_kg_vocabulary.py asserts
# these keys equal EDGE_TYPES + CONCEPT_EDGE_TYPES equal the YARRRML's
# predicate local names — the one-vocabulary invariant.
EDGE_TYPE_TO_RML_FILE: dict[str, tuple[str, tuple[str, ...]]] = {
    "hasEntryType":  ("entry_type_edges.csv", ("url", "slug")),
    "hasTag":        ("tag_edges.csv",        ("url", "tag")),
    "partOfSeries":  ("series_edges.csv",     ("url", "parent_url")),
    "relatesToMeme": ("relates_edges.csv",    ("url", "target_url")),
    "citesExternal": ("cites_edges.csv",      ("url", "target_url")),
    "broader":       ("broader_edges.csv",    ("narrower", "broader")),
}

RML_NODE_FILES: dict[str, tuple[str, ...]] = {
    "frames.csv": ("url", "title", "category", "status"),
    "types.csv":  ("slug", "label"),
    "scheme.csv": ("iri", "label"),     # one row: the SKOS scheme itself
}

PG_NODES_HEADER = ("id", "label", "kind", "category", "status")
PG_EDGES_HEADER = ("source", "target", "type")

assert set(EDGE_TYPE_TO_RML_FILE) == set(EDGE_TYPES) | set(CONCEPT_EDGE_TYPES), (
    "serialize.py's RML file table drifted from the edge vocabulary")


def _strip(prefix: str, value: str) -> str:
    return value[len(prefix):] if value.startswith(prefix) else value


def _rml_row(edge: dict) -> tuple[str, str] | None:
    """One edge -> the two-column row its RML CSV expects, or None."""
    etype = edge.get("type")
    src, dst = edge.get("src"), edge.get("dst")
    if etype not in EDGE_TYPE_TO_RML_FILE or not src or not dst:
        return None
    if etype == "hasEntryType":
        return (src, _strip("type:", dst))
    if etype == "hasTag":
        return (src, _strip("tag:", dst))
    if etype == "broader":
        return (_strip("type:", src), _strip("type:", dst))
    return (src, dst)


class _AtomicFiles:
    """Open N files as ``.tmp``; on clean exit replace into place, on error
    leave the ``.tmp`` files behind and the real names untouched."""

    def __init__(self) -> None:
        self._stack = contextlib.ExitStack()
        self._pending: list[tuple[str, str]] = []
        self.paths: dict[str, str] = {}

    def __enter__(self) -> "_AtomicFiles":
        self._stack.__enter__()
        return self

    def open_csv(self, key: str, path: str, header: Iterable[str]):
        tmp = path + ".tmp"
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fh = self._stack.enter_context(
            open(tmp, "w", encoding="utf-8", newline=""))
        writer = csv.writer(fh)
        writer.writerow(list(header))
        self._pending.append((tmp, path))
        self.paths[key] = path
        return writer

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stack.__exit__(exc_type, exc, tb)   # close every handle first
        if exc_type is None:
            for tmp, final in self._pending:
                os.replace(tmp, final)


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_build(nodes: NodeSource, edges: EdgeSource, out_dir: str, *,
                build_id: str, stamps: dict[str, Any] | None = None,
                exclude_kinds: Iterable[str] = (), top_tags: int = 0
                ) -> dict[str, Any]:
    """Write every representation of one build into ``out_dir``.

    ``exclude_kinds`` / ``top_tags`` shape ONLY the property-graph view
    CSVs (they exist so a 100k-tag hairball can be opened in Cosmograph);
    the RDF and RML outputs are always the full graph. The filters used are
    recorded in the manifest — the July CSVs carried no such record, which
    is why a 450k-edge staleness was undiagnosable from the files.
    """
    stamps = dict(stamps or {})
    stamps.setdefault("build_id", build_id)
    exclude = set(exclude_kinds)
    unknown = exclude - set(NODE_KINDS)
    if unknown:
        raise ValueError(f"exclude_kinds not in NODE_KINDS: {sorted(unknown)}")

    os.makedirs(os.path.join(out_dir, RML_DIR), exist_ok=True)
    rows: Counter[str] = Counter()
    nodes_by_kind: Counter[str] = Counter()
    edges_by_type: Counter[str] = Counter()

    # -- pass 1: RML node CSVs -------------------------------------------
    with _AtomicFiles() as files:
        frames = files.open_csv("frames.csv",
                                os.path.join(out_dir, RML_DIR, "frames.csv"),
                                RML_NODE_FILES["frames.csv"])
        types_ = files.open_csv("types.csv",
                                os.path.join(out_dir, RML_DIR, "types.csv"),
                                RML_NODE_FILES["types.csv"])
        # One-row source for the SKOS scheme declaration, so the RML path
        # derives <mk:EntryTypeScheme> a skos:ConceptScheme (+ label) from
        # the mapping like everything else. Those two triples were in the
        # published graph but in NO source in the repository — not the
        # .yarrrml, not the committed .ttl — so the artifact could not be
        # reproduced. Now it can.
        scheme = files.open_csv("scheme.csv",
                                os.path.join(out_dir, RML_DIR, "scheme.csv"),
                                RML_NODE_FILES["scheme.csv"])
        scheme.writerow([rdf.SCHEME_IRI, rdf.SCHEME_LABEL])
        rows["scheme.csv"] = 1
        seen_types: set[str] = set()
        for node in nodes():
            kind = node.get("kind")
            nodes_by_kind[kind or "(none)"] += 1
            if kind == "frame":
                frames.writerow([node["id"], node.get("label") or "",
                                 node.get("category") or "",
                                 node.get("status") or ""])
                rows["frames.csv"] += 1
            elif kind == "entry_type_concept":
                slug = _strip("type:", node["id"])
                if slug not in seen_types:
                    seen_types.add(slug)
                    types_.writerow([slug, rdf.concept_pref_label(slug)])
                    rows["types.csv"] += 1
        rml_node_paths = dict(files.paths)

    # -- pass 2: RML edge CSVs ---------------------------------------------
    with _AtomicFiles() as files:
        writers = {
            etype: files.open_csv(name, os.path.join(out_dir, RML_DIR, name), header)
            for etype, (name, header) in EDGE_TYPE_TO_RML_FILE.items()}
        seen_edges: set[tuple[str, str, str]] = set()
        for edge in edges():
            etype = edge.get("type")
            edges_by_type[etype or "(none)"] += 1
            row = _rml_row(edge)
            if row is None:
                continue
            key = (etype, *row)
            if key in seen_edges:
                continue            # RDF is a set; so is each RML file
            seen_edges.add(key)
            writers[etype].writerow(row)
            rows[EDGE_TYPE_TO_RML_FILE[etype][0]] += 1
        rml_edge_paths = dict(files.paths)

    # -- pass 3 (+4): property-graph view CSVs -----------------------------
    tag_allowed: set[str] | None = None
    if top_tags and "tag_concept" not in exclude:
        degree: Counter[str] = Counter()
        for edge in edges():
            if edge.get("type") == "hasTag":
                degree[edge["dst"]] += 1
        tag_allowed = {t for t, _ in degree.most_common(top_tags)}

    kept: set[str] = set()
    with _AtomicFiles() as files:
        pg_nodes = files.open_csv("kg_view_nodes.csv",
                                  os.path.join(out_dir, "kg_view_nodes.csv"),
                                  PG_NODES_HEADER)
        for node in nodes():
            kind = node.get("kind")
            if kind in exclude:
                continue
            if (kind == "tag_concept" and tag_allowed is not None
                    and node["id"] not in tag_allowed):
                continue
            kept.add(node["id"])
            pg_nodes.writerow([node["id"], node.get("label") or "", kind,
                               node.get("category") or "",
                               node.get("status") or ""])
            rows["kg_view_nodes.csv"] += 1
        pg_node_path = files.paths["kg_view_nodes.csv"]

    with _AtomicFiles() as files:
        pg_edges = files.open_csv("kg_view_edges.csv",
                                  os.path.join(out_dir, "kg_view_edges.csv"),
                                  PG_EDGES_HEADER)
        # A set here too, so that with no view filters this file, the RML
        # CSVs and graph.nt all describe the same edges row-for-row. The
        # exporter this replaces wrote a bag, which is why its counts could
        # never be reconciled against the RDF.
        seen_pg: set[tuple[str, str, str]] = set()
        for edge in edges():
            key = (edge["src"], edge["type"], edge["dst"])
            if key in seen_pg or not (edge["src"] in kept and edge["dst"] in kept):
                continue
            seen_pg.add(key)
            pg_edges.writerow([edge["src"], edge["dst"], edge["type"]])
            rows["kg_view_edges.csv"] += 1
        pg_edge_path = files.paths["kg_view_edges.csv"]

    # -- pass 5: RDF ---------------------------------------------------------
    nt_path = os.path.join(out_dir, "graph.nt")
    nt_tmp = nt_path + ".tmp"
    nt = rdf.write_nt(nodes(), edges(), nt_tmp, stamps=stamps)
    os.replace(nt_tmp, nt_path)

    # -- manifest, last ---------------------------------------------------------
    files_out: dict[str, dict[str, Any]] = {}
    for name, path in {**rml_node_paths, **rml_edge_paths,
                       "kg_view_nodes.csv": pg_node_path,
                       "kg_view_edges.csv": pg_edge_path}.items():
        files_out[name] = {"path": os.path.relpath(path, out_dir),
                           "rows": rows[name], "sha256": _sha256(path)}
    files_out["graph.nt"] = {"path": "graph.nt", "triples": nt["triples"],
                             "sha256": nt["sha256"]}

    manifest = {
        "build_id": build_id,
        "stamps": stamps,
        "counts": {
            "nodes": sum(nodes_by_kind.values()),
            "edges": sum(edges_by_type.values()),
            "nodes_by_kind": dict(nodes_by_kind),
            "edges_by_type": dict(edges_by_type),
            "triples": nt["triples"],
        },
        "view_filters": {"exclude_kinds": sorted(exclude), "top_tags": top_tags},
        "files": files_out,
    }
    manifest_path = os.path.join(out_dir, "manifest.json")
    with open(manifest_path + ".tmp", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, default=str)
    os.replace(manifest_path + ".tmp", manifest_path)
    return manifest


def load_manifest(out_dir: str) -> dict[str, Any]:
    with open(os.path.join(out_dir, "manifest.json"), encoding="utf-8") as fh:
        return json.load(fh)
