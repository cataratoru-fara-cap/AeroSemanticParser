"""
kg/serialize.py — one build, every on-disk representation, one code path
===========================================================================
Pure: no Mongo, no Airflow. Given the nodes and edges of one build, writes
into a build directory:

    graph.nt                 the RDF graph (kg/rdf.py)             — the product
    rml_data/*.csv           the CSVs kg_config/kg_mapping.yarrrml.yml reads —
                             the input to the morph-kgc VALIDATION path
    kg_view_nodes.csv        a slim property-graph view for Cosmograph / Gephi
    kg_view_edges.csv        (the full property graph is Mongo and Neo4j)
    ontology.ttl             copy of kg_config/memeatlas.ttl, if given
    manifest.json            build_id, stamps, per-file sha256 and row counts

Every file is a projection of the SAME node/edge stream from kg/build.py.
The exporters this replaced derived the RDF by a second copy of the loop,
and the two drifted 14,563 edges apart unnoticed.

RML CSV shape
-------------
One wide CSV per node kind (optional columns left empty — morph-kgc emits
nothing for an empty cell, verified by probe), one CSV per list-valued frame
property, one CSV per edge type, and one OCCURRENCE CSV per edge type that
carries occurrences: a row per occurrence, which the mapping turns into
RDF-star annotations on the quoted edge. Every column is declared in the
tables below; tests/test_kg_vocabulary.py asserts these files are exactly
the sources the YARRRML mapping reads.

Inputs are factories, not iterables
-----------------------------------
``nodes`` and ``edges`` are zero-argument callables returning a fresh
iterable each time; each output walks the stream again rather than
buffering ~1M nodes. ``assume_unique=True`` (what the DAG passes, since the
store keys every node and edge by a unique ``_id``) also skips the dedupe
sets, which at this size would otherwise cost hundreds of MB.

Nothing is half-written: every file is ``<name>.tmp`` until complete, then
``os.replace``d; the manifest is written last.
"""
from __future__ import annotations

import contextlib
import csv
import hashlib
import json
import os
import shutil
from collections import Counter
from typing import Any, Callable, Iterable

from modules.kg import rdf
from modules.kg.build import (EDGE_TYPES, NODE_KINDS, OCCURRENCE_EDGE_TYPES,
                              OCCURRENCE_FIELDS)
from modules.kg.taxonomy import CONCEPT_EDGE_TYPES

__all__ = [
    "EDGE_TYPE_TO_RML_FILE", "RML_NODE_FILES", "RML_LIST_FILES", "RESERVED_COLUMNS",
    "OCCURRENCE_RML_FILES",
    "RML_CONCEPT_FILES", "PG_NODES_HEADER", "PG_EDGES_HEADER", "RML_DIR",
    "all_rml_files", "write_build", "load_manifest",
]

NodeSource = Callable[[], Iterable[dict]]
EdgeSource = Callable[[], Iterable[dict]]

RML_DIR = "rml_data"

# file -> (node kind, ((column, node property), ...)). "id" is rendered as
# the node's IRI; "category_class" is derived via rdf.category_class.
RML_NODE_FILES: dict[str, tuple[str, tuple[tuple[str, str], ...]]] = {
    "frames.csv": ("frame", (
        ("url", "id"), ("title", "label"), ("category_class", "category"),
        ("status", "status"), ("year", "year"), ("from", "from"),
        ("about", "about"), ("added", "added"), ("last_updated", "last_updated"),
        ("description", "description"), ("corpus_status", "corpus_status"),
        ("parser_version", "parser_version"), ("parsed_at", "parsed_at"),
        ("scraped_at", "scraped_at"))),
    "images.csv": ("image", (
        ("iri", "id"), ("width", "width"), ("height", "height"))),
}

# file -> (frame list property, value column)
RML_LIST_FILES: dict[str, tuple[str, str]] = {
    "frame_badges.csv": ("badges", "badge"),
    "frame_aliases.csv": ("aliases", "alias"),
    "frame_section_texts.csv": ("section_texts", "text"),
    "frame_corpus_missing.csv": ("corpus_missing", "missing"),
}

# Concept/scheme sources with their own shape.
RML_CONCEPT_FILES: dict[str, tuple[str, ...]] = {
    "types.csv": ("slug", "label"),
    "scheme.csv": ("iri", "label"),
}

# edge type -> (RML csv name, header). Values: ids are rendered through
# _rml_id, so type:/tag:/region:/image: prefixes become the slug, literal,
# or IRI the mapping expects.
EDGE_TYPE_TO_RML_FILE: dict[str, tuple[str, tuple[str, str]]] = {
    "hasEntryType":  ("entry_type_edges.csv", ("url", "slug")),
    "hasTag":        ("tag_edges.csv",        ("url", "tag")),
    "hasRegion":     ("region_edges.csv",     ("url", "region")),
    "partOfSeries":  ("series_edges.csv",     ("url", "parent_url")),
    "relatesToMeme": ("relates_edges.csv",    ("url", "target_url")),
    "citesExternal": ("cites_edges.csv",      ("url", "target_url")),
    "subTypeOf":     ("subtype_edges.csv",    ("narrower", "broader")),
    "hasImage":      ("image_edges.csv",      ("url", "image")),
}

# edge type -> (occurrence csv, header): one row per occurrence, every
# occurrence field a column (empty when absent). The mapping reads each as
# a quotedNonAsserted edge plus one annotation per column.
OCCURRENCE_RML_FILES: dict[str, tuple[str, tuple[str, ...]]] = {
    "relatesToMeme": ("relates_occurrences.csv", ("src", "dst") + OCCURRENCE_FIELDS),
    "citesExternal": ("cites_occurrences.csv", ("src", "dst") + OCCURRENCE_FIELDS),
    "hasImage": ("image_occurrences.csv", ("src", "dst") + OCCURRENCE_FIELDS),
}
# Column names are also morph-kgc dataframe columns once read, and morph-kgc
# uses some names itself: a CSV column called "subject" is silently
# overwritten, so every hasImage triple came out as <> mk:hasImage <img>.
# Found by the end-to-end probe; the vocabulary test forbids these names.
RESERVED_COLUMNS = frozenset({"subject", "predicate", "object", "graph"})

PG_NODES_HEADER = ("id", "label", "kind", "category", "status")
PG_EDGES_HEADER = ("source", "target", "type")

assert set(EDGE_TYPE_TO_RML_FILE) == set(EDGE_TYPES) | set(CONCEPT_EDGE_TYPES), (
    "serialize.py's RML file table drifted from the edge vocabulary")
assert {k for k, _ in RML_NODE_FILES.values()} | {"frame_stub", "entry_type_concept",
        "tag_concept", "region_concept", "external_ref"} == set(NODE_KINDS), (
    "serialize.py's node file table drifted from NODE_KINDS")
assert set(OCCURRENCE_RML_FILES) == set(OCCURRENCE_EDGE_TYPES), (
    "serialize.py's occurrence file table drifted from OCCURRENCE_EDGE_TYPES")


def all_rml_files() -> set[str]:
    """Every file under rml_data/ a build writes."""
    return (set(RML_NODE_FILES) | set(RML_LIST_FILES) | set(RML_CONCEPT_FILES)
            | {name for name, _ in EDGE_TYPE_TO_RML_FILE.values()}
            | {name for name, _ in OCCURRENCE_RML_FILES.values()})


_ID_PREFIXES = ("type:", "tag:", "region:", "image:")


def _rml_id(value: str) -> str:
    for prefix in _ID_PREFIXES:
        if value.startswith(prefix):
            return value[len(prefix):]
    return value


def _cell(value: Any) -> str:
    return "" if value in (None, "") else str(value)


def _node_row(node: dict, columns: tuple[tuple[str, str], ...]) -> list[str]:
    row = []
    for column, prop in columns:
        if prop == "id":
            row.append(rdf.node_iri(node["id"]))
        elif column == "category_class":
            row.append(_cell(rdf.category_class(node.get("category"))))
        else:
            row.append(_cell(node.get(prop)))
    return row


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
                exclude_kinds: Iterable[str] = (), top_tags: int = 0,
                ontology_path: str | None = None,
                assume_unique: bool = False) -> dict[str, Any]:
    """Write every representation of one build into ``out_dir``.

    ``exclude_kinds`` / ``top_tags`` shape ONLY the view CSVs; the RDF and
    RML outputs are always the full graph. The filters are recorded in the
    manifest.
    """
    stamps = dict(stamps or {})
    stamps.setdefault("build_id", build_id)
    exclude = set(exclude_kinds)
    unknown = exclude - set(NODE_KINDS)
    if unknown:
        raise ValueError(f"exclude_kinds not in NODE_KINDS: {sorted(unknown)}")

    rml = os.path.join(out_dir, RML_DIR)
    os.makedirs(rml, exist_ok=True)
    rows: Counter[str] = Counter()
    nodes_by_kind: Counter[str] = Counter()
    edges_by_type: Counter[str] = Counter()
    paths: dict[str, str] = {}

    # -- pass 1: RML node, list and concept CSVs ----------------------------
    with _AtomicFiles() as files:
        node_writers = {
            kind: (name, files.open_csv(name, os.path.join(rml, name),
                                        [c for c, _ in columns]), columns)
            for name, (kind, columns) in RML_NODE_FILES.items()}
        list_writers = {
            prop: (name, files.open_csv(name, os.path.join(rml, name), ("url", column)))
            for name, (prop, column) in RML_LIST_FILES.items()}
        types_w = files.open_csv("types.csv", os.path.join(rml, "types.csv"),
                                 RML_CONCEPT_FILES["types.csv"])
        scheme_w = files.open_csv("scheme.csv", os.path.join(rml, "scheme.csv"),
                                  RML_CONCEPT_FILES["scheme.csv"])
        scheme_w.writerow([rdf.SCHEME_IRI, rdf.SCHEME_LABEL])
        rows["scheme.csv"] = 1

        seen_types: set[str] = set()
        for node in nodes():
            kind = node.get("kind")
            nodes_by_kind[kind or "(none)"] += 1
            if kind in node_writers:
                name, writer, columns = node_writers[kind]
                writer.writerow(_node_row(node, columns))
                rows[name] += 1
            if kind == "frame":
                url = node["id"]
                for prop, (name, writer) in list_writers.items():
                    for value in node.get(prop) or []:
                        if value not in (None, ""):
                            writer.writerow([url, value])
                            rows[name] += 1
            elif kind == "entry_type_concept":
                slug = _rml_id(node["id"])
                if slug not in seen_types:
                    seen_types.add(slug)
                    types_w.writerow([slug, rdf.concept_pref_label(slug)])
                    rows["types.csv"] += 1
        paths.update(files.paths)

    # -- pass 2: RML edge and occurrence CSVs ---------------------------------
    with _AtomicFiles() as files:
        writers = {
            etype: files.open_csv(name, os.path.join(rml, name), header)
            for etype, (name, header) in EDGE_TYPE_TO_RML_FILE.items()}
        occ_writers = {
            etype: (name, files.open_csv(name, os.path.join(rml, name), header))
            for etype, (name, header) in OCCURRENCE_RML_FILES.items()}
        seen_edges: set[tuple[str, str, str]] = set()
        for edge in edges():
            etype = edge.get("type")
            edges_by_type[etype or "(none)"] += 1
            if etype not in writers or not edge.get("src") or not edge.get("dst"):
                continue
            row = (_rml_id(edge["src"]), _rml_id(edge["dst"]))
            if not assume_unique:
                key = (etype, *row)
                if key in seen_edges:
                    continue            # RDF is a set; so is each RML file
                seen_edges.add(key)
            writers[etype].writerow(row)
            rows[EDGE_TYPE_TO_RML_FILE[etype][0]] += 1
            if etype in occ_writers:
                name, occ_writer = occ_writers[etype]
                for occ in edge.get("occurrences") or ():
                    occ_writer.writerow([*row, *(_cell(occ.get(f)) for f in OCCURRENCE_FIELDS)])
                    rows[name] += 1
        paths.update(files.paths)

    # -- pass 3 (+4): property-graph view CSVs ------------------------------
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
        paths.update(files.paths)

    with _AtomicFiles() as files:
        pg_edges = files.open_csv("kg_view_edges.csv",
                                  os.path.join(out_dir, "kg_view_edges.csv"),
                                  PG_EDGES_HEADER)
        seen_pg: set[tuple[str, str, str]] = set()
        for edge in edges():
            if not (edge["src"] in kept and edge["dst"] in kept):
                continue
            key = (edge["src"], edge["type"], edge["dst"])
            if not assume_unique:
                if key in seen_pg:
                    continue
                seen_pg.add(key)
            pg_edges.writerow([edge["src"], edge["dst"], edge["type"]])
            rows["kg_view_edges.csv"] += 1
        paths.update(files.paths)

    # -- pass 5: RDF ---------------------------------------------------------
    nt_path = os.path.join(out_dir, "graph.nt")
    nt = rdf.write_nt(nodes(), edges(), nt_path + ".tmp", stamps=stamps,
                      dedupe=not assume_unique)
    os.replace(nt_path + ".tmp", nt_path)

    # -- ontology --------------------------------------------------------------
    files_out: dict[str, dict[str, Any]] = {}
    if ontology_path:
        onto = os.path.join(out_dir, "ontology.ttl")
        shutil.copyfile(ontology_path, onto + ".tmp")
        os.replace(onto + ".tmp", onto)
        files_out["ontology.ttl"] = {"path": "ontology.ttl", "sha256": _sha256(onto)}

    # -- manifest, last ---------------------------------------------------------
    for name, path in paths.items():
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
