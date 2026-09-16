"""
kg/rdf.py — (nodes, edges) -> canonical N-Triples
====================================================
Pure: no Mongo, no Airflow, no rdflib. N-Triples is one triple per line, so
serializing it directly keeps the RDF path dependency-free and streaming —
important at ~800k triples, where building an in-memory graph object first
would cost more than the whole rest of the build.

This is the product path. The YARRRML/RML mapping in
``dags/kg_config/kg_mapping.yarrrml.yml`` is kept as a *validation* path: a
separate DAG compiles it, runs morph-kgc over the same corpus, and diffs the
two outputs (see kg/ntdiff.py). That gate exists because the two
representations previously drifted 14,571 triples apart without anything
noticing.

Vocabulary
----------
Predicate local names are identical to kg/build.py's edge ``type`` strings
and kg/taxonomy.py's ``CONCEPT_EDGE_TYPES``; tests/test_kg_vocabulary.py
pins that against the YARRRML file.

Three modelling choices are deliberate, and match what the RML path emits —
the diff gate is only meaningful if both sides agree on scope:

  * **Only real frames and entry_type concepts get node triples.** The RML
    mapping types rows of ``frames.csv`` and ``types.csv``; a ``frame_stub``
    appears in no CSV of its own, so it gets no ``rdf:type`` and no label.
    Emitting them here would show ~9,500 phantom divergences every run.
  * **Tags stay literals**, per the YARRRML note: 102k distinct tags with
    heavy singular/plural duplication are not clean enough to mint as SKOS
    concepts. The property graph does have ``tag_concept`` nodes, so this is
    a real asymmetry between the two representations, recorded here rather
    than discovered later.
  * **``external_ref`` nodes carry no triples of their own.** They exist in
    the property graph to give ``citesExternal`` a typed endpoint; in RDF
    the IRI is the endpoint.

Set semantics is load-bearing: RDF is a set, the property-graph edge list is
a bag. ``iter_triples`` emits each distinct (s, p, o) exactly once, or the
diff reports permanent phantom deltas on repeated edges.
"""
from __future__ import annotations

import hashlib
from typing import Any, Iterable, Iterator

__all__ = [
    "PREFIXES", "EDGE_TYPE_TO_PRED", "NODE_KIND_TO_CLASS", "NODE_ATTR_TO_PRED",
    "OBJECT_IS_LITERAL", "TYPES_BASE", "escape_literal", "node_iri",
    "iter_triples", "write_nt",
]

PREFIXES: dict[str, str] = {
    "mk": "https://meme4.science/atlas/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
}

# kg/build.py mints entry_type concept ids as "type:<slug>"; the mapping
# turns the slug into this IRI. Kept in one place so the two never drift.
TYPES_BASE = "https://knowyourmeme.com/types/"

RDF_TYPE = PREFIXES["rdf"] + "type"

# The SKOS scheme every entry_type concept declares itself in. The scheme
# itself must be declared too — an `skos:inScheme` pointing at an undeclared
# resource is incomplete SKOS. These two triples were in the published graph
# but derivable from NOTHING in the repository: not the .yarrrml, not the
# committed .rml.ttl, not a fresh compile, and no commit ever mentioned
# ConceptScheme. The artifact could not be reproduced from source. The RML
# path now derives them from a one-row scheme.csv via an `entry_type_scheme`
# mapping rule, and this module emits the same two triples directly.
SCHEME_IRI = PREFIXES["mk"] + "EntryTypeScheme"
SCHEME_CLASS = PREFIXES["skos"] + "ConceptScheme"
SCHEME_LABEL = "MemeAtlas entry-type taxonomy"

NODE_KIND_TO_CLASS: dict[str, str] = {
    "frame": PREFIXES["mk"] + "MemeFrame",
    "entry_type_concept": PREFIXES["skos"] + "Concept",
    # frame_stub / tag_concept / external_ref: deliberately absent, see above.
}

NODE_ATTR_TO_PRED: dict[str, str] = {
    "label": PREFIXES["rdfs"] + "label",
    "category": PREFIXES["mk"] + "category",
    "status": PREFIXES["mk"] + "status",
}

EDGE_TYPE_TO_PRED: dict[str, str] = {
    "hasEntryType": PREFIXES["mk"] + "hasEntryType",
    "hasTag": PREFIXES["mk"] + "hasTag",
    "partOfSeries": PREFIXES["mk"] + "partOfSeries",
    "relatesToMeme": PREFIXES["mk"] + "relatesToMeme",
    "citesExternal": PREFIXES["mk"] + "citesExternal",
    "broader": PREFIXES["skos"] + "broader",
}

# Edge types whose object is a plain literal rather than an IRI.
OBJECT_IS_LITERAL: frozenset[str] = frozenset({"hasTag"})

_ESCAPES = str.maketrans({
    "\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t",
})


def escape_literal(value: str) -> str:
    """Escape a string for an N-Triples quoted literal.

    Not theoretical: 1,141 frame titles in the live corpus contain a double
    quote or a backslash. None contain a newline, which is what makes the
    line-based diff in kg/ntdiff.py safe — but the escape is applied anyway,
    because "no newlines today" is a property of the data, not the schema.
    """
    return str(value).translate(_ESCAPES)


def concept_pref_label(label: str) -> str:
    """Render an entry_type slug as a human-readable skos:prefLabel.

    ``image-macro`` -> ``image macro``. The property graph keeps the raw
    slug as the node label because there it is an identifier; a prefLabel is
    for people, so the hyphens go. This mirrors what types.csv does
    (``slug.replace("-", " ")``) and therefore what the published graph
    already contains — 29 of the 119 slugs are affected.
    """
    return str(label).replace("-", " ")


def node_iri(node_id: str) -> str:
    """The IRI for a property-graph node id.

    Frame and external ids are already URLs. entry_type concepts are minted
    as ``type:<slug>`` and map onto the KYM types namespace, exactly as the
    RML mapping does. Tag ids never reach here — tags are literals.
    """
    if node_id.startswith("type:"):
        return TYPES_BASE + node_id[len("type:"):]
    return node_id


def _triple(subject: str, predicate: str, obj: str, *, literal: bool) -> str:
    tail = f'"{escape_literal(obj)}"' if literal else f"<{obj}>"
    return f"<{subject}> <{predicate}> {tail} ."


def iter_triples(nodes: Iterable[dict], edges: Iterable[dict], *,
                 stamps: dict[str, Any] | None = None) -> Iterator[str]:
    """Stream canonical N-Triples lines for one build.

    Each distinct (s, p, o) is emitted once. ``stamps`` adds provenance
    triples describing the build itself, so a SPARQL client can ask which
    build it is looking at and be sure the answer came from the same file
    as the data around it.
    """
    seen: set[str] = set()

    def emit(subject, predicate, obj, *, literal=False):
        line = _triple(subject, predicate, obj, literal=literal)
        if line not in seen:
            seen.add(line)
            return line
        return None

    for node in nodes:
        kind = node.get("kind")
        cls = NODE_KIND_TO_CLASS.get(kind)
        if cls is None:
            continue
        subject = node_iri(node["id"])

        line = emit(subject, RDF_TYPE, cls)
        if line:
            yield line

        if kind == "entry_type_concept":
            # Declare the scheme itself, once, the first time a concept
            # needs it — so a graph with no concepts carries no orphan
            # scheme triples.
            line = emit(SCHEME_IRI, RDF_TYPE, SCHEME_CLASS)
            if line:
                yield line
            line = emit(SCHEME_IRI, NODE_ATTR_TO_PRED["label"], SCHEME_LABEL,
                        literal=True)
            if line:
                yield line

            line = emit(subject, PREFIXES["skos"] + "inScheme", SCHEME_IRI)
            if line:
                yield line
            label = node.get("label")
            if label:
                line = emit(subject, PREFIXES["skos"] + "prefLabel",
                            concept_pref_label(label), literal=True)
                if line:
                    yield line
            continue

        for attr, predicate in NODE_ATTR_TO_PRED.items():
            value = node.get(attr)
            if value in (None, "", "None"):
                continue
            line = emit(subject, predicate, value, literal=True)
            if line:
                yield line

    for edge in edges:
        predicate = EDGE_TYPE_TO_PRED.get(edge.get("type"))
        if predicate is None:
            continue
        subject = node_iri(edge["src"])
        dst = edge["dst"]
        if edge["type"] in OBJECT_IS_LITERAL:
            # The property graph mints "tag:<label>"; RDF wants the label.
            obj = dst[len("tag:"):] if dst.startswith("tag:") else dst
            line = emit(subject, predicate, obj, literal=True)
        else:
            line = emit(subject, predicate, node_iri(dst))
        if line:
            yield line

    if stamps:
        current = PREFIXES["mk"] + "currentBuild"
        for key, predicate in (
            ("build_id", "buildId"), ("snapshot_at", "snapshotAt"),
            ("kg_build_version", "kgBuildVersion"),
            ("taxonomy_version", "taxonomyVersion"),
        ):
            value = stamps.get(key)
            if value in (None, ""):
                continue
            line = emit(current, PREFIXES["mk"] + predicate, str(value),
                        literal=True)
            if line:
                yield line


def write_nt(nodes: Iterable[dict], edges: Iterable[dict], path: str, *,
             stamps: dict[str, Any] | None = None) -> dict[str, Any]:
    """Serialize to ``path``. Returns {triples, sha256, path}.

    The sha256 is of the bytes written, and goes into the build manifest so
    a consumer can tell whether the file it holds is the one the run
    recorded. The July CSV artifacts carried no such stamp, which is why a
    450k-edge staleness went unnoticed for two months.
    """
    digest = hashlib.sha256()
    count = 0
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for line in iter_triples(nodes, edges, stamps=stamps):
            payload = line + "\n"
            fh.write(payload)
            digest.update(payload.encode("utf-8"))
            count += 1
    return {"triples": count, "sha256": digest.hexdigest(), "path": path}
