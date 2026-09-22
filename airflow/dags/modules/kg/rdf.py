"""
kg/rdf.py — (nodes, edges) -> canonical N-Triples, in IMKG's vocabulary
==========================================================================
Pure: no Mongo, no Airflow, no rdflib. N-Triples is one triple per line, so
serializing it directly keeps the RDF path dependency-free and streaming.

This is the product path. The YARRRML mapping in
``dags/kg_config/kg_mapping.yarrrml.yml`` is an independent VALIDATION path:
kym_kg_validate compiles it, runs morph-kgc over the RML CSVs, and diffs the
result against this module's output (kg/ntdiff.py).

The crosswalk: MemeAtlas extends IMKG
-------------------------------------
kg/build.py speaks property-graph names; this module decides what each one
means in RDF. Where IMKG (Tommasini, Ilievski & Wijesiriwardene, ESWC 2023,
github.com/riccardotommasini/imkg) already models a thing, its terms are
reused VERBATIM, so an IMKG query runs unchanged against MemeAtlas:

    frame                   a m4s:MediaFrame ; a kym:<Category>
    hasEntryType            rdf:type kymt:<slug>
    label (title)           m4s:title  (and rdfs:label for generic tools)
    status / year / from    m4s:status / m4s:year / m4s:from
    about                   m4s:about
    added / last_updated    m4s:added / m4s:last_update_source
    hasTag                  m4s:tag "<literal>"
    partOfSeries            skos:broader  (+ skos:narrower, as IMKG emits)
    fromAbout / fromTags    m4s:fromAbout / m4s:fromTags -> a Wikidata item
                            (6.1.0; IMKG's textual-enrichment predicates)

Everything else is a MemeAtlas extension under ``mk:`` and is declared,
with its alignment to IMKG / schema.org / SKOS, in
``kg_config/memeatlas.ttl``. tests/test_kg_vocabulary.py asserts that every
``mk:`` term this module can emit is declared there.

Occurrences are RDF-star annotations
------------------------------------
kg/build.py puts what is particular to one mention of a target — the anchor
text of a link, the heading it sat under, a citation's text and number, an
image's role, alt text and caption — in the ``occurrences`` list of the
frame-level edge. RDF has no edge properties, so each distinct
(field, value) becomes an annotation on the quoted edge::

    <frame> mk:citesExternal <url> .
    << <frame> mk:citesExternal <url> >> mk:citationText "Wikipedia – Doge" .

The edge itself is still asserted, so a plain SPARQL query sees the same
graph as before and ``<< ?f mk:citesExternal ?u >> mk:citationText ?t``
reads the annotation (verified on Fuseki / Jena 5.1). Annotations are a
set: when one frame mentions the same target twice, the property graph
keeps which anchor text went with which heading; RDF keeps both values.

Deliberate differences from IMKG, recorded so nobody rediscovers them:
  * ``m4s:year`` is typed ``xsd:integer`` (IMKG leaves it untyped) so SPARQL
    range filters work; ``m4s:added`` / ``m4s:last_update_source`` are
    ``xsd:dateTime`` (IMKG's mapping says ``xsd:timestamp``, which is not an
    XSD datatype).
  * The curated entry-type hierarchy is ``rdfs:subClassOf`` between the
    ``kymt:`` classes, not ``skos:broader`` — IMKG uses ``skos:broader``
    for frame series, and one predicate must not carry two meanings.
  * ``kym:<Category>`` casing follows the paper's ``kym:Meme``; IMKG's raw
    category values are not published, so exact byte-equality with IMKG's
    class IRIs is unverified.
  * ``mk:badge`` (5.0.0) is an ``owl:ObjectProperty`` to a ``badge_concept``
    resource, not the ``owl:DatatypeProperty`` literal it was in 4.0.0 — a
    documented ontology break, not a silent one.
  * Wikidata items (6.1.0) are their canonical entity IRIs,
    ``http://www.wikidata.org/entity/Q42`` — IMKG emitted the HTML page URL
    ``https://www.wikidata.org/wiki/Q42``, which names a document ABOUT the
    item, and which no Wikidata query or dump uses. With the entity IRI a
    federated ``SERVICE <https://query.wikidata.org/sparql>`` joins as is.

Scope rules that keep this path and the RML path in agreement:
  * Only nodes with a declared class get node triples; ``frame_stub``,
    ``external_ref`` and ``tag_concept`` never do (the latter is a literal
    in RDF, per IMKG's own tag-as-literal convention). ``region_concept``
    is likewise a literal. ``origin_concept``/``badge_concept`` (5.0.0) DO
    get node triples (``skos:Concept``, no ``rdfs:Class``) — unlike tags,
    these are MemeAtlas's own concepts with no IMKG literal convention to
    match. An ``image`` gets its class and size. A ``wikidata_entity``
    (6.1.0) gets its ``rdfs:label`` and NO class: the resource is
    Wikidata's, and asserting a MemeAtlas class on it would be a statement
    about somebody else's item.
  * ``coOccursWith`` (kg/cooccurs.py) never reaches RDF (5.0.1): tags are
    its only source now (entry_type's was removed — needless alongside
    its curated subTypeOf hierarchy) and tag_concept has no RDF resource
    for a triple to attach to. Property-graph-only, unconditionally; not
    in EDGE_PREDICATES at all. See ``kg_config/MODEL.md``'s
    "Not yet modelled".
  * An absent value emits nothing — morph-kgc emits nothing for an empty
    CSV cell, verified by probe.
  * Set semantics: each distinct (s, p, o) once.
"""
from __future__ import annotations

import hashlib
from typing import Any, Iterable, Iterator

__all__ = [
    "PREFIXES", "TYPES_BASE", "KYM_CLASS_BASE", "SCHEME_IRI", "SCHEME_CLASS",
    "SCHEME_LABEL", "ORIGIN_SCHEME_IRI", "ORIGIN_SCHEME_LABEL",
    "BADGE_SCHEME_IRI", "BADGE_SCHEME_LABEL", "NODE_SCHEMES", "NODE_CLASSES",
    "WD",
    "NODE_LITERALS", "EDGE_PREDICATES", "OCCURRENCE_PREDICATES",
    "PROVENANCE_PREDICATES", "escape_literal",
    "concept_pref_label", "category_class", "node_iri", "edge_object",
    "quoted", "all_predicates", "constant_classes", "iter_triples", "write_nt",
]

PREFIXES: dict[str, str] = {
    "m4s": "https://meme4.science/",               # IMKG
    "mk": "https://meme4.science/atlas/",          # MemeAtlas extension
    "kym": "https://knowyourmeme.com/memes/",      # IMKG: category classes
    "kymt": "https://knowyourmeme.com/types/",     # IMKG: entry-type classes
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    # 6.1.0: the linked entities' own IRIs (kg/wikidata.WD_ENTITY). Objects
    # only — MemeAtlas never uses a wd: term as a predicate or a class.
    "wd": "http://www.wikidata.org/entity/",
}
M4S, MK, KYM, KYMT = (PREFIXES[p] for p in ("m4s", "mk", "kym", "kymt"))
WD = PREFIXES["wd"]
SKOS, RDFS, RDF, XSD = (PREFIXES[p] for p in ("skos", "rdfs", "rdf", "xsd"))

TYPES_BASE = KYMT
KYM_CLASS_BASE = KYM
RDF_TYPE = RDF + "type"
XSD_INTEGER = XSD + "integer"
XSD_DATETIME = XSD + "dateTime"
XSD_DECIMAL = XSD + "decimal"

# The SKOS scheme every entry_type concept is in. An skos:inScheme pointing
# at an undeclared resource is incomplete SKOS; these two triples were in
# the old published graph but derivable from nothing in the repository. The
# RML path derives them from a one-row scheme.csv.
SCHEME_IRI = MK + "EntryTypeScheme"
SCHEME_CLASS = SKOS + "ConceptScheme"
SCHEME_LABEL = "MemeAtlas entry-type taxonomy"

# 5.0.0: two more concept kinds, each with their own scheme (see
# NODE_SCHEMES below) — origin_concept/badge_concept are MemeAtlas
# inventions (unlike entry_type_concept's kymt:, IMKG's own namespace), so
# their scheme IRIs live under mk: too.
ORIGIN_SCHEME_IRI = MK + "OriginScheme"
ORIGIN_SCHEME_LABEL = "MemeAtlas origin taxonomy"
BADGE_SCHEME_IRI = MK + "BadgeScheme"
BADGE_SCHEME_LABEL = "MemeAtlas badge taxonomy"

# concept node kind -> (scheme IRI, scheme label). Generalizes the single
# entry_type scheme above to every concept kind that has one; iter_triples
# declares each scheme once, on first sight of a node of its kind.
NODE_SCHEMES: dict[str, tuple[str, str]] = {
    "entry_type_concept": (SCHEME_IRI, SCHEME_LABEL),
    "origin_concept": (ORIGIN_SCHEME_IRI, ORIGIN_SCHEME_LABEL),
    "badge_concept": (BADGE_SCHEME_IRI, BADGE_SCHEME_LABEL),
}

# node kind -> classes every node of that kind has. ``frame`` also gets
# kym:<Category>. origin_concept/badge_concept get no rdfs:Class (unlike
# entry_type_concept): hasOrigin/hasBadge are plain object properties to a
# concept resource, not rdf:type class-membership.
NODE_CLASSES: dict[str, tuple[str, ...]] = {
    "frame": (M4S + "MediaFrame",),
    "entry_type_concept": (RDFS + "Class", SKOS + "Concept"),
    "origin_concept": (SKOS + "Concept",),
    "badge_concept": (SKOS + "Concept",),
    "image": (MK + "Image",),
    # 6.0.0. mk:Event is aligned to sem:Event (EventKG's own base) and
    # schema:Event in memeatlas.ttl, but MemeAtlas emits only its own term
    # — the same treatment mk:Image gets against schema:ImageObject.
    # Aligning costs one @prefix line in the ontology; EMITTING a foreign
    # class would cost an entry in PREFIXES, one in the YARRRML prefixes
    # block, a member of constant_classes(), and ~120k redundant triples
    # asserting a class MemeAtlas does not own and cannot comment on.
    "event": (MK + "Event",),
    # 6.1.0. Deliberately EMPTY, not absent: the node still gets its label
    # (NODE_LITERALS below), but no rdf:type — see the module docstring.
    "wikidata_entity": (),
}

# node kind -> (property, predicate, datatype | None). A list-valued
# property yields one triple per item.
NODE_LITERALS: dict[str, tuple[tuple[str, str, str | None], ...]] = {
    "frame": (
        ("label", M4S + "title", None),
        ("label", RDFS + "label", None),
        ("status", M4S + "status", None),
        ("year", M4S + "year", XSD_INTEGER),
        ("from", M4S + "from", None),
        ("about", M4S + "about", None),
        # 5.1.0: IMKG's own terms for the two narrative sections that used
        # to be deferred. They are DATATYPE properties there — IMKG keeps
        # About/Origin/Spread as literals on the frame — so the event layer
        # hangs off mk: terms of its own rather than re-typing these.
        ("origin_text", M4S + "origin", None),
        ("spread_text", M4S + "spread", None),
        ("added", M4S + "added", XSD_DATETIME),
        ("last_updated", M4S + "last_update_source", XSD_DATETIME),
        ("description", MK + "description", None),
        ("aliases", SKOS + "altLabel", None),
        ("section_texts", MK + "sectionText", None),
        ("corpus_status", MK + "corpusStatus", None),
        ("corpus_missing", MK + "corpusMissing", None),
        ("parser_version", MK + "parserVersion", None),
        ("parsed_at", MK + "parsedAt", XSD_DATETIME),
        ("scraped_at", MK + "scrapedAt", XSD_DATETIME),
    ),
    "entry_type_concept": (
        ("label", SKOS + "prefLabel", None),       # rendered by concept_pref_label
    ),
    "origin_concept": (
        ("label", SKOS + "prefLabel", None),
    ),
    "badge_concept": (
        ("label", SKOS + "prefLabel", None),
    ),
    "image": (
        ("width", MK + "width", XSD_INTEGER),
        ("height", MK + "height", XSD_INTEGER),
    ),
    # 6.0.0. ``date`` is deliberately ABSENT: it is recoverable from
    # (eventStart, datePrecision), and a CSV column of bare years is the
    # one value in the whole RML surface pandas could read as a number
    # inside morph-kgc — the same family of bug as the two real KYM tags
    # spelled "null" that forced na_values to be empty. ``actors`` is
    # list-valued, so it yields one triple per actor.
    "event": (
        ("source_text", MK + "sourceText", None),
        ("source_section", MK + "sourceSection", None),
        ("date_precision", MK + "datePrecision", None),
        ("date_basis", MK + "dateBasis", None),
        ("date_text", MK + "dateText", None),
        # xsd:dateTime, not xsd:date: sem:hasBeginTimeStamp's range is
        # dateTime, so xsd:date under an rdfs:subPropertyOf would be a
        # range mismatch — and it keeps build.iso_utc's strftime the one
        # place this repo formats a timestamp.
        ("date_start", MK + "eventStart", XSD_DATETIME),
        ("date_end", MK + "eventEnd", XSD_DATETIME),
        ("location", MK + "eventLocation", None),
        ("location_type", MK + "locationType", None),
        ("certainty", MK + "certainty", None),
        ("actors", MK + "eventActor", None),
        ("extraction_model", MK + "extractionModel", None),
        ("extraction_version", MK + "extractionVersion", None),
    ),
    # 6.1.0. The label only: the description is Wikidata's to state (and a
    # consumer who wants it has the IRI), and mk:description's domain is
    # m4s:MediaFrame, which would make every linked item a media frame.
    "wikidata_entity": (
        ("label", RDFS + "label", None),
    ),
}

# edge type -> (predicate, object is a literal?, inverse predicate | None)
EDGE_PREDICATES: dict[str, tuple[str, bool, str | None]] = {
    "hasEntryType": (RDF_TYPE, False, None),
    "hasTag": (M4S + "tag", True, None),
    "hasRegion": (MK + "region", True, None),
    "hasOrigin": (MK + "hasOrigin", False, None),
    "hasBadge": (MK + "badge", False, None),      # was a literal; see memeatlas.ttl
    "partOfSeries": (SKOS + "broader", False, SKOS + "narrower"),
    "relatesToMeme": (MK + "relatesToMeme", False, None),
    "citesExternal": (MK + "citesExternal", False, None),
    "subTypeOf": (RDFS + "subClassOf", False, None),
    "hasImage": (MK + "hasImage", False, None),
    "hasEvent": (MK + "hasEvent", False, None),      # 6.0.0
    # From an event to what it was attached to by position (kg/events.py).
    "eventLink": (MK + "eventLink", False, None),
    "eventCitation": (MK + "eventCitation", False, None),
    "eventEmbed": (MK + "eventEmbed", False, None),
    "eventImage": (MK + "eventImage", False, None),
    "eventDateAnchor": (MK + "dateAnchoredTo", False, None),
    # 6.1.0: frame -> a Wikidata item, named by the field the mention was
    # read from. The first two are IMKG's own predicates, verbatim (kym/
    # mappings/kym.media.frames.textual.enrichment.yaml in the IMKG repo);
    # IMKG never linked titles, so that one is MemeAtlas's.
    "fromTitle": (MK + "fromTitle", False, None),
    "fromTags": (M4S + "fromTags", False, None),
    "fromAbout": (M4S + "fromAbout", False, None),
    # coOccursWith is NOT here (5.0.1): entry_type's statistical edges were
    # removed (needless alongside its curated subTypeOf hierarchy), and
    # tags — the only remaining source — have no RDF resource to attach a
    # triple to (tag_concept; hasTag's object is a literal). So
    # coOccursWith is property-graph-only, unconditionally, and needs no
    # entry in this table: the edge loop below skips any type not listed
    # here, same as it always has for an unrecognized type.
}

# occurrence field -> (annotation predicate, datatype | None). Annotates the
# quoted frame-level edge; see "Occurrences are RDF-star annotations".
OCCURRENCE_PREDICATES: dict[str, tuple[str, str | None]] = {
    "anchor_text": (MK + "anchorText", None),
    "in_section": (MK + "inSection", None),
    "citation_text": (MK + "citationText", None),
    "citation_index": (MK + "citationIndex", XSD_INTEGER),
    "site_name": (MK + "siteName", None),
    "role": (MK + "imageRole", None),
    "alt_text": (MK + "altText", None),
    "caption": (MK + "caption", None),
    # 6.1.0, on the quoted fromTitle/fromTags/fromAbout edge.
    "mention_text": (MK + "mentionText", None),
    "link_score": (MK + "linkScore", XSD_DECIMAL),
    "link_method": (MK + "linkMethod", None),
    "ner_label": (MK + "nerLabel", None),
}

# Triples describing the build rather than the corpus. The RML path has no
# notion of a build, so these are the ONE legitimate difference the diff
# gate tolerates.
_STAMP_PREDICATES = (("build_id", "buildId"), ("snapshot_at", "snapshotAt"),
                     ("kg_build_version", "kgBuildVersion"),
                     ("taxonomy_version", "taxonomyVersion"))
PROVENANCE_PREDICATES: tuple[str, ...] = tuple(MK + p for _, p in _STAMP_PREDICATES)
CURRENT_BUILD = MK + "currentBuild"

_ID_PREFIXES_TO_STRIP = ("tag:", "region:")


_ESCAPES = str.maketrans({
    "\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t",
})


def escape_literal(value: Any) -> str:
    """Escape a string for an N-Triples quoted literal.

    1,141 frame titles contain a quote or backslash; section text contains
    newlines and tabs. kg/ntdiff.py compares literals by decoded value, so
    this may escape more than morph-kgc does (it writes TAB raw) without the
    gate calling it a difference.
    """
    return str(value).translate(_ESCAPES)


def concept_pref_label(label: str) -> str:
    """``image-macro`` -> ``image macro``: a prefLabel is for people."""
    return str(label).replace("-", " ")


def category_class(category: str | None) -> str | None:
    """KYM category -> IMKG's class local name (``meme`` -> ``Meme``)."""
    if not category or category == "unknown":
        return None
    return str(category).capitalize()


def node_iri(node_id: str) -> str:
    """The IRI for a property-graph node id."""
    if node_id.startswith("type:"):
        return KYMT + node_id[len("type:"):]
    if node_id.startswith("origin:"):
        return MK + "origin/" + node_id[len("origin:"):]
    if node_id.startswith("badge:"):
        return MK + "badge/" + node_id[len("badge:"):]
    if node_id.startswith("event:"):
        return MK + "event/" + node_id[len("event:"):]
    if node_id.startswith("image:"):
        return node_id[len("image:"):]
    if node_id.startswith("wd:"):
        return WD + node_id[len("wd:"):]
    return node_id


def edge_object(etype: str, dst: str) -> str:
    """The object of an edge's triple: a literal value or an IRI."""
    _, is_literal, _ = EDGE_PREDICATES[etype]
    if is_literal:
        for prefix in _ID_PREFIXES_TO_STRIP:
            if dst.startswith(prefix):
                return dst[len(prefix):]
        return dst
    return node_iri(dst)


def quoted(s: str, pred: str, obj: str) -> str:
    """An RDF-star quoted triple term: ``<< <s> <p> <o> >>``. IRIs only —
    every annotated edge has an IRI object."""
    return f"<< <{s}> <{pred}> <{obj}> >>"


def all_predicates(include_provenance: bool = False) -> set[str]:
    """Every predicate IRI this module can emit (not counting rdf:type)."""
    preds = {p for rows in NODE_LITERALS.values() for _, p, _ in rows}
    preds.update(p for p, _ in OCCURRENCE_PREDICATES.values())
    for pred, _, inverse in EDGE_PREDICATES.values():
        preds.add(pred)
        if inverse:
            preds.add(inverse)
    preds.add(SKOS + "inScheme")
    preds.add(RDFS + "label")
    if include_provenance:
        preds.update(PROVENANCE_PREDICATES)
    preds.discard(RDF_TYPE)
    return preds


def constant_classes() -> set[str]:
    """Every class IRI emitted from a constant (not from a data value)."""
    classes = {c for cs in NODE_CLASSES.values() for c in cs}
    classes.add(SCHEME_CLASS)
    return classes


def _literal(value: Any, datatype: str | None) -> str:
    body = f'"{escape_literal(value)}"'
    return f"{body}^^<{datatype}>" if datatype else body


def iter_triples(nodes: Iterable[dict], edges: Iterable[dict], *,
                 stamps: dict[str, Any] | None = None,
                 dedupe: bool = True) -> Iterator[str]:
    """Stream canonical N-Triples lines for one build.

    ``dedupe`` keeps a set of 8-byte digests of emitted lines. The DAG
    passes False: its input comes from the store, where every node and edge
    is unique by ``_id``, and every mapping below is injective on unique
    input — so dedupe would only cost ~4M digests of memory.
    """
    seen: set[bytes] = set()

    def emit(line: str) -> str | None:
        if dedupe:
            h = hashlib.blake2b(line.encode("utf-8"), digest_size=8).digest()
            if h in seen:
                return None
            seen.add(h)
        return line

    scheme_declared: set[str] = set()   # scheme IRIs already declared, by kind's scheme

    for node in nodes:
        kind = node.get("kind")
        if kind not in NODE_CLASSES:
            continue
        s = node_iri(node["id"])

        classes = list(NODE_CLASSES[kind])
        if kind == "frame":
            cat = category_class(node.get("category"))
            if cat:
                classes.append(KYM + cat)
        for cls in classes:
            line = emit(f"<{s}> <{RDF_TYPE}> <{cls}> .")
            if line:
                yield line

        scheme = NODE_SCHEMES.get(kind)
        if scheme:
            scheme_iri, scheme_label = scheme
            if scheme_iri not in scheme_declared:
                scheme_declared.add(scheme_iri)
                for line in (f"<{scheme_iri}> <{RDF_TYPE}> <{SCHEME_CLASS}> .",
                             f"<{scheme_iri}> <{RDFS}label> {_literal(scheme_label, None)} ."):
                    line = emit(line)
                    if line:
                        yield line
            line = emit(f"<{s}> <{SKOS}inScheme> <{scheme_iri}> .")
            if line:
                yield line

        for prop, pred, datatype in NODE_LITERALS.get(kind, ()):
            value = node.get(prop)
            if value in (None, "", []):
                continue
            if kind in ("entry_type_concept", "origin_concept") and prop == "label":
                value = concept_pref_label(value)     # "united-states" -> "united states"
            for v in (value if isinstance(value, list) else [value]):
                if v in (None, ""):
                    continue
                line = emit(f"<{s}> <{pred}> {_literal(v, datatype)} .")
                if line:
                    yield line

    for edge in edges:
        etype = edge.get("type")
        if etype not in EDGE_PREDICATES:
            # coOccursWith always lands here (not a recognized predicate;
            # property-graph-only, see the module docstring) — same path
            # as any other type this module doesn't know.
            continue
        pred, is_literal, inverse = EDGE_PREDICATES[etype]
        s = node_iri(edge["src"])
        o = edge_object(etype, edge["dst"])
        obj = _literal(o, None) if is_literal else f"<{o}>"
        line = emit(f"<{s}> <{pred}> {obj} .")
        if line:
            yield line
        if inverse:
            line = emit(f"<{o}> <{inverse}> <{s}> .")
            if line:
                yield line
        occurrences = edge.get("occurrences")
        if occurrences and not is_literal:
            subject = quoted(s, pred, o)
            done: set[tuple[str, Any]] = set()      # a set per edge, always:
            for occ in occurrences:                 # repeats are common here
                for field, value in occ.items():
                    if value in (None, "") or field not in OCCURRENCE_PREDICATES \
                            or (field, value) in done:
                        continue
                    done.add((field, value))
                    ann_pred, datatype = OCCURRENCE_PREDICATES[field]
                    line = emit(f"{subject} <{ann_pred}> {_literal(value, datatype)} .")
                    if line:
                        yield line

    if stamps:
        for key, local in _STAMP_PREDICATES:
            value = stamps.get(key)
            if value in (None, ""):
                continue
            line = emit(f"<{CURRENT_BUILD}> <{MK}{local}> {_literal(value, None)} .")
            if line:
                yield line


def write_nt(nodes: Iterable[dict], edges: Iterable[dict], path: str, *,
             stamps: dict[str, Any] | None = None,
             dedupe: bool = True) -> dict[str, Any]:
    """Serialize to ``path``. Returns {triples, sha256, path}."""
    digest = hashlib.sha256()
    count = 0
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for line in iter_triples(nodes, edges, stamps=stamps, dedupe=dedupe):
            payload = line + "\n"
            fh.write(payload)
            digest.update(payload.encode("utf-8"))
            count += 1
    return {"triples": count, "sha256": digest.hexdigest(), "path": path}
