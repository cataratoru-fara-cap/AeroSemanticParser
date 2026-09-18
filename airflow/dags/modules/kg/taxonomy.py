"""
kg/taxonomy.py — the curated entry_type taxonomy, loaded and validated
=========================================================================
Pure: no Mongo, no Airflow. Reads ``dags/kg_config/entry_type_taxonomy.yaml``
— the reviewed, hand-authored record of which entry_type pairs are a real
is-a relation and which are not — and turns the two encodable buckets into
concept edges for the graph.

Why this module exists
----------------------
The YAML was previously read by nothing. Its 13 edges had been retyped by
hand into a ``CURATED_BROADER_EDGES`` tuple in the RML exporter, with a
comment asking whoever edits it to keep the two in sync manually. Two
things went wrong, both silently:

  1. The YAML did not parse (flow style + unquoted prose), so even someone
     who wanted to automate the sync could not. See the header in the YAML.
  2. The hand-typed constant included ``model -> influencer``, which the
     YAML filed under ``contested`` — "is a model a KIND of influencer or a
     co-occurring career? ... sample before promoting" — and which the
     exporter's own docstring says to exclude. It shipped anyway, as an
     asserted ``skos:broader`` triple in the published graph.

     That pair has since been sampled and promoted to ``broader_confirmed``
     by curator decision, so the edge itself was right. The defect was
     never the edge: it was that a transcribed constant could contradict
     the reviewed record for months with nothing able to notice.

So ``check_consistency`` below makes that class of error impossible:
a pair that appears in an encodable bucket *and* in ``contested`` (or in
``demoted`` or ``do_not_encode_as_broader``) is a hard failure, not a
warning. The reviewed record is the source of truth; nothing downstream
gets to disagree with it quietly.

Buckets
-------
Encoded as ``subTypeOf`` edges (RDF: ``rdfs:subClassOf``):
    broader_confirmed          semantics and statistics agree
    broader_semantic_only      semantically clear, statistically invisible

Never encoded, but carried into the run summary so they stay visible
rather than becoming folklore:
    contested                  stats and semantics disagree — sample first
    demoted                    stats suggested an edge, meaning rejected it
    do_not_encode_as_broader   real relation, wrong type (causal, part-of)
    crosscutting_qualifiers    adjectival facets that have no parent
    missing_umbrellas          true parents that do not exist as types yet

Edge direction is ``narrower --subTypeOf--> broader``, matching the
``narrower,broader`` header of the RML CSV. The bucket names keep the
curators' word "broader"; the EDGE is ``subTypeOf``.

Why not ``skos:broader``: MemeAtlas extends IMKG, and IMKG already uses
``skos:broader`` between MEDIA FRAMES for "Part of a series on". Entry types
in IMKG are classes (a frame is ``rdf:type kymt:<slug>``; the paper calls
them "subclasses of Image macro"), so a curated "a streamer is a kind of
creator" is ``rdfs:subClassOf`` between those classes. Using ``skos:broader``
here as well would have given one predicate two meanings in the same graph.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable

__all__ = [
    "BUCKETS", "ENCODED_BUCKETS", "CONCEPT_EDGE_TYPES",
    "BroaderEdge", "Taxonomy", "TaxonomyError", "load", "validate",
]

# Kept as a tuple to sit alongside build.EDGE_TYPES in the vocabulary
# invariant. Maps to rdfs:subClassOf in kg/rdf.py — see the module docstring.
CONCEPT_EDGE_TYPE = "subTypeOf"
CONCEPT_EDGE_TYPES: tuple[str, ...] = (CONCEPT_EDGE_TYPE,)

ENCODED_BUCKETS: tuple[str, ...] = ("broader_confirmed", "broader_semantic_only")

# Buckets that assert a pair is NOT an is-a edge. A pair here must never
# also appear in an encoded bucket.
WITHHELD_BUCKETS: tuple[str, ...] = (
    "contested", "demoted", "do_not_encode_as_broader")

BUCKETS: tuple[str, ...] = ENCODED_BUCKETS + WITHHELD_BUCKETS + (
    "crosscutting_qualifiers", "missing_umbrellas")


class TaxonomyError(ValueError):
    """The curated file is malformed or self-contradictory.

    Raised rather than returned: a taxonomy that contradicts itself must
    stop a build, because every downstream artifact would encode the
    contradiction.
    """


@dataclass(frozen=True)
class BroaderEdge:
    narrower: str
    broader: str
    bucket: str
    evidence: str | None = None
    rationale: str | None = None
    cooccur: int | None = None
    containment: float | None = None
    pmi_bits: float | None = None

    @property
    def pair(self) -> tuple[str, str]:
        return (self.narrower, self.broader)


@dataclass(frozen=True)
class Taxonomy:
    """One parsed, self-consistent taxonomy file."""

    version: str                      # sha256 of the file bytes
    path: str
    edges: tuple[BroaderEdge, ...]
    bucket_counts: dict[str, int]
    withheld: tuple[dict[str, Any], ...]      # pairs deliberately not encoded
    qualifiers: tuple[str, ...]               # crosscutting, parentless
    missing_umbrellas: tuple[dict[str, Any], ...]
    notes: tuple[str, ...]                    # free-standing guidance entries

    def concept_edges(self, prefix: str = "type:") -> list[dict[str, str]]:
        """Concept-to-concept edges in kg/build.py's node-id vocabulary.

        Node ids match build.py's ``type:<slug>`` convention by default, so
        these drop straight into the same node/edge stream as everything
        else — one graph, one id space, no join step. ``kg/origin.py``
        passes ``prefix="origin:"`` to reuse this over the same bucket
        shape for a different concept kind.
        """
        return [{"src": f"{prefix}{e.narrower}",
                 "dst": f"{prefix}{e.broader}",
                 "type": CONCEPT_EDGE_TYPE}
                for e in self.edges]

    def slugs(self) -> set[str]:
        """Every entry_type slug the encoded edges reference."""
        return {s for e in self.edges for s in e.pair}

    def summary(self) -> dict[str, Any]:
        """The shape that reaches the run summary and the dashboard."""
        return {
            "taxonomy_version": self.version,
            "broader_edges": len(self.edges),
            "buckets": dict(self.bucket_counts),
            "withheld_pairs": len(self.withheld),
            "crosscutting_qualifiers": list(self.qualifiers),
        }


def _as_items(raw: Any, bucket: str) -> list[dict]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise TaxonomyError(f"bucket {bucket!r} must be a list, got {type(raw).__name__}")
    for it in raw:
        if not isinstance(it, dict):
            raise TaxonomyError(f"bucket {bucket!r} contains a non-mapping item: {it!r}")
    return raw


def _pair_of(item: dict, bucket: str) -> tuple[str, str] | None:
    """Normalise the two shapes the file uses for naming a pair."""
    if "narrower" in item and "broader" in item:
        return (str(item["narrower"]), str(item["broader"]))
    if "pair" in item:
        pair = item["pair"]
        if not isinstance(pair, list) or len(pair) != 2:
            raise TaxonomyError(
                f"{bucket}: 'pair' must be a 2-item list, got {pair!r}")
        return (str(pair[0]), str(pair[1]))
    return None


def load(path: str) -> Taxonomy:
    """Parse and structurally validate the curated taxonomy file."""
    import yaml   # declared in requirements.txt; lazy so import stays cheap

    with open(path, "rb") as fh:
        raw_bytes = fh.read()
    version = hashlib.sha256(raw_bytes).hexdigest()

    try:
        doc = yaml.safe_load(raw_bytes.decode("utf-8"))
    except yaml.YAMLError as exc:
        # The file did not parse for its entire prior life. Say so loudly
        # instead of letting a caller fall back to an empty taxonomy.
        raise TaxonomyError(f"{path} is not valid YAML: {exc}") from None

    return _parse(doc, version=version, path=str(path))


def _parse(doc: Any, *, version: str, path: str) -> Taxonomy:
    """The part of ``load()`` with no file I/O: a parsed YAML dict in,
    a validated ``Taxonomy`` out. Split out so ``kg/origin.py`` can parse
    the bucket section of its own curated file (which sits alongside an
    ``aliases:`` map ``load()`` itself knows nothing about) through this
    exact same validation, instead of re-implementing it.
    """
    if not isinstance(doc, dict):
        raise TaxonomyError(f"{path}: top level must be a mapping of buckets")

    unknown = set(doc) - set(BUCKETS)
    if unknown:
        # A typo'd bucket name would otherwise silently drop curated work.
        raise TaxonomyError(
            f"{path}: unknown bucket(s) {sorted(unknown)}; known: {list(BUCKETS)}")

    bucket_counts = {b: len(_as_items(doc.get(b), b)) for b in BUCKETS}

    edges: list[BroaderEdge] = []
    for bucket in ENCODED_BUCKETS:
        for item in _as_items(doc.get(bucket), bucket):
            pair = _pair_of(item, bucket)
            if pair is None:
                raise TaxonomyError(
                    f"{bucket}: item lacks narrower/broader: {item!r}")
            narrower, broader = pair
            if narrower == broader:
                raise TaxonomyError(f"{bucket}: self-edge {narrower!r}")
            edges.append(BroaderEdge(
                narrower=narrower, broader=broader, bucket=bucket,
                evidence=item.get("evidence"), rationale=item.get("rationale"),
                cooccur=item.get("cooccur"), containment=item.get("containment"),
                pmi_bits=item.get("pmi_bits")))

    withheld: list[dict[str, Any]] = []
    for bucket in WITHHELD_BUCKETS:
        for item in _as_items(doc.get(bucket), bucket):
            pair = _pair_of(item, bucket)
            if pair is None:
                continue          # a free-standing note, not a pair
            withheld.append({
                "pair": list(pair), "bucket": bucket,
                "reason": item.get("question") or item.get("now")
                          or item.get("relation")})

    qualifiers = tuple(str(it["type"]) for it in
                       _as_items(doc.get("crosscutting_qualifiers"),
                                 "crosscutting_qualifiers")
                       if "type" in it)

    umbrellas, notes = [], []
    for item in _as_items(doc.get("missing_umbrellas"), "missing_umbrellas"):
        if "missing" in item:
            umbrellas.append({"missing": item["missing"],
                              "children": list(item.get("children") or [])})
        elif "note" in item:
            # The author placed a free-standing guidance entry in this list;
            # kept where they put it rather than restructured.
            notes.append(str(item["note"]))

    tax = Taxonomy(
        version=version, path=str(path), edges=tuple(edges),
        bucket_counts=bucket_counts, withheld=tuple(withheld),
        qualifiers=qualifiers, missing_umbrellas=tuple(umbrellas),
        notes=tuple(notes))
    check_consistency(tax)
    return tax


def check_consistency(tax: Taxonomy) -> None:
    """Refuse a taxonomy that contradicts itself.

    The specific failure this exists to prevent: ``model -> influencer`` was
    encoded as skos:broader while the same pair sat in ``contested`` marked
    "sample before promoting". Nothing checked, so it reached the published
    graph. Order-insensitive, because a contested pair is listed as
    ``[influencer, model]`` while the edge is ``model -> influencer``.
    """
    seen: dict[tuple[str, str], str] = {}
    for e in tax.edges:
        key = tuple(sorted(e.pair))
        if key in seen:
            raise TaxonomyError(
                f"duplicate edge for {key}: in {seen[key]!r} and {e.bucket!r}")
        seen[key] = e.bucket

    for w in tax.withheld:
        key = tuple(sorted(w["pair"]))
        if key in seen:
            raise TaxonomyError(
                f"{key} is encoded as a broader edge (bucket {seen[key]!r}) but "
                f"is also listed under {w['bucket']!r}, which withholds it. "
                f"The curated record must not contradict itself — resolve it "
                f"in the YAML, not downstream.")

    # A cycle in the subtype hierarchy is not a taxonomy.
    parents = {e.narrower: e.broader for e in tax.edges}
    for start in parents:
        slow, fast = start, parents.get(start)
        while fast is not None and parents.get(fast) is not None:
            if slow == fast:
                raise TaxonomyError(f"cycle in broader chain through {start!r}")
            slow, fast = parents[slow], parents.get(parents[fast])


def validate(tax: Taxonomy, census: dict) -> dict[str, Any]:
    """Check the taxonomy against a census of what the corpus actually uses.

    Returns a report as DATA — it does not raise and does not print. The
    previous implementation printed skipped edges to a stdout nobody read
    (``kg_export_rml.py``), so a curated edge silently vanishing from the
    graph left no trace anywhere. These counts reach the run summary.
    """
    known = set(census.get("value_counts") or {})
    missing = sorted(s for s in tax.slugs() if s not in known)
    encodable = [e for e in tax.edges
                 if e.narrower in known and e.broader in known]
    dropped = [{"narrower": e.narrower, "broader": e.broader,
                "bucket": e.bucket,
                "missing": [s for s in e.pair if s not in known]}
               for e in tax.edges if e not in encodable]

    return {
        "taxonomy_version": tax.version,
        "edges_declared": len(tax.edges),
        "edges_encoded": len(encodable),
        "edges_dropped": dropped,
        "slugs_missing_from_census": missing,
        "withheld_pairs": [w["pair"] for w in tax.withheld],
        "buckets": dict(tax.bucket_counts),
    }


def encodable_edges(tax: Taxonomy, census: dict,
                    prefix: str = "type:") -> list[dict[str, str]]:
    """concept_edges(), restricted to slugs the corpus actually uses."""
    known = set(census.get("value_counts") or {})
    return [{"src": f"{prefix}{e.narrower}", "dst": f"{prefix}{e.broader}",
             "type": CONCEPT_EDGE_TYPE}
            for e in tax.edges if e.narrower in known and e.broader in known]
