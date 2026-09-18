"""
kg/cooccurs.py — statistical coOccursWith edges from a kg/census.py census
==============================================================================
Pure: no Mongo, no Airflow. Converts a census's ``pair_cooccurrence`` (see
kg/census.py) into graph edges. A separate module from kg/taxonomy.py on
purpose: ``subTypeOf`` is a human curator's judgement call, ``coOccursWith``
is a threshold applied to a corpus statistic — two different provenance
stories that kg/taxonomy.py's own docstring is emphatic about not
conflating, so they get separate DAG tasks and separate modules, even
though both end up as concept-to-concept edges in the same graph.

RDF scope note: ``coOccursWith`` is emitted for BOTH entry_type and tag
pairs at this layer — this module doesn't know or care that tag_concept
has no RDF resource today (kg/rdf.py and kg/serialize.py each apply that
restriction on the RDF-facing path only; the property graph gets every
edge this module produces, for either domain).
"""
from __future__ import annotations

__all__ = ["COOCCURS_EDGE_TYPE", "COOCCURS_EDGE_TYPES", "edges_from_census"]

COOCCURS_EDGE_TYPE = "coOccursWith"
COOCCURS_EDGE_TYPES: tuple[str, ...] = (COOCCURS_EDGE_TYPE,)


def edges_from_census(census: dict, node_prefix: str, *,
                      min_count: int | None = None) -> list[dict]:
    """``census["pair_cooccurrence"]`` -> ``coOccursWith`` edge dicts.

    Each census row is already ``{"a", "b", "count"}`` with ``a < b``
    lexicographically (kg/census.py's ``run_census`` sorts each entry's
    values before pairing them), so the canonical ordering that keeps this
    idempotent — never emitting both A->B and B->A — falls out for free;
    nothing here needs to re-sort or dedupe.

    ``min_count``, if given, applies a STRICTER bar than the census's own
    ``pair_count_min_filter`` (it can only narrow, since the census has
    already dropped anything below its own filter) — for materializing
    graph edges at a tighter threshold than the evidence view a curator
    reviews.
    """
    rows = census.get("pair_cooccurrence") or []
    edges = []
    for row in rows:
        if min_count is not None and row["count"] < min_count:
            continue
        edges.append({"src": f"{node_prefix}{row['a']}",
                      "dst": f"{node_prefix}{row['b']}",
                      "type": COOCCURS_EDGE_TYPE})
    return edges
