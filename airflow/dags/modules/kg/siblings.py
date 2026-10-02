"""
kg/siblings.py — sharesSameSeries edges between the frames of one series
==========================================================================
Pure: no Mongo, no Airflow. Turns a build's ``partOfSeries`` edges into a
``sharesSameSeries`` edge between every two frames with the same series
parent (KYM's "Part of a series on X").

Why explicit (6.6.0; Riccardo's review, 2026-10-02)
---------------------------------------------------
Until 6.5.0 two frames of one series were linked only implicitly, through
the parent they both point to. IMKG had the relation explicitly: its
spider read each page's Related Entries box — the other entries of the
same series — into ``siblings`` and mapped them to ``rdfs:seeAlso``
(kym/mappings/kym.media.frames.yaml). MemeAtlas's parser stopped reading
that box (kym_parse.py: truncated, and fragile across layouts) and keeps
``series_parent`` alone, so here the relation is DERIVED from
``series_parent``, complete rather than truncated.

``relatesToMeme`` is no substitute: in 6.5.0 only 3,602 of the 679,392
sibling pairs are linked by it, in either direction.

Shape
-----
One edge per unordered pair, ``src < dst`` — the canonical ordering
kg/cooccurs.py uses — so Neo4j holds each pair once and is queried
undirected, ``(a)-[:sharesSameSeries]-(b)``. RDF states it both ways with
IMKG's own term, ``rdfs:seeAlso`` (kg/rdf.py), so IMKG's sibling queries
run unchanged — as IMKG's data has it both ways, each page listing the
others.

The count is quadratic in a series' size: n frames give n(n-1)/2 edges.
In 6.5.0, 2,077 series have two or more frames, for 679,392 pairs;
TikTok's 543 frames alone give 147,153. Not capped — the requirement is
every frame of the same series.

A frame has at most one series parent (``series_parent`` is one link), so
each pair comes from exactly one series and nothing here is emitted
twice.
"""
from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from typing import Iterable, Iterator

__all__ = ["SIBLING_EDGE_TYPE", "SIBLING_EDGE_TYPES", "sibling_edges"]

SIBLING_EDGE_TYPE = "sharesSameSeries"
SIBLING_EDGE_TYPES: tuple[str, ...] = (SIBLING_EDGE_TYPE,)


def sibling_edges(series_edges: Iterable[dict]) -> Iterator[dict]:
    """``partOfSeries`` edges -> one ``sharesSameSeries`` edge per pair of
    frames with the same parent, ``src < dst``.

    Every input edge is read before the first pair is yielded, so a store
    cursor passed in is drained before the caller starts writing. Edges of
    other types, and a frame naming itself as its parent, are ignored.
    Deterministic: series in id order, then pairs in id order.
    """
    children: dict[str, set[str]] = defaultdict(set)
    for edge in series_edges:
        src, dst = edge.get("src"), edge.get("dst")
        if edge.get("type") == "partOfSeries" and src and dst and src != dst:
            children[dst].add(src)
    for parent in sorted(children):
        for a, b in combinations(sorted(children[parent]), 2):
            yield {"src": a, "dst": b, "type": SIBLING_EDGE_TYPE}
