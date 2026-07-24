"""
kg_build.py — KYMEntryScrape doc -> KG nodes/edges (pure transform, no Mongo)
==============================================================================
Instance-level graph only: a frame connected to its entry_type concepts,
tag concepts, and series parent. Concept-to-concept edges (broader/narrower
within entry_type facets, tag co-occurrence relatedTo) are a SEPARATE
enrichment layer, added later from kg_census output onto this base graph —
this module only emits what's directly liftable from one entry doc,
mirroring kym_parse's "pure, one page in" boundary.

Node kinds : frame, frame_stub, entry_type_concept, tag_concept
Edge types : hasEntryType, hasTag, partOfSeries

frame_stub: series_parent points at another entry's URL, which may not
have been processed yet in this pass. A stub node is emitted so the edge
always has a valid target; kg_store upgrades stub -> frame once that url's
own doc is processed (never the reverse).
"""
from __future__ import annotations


def build_nodes_and_edges(entry: dict) -> tuple[list[dict], list[dict]]:
    """One `entries` doc (as stored by parse_store) -> (nodes, edges)."""
    url = entry.get("url")
    if not url:
        return [], []

    nodes = [{
        "id": url,
        "kind": "frame",
        "label": entry.get("title"),
        "category": entry.get("category"),
        "status": entry.get("status"),
    }]
    edges: list[dict] = []

    for t in entry.get("entry_type") or []:
        concept_id = f"type:{t}"
        nodes.append({"id": concept_id, "kind": "entry_type_concept", "label": t})
        edges.append({"src": url, "dst": concept_id, "type": "hasEntryType"})

    for tag in entry.get("tags") or []:
        tag_norm = tag.strip().lower()
        if not tag_norm:
            continue
        concept_id = f"tag:{tag_norm}"
        nodes.append({"id": concept_id, "kind": "tag_concept", "label": tag_norm})
        edges.append({"src": url, "dst": concept_id, "type": "hasTag"})

    sp = entry.get("series_parent")
    if sp:
        nodes.append({"id": sp, "kind": "frame_stub", "label": None,
                       "category": None, "status": None})
        edges.append({"src": url, "dst": sp, "type": "partOfSeries"})

    return nodes, edges