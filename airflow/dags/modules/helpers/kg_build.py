"""
kg_build.py — KYMEntryScrape doc -> KG nodes/edges (pure transform, no Mongo)
==============================================================================
Instance-level graph only: a frame connected to its entry_type concepts,
tag concepts, series parent, and now its outbound links. Concept-to-concept
edges (broader/narrower within entry_type facets, tag co-occurrence
relatedTo) are a SEPARATE enrichment layer, added later from kg_census
output onto this base graph — this module only emits what's directly
liftable from one entry doc, mirroring kym_parse's "pure, one page in"
boundary.

Node kinds : frame, frame_stub, entry_type_concept, tag_concept, external_ref
Edge types : hasEntryType, hasTag, partOfSeries, relatesToMeme, citesExternal

frame_stub: series_parent / relatesToMeme point at another entry's URL,
which may not have been processed yet in this pass. A stub node is emitted
so the edge always has a valid target; kg_store upgrades stub -> frame once
that url's own doc is processed (never the reverse). Per the EventKG design
brief, the target's URL path segment gives a coarse category guess for the
stub — stored under the SAME `category` key real frame nodes use (not a
separate `kymCategory` property), since it's the same information via a
cheaper heuristic route, valid only until the real frame overwrites the stub.

relatesToMeme / citesExternal are pulled from every link-bearing field
(section body links, additional_references, external_references) and
classified by the link's actual host, not by which field it came from — a
citation in `external_references` that happens to point back to KYM is
still relatesToMeme; a body link out to Wikipedia is still citesExternal.
"""
from __future__ import annotations

from urllib.parse import urlparse

_KYM_HOSTS = {"knowyourmeme.com", "www.knowyourmeme.com"}

# Coarse URL-path -> category guess for stub nodes we haven't scraped yet.
# Mirrors kym_parse.py's own small, dependency-free namespace-pattern table
# rather than importing it, per this project's established module-boundary
# convention. Longest/most-specific prefixes first.
_CATEGORY_PATH_PATTERNS: tuple[tuple[str, str], ...] = (
    ("/memes/subcultures/", "subculture"),
    ("/memes/people/", "person"),
    ("/memes/events/", "event"),
    ("/memes/sites/", "site"),
    ("/sensitive/memes/", "meme"),
    ("/memes/", "meme"),
    ("/people/", "person"),
    ("/cultures/", "culture"),
    ("/subcultures/", "subculture"),
    ("/events/", "event"),
    ("/sites/", "site"),
)


def _is_kym_url(url: str) -> bool:
    return urlparse(url).netloc in _KYM_HOSTS


def _guess_category(url: str) -> str | None:
    path = urlparse(url).path
    for prefix, cat in _CATEGORY_PATH_PATTERNS:
        if path.startswith(prefix):
            return cat
    return None


def _iter_link_urls(entry: dict):
    """All link-bearing fields on one entry doc, url only, text discarded
    (target labels come from the target's own scrape, not the linking page)."""
    for section in entry.get("sections") or []:
        for link in section.get("links") or []:
            u = link.get("url")
            if u:
                yield u
    for ref in entry.get("additional_references") or []:
        u = ref.get("url")
        if u:
            yield u
    for ref in entry.get("external_references") or []:
        u = ref.get("url")
        if u:
            yield u


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
                       "category": _guess_category(sp), "status": None})
        edges.append({"src": url, "dst": sp, "type": "partOfSeries"})

    seen_internal: set[str] = {sp} if sp else set()
    seen_external: set[str] = set()
    for link_url in _iter_link_urls(entry):
        if link_url == url:
            continue  # self-link, not a meaningful edge
        if _is_kym_url(link_url):
            if link_url in seen_internal:
                continue
            seen_internal.add(link_url)
            nodes.append({"id": link_url, "kind": "frame_stub", "label": None,
                           "category": _guess_category(link_url), "status": None})
            edges.append({"src": url, "dst": link_url, "type": "relatesToMeme"})
        else:
            if link_url in seen_external:
                continue
            seen_external.add(link_url)
            nodes.append({"id": link_url, "kind": "external_ref", "label": None})
            edges.append({"src": url, "dst": link_url, "type": "citesExternal"})

    return nodes, edges