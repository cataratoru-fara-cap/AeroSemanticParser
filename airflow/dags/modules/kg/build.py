"""
kg/build.py — one KYMEntryScrape doc -> KG nodes/edges (pure transform, no Mongo)
==================================================================================
The ONLY producer of the graph. Every representation — the Mongo property
graph, Neo4j, the N-Triples in graph.nt, the RML CSVs — is a projection of
what this module emits, so they cannot disagree about what exists; they can
only render it differently (kg/rdf.py, kg/serialize.py).

MemeAtlas extends IMKG
----------------------
MemeAtlas is positioned as an extension of IMKG (Tommasini, Ilievski &
Wijesiriwardene, ESWC 2023). Where the two describe the same thing the
IMKG terms are reused verbatim (see kg/rdf.py and kg_config/MODEL.md);
everything IMKG does not model lives under the MemeAtlas namespace and is
declared in kg_config/memeatlas.ttl. This module deals in property-graph
names only; the IMKG/RDF crosswalk is kg/rdf.py's job.

What an entry becomes
---------------------
Every field the parser extracts is carried somewhere, with two deliberate
exceptions, both documented rather than silent:

  * the Origin and Spread SECTIONS (their text and images) are deferred to
    the event-extraction task — ``DEFERRED_SECTION_KINDS`` — which will
    model them properly rather than as a blob of text. Links inside them
    still feed the frame-level ``relatesToMeme`` / ``citesExternal`` edges,
    exactly as before.
  * pipeline-internal stamps (``dom_content_sha256``,
    ``corpus_policy_version``, ``schema_version``) are provenance of the
    pipeline, not of the meme, and stay in ``entries``.

The ``origin`` FIELD is not the Origin section: it is the infobox platform
("TikTok", "Twitter"), which IMKG maps as ``m4s:from``. It is carried here
as ``from``.

``meta``: its 15 keys are either site-wide constants (og:site_name,
twitter:card, …) or duplicates of fields already carried (og:title ≈ title,
og:url = url, og:image = og_image). The three description keys are
identical in every entry, so one ``description`` covers them, and the image
dimensions go on the image node.

Node kinds
    frame               a scraped KYM entry (IMKG: m4s:MediaFrame)
    frame_stub          a KYM url referenced but not scraped (yet)
    entry_type_concept  "type:<slug>"         (IMKG: kymt:<slug>, a class)
    tag_concept         "tag:<label>"         (RDF: an m4s:tag literal)
    region_concept      "region:<name>"       (RDF: an mk:region literal)
    external_ref        a non-KYM url
    section             <atlas>/entry/<id>/section/<i>
    link                <atlas>/entry/<id>/section/<i>/link/<j>
    reference           <atlas>/entry/<id>/reference/<j>
                        <atlas>/entry/<id>/additional-reference/<j>
    image               "image:<src url>"

Edge types
    hasEntryType  hasTag  hasRegion  partOfSeries  relatesToMeme
    citesExternal hasSection hasLink linksTo hasReference refersTo hasImage
  plus the concept edge ``subTypeOf`` (kg/taxonomy.py).

Identifiers for things MemeAtlas mints (sections, links, references) live
under ``https://meme4.science/atlas/entry/<sha1(url)>/`` — the same sha1 the
stores use as the entry's ``_id``, so a section IRI joins back to Mongo
without a lookup. Positions are indices into the parsed lists INCLUDING
deferred sections, so an IRI does not change when the deferral is lifted.

Images are "image:"-prefixed: a body link may point straight at an image
file, and without the prefix that url would be both an ``external_ref`` and
an ``image`` node under one id, with whichever write came last deciding its
kind. In RDF both are the same resource, which is correct there.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

_KYM_HOSTS = {"knowyourmeme.com", "www.knowyourmeme.com"}

# 3.0.0: IMKG alignment + the full parsed record (sections, links,
# references, images, regions, provenance). Bumping this makes the staleness
# gate rebuild every published graph.
KG_BUILD_VERSION = "3.0.0"

ATLAS_BASE = "https://meme4.science/atlas/"

NODE_KINDS: tuple[str, ...] = (
    "frame", "frame_stub", "entry_type_concept", "tag_concept",
    "region_concept", "external_ref", "section", "link", "reference", "image",
)
EDGE_TYPES: tuple[str, ...] = (
    "hasEntryType", "hasTag", "hasRegion", "partOfSeries", "relatesToMeme",
    "citesExternal", "hasSection", "hasLink", "linksTo", "hasReference",
    "refersTo", "hasImage",
)

# Handled by the event-extraction task. One constant, so lifting the
# deferral is a one-line change and every IRI stays stable.
DEFERRED_SECTION_KINDS: frozenset[str] = frozenset({"origin", "spread"})

# The frame's literal-valued properties, in the order kg/rdf.py and
# kg/serialize.py render them. List-valued ones are marked.
FRAME_PROPERTIES: tuple[str, ...] = (
    "label", "category", "status", "year", "from", "about", "description",
    "added", "last_updated", "badges", "aliases", "corpus_status",
    "corpus_missing", "parser_version", "parsed_at", "scraped_at",
)
LIST_PROPERTIES: frozenset[str] = frozenset({"badges", "aliases", "corpus_missing"})

# Coarse URL-path -> category guess for stub nodes we haven't scraped yet.
# Longest/most-specific prefixes first.
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


def entry_id(url: str) -> str:
    """sha1(url) — identical to mongo_base.url_doc_id, the entries ``_id``.

    Recomputed here rather than imported so this module stays free of the
    store layer; tests/test_kg_build.py pins that the two agree.
    """
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def iso_utc(value) -> str | None:
    """A timestamp as ``YYYY-MM-DDTHH:MM:SSZ``, or None.

    Accepts unix seconds (kym_added), datetimes (parsed_at, naive = UTC as
    pymongo returns them) and ISO strings. One formatter for every
    representation: the RDF and RML paths must produce byte-identical
    xsd:dateTime lexical forms or the diff gate reports them as different.
    """
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    else:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _int_or_none(value) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _compact(node: dict) -> dict:
    """Drop absent values so every store holds only what was parsed.

    id and kind are always kept. An empty list or string is "not parsed",
    not a value — and morph-kgc emits nothing for an empty CSV cell, so the
    RDF path must not either.
    """
    return {k: v for k, v in node.items()
            if k in ("id", "kind") or v not in (None, "", [])}


def guess_stub_node(url: str) -> dict:
    """The placeholder node for a KYM url this pass has not scraped yet.

    The store drops stubs on write and materialises them once per build for
    edge targets that ended up with no node, which makes "never downgrade a
    frame to a stub" structurally impossible instead of defended by a
    per-node read-before-write.
    """
    return {"id": url, "kind": "frame_stub", "label": None,
            "category": _guess_category(url), "status": None}


def target_node(url: str) -> dict:
    """The node a link or reference points at: a stub for KYM, else external."""
    if _is_kym_url(url):
        return guess_stub_node(url)
    return {"id": url, "kind": "external_ref", "label": None}


def image_node_id(src: str) -> str:
    return f"image:{src}"


def _iter_link_urls(entry: dict):
    """All link-bearing fields on one entry doc, url only — including links
    inside deferred sections: the frame-level edges predate the deferral
    and must not regress because a section's text is modelled later."""
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


def _about_text(sections: list[dict]) -> str | None:
    """IMKG puts the About narrative on the frame (m4s:about), so it lives
    there — and NOT also on the about section node, which would store the
    same text twice."""
    paragraphs = [p for s in sections if s.get("kind") == "about"
                  for p in (s.get("text") or []) if p]
    return "\n\n".join(paragraphs) or None


def build_nodes_and_edges(entry: dict) -> tuple[list[dict], list[dict]]:
    """One `entries` doc (as stored by parse_store) -> (nodes, edges)."""
    url = entry.get("url")
    if not url:
        return [], []

    eid = entry_id(url)
    base = f"{ATLAS_BASE}entry/{eid}/"
    sections = entry.get("sections") or []
    meta = entry.get("meta") or {}

    nodes: list[dict] = [_compact({
        "id": url,
        "kind": "frame",
        "label": entry.get("title"),
        "category": entry.get("category"),
        "status": entry.get("status"),
        "year": entry.get("year"),
        "from": entry.get("origin"),
        "about": _about_text(sections),
        "description": (meta.get("description") or meta.get("og:description")
                        or meta.get("twitter:description")),
        "added": iso_utc(entry.get("kym_added")),
        "last_updated": iso_utc(entry.get("kym_last_updated")),
        "badges": list(entry.get("badges") or []),
        "aliases": list(entry.get("aliases") or []),
        "corpus_status": entry.get("corpus_status"),
        "corpus_missing": list(entry.get("corpus_missing") or []),
        "parser_version": entry.get("parser_version"),
        "parsed_at": iso_utc(entry.get("parsed_at")),
        "scraped_at": iso_utc(entry.get("scraped_at")),
    })]
    edges: list[dict] = []

    def edge(src: str, etype: str, dst: str) -> None:
        edges.append({"src": src, "dst": dst, "type": etype})

    # -- classification ---------------------------------------------------
    for t in entry.get("entry_type") or []:
        concept_id = f"type:{t}"
        nodes.append({"id": concept_id, "kind": "entry_type_concept", "label": t})
        edge(url, "hasEntryType", concept_id)

    for tag in entry.get("tags") or []:
        tag_norm = tag.strip().lower()
        if not tag_norm:
            continue
        concept_id = f"tag:{tag_norm}"
        nodes.append({"id": concept_id, "kind": "tag_concept", "label": tag_norm})
        edge(url, "hasTag", concept_id)

    for region in entry.get("region") or []:
        region = region.strip()
        if not region:
            continue
        concept_id = f"region:{region}"
        nodes.append({"id": concept_id, "kind": "region_concept", "label": region})
        edge(url, "hasRegion", concept_id)

    # -- the entry's own image ---------------------------------------------
    # template_image_url is currently a copy of og:image in the parser; a
    # distinct value still gets its own node rather than being dropped.
    for i, src in enumerate(dict.fromkeys(
            s for s in (entry.get("og_image"), entry.get("template_image_url")) if s)):
        img = {"id": image_node_id(src), "kind": "image"}
        if i == 0 and src == entry.get("og_image"):
            img["width"] = _int_or_none(meta.get("og:image:width"))
            img["height"] = _int_or_none(meta.get("og:image:height"))
        nodes.append(_compact(img))
        edge(url, "hasImage", img["id"])

    # -- series + frame-level link edges (unchanged semantics) -------------
    sp = entry.get("series_parent")
    if sp:
        nodes.append(guess_stub_node(sp))
        edge(url, "partOfSeries", sp)

    seen_internal: set[str] = {sp} if sp else set()
    seen_external: set[str] = set()
    for link_url in _iter_link_urls(entry):
        if link_url == url:
            continue  # self-link, not a meaningful frame-level edge
        if _is_kym_url(link_url):
            if link_url in seen_internal:
                continue
            seen_internal.add(link_url)
            nodes.append(guess_stub_node(link_url))
            edge(url, "relatesToMeme", link_url)
        else:
            if link_url in seen_external:
                continue
            seen_external.add(link_url)
            nodes.append({"id": link_url, "kind": "external_ref", "label": None})
            edge(url, "citesExternal", link_url)

    # -- the body: sections, their links and images -------------------------
    for i, section in enumerate(sections):
        kind = section.get("kind")
        if kind in DEFERRED_SECTION_KINDS:
            continue
        sid = f"{base}section/{i}"
        paragraphs = [p for p in (section.get("text") or []) if p]
        nodes.append(_compact({
            "id": sid, "kind": "section",
            "section_kind": kind,
            "heading": section.get("heading"),
            "position": i,
            "level": section.get("level"),
            "text": None if kind == "about" else ("\n\n".join(paragraphs) or None),
        }))
        edge(url, "hasSection", sid)

        for j, link in enumerate(section.get("links") or []):
            target = link.get("url")
            if not target:
                continue
            lid = f"{sid}/link/{j}"
            nodes.append(_compact({"id": lid, "kind": "link",
                                   "anchor_text": (link.get("text") or "").strip()}))
            edge(sid, "hasLink", lid)
            nodes.append(target_node(target))
            edge(lid, "linksTo", target)

        for image in section.get("images") or []:
            src = image.get("src")
            if not src:
                continue
            img_id = image_node_id(src)
            nodes.append(_compact({"id": img_id, "kind": "image",
                                   "alt": image.get("alt"),
                                   "caption": image.get("caption")}))
            edge(sid, "hasImage", img_id)

    # -- references ------------------------------------------------------------
    for j, ref in enumerate(entry.get("external_references") or []):
        target = ref.get("url")
        if not target:
            continue
        rid = f"{base}reference/{j}"
        nodes.append(_compact({"id": rid, "kind": "reference",
                               "ref_class": "ExternalReference",
                               "index": ref.get("index"),
                               "citation_text": ref.get("text")}))
        edge(url, "hasReference", rid)
        nodes.append(target_node(target))
        edge(rid, "refersTo", target)

    for j, ref in enumerate(entry.get("additional_references") or []):
        target = ref.get("url")
        if not target:
            continue
        rid = f"{base}additional-reference/{j}"
        nodes.append(_compact({"id": rid, "kind": "reference",
                               "ref_class": "AdditionalReference",
                               "site_name": ref.get("name")}))
        edge(url, "hasReference", rid)
        nodes.append(target_node(target))
        edge(rid, "refersTo", target)

    return nodes, edges
