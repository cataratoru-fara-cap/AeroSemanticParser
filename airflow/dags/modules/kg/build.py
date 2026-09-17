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

What becomes a node (4.0.0)
---------------------------
A node is something other things can have in common: a frame, a type, a
tag, a region, a URL, an image file. Everything else is a property of the
node it belongs to, or of the edge it qualifies. IMKG follows the same
rule — its About, Origin and Spread are literals on the media frame.

3.0.0 also minted a node per page section, per body link and per
reference. None of those is ever shared, and on the real corpus they did
not earn their place:

  * ~88k of 135k section nodes held no text at all (the External
    References, Search Interest and gallery sections), and About text was
    already ``m4s:about`` on the frame;
  * every Link node had exactly one ``hasLink`` in and one ``linksTo`` out
    (148,542 of each), and every Reference one ``hasReference`` and one
    ``refersTo`` (224,843) — a two-edge detour around the frame-level
    ``relatesToMeme`` / ``citesExternal`` edges that already existed, kept
    only to hold anchor or citation text;
  * an image's alt text and caption sat on the SHARED image node, so the
    last page to be written overwrote the others (187 images).

So now:

  * a section with text is one ``section_texts`` entry on the frame,
    ``"<heading>\\n\\n<paragraphs>"`` — heading and text stay together,
    page order is kept, empty sections vanish;
  * what was particular to ONE occurrence — anchor text, the heading it sat
    under, citation text and number, a reference's site name, an image's
    role, alt text and caption — is an entry in the ``occurrences`` list
    of the frame-level edge it qualifies (``relatesToMeme``,
    ``citesExternal``, ``hasImage``). Neo4j stores those as edge
    properties, RDF as RDF-star annotations on the quoted edge.

What an entry becomes
---------------------
Every field the parser extracts is carried somewhere, with two deliberate
exceptions, both documented rather than silent:

  * the Origin and Spread SECTIONS (their text, images, and the anchor text
    of links inside them) are deferred to the event-extraction task —
    ``DEFERRED_SECTION_KINDS``. Links inside them still feed the
    frame-level ``relatesToMeme`` / ``citesExternal`` edges, as before.
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
dimensions go on the image node — they belong to the file, not to a page.

Node kinds
    frame               a scraped KYM entry (IMKG: m4s:MediaFrame)
    frame_stub          a KYM url referenced but not scraped (yet)
    entry_type_concept  "type:<slug>"         (IMKG: kymt:<slug>, a class)
    tag_concept         "tag:<label>"         (RDF: an m4s:tag literal)
    region_concept      "region:<name>"       (RDF: an mk:region literal)
    external_ref        a non-KYM url
    image               "image:<src url>"

Edge types
    hasEntryType  hasTag  hasRegion  partOfSeries  relatesToMeme
    citesExternal hasImage
  plus the concept edge ``subTypeOf`` (kg/taxonomy.py).

Images are "image:"-prefixed: a body link may point straight at an image
file, and without the prefix that url would be both an ``external_ref`` and
an ``image`` node under one id, with whichever write came last deciding its
kind. In RDF both are the same resource, which is correct there.
"""
from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlparse

_KYM_HOSTS = {"knowyourmeme.com", "www.knowyourmeme.com"}

# 4.0.0: section / link / reference nodes dissolved into frame properties
# and edge occurrences. Bumping this makes the staleness gate rebuild.
KG_BUILD_VERSION = "4.0.0"

NODE_KINDS: tuple[str, ...] = (
    "frame", "frame_stub", "entry_type_concept", "tag_concept",
    "region_concept", "external_ref", "image",
)
EDGE_TYPES: tuple[str, ...] = (
    "hasEntryType", "hasTag", "hasRegion", "partOfSeries", "relatesToMeme",
    "citesExternal", "hasImage",
)

# Handled by the event-extraction task. One constant, so lifting the
# deferral is a one-line change.
DEFERRED_SECTION_KINDS: frozenset[str] = frozenset({"origin", "spread"})

# The frame's literal-valued properties, in the order kg/rdf.py and
# kg/serialize.py render them. List-valued ones are marked.
FRAME_PROPERTIES: tuple[str, ...] = (
    "label", "category", "status", "year", "from", "about", "description",
    "added", "last_updated", "badges", "aliases", "section_texts",
    "corpus_status", "corpus_missing", "parser_version", "parsed_at",
    "scraped_at",
)
LIST_PROPERTIES: frozenset[str] = frozenset(
    {"badges", "aliases", "section_texts", "corpus_missing"})

# The edges that carry an ``occurrences`` list, and every field an
# occurrence may hold. kg/rdf.py, kg/serialize.py and kg/loaders.py all
# render from these two tables.
OCCURRENCE_EDGE_TYPES: tuple[str, ...] = ("relatesToMeme", "citesExternal", "hasImage")
OCCURRENCE_FIELDS: tuple[str, ...] = (
    "anchor_text", "in_section", "citation_text", "citation_index",
    "site_name", "role", "alt_text", "caption",
)

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


def _occurrence(**fields) -> dict | None:
    """One occurrence, absent values dropped; None if nothing is left."""
    occ = {k: (v.strip() if isinstance(v, str) else v) for k, v in fields.items()}
    occ = {k: v for k, v in occ.items() if v not in (None, "")}
    return occ or None


def guess_stub_node(url: str) -> dict:
    """The placeholder node for a KYM url this pass has not scraped yet.

    The store drops stubs on write and materialises them once per build for
    edge targets that ended up with no node, which makes "never downgrade a
    frame to a stub" structurally impossible instead of defended by a
    per-node read-before-write.
    """
    return {"id": url, "kind": "frame_stub", "label": None,
            "category": _guess_category(url), "status": None}


def image_node_id(src: str) -> str:
    return f"image:{src}"


def _iter_links(entry: dict):
    """(url, occurrence | None) for every link-bearing field, in page order.

    Links inside deferred sections yield no occurrence: their anchor text
    is section content, deferred with the section. The frame-level edge
    they imply predates the deferral and is still emitted.
    """
    for section in entry.get("sections") or []:
        deferred = section.get("kind") in DEFERRED_SECTION_KINDS
        for link in section.get("links") or []:
            url = link.get("url")
            if url:
                yield url, None if deferred else _occurrence(
                    anchor_text=link.get("text"), in_section=section.get("heading"))
    for ref in entry.get("additional_references") or []:
        url = ref.get("url")
        if url:
            yield url, _occurrence(site_name=ref.get("name"))
    for ref in entry.get("external_references") or []:
        url = ref.get("url")
        if url:
            yield url, _occurrence(citation_text=ref.get("text"),
                                   citation_index=_int_or_none(ref.get("index")))


def _about_text(sections: list[dict]) -> str | None:
    """IMKG puts the About narrative on the frame (m4s:about)."""
    paragraphs = [p for s in sections if s.get("kind") == "about"
                  for p in (s.get("text") or []) if p]
    return "\n\n".join(paragraphs) or None


def _section_texts(sections: list[dict]) -> list[str]:
    """Every other section that has text, as ``heading\\n\\ntext``, in page
    order. About is ``about``; deferred kinds are deferred; a section with
    no paragraphs (galleries, embeds, the references list) has nothing to
    say and is not kept."""
    out = []
    for section in sections:
        if section.get("kind") in DEFERRED_SECTION_KINDS or section.get("kind") == "about":
            continue
        body = "\n\n".join(p for p in (section.get("text") or []) if p)
        if not body:
            continue
        heading = (section.get("heading") or "").strip()
        out.append(f"{heading}\n\n{body}" if heading else body)
    return out


def build_nodes_and_edges(entry: dict) -> tuple[list[dict], list[dict]]:
    """One `entries` doc (as stored by parse_store) -> (nodes, edges)."""
    url = entry.get("url")
    if not url:
        return [], []

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
        "section_texts": _section_texts(sections),
        "corpus_status": entry.get("corpus_status"),
        "corpus_missing": list(entry.get("corpus_missing") or []),
        "parser_version": entry.get("parser_version"),
        "parsed_at": iso_utc(entry.get("parsed_at")),
        "scraped_at": iso_utc(entry.get("scraped_at")),
    })]
    edges: list[dict] = []
    by_key: dict[tuple[str, str], dict] = {}

    def edge(etype: str, dst: str, occurrence: dict | None = None) -> dict:
        """The frame's one edge of this type to ``dst``, created on first
        use; each further mention only adds an occurrence."""
        e = by_key.get((etype, dst))
        if e is None:
            e = by_key[(etype, dst)] = {"src": url, "dst": dst, "type": etype}
            edges.append(e)
        if occurrence:
            e.setdefault("occurrences", []).append(occurrence)
        return e

    # -- classification ---------------------------------------------------
    for t in entry.get("entry_type") or []:
        concept_id = f"type:{t}"
        nodes.append({"id": concept_id, "kind": "entry_type_concept", "label": t})
        edge("hasEntryType", concept_id)

    for tag in entry.get("tags") or []:
        tag_norm = tag.strip().lower()
        if not tag_norm:
            continue
        concept_id = f"tag:{tag_norm}"
        nodes.append({"id": concept_id, "kind": "tag_concept", "label": tag_norm})
        edge("hasTag", concept_id)

    for region in entry.get("region") or []:
        region = region.strip()
        if not region:
            continue
        concept_id = f"region:{region}"
        nodes.append({"id": concept_id, "kind": "region_concept", "label": region})
        edge("hasRegion", concept_id)

    # -- images: the page's own, then those shown in its sections ------------
    # template_image_url is currently a copy of og:image in the parser; a
    # distinct value still gets its own node rather than being dropped.
    page_images = list(dict.fromkeys(
        s for s in (entry.get("og_image"), entry.get("template_image_url")) if s))
    for src in page_images:
        img = {"id": image_node_id(src), "kind": "image"}
        if src == entry.get("og_image"):
            img["width"] = _int_or_none(meta.get("og:image:width"))
            img["height"] = _int_or_none(meta.get("og:image:height"))
        nodes.append(_compact(img))
        edge("hasImage", img["id"], _occurrence(role="page"))

    for section in sections:
        if section.get("kind") in DEFERRED_SECTION_KINDS:
            continue
        for image in section.get("images") or []:
            src = image.get("src")
            if not src:
                continue
            img_id = image_node_id(src)
            nodes.append({"id": img_id, "kind": "image"})
            edge("hasImage", img_id, _occurrence(
                role="section", in_section=section.get("heading"),
                alt_text=image.get("alt"), caption=image.get("caption")))

    # -- series + frame-level link edges ------------------------------------
    sp = entry.get("series_parent")
    if sp:
        nodes.append(guess_stub_node(sp))
        edge("partOfSeries", sp)

    for link_url, occurrence in _iter_links(entry):
        if link_url == url or link_url == sp:
            continue  # a self-link, or the series parent (partOfSeries says it)
        if _is_kym_url(link_url):
            if ("relatesToMeme", link_url) not in by_key:
                nodes.append(guess_stub_node(link_url))
            edge("relatesToMeme", link_url, occurrence)
        else:
            if ("citesExternal", link_url) not in by_key:
                nodes.append({"id": link_url, "kind": "external_ref", "label": None})
            edge("citesExternal", link_url, occurrence)

    return nodes, edges
