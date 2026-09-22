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

5.0.0: taxonomy treatment for the rest of the frame's fields
--------------------------------------------------------------
``category`` needed nothing (6 flat values, already ``rdf:type
kym:<Category>``; see kg_config/MODEL.md). Three more fields got the same
"is it shared, does it need canonicalizing, does it need a hierarchy"
review taxonomy already got for entry_type:

  * ``badges`` moves from a frame literal to ``badge_concept`` /
    ``hasBadge`` — trivial (only ``"Sensitive"`` is observed corpus-wide
    today), built for consistency and to be ready if KYM adds more.
  * ``origin`` (the infobox field, still carried verbatim as ``from`` —
    see below) gets an ADDED canonicalization layer, ``origin_concept`` /
    ``hasOrigin``, via kg/origin.py's curated alias map. This is NOT a
    "platform" concept: the real corpus shows the field holds platforms,
    countries, franchises, companies, games and people, with real
    duplication in every category ("United States" / "USA" / "America";
    "Avengers: Endgame" / "Avengers: Endgame (Film)"). A curated
    ``subTypeOf`` hierarchy is layered on top, but only over the subset of
    canonical slugs that are actually platforms — see kg/origin.py's
    module docstring for why the rest stay flat.
  * ``tags`` gets plural-folding (kg/tag_normalize.py) so
    ``catchphrase``/``catchphrases`` etc. merge into one node, plus
    statistical ``coOccursWith`` edges (kg/cooccurs.py) from
    kg/census.py's co-occurrence output, bounded to the top ~1,000
    tags by frequency. ``coOccursWith`` is a separate, statistics-derived
    edge type, not emitted by this function — see kg/cooccurs.py and
    kym_kg_dag.py. ``entry_type`` does NOT get it (5.0.1): alongside a
    curated ``subTypeOf`` hierarchy it was judged needless statistical
    noise; tags keep it because they have no curated alternative.
    ``coOccursWith`` is consequently property-graph-only now (Mongo,
    Neo4j) — ``tag_concept`` has no RDF resource to attach a triple to,
    so it never reaches ``graph.nt`` at all; see kg/rdf.py.

What an entry becomes
---------------------
Every field the parser extracts is carried somewhere — no exceptions, as
of 5.1.0 — except the pipeline-internal stamps (``dom_content_sha256``,
``corpus_policy_version``, ``schema_version``), which are provenance of
the pipeline rather than of the meme and stay in ``entries``.

Until 5.1.0 the Origin and Spread SECTIONS were the standing exception,
deferred to the event-extraction task: their text, their images, and the
anchor text of links inside them were all withheld, while the links
themselves still fed the frame-level ``relatesToMeme`` /
``citesExternal`` edges. All three are now carried like any other
section's — the text as ``origin_text`` / ``spread_text``
(``NARRATIVE_SECTION_PROPERTIES``), the rest by simply no longer being
special-cased. Events extracted FROM that text are a separate layer
(kg/events.py) passed into this function as data.

The ``origin`` FIELD is not the Origin section: it is the infobox's free-text
Origin line ("TikTok", "United States", "The Simpsons", ...), which IMKG
maps as ``m4s:from``. It is carried here verbatim as ``from``, AND (5.0.0)
canonicalized into ``origin_concept``/``hasOrigin`` when an
``origin_resolver`` is supplied — see kg/origin.py.

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
    origin_concept      "origin:<slug>"       (kg/origin.py; mk:OriginScheme)
    badge_concept       "badge:<label>"       (mk:BadgeScheme)
    external_ref        a non-KYM url
    image               "image:<src url>"
    event               "event:<id>"          (6.0.0; mk:Event, DERIVED —
                        see kg/events.py and "What becomes a node" below)
    wikidata_entity     "wd:<QID>"            (6.1.0; a Wikidata item, linked
                        from the title, tags and About — see below)

Edge types
    hasEntryType  hasTag  hasRegion  hasOrigin  hasBadge  partOfSeries
    relatesToMeme  citesExternal  hasImage  hasEvent
    fromTitle  fromTags  fromAbout                      (6.1.0, to a wikidata_entity)
  and, from an event node: eventLink  eventCitation  eventEmbed  eventImage
  eventDateAnchor
  plus the concept edges ``subTypeOf`` (kg/taxonomy.py, kg/origin.py) and
  ``coOccursWith`` (kg/cooccurs.py, tags only as of 5.0.1) — neither
  emitted by this function.

6.1.0: Wikidata entities
------------------------
The entities kg/entities.py recognised in the title, the tags and the
About section and linked to Wikidata (modules/entity_store.py), passed in
as data exactly like events. One ``wikidata_entity`` node per QID, shared
by every frame that mentions it — the first node kind whose IRI is
somebody else's (Wikidata's), which is why it mints nothing and why it
carries only the label and description it was linked under. The frame's
edge to it is named after the FIELD the mention was read from, as IMKG
names them (m4s:fromAbout, m4s:fromTags; fromTitle is MemeAtlas's), and
each mention is an occurrence on that edge: the words on the page, the
link score, how the span was found, and the NER label if any.

Like events, these are DERIVED — a linker's reading, not a parsed value —
and the occurrence says how sure it was. Which of them are worth keeping
is a later curation step (gap 09); this function carries them all.

Images are "image:"-prefixed: a body link may point straight at an image
file, and without the prefix that url would be both an ``external_ref`` and
an ``image`` node under one id, with whichever write came last deciding its
kind. In RDF both are the same resource, which is correct there.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable, Sequence
from urllib.parse import urlparse

from . import tag_normalize

_KYM_HOSTS = {"knowyourmeme.com", "www.knowyourmeme.com"}

# 6.1.0: the entity layer. Entities recognised in the title, tags and About
# (kg/entities.py, modules/entity_store.py) become `wikidata_entity` nodes
# with fromTitle / fromTags / fromAbout edges from their frame. MINOR:
# additive, and unlike events it mints no IRI — a Wikidata item has its own.
# 6.0.0: the event layer. Events extracted from the Origin/Spread
# narrative (kg/events.py, modules/event_store.py) become `event` nodes
# with an mk:hasEvent edge from their frame. MAJOR because it amends the
# graph's foundational rule — see "What becomes a node" below: an event is
# the first IRI MemeAtlas mints for page-derived content since 4.0.0
# dissolved the section/link/reference nodes, and the first node in the
# graph that is DERIVED (a model's reading) rather than parsed.
# 5.1.0: the Origin/Spread deferral is lifted — their text becomes the
# frame's origin_text/spread_text (IMKG's m4s:origin/m4s:spread), and their
# images and link anchor text now reach the graph like every other
# section's. 5.0.1: entry_type's statistical coOccursWith edges removed
# (curator feedback: needless alongside the curated subTypeOf hierarchy) —
# tags keep theirs, since tags have no curated alternative. 5.0.0: badges
# and origin promoted from frame literals / a literal string to concepts
# (badge_concept/hasBadge, origin_concept/hasOrigin); tags plural-folded
# (kg/tag_normalize.py). Bumping this makes the staleness gate rebuild.
KG_BUILD_VERSION = "6.1.0"

NODE_KINDS: tuple[str, ...] = (
    "frame", "frame_stub", "entry_type_concept", "tag_concept",
    "region_concept", "origin_concept", "badge_concept", "external_ref",
    "image", "event", "wikidata_entity",
)
EDGE_TYPES: tuple[str, ...] = (
    "hasEntryType", "hasTag", "hasRegion", "hasOrigin", "hasBadge",
    "partOfSeries", "relatesToMeme", "citesExternal", "hasImage", "hasEvent",
    "eventLink", "eventCitation", "eventEmbed", "eventImage",
    "eventDateAnchor", "fromTitle", "fromTags", "fromAbout",
)

# The three narrative sections IMKG keeps as frame literals, mapped to the
# frame property each one becomes. Every OTHER section with text goes into
# ``section_texts``, so a kind listed here must never be emitted twice.
#
# 5.1.0 replaced DEFERRED_SECTION_KINDS = {"origin", "spread"} with this.
# The deferral withheld three separate things from the graph — the section
# text, the images inside those sections, and the anchor text of links
# inside them — which made "every field the parser extracts reaches the
# graph" false in a way no test could state cleanly. All three are lifted:
# the text lands here, the images and anchor text simply stop being
# special-cased. The events themselves are a SEPARATE layer over this text
# (kg/events.py, modules/event_store.py) and are passed in as data.
#
# The frame property is ``origin_text``, NOT ``origin``: FRAME_PROPERTIES
# already has ``from``, which comes from entry["origin"] — the infobox
# line, a different field entirely (see below).
NARRATIVE_SECTION_PROPERTIES: dict[str, str] = {
    "about": "about", "origin": "origin_text", "spread": "spread_text",
}

# The frame's literal-valued properties, in the order kg/rdf.py and
# kg/serialize.py render them. List-valued ones are marked. "badges" left
# in 5.0.0: it is now an edge (hasBadge -> badge_concept), not a literal —
# see the badge-minting loop in build_nodes_and_edges. "from" (the raw,
# uncanonicalized origin string) stays a literal; origin_concept/hasOrigin
# is an added layer, not a replacement — see kg/origin.py.
FRAME_PROPERTIES: tuple[str, ...] = (
    "label", "category", "status", "year", "from", "about", "origin_text",
    "spread_text", "description", "added", "last_updated", "aliases",
    "section_texts", "corpus_status", "corpus_missing", "parser_version",
    "parsed_at", "scraped_at",
)
LIST_PROPERTIES: frozenset[str] = frozenset(
    {"aliases", "section_texts", "corpus_missing"})

# An event node's properties, in the order kg/rdf.py and kg/serialize.py
# render them. Populated from an `events` doc (modules/event_store.py),
# which got them from kg/events.py, which got them from the model under
# kg_config/event_extraction_schema.json.
#
# ``date`` is carried for Mongo and Neo4j but emits NO triple: it is
# recoverable from (date_start, date_precision), and as a CSV column of
# bare years it is the one value in the whole RML surface that pandas
# could infer as a number inside morph-kgc. This project has already lost
# two triples to dtype inference (the real KYM tags spelled "null", which
# is why morph_kgc.ini runs with na_values empty); one such column is
# enough.
EVENT_PROPERTIES: tuple[str, ...] = (
    "source_text", "source_section", "date", "date_precision",
    "date_basis", "date_text", "date_start", "date_end", "location", "location_type",
    "certainty", "actors", "extraction_model", "extraction_version",
)

# Edges FROM an event node (6.0.0, extraction 2.0.0) to what kg/events.py
# attached to it by position — never chosen by the model. Every target is
# already a node: section links and references produce frame-level
# relatesToMeme/citesExternal targets, embeds likewise (parser 1.6.0), and
# section photos are image nodes.
EVENT_MEDIA_EDGES: dict[str, str] = {
    "link": "eventLink",          # a hyperlink inside the event's sentences
    "citation": "eventCitation",  # the reference its [n] marker points to
}
EVENT_EMBED_EDGE = "eventEmbed"   # an embedded post shown with its paragraph
EVENT_IMAGE_EDGE = "eventImage"   # a photo shown with its paragraph
# A date the pipeline worked out from "that same day" points at the event it
# was counted from, so a derived date is always traceable to a stated one.
EVENT_DATE_ANCHOR_EDGE = "eventDateAnchor"
EVENT_LIST_PROPERTIES: frozenset[str] = frozenset({"actors"})

# 6.1.0. The field a mention was read from (kg/entities.SOURCE_FIELDS) ->
# the frame's edge to the entity. IMKG's own names where IMKG had one
# (m4s:fromAbout, m4s:fromTags — kym.media.frames.textual.enrichment.yaml);
# IMKG never linked titles.
ENTITY_FIELD_EDGES: dict[str, str] = {
    "title": "fromTitle", "tag": "fromTags", "about": "fromAbout",
}
# A wikidata_entity node's properties. ``qid`` duplicates the id's tail so
# a Cypher query can say ``e.qid = 'Q42'``; only ``label`` reaches RDF.
WIKIDATA_ENTITY_PROPERTIES: tuple[str, ...] = ("qid", "label", "description")

# The edges that carry an ``occurrences`` list, and every field an
# occurrence may hold. kg/rdf.py, kg/serialize.py and kg/loaders.py all
# render from these two tables.
OCCURRENCE_EDGE_TYPES: tuple[str, ...] = (
    "relatesToMeme", "citesExternal", "hasImage",
    "fromTitle", "fromTags", "fromAbout")          # 6.1.0: one per mention
OCCURRENCE_FIELDS: tuple[str, ...] = (
    "anchor_text", "in_section", "citation_text", "citation_index",
    "site_name", "role", "alt_text", "caption",
    # 6.1.0, on the entity edges: the words on the page, the linker's score
    # (0..1), how the span was found (kg/entities.METHODS), the NER label.
    "mention_text", "link_score", "link_method", "ner_label",
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


_DATE_FORMATS = {"day": "%Y-%m-%d", "month": "%Y-%m", "year": "%Y"}


def date_range(date: str | None, precision: str) -> tuple[str | None, str | None]:
    """An event's date at its stated precision -> (start, end), inclusive.

        day    2013-05-04 -> 2013-05-04T00:00:00Z .. 2013-05-04T23:59:59Z
        month  2013-05    -> 2013-05-01T00:00:00Z .. 2013-05-31T23:59:59Z
        year   2013       -> 2013-01-01T00:00:00Z .. 2013-12-31T23:59:59Z
        none / unparseable -> (None, None); an absent value emits nothing

    An INTERVAL, not a point, because that is what the source actually
    said: "early 2013" is a year, and flattening it to 2013-01-01 would
    invent a January the page never mentioned. Keeping both ends lets
    SPARQL range queries work without any consumer having to know the
    precision — while mk:datePrecision still says how much was really known.

    The lexical form comes from the same strftime as iso_utc, deliberately:
    the RDF and RML paths must produce byte-identical xsd:dateTime or the
    diff gate reports them as different. Derived HERE rather than in
    kg/events.py because which triples to emit is a modelling decision,
    and it keeps this module from importing an HTTP client transitively.
    """
    fmt = _DATE_FORMATS.get(precision or "none")
    if not fmt or not date:
        return None, None
    try:
        start = datetime.strptime(str(date), fmt).replace(tzinfo=timezone.utc)
    except ValueError:
        return None, None
    if precision == "day":
        end = start.replace(hour=23, minute=59, second=59)
    elif precision == "month":
        if start.month == 12:
            nxt = start.replace(year=start.year + 1, month=1)
        else:
            nxt = start.replace(month=start.month + 1)
        end = nxt - timedelta(seconds=1)
    else:
        end = start.replace(year=start.year + 1) - timedelta(seconds=1)
    fmt_out = "%Y-%m-%dT%H:%M:%SZ"
    return start.strftime(fmt_out), end.strftime(fmt_out)


def wikidata_node_id(qid: str) -> str:
    """``wd:Q42``. The prefix keeps a QID from ever colliding with another
    node id, and kg/rdf.py turns it into Wikidata's own entity IRI."""
    return f"wd:{qid}"


def event_node_id(event_id: str) -> str:
    """``event:<id>``. The id itself is minted once, in kg/events.py, and
    stored — so there is exactly one implementation of the recipe."""
    return f"event:{event_id}"


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

    5.1.0: every section's links now yield their anchor text. Until then
    the Origin and Spread sections yielded ``None`` here — their anchor
    text was section content, withheld with the section.
    """
    for section in entry.get("sections") or []:
        for link in section.get("links") or []:
            url = link.get("url")
            if url:
                yield url, _occurrence(
                    anchor_text=link.get("text"), in_section=section.get("heading"))
        # Parser 1.6.0: embedded posts (a TikTok, a tweet, a reel). A link
        # like any other; the platform is the site it lives on.
        for embed in section.get("embeds") or []:
            url = embed.get("url")
            if url:
                yield url, _occurrence(site_name=embed.get("platform"),
                                       in_section=section.get("heading"))
    for ref in entry.get("additional_references") or []:
        url = ref.get("url")
        if url:
            yield url, _occurrence(site_name=ref.get("name"))
    for ref in entry.get("external_references") or []:
        url = ref.get("url")
        if url:
            yield url, _occurrence(citation_text=ref.get("text"),
                                   citation_index=_int_or_none(ref.get("index")))


def _kind_text(sections: list[dict], kind: str) -> str | None:
    """One narrative section's paragraphs, joined — IMKG keeps About,
    Origin and Spread as literals on the frame (m4s:about, m4s:origin,
    m4s:spread). Every matching section contributes: a page that splits
    its Origin across two headings would otherwise silently lose one."""
    paragraphs = [p for s in sections if s.get("kind") == kind
                  for p in (s.get("text") or []) if p]
    return "\n\n".join(paragraphs) or None


def _section_texts(sections: list[dict]) -> list[str]:
    """Every other section that has text, as ``heading\\n\\ntext``, in page
    order. The three narrative kinds are frame properties of their own
    (NARRATIVE_SECTION_PROPERTIES) and must not double-emit here; a section
    with no paragraphs (galleries, embeds, the references list) has nothing
    to say and is not kept."""
    out = []
    for section in sections:
        if section.get("kind") in NARRATIVE_SECTION_PROPERTIES:
            continue
        body = "\n\n".join(p for p in (section.get("text") or []) if p)
        if not body:
            continue
        heading = (section.get("heading") or "").strip()
        out.append(f"{heading}\n\n{body}" if heading else body)
    return out


def build_nodes_and_edges(
        entry: dict, *,
        origin_resolver: Callable[[str], str] | None = None,
        tag_denylist: frozenset[str] = frozenset(),
        events: Sequence[dict] = (),
        entities: Sequence[dict] = (),
) -> tuple[list[dict], list[dict]]:
    """One `entries` doc (as stored by parse_store) -> (nodes, edges).

    ``origin_resolver`` is the one place this function's shape has to bend:
    unlike ``hasEntryType``/``hasTag`` (minted verbatim from the raw
    value), ``hasOrigin`` needs the curated alias map in
    ``kg_config/origin_taxonomy.yaml`` to resolve a raw origin string to
    its canonical slug (see kg/origin.py) — data this function has no
    other channel to receive, called per-entry inside a mapped Airflow
    task. Left at its default ``None``, no ``hasOrigin`` edge is emitted
    and every existing caller is unaffected.

    ``tag_denylist`` is kg/tag_normalize.py's curated do-not-fold set
    (``kg_config/tag_normalization_exceptions.yaml``). Left at its default
    empty set, plural folding still runs (it needs no external data to be
    useful) but nothing is exempted from it.

    ``events`` (6.0.0) is this entry's extracted events, as stored by
    modules/event_store.py. DATA, not a callable like ``origin_resolver``,
    and the difference is the point: a resolver is a function OF A VALUE
    shared by every entry, while events are per-entry data — a callable
    would either hit Mongo once per entry or close over a pre-fetched
    dict, which is passing data with extra indirection. Left at its
    default, no event node or edge is emitted and every existing caller is
    unaffected.

    ``entities`` (6.1.0) is this entry's linked mentions, as stored by
    modules/entity_store.py — data for the same reason events are.
    """
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
        "about": _kind_text(sections, "about"),
        "origin_text": _kind_text(sections, "origin"),
        "spread_text": _kind_text(sections, "spread"),
        "description": (meta.get("description") or meta.get("og:description")
                        or meta.get("twitter:description")),
        "added": iso_utc(entry.get("kym_added")),
        "last_updated": iso_utc(entry.get("kym_last_updated")),
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
        tag_norm = tag_normalize.fold(tag.strip().lower(), tag_denylist)
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

    for badge in entry.get("badges") or []:
        badge = badge.strip()
        if not badge:
            continue
        concept_id = f"badge:{badge.lower()}"
        nodes.append({"id": concept_id, "kind": "badge_concept", "label": badge})
        edge("hasBadge", concept_id)

    origin_raw = entry.get("origin")
    if origin_raw and origin_resolver is not None:
        slug = origin_resolver(origin_raw)
        concept_id = f"origin:{slug}"
        nodes.append({"id": concept_id, "kind": "origin_concept", "label": slug})
        edge("hasOrigin", concept_id)

    # -- events extracted from the Origin/Spread narrative (6.0.0) ----------
    # The first nodes in this graph that are DERIVED rather than parsed,
    # which is why every one carries the model that produced it: a
    # consumer must be able to tell a model's reading from a scraped fact.
    for ev in events:
        event_id = ev.get("event_id")
        if not event_id:
            continue
        start, end = date_range(ev.get("date"),
                                ev.get("date_precision") or "none")
        node_id = event_node_id(event_id)
        nodes.append(_compact({
            "id": node_id, "kind": "event",
            "source_text": ev.get("source_text"),
            "source_section": ev.get("source_section"),
            "date": ev.get("date"),          # property graph only, no triple
            "date_precision": ev.get("date_precision"),
            "date_basis": ev.get("date_basis"),
            "date_text": ev.get("date_text"),
            "date_start": start, "date_end": end,
            "location": ev.get("location"),
            "location_type": ev.get("location_type"),
            "certainty": ev.get("certainty"),
            "actors": list(ev.get("actors") or []),
            "extraction_model": ev.get("model"),
            "extraction_version": ev.get("extraction_version"),
        }))
        edge("hasEvent", node_id)
        # What the event was attached to, by position. Written directly
        # rather than through edge(): that closure is the FRAME's edges.
        attached: set[tuple[str, str]] = set()

        def event_edge(etype: str, dst: str | None) -> None:
            if dst and (etype, dst) not in attached:
                attached.add((etype, dst))
                edges.append({"src": node_id, "dst": dst, "type": etype})

        for link in ev.get("links") or []:
            event_edge(EVENT_MEDIA_EDGES.get(link.get("kind"), "eventLink"),
                       link.get("url"))
        for embed in ev.get("embeds") or []:
            event_edge(EVENT_EMBED_EDGE, embed.get("url"))
        for image in ev.get("images") or []:
            if image.get("src"):
                event_edge(EVENT_IMAGE_EDGE, image_node_id(image["src"]))
        if ev.get("date_anchor"):
            event_edge(EVENT_DATE_ANCHOR_EDGE, event_node_id(ev["date_anchor"]))

    # -- Wikidata entities from the title, tags and About (6.1.0) --------------
    # One node per QID, one frame edge per (field, QID), one occurrence per
    # mention: "Doge" three times in the About is one fromAbout edge with
    # three occurrences, and the same item from a tag is a separate fromTags
    # edge — which field said it is part of what was said.
    for m in entities:
        qid = m.get("qid")
        etype = ENTITY_FIELD_EDGES.get(m.get("field"))
        if not qid or not etype:
            continue
        node_id = wikidata_node_id(qid)
        nodes.append(_compact({"id": node_id, "kind": "wikidata_entity",
                               "qid": qid, "label": m.get("label"),
                               "description": m.get("description")}))
        edge(etype, node_id, _occurrence(
            mention_text=m.get("text"), link_score=m.get("score"),
            link_method=m.get("method"), ner_label=m.get("ner_label")))

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
