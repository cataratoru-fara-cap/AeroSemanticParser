"""kg/build.py: one entries doc -> (nodes, edges), the single producer both the
property graph and the RDF are generated from.

Pinned: the series parent is not also a frame citation (the RML exporter once
re-implemented this loop and emitted 14,571 extra mk:relatesToMeme triples);
a node is something other things can share, what is particular to one mention
lives on the frame-level edge (4.0.0); every field the parser extracts lands
somewhere, Origin and Spread included (5.1.0, against the real doge.html).
"""
from collections import Counter
from datetime import datetime

import pytest

from helpers import FIXTURES
from modules.kg import build
from modules.kg import events as kg_events
from modules.kym_parse import parse_entry

URL = "https://knowyourmeme.com/memes/doge"
SENS = "https://knowyourmeme.com/sensitive/memes/doge"
PARENT = "https://knowyourmeme.com/memes/shiba-inu"
OTHER = "https://knowyourmeme.com/memes/cheems"
EXTERNAL = "https://en.wikipedia.org/wiki/Doge_(meme)"
IMG = "https://i.kym-cdn.com/photos/images/original/000/1.jpg"
TIKTOK = "https://www.tiktok.com/@a/video/1"
EID = "event:abc123def4-0011223344"


def entry(**over):
    return {"url": URL, "title": "Doge", "category": "meme", "status": "confirmed", **over}


def section(kind="notable_examples", heading="Notable Examples", text=(), links=(), images=()):
    return {"kind": kind, "heading": heading, "level": 2, "text": list(text),
            "links": list(links), "images": list(images)}


def links(*urls, text="x"):
    return [section(links=[{"url": u, "text": text} for u in urls])]


def graph(**over):
    """(nodes by id — later occurrences win, as the store's merge does —, edges)."""
    kw = {k: over.pop(k) for k in ("origin_resolver", "tag_denylist", "events", "entities",
                                   "kept_address", "also_at") if k in over}
    nodes, edges = build.build_nodes_and_edges(entry(**over), **kw)
    ids: dict[str, dict] = {}
    for n in nodes:
        ids.setdefault(n["id"], {}).update(n)
    return ids, edges


def edge_set(edges):
    return {(e["src"], e["type"], e["dst"]) for e in edges}


def typed(edges, *types):
    return [e for e in edges if e["type"] in types]


def the_edge(edges, etype, dst):
    [e] = [e for e in edges if e["type"] == etype and e["dst"] == dst]
    return e


# -- vocabulary ---------------------------------------------------------------

def test_the_vocabulary_and_version():
    assert set(build.NODE_KINDS) == {"frame", "frame_stub", "entry_type_concept", "tag_concept",
                                     "region_concept", "origin_concept", "badge_concept", "external_ref",
                                     "image", "event", "wikidata_entity", "template"}
    assert set(build.EDGE_TYPES) == {
        "hasEntryType", "hasTag", "hasRegion", "hasOrigin", "hasBadge", "partOfSeries", "citesMediaFrame",
        "citesExternal", "hasImage", "hasEvent", "eventLink", "eventCitation", "eventEmbed", "eventImage",
        "eventDateAnchor", "nextInStory", "fromTitle", "fromTags", "fromAbout", "hasTemplate",
        "templateImage", "imgflipPage", "fromImage"}
    assert set(build.OCCURRENCE_EDGE_TYPES) <= set(build.EDGE_TYPES)
    assert build.KG_BUILD_VERSION == "7.1.0"
    assert build.STORY_SECTIONS == kg_events.SOURCE_SECTIONS


def assert_in_vocabulary(ids, edges):
    assert {n["kind"] for n in ids.values()} <= set(build.NODE_KINDS)
    assert {e["type"] for e in edges} <= set(build.EDGE_TYPES)
    for e in edges:
        for occ in e.get("occurrences") or ():
            assert e["type"] in build.OCCURRENCE_EDGE_TYPES
            assert occ and set(occ) <= set(build.OCCURRENCE_FIELDS)


def test_what_is_emitted_stays_in_the_vocabulary():
    assert_in_vocabulary(*graph(
        entry_type=["meme"], tags=["shiba"], region=["Japan"], series_parent=PARENT, og_image=IMG,
        sections=[section(text=["t"], links=[{"url": OTHER, "text": "x"}],
                          images=[{"src": IMG, "alt": "a", "caption": "c"}])],
        additional_references=[{"url": EXTERNAL, "name": "Wikipedia"}],
        external_references=[{"url": EXTERNAL, "index": 1, "text": "w"}]))
    assert_in_vocabulary(*graph(entities=[SHIBA, DOGE]))


# -- the frame ----------------------------------------------------------------

def test_the_frame_carries_its_attributes():
    ids, _ = graph(year=2013, origin="Tumblr", badges=["Sensitive"], aliases=["Shibe"],
                   kym_added=1_300_000_000, kym_last_updated=1_700_000_000, corpus_status="ready",
                   corpus_missing=[], parser_version="1.2.3", parsed_at=datetime(2026, 9, 1, 12),
                   meta={"description": "Doge is a meme."})
    frame = ids[URL]
    assert {k: frame[k] for k in ("kind", "label", "category", "status", "year", "from", "aliases", "added",
                                  "last_updated", "parsed_at", "description", "corpus_status",
                                  "parser_version")} == {
        "kind": "frame", "label": "Doge", "category": "meme", "status": "confirmed", "year": 2013,
        "from": "Tumblr", "aliases": ["Shibe"], "added": "2011-03-13T07:06:40Z",
        "last_updated": "2023-11-14T22:13:20Z", "parsed_at": "2026-09-01T12:00:00Z",
        "description": "Doge is a meme.", "corpus_status": "ready", "parser_version": "1.2.3"}
    assert "badges" not in frame          # 5.0.0: an edge, not a literal


def test_absent_values_are_not_stored_and_no_url_is_nothing():
    # morph-kgc emits nothing for an empty cell; neither may any store
    frame = graph(badges=[], year=None)[0][URL]
    assert not {"badges", "year", "corpus_missing", "section_texts", "also_at"} & set(frame)
    assert build.build_nodes_and_edges({"title": "x"}) == ([], [])


def test_sections_are_frame_properties_not_nodes():
    ids, _ = graph(sections=[
        section("about", "About", ["one", "two"]), section("other", "History", ["p1", "", "p2"]),
        section("origin", "Origin", ["deferred"]),
        section("various_examples", "Various Examples", images=[{"src": IMG}]),
        section("other", "Reception", ["p3"]), section("other", "", ["headless"])])
    assert ids[URL]["about"] == "one\n\ntwo"
    assert ids[URL]["section_texts"] == ["History\n\np1\n\np2", "Reception\n\np3", "headless"]
    assert {n["kind"] for n in ids.values()} == {"frame", "image"}
    assert "section_texts" not in graph(sections=[section("about", "About", ["only here"])])[0][URL]


@pytest.mark.parametrize("value, want", [
    (1788264000, "2026-09-01T12:00:00Z"), (datetime(2026, 9, 1, 12), "2026-09-01T12:00:00Z"),
    ("2026-09-01T12:00:00+00:00", "2026-09-01T12:00:00Z"), ("2026-09-01T14:00:00+02:00", "2026-09-01T12:00:00Z"),
    (None, None), ("", None), ("yesterday", None), (True, None), (object(), None),
])
def test_iso_utc_gives_one_lexical_form(value, want):
    assert build.iso_utc(value) == want


# -- concepts -----------------------------------------------------------------

def test_entry_types_tags_regions_and_badges_become_concepts():
    ids, edges = graph(entry_type=["exploitable", "image-macro"], tags=["  Shiba  ", "   ", "", "Catchphrases"],
                       region=[" Japan ", ""], badges=[" Sensitive ", ""])
    assert ids["type:exploitable"]["kind"] == "entry_type_concept"
    assert {(URL, "hasEntryType", "type:exploitable"), (URL, "hasTag", "tag:shiba"),
            (URL, "hasTag", "tag:catchphrase")} <= edge_set(edges)            # lowercased, stripped, folded
    assert [n["id"] for n in ids.values() if n["kind"] == "tag_concept"] == ["tag:shiba", "tag:catchphrase"]
    assert ids["region:Japan"]["kind"] == "region_concept"
    assert typed(edges, "hasRegion") == [{"src": URL, "type": "hasRegion", "dst": "region:Japan"}]
    assert (ids["badge:sensitive"]["kind"], ids["badge:sensitive"]["label"]) == ("badge_concept", "Sensitive")
    assert typed(edges, "hasBadge") == [{"src": URL, "type": "hasBadge", "dst": "badge:sensitive"}]
    assert graph(tags=["   ", ""])[1] == []


def test_the_tag_denylist_exempts_a_tag_from_folding():
    assert "tag:news" in graph(tags=["News"], tag_denylist=frozenset({"news"}))[0]


def test_origin_is_a_concept_only_through_the_resolver():
    lower = lambda raw: raw.lower()                                # noqa: E731
    ids, edges = graph(origin="Twitter")                           # no resolver: back-compat
    assert not [n for n in ids.values() if n["kind"] == "origin_concept"] and not typed(edges, "hasOrigin")
    assert ids[URL]["from"] == "Twitter"
    ids, edges = graph(origin="Twitter", origin_resolver=lower)
    assert ids["origin:twitter"]["kind"] == "origin_concept"
    assert typed(edges, "hasOrigin") == [{"src": URL, "type": "hasOrigin", "dst": "origin:twitter"}]
    assert ids[URL]["from"] == "Twitter"                           # an added layer, not a replacement
    assert not typed(graph(origin="", origin_resolver=lower)[1], "hasOrigin")


# -- links: by what they point at, never by the field ---------------------------

def test_the_series_parent_is_a_stub_and_a_series_edge_and_nothing_else():
    ids, edges = graph(series_parent=PARENT)
    assert ids[PARENT]["kind"] == "frame_stub" and ids[PARENT] == build.guess_stub_node(PARENT)
    assert (URL, "partOfSeries", PARENT) in edge_set(edges)
    # THE REGRESSION: the parent is linked in the body too, as on every real page
    _, edges = graph(series_parent=PARENT, sections=[section(links=[{"url": PARENT, "text": "Shiba"}])])
    assert {e["type"] for e in edges if e["dst"] == PARENT} == {"partOfSeries"}


def test_links_are_classified_by_their_target():
    ids, edges = graph(external_references=[{"url": OTHER}], sections=links(EXTERNAL, text="wiki"))
    assert {(URL, "citesMediaFrame", OTHER), (URL, "citesExternal", EXTERNAL)} <= edge_set(edges)
    assert ids[EXTERNAL]["kind"] == "external_ref"
    assert graph(sections=links(URL))[1] == []                     # a self-link is no edge
    _, edges = graph(sections=links("https://a.example/1", text=""),
                     additional_references=[{"url": "https://b.example/2", "name": "B"}],
                     external_references=[{"url": "https://c.example/3"}])
    assert len(typed(edges, "citesExternal")) == 3                 # every link-bearing field is read
    www = "https://www.knowyourmeme.com/memes/pepe"
    assert (URL, "citesMediaFrame", www) in edge_set(graph(sections=links(www, text="pepe"))[1])


def test_every_entry_path_is_a_media_frame():
    # KYM's older paths are still linked from pages, and are entries
    targets = ["https://knowyourmeme.com/memes/subcultures/akira", "https://knowyourmeme.com/sensitive/memes/goatse",
               "https://knowyourmeme.com/people/elon-musk", "https://knowyourmeme.com/sites/Reddit",
               "https://knowyourmeme.com/memes/assassins-creed-logo?ref=related-entries"]
    ids, edges = graph(sections=links(*targets))
    assert {e["dst"] for e in typed(edges, "citesMediaFrame")} == set(targets)
    assert {ids[t]["kind"] for t in targets} == {"frame_stub"}


def test_a_kym_page_that_is_not_an_entry_is_cites_external():
    # 7.0.0: 2,841 such links in 6.5.0 pointed at frame stubs
    targets = ["https://knowyourmeme.com/photos/1220637-who-would-win",
               "https://knowyourmeme.com/videos/14824-abandon-thread", "https://knowyourmeme.com/news/some-story",
               "https://knowyourmeme.com/editorials/guides/whats-the-41-meme",
               "https://knowyourmeme.com/types/remix?status=all", "https://knowyourmeme.com/users/olivia-gulin",
               "https://knowyourmeme.com/forums/general/topics/3937-faq", "https://knowyourmeme.com/login",
               "https://knowyourmeme.com/search?q=japan", "https://knowyourmeme.com/memes",
               "https://knowyourmeme.com/memes/subcultures/"]
    ids, edges = graph(sections=links(*targets))
    assert {e["type"] for e in edges} == {"citesExternal"} and {e["dst"] for e in edges} == set(targets)
    assert {ids[t]["kind"] for t in targets} == {"external_ref"}


@pytest.mark.parametrize("url, category", [
    ("https://knowyourmeme.com/memes/people/x", "person"), ("https://knowyourmeme.com/memes/events/x", "event"),
    ("https://knowyourmeme.com/memes/x", "meme"), ("https://knowyourmeme.com/memes/subcultures/x", "subculture"),
    ("https://knowyourmeme.com/photos/1", None),        # unknown: guess nothing rather than wrong
])
def test_a_stub_guesses_its_category_from_the_path(url, category):
    assert build.guess_stub_node(url)["category"] == category


# -- occurrences ----------------------------------------------------------------

def test_repeated_mentions_are_one_edge_with_every_occurrence():
    _, edges = graph(sections=[section(heading="About", links=[{"url": OTHER, "text": " Cheems "}]),
                               section("other", "Spread", links=[{"url": OTHER, "text": "the dog"}])],
                     additional_references=[{"url": OTHER, "name": "KYM"}],
                     external_references=[{"url": OTHER, "index": 4, "text": "Cheems – KYM"}])
    assert the_edge(edges, "citesMediaFrame", OTHER)["occurrences"] == [
        {"anchor_text": "Cheems", "in_section": "About"}, {"anchor_text": "the dog", "in_section": "Spread"},
        {"site_name": "KYM"}, {"citation_text": "Cheems – KYM", "citation_index": 4}]


def test_occurrences_from_origin_and_empty_mentions():
    # until 5.1.0 an origin/spread link yielded the edge but no occurrence
    _, edges = graph(sections=[section("origin", "Origin", links=[{"url": OTHER, "text": "x"}])])
    assert the_edge(edges, "citesMediaFrame", OTHER)["occurrences"] == [{"anchor_text": "x", "in_section": "Origin"}]
    _, edges = graph(sections=[section(heading="", links=[{"url": EXTERNAL, "text": "  "}])])
    assert "occurrences" not in the_edge(edges, "citesExternal", EXTERNAL)


def test_links_and_references_are_no_nodes():
    ids, edges = graph(sections=links(EXTERNAL, text="w"), external_references=[{"url": EXTERNAL, "index": 1}])
    assert Counter(n["kind"] for n in ids.values()) == {"frame": 1, "external_ref": 1}
    assert [e["type"] for e in edges] == ["citesExternal"]


def test_an_image_on_the_page_and_in_sections_is_one_edge():
    ids, edges = graph(og_image=IMG, meta={"og:image:width": "600", "og:image:height": "nope"},
                       sections=[section(images=[{"src": IMG, "alt": "doge", "caption": "wow"}]),
                                 section("origin", "Origin", images=[{"src": IMG, "caption": "in origin"}])])
    assert ids[f"image:{IMG}"] == {"id": f"image:{IMG}", "kind": "image", "width": 600}
    assert the_edge(edges, "hasImage", f"image:{IMG}")["occurrences"] == [
        {"role": "page"}, {"role": "section", "in_section": "Notable Examples", "alt_text": "doge", "caption": "wow"},
        {"role": "section", "in_section": "Origin", "caption": "in origin"}]


def test_a_caption_lives_on_the_edge_and_image_ids_cannot_collide_with_links():
    _, a = graph(sections=[section(images=[{"src": IMG, "caption": "first"}])])
    ids, b = graph(url=OTHER, sections=[section(images=[{"src": IMG, "caption": "second"}])])
    assert "caption" not in ids[f"image:{IMG}"]
    assert (a[0]["occurrences"][0]["caption"], b[0]["occurrences"][0]["caption"]) == ("first", "second")
    ids, _ = graph(sections=[section(links=[{"url": IMG, "text": "img"}], images=[{"src": IMG}])])
    assert (ids[IMG]["kind"], ids[f"image:{IMG}"]["kind"]) == ("external_ref", "image")


# -- the real page, through the real parser (every section in scope, 5.1.0) ----

@pytest.fixture(scope="module")
def doge():
    doc = parse_entry((FIXTURES / "doge.html").read_text(encoding="utf-8")).model_dump(
        mode="json", exclude_none=True)
    nodes, edges = build.build_nodes_and_edges(doc)
    ids = {}
    for n in nodes:
        ids.setdefault(n["id"], {}).update(n)

    def occ(*types, **match):
        return [o for e in edges if e["type"] in types for o in e.get("occurrences", [])
                if all(o.get(k) == v for k, v in match.items())]
    return doc, ids[doc["url"]], edges, occ


def test_the_real_page_keeps_every_section(doge):
    doc, frame, _, _ = doge
    assert {"origin", "spread"} <= {s["kind"] for s in doc["sections"]}   # the fixture has what is covered
    for kind, prop in build.NARRATIVE_SECTION_PROPERTIES.items():
        assert frame[prop].strip(), kind
        assert not [t for t in frame["section_texts"] if frame[prop] in t]
    kept = [s for s in doc["sections"] if s["kind"] not in build.NARRATIVE_SECTION_PROPERTIES and any(s.get("text"))]
    assert kept and len(frame["section_texts"]) == len(kept)
    assert {"label", "category", "status", "year", "from", "about", "origin_text", "spread_text", "added",
            "last_updated"} <= set(frame)


def test_the_real_page_keeps_every_image_embed_link_and_reference(doge):
    doc, _, edges, occ = doge
    assert len(occ("hasImage", role="section")) == sum(len(s.get("images", [])) for s in doc["sections"])
    embeds = [(s["heading"], e["platform"]) for s in doc["sections"] for e in s.get("embeds", [])]
    got = {(o["in_section"], o["site_name"]) for o in occ("citesExternal", "citesMediaFrame")
           if "site_name" in o and "in_section" in o}
    assert embeds and set(embeds) <= got                   # parser 1.6.0: Instagram reels
    headings = {o["in_section"] for o in occ("citesMediaFrame", "citesExternal") if "in_section" in o}
    for kind in ("origin", "spread"):
        assert next(s["heading"] for s in doc["sections"] if s["kind"] == kind) in headings
    refs = occ("citesMediaFrame", "citesExternal")
    # an embed also carries a site name, but always inside a section
    assert len([o for o in refs if "citation_text" in o or "citation_index" in o]) == len(doc.get("external_references", []))
    assert len([o for o in refs if "site_name" in o and "in_section" not in o]) == len(doc.get("additional_references", []))
    assert len(typed(edges, "hasBadge")) == len(doc.get("badges") or [])
    _, with_origin = build.build_nodes_and_edges(doc, origin_resolver=lambda raw: raw.lower())
    assert [e["dst"] for e in typed(with_origin, "hasOrigin")] == [f"origin:{doc['origin'].lower()}"]


# -- event dates: intervals, asserted literally (morph-kgc must reproduce them) --

@pytest.mark.parametrize("date, precision, want", [
    ("2013-05-04", "day", ("2013-05-04T00:00:00Z", "2013-05-04T23:59:59Z")),
    ("2013-05", "month", ("2013-05-01T00:00:00Z", "2013-05-31T23:59:59Z")),
    ("2013", "year", ("2013-01-01T00:00:00Z", "2013-12-31T23:59:59Z")),
    ("2013-12", "month", ("2013-12-01T00:00:00Z", "2013-12-31T23:59:59Z")),
    ("2024-02", "month", ("2024-02-01T00:00:00Z", "2024-02-29T23:59:59Z")),   # a leap year
    ("2023-02", "month", ("2023-02-01T00:00:00Z", "2023-02-28T23:59:59Z")),
    (None, "none", (None, None)), ("", "day", (None, None)), ("2013", "none", (None, None)),
    ("2013", "day", (None, None)), ("not a date", "year", (None, None)), ("2013-05-04", None, (None, None)),
])
def test_a_date_is_the_interval_of_its_precision(date, precision, want):
    assert build.date_range(date, precision) == want


# -- events ---------------------------------------------------------------------

EV = {"event_id": "abc123def4-0011223344", "sentences": [1],
      "source_text": "The photo was posted to Tumblr on February 23rd, 2010.", "source_section": "origin",
      "date": "2010-02-23", "date_precision": "day", "date_text": "February 23rd, 2010", "locations": ["Tumblr"],
      "location_type": "platform", "certainty": "confirmed", "actors": ["Atsuko Sato"],
      "model": "ministral-3:14b", "extraction_version": "1.0.0"}


def from_event(edges, eid=EID):
    return {(e["type"], e["dst"]) for e in edges if e["src"] == eid}


def test_one_event_is_one_node_and_one_plain_edge():
    ids, edges = graph(events=[EV])
    node = ids[EID]
    assert {k: node[k] for k in ("kind", "source_text", "source_section", "actors", "extraction_model",
                                 "date_start", "date_end")} == {
        "kind": "event", "source_text": EV["source_text"], "source_section": "origin", "actors": ["Atsuko Sato"],
        "extraction_model": "ministral-3:14b", "date_start": "2010-02-23T00:00:00Z", "date_end": "2010-02-23T23:59:59Z"}
    assert "summary" not in node and set(node) - {"id", "kind"} <= set(build.EVENT_PROPERTIES)
    e = the_edge(edges, "hasEvent", EID)
    assert e["src"] == URL and "occurrences" not in e
    assert from_event(edges) == set()                      # no media: only its hasEvent edge


def test_no_events_duplicate_or_id_less_events():
    for events, n in (([], 0), ([EV, dict(EV)], 1), ([dict(EV, event_id=None)], 0)):
        ids, edges = graph(events=events)
        assert len([x for x in ids.values() if x["kind"] == "event"]) == n
        assert len(typed(edges, "hasEvent")) == n


def test_attached_media_are_event_edges_and_a_relative_date_points_at_its_anchor():
    ev = dict(EV, links=[{"url": OTHER, "text": "Cheems", "kind": "link"},
                         {"url": EXTERNAL, "text": "[3]", "kind": "citation"}],
              embeds=[{"url": TIKTOK, "platform": "tiktok"}], images=[{"src": IMG, "caption": "the photo"}])
    assert from_event(graph(events=[ev])[1]) == {("eventLink", OTHER), ("eventCitation", EXTERNAL),
                                                 ("eventEmbed", TIKTOK), ("eventImage", f"image:{IMG}")}
    anchor = dict(EV, event_id="anchor01-0000000000")
    derived = dict(EV, event_id="derived1-1111111111", date_basis="relative", date_anchor="anchor01-0000000000")
    ids, edges = graph(events=[anchor, derived])
    assert ids["event:derived1-1111111111"]["date_basis"] == "relative"
    assert ("eventDateAnchor", "event:anchor01-0000000000") in from_event(edges, "event:derived1-1111111111")


def test_an_undated_event_emits_no_interval():
    node = graph(events=[dict(EV, date=None, date_precision="none")])[0][EID]
    assert "date_start" not in node and "date_end" not in node and node["date_precision"] == "none"


def story(events):
    chain = {e["src"]: e["dst"] for e in typed(graph(events=events)[1], "nextInStory")}
    out = list(set(chain) - set(chain.values()))
    while out and out[-1] in chain:
        out.append(chain[out[-1]])
    return [n.split("-")[0].split(":")[1] for n in out], len(chain)


def told(eid, section, sentences, date_text=None, text="x", **over):
    return dict(EV, event_id=f"{eid}-0000000000", source_section=section, sentences=sentences,
                date_text=date_text, source_text=text, **over)


@pytest.mark.parametrize("events, order, n_links", [
    # arrival order does not matter: spread before origin, a later sentence first
    ([told("s2", "spread", [3]), told("o1", "origin", [1]), told("s1", "spread", [1, 2]),
      told("o2", "origin", [2, 3])], ["o1", "o2", "s1", "s2"], 3),
    ([told("b", "spread", [1], "April 8th, 2024", "In April 2024, he posted memes, with one example on April 8th, 2024."),
      told("a", "spread", [1], "In April 2024", "In April 2024, he posted memes, with one example on April 8th, 2024.")],
     ["a", "b"], 1),                                    # two events of one sentence: their date words
    # spengbab: "a month earlier" is told after the December post and stays after it
    ([told("dec", "origin", [2], date="2006-12", date_precision="month"),
      told("nov", "origin", [3], date="2006-11", date_precision="month")], ["dec", "nov"], 1),
    ([EV], [], 0), ([EV, dict(EV)], [], 0),            # one event, or a repeated id: no link
])
def test_the_story_is_one_chain_in_page_order(events, order, n_links):
    got, links_ = story(events)
    assert links_ == n_links and (not order or got == order)


def test_events_and_entities_do_not_disturb_the_rest_of_the_graph():
    plain_nodes, plain_edges = build.build_nodes_and_edges(entry(tags=["shiba"]))
    nodes, edges = build.build_nodes_and_edges(entry(tags=["shiba"]), events=[EV])
    assert [n for n in nodes if n["kind"] != "event"] == plain_nodes
    assert [e for e in edges if e["type"] != "hasEvent"] == plain_edges
    nodes, edges = build.build_nodes_and_edges(entry(tags=["shiba"]), entities=[SHIBA, DOGE])
    assert [n for n in nodes if n["kind"] != "wikidata_entity"] == plain_nodes
    assert [e for e in edges if e["type"] not in build.ENTITY_FIELD_EDGES.values()] == plain_edges


# -- Wikidata entities (6.1.0), data like events ----------------------------------

SHIBA = {"field": "about", "text": "Shiba Inus", "qid": "Q39315", "label": "Shiba Inu",
         "description": "dog breed", "score": 0.83, "method": "ner", "ner_label": "ORG"}
DOGE = {"field": "title", "text": "Doge", "qid": "Q15894956", "label": "Doge",
        "description": "Internet meme", "score": 1.0, "method": "kym_id"}


def test_no_entities_means_no_entity_node_or_edge():
    ids, edges = graph()
    assert not [n for n in ids.values() if n["kind"] == "wikidata_entity"]
    assert not {e["type"] for e in edges} & set(build.ENTITY_FIELD_EDGES.values())


def test_a_link_is_a_shared_node_and_a_field_named_edge_with_its_occurrences():
    ids, edges = graph(entities=[dict(SHIBA, relevance_basis="judge"), dict(SHIBA, text="Shiba Inu", score=0.9)])
    assert ids["wd:Q39315"] == {"id": "wd:Q39315", "kind": "wikidata_entity", "qid": "Q39315",
                                "label": "Shiba Inu", "description": "dog breed"}
    e = the_edge(edges, "fromAbout", "wd:Q39315")
    assert e["src"] == URL
    assert e["occurrences"] == [   # 6.5.0: a kept link says why it was kept
        {"mention_text": "Shiba Inus", "link_score": 0.83, "link_method": "ner", "ner_label": "ORG",
         "relevance_basis": "judge"},
        {"mention_text": "Shiba Inu", "link_score": 0.9, "link_method": "ner", "ner_label": "ORG"}]


def test_each_field_has_its_own_edge_type_and_an_absent_label_is_not_stored():
    tag = dict(SHIBA, field="tag", text="shiba inu", method="tag", ner_label=None)
    _, edges = graph(entities=[DOGE, tag, SHIBA])
    assert {(e["type"], e["dst"]) for e in typed(edges, *build.ENTITY_FIELD_EDGES.values())} == {
        ("fromTitle", "wd:Q15894956"), ("fromTags", "wd:Q39315"), ("fromAbout", "wd:Q39315")}
    [occ] = the_edge(edges, "fromTitle", "wd:Q15894956")["occurrences"]
    assert "ner_label" not in occ and occ["link_method"] == "kym_id"


def test_a_link_without_a_qid_or_a_known_field_is_skipped():
    ids, _ = graph(entities=[dict(SHIBA, qid=None), dict(SHIBA, field="spread")])
    assert not [n for n in ids.values() if n["kind"] == "wikidata_entity"]


# -- one entry, one address (gap 14) -------------------------------------------------

def test_links_to_a_dropped_address_reach_the_kept_one():
    child, old_parent = "https://knowyourmeme.com/memes/swole-doge", "https://knowyourmeme.com/memes/old-parent"
    _, edges = graph(url=child, series_parent=URL, sections=[section(links=[
        {"url": URL, "text": "Doge"}, {"url": old_parent, "text": "p"}])],
        kept_address={URL: SENS, old_parent: PARENT})
    got = edge_set(edges)
    assert {(child, "partOfSeries", SENS), (child, "citesMediaFrame", PARENT)} <= got
    assert not [e for e in edges if e["dst"] in (URL, old_parent)]
    assert (child, "citesMediaFrame", SENS) not in got     # the parent is not also a citation
    # a link to its own other address is a self-link
    _, edges = graph(url=SENS, series_parent=URL, sections=links(URL, text="Doge"), kept_address={URL: SENS})
    assert not typed(edges, "partOfSeries", "citesMediaFrame")
    _, edges = graph(url=OTHER, events=[dict(EV, links=[{"url": URL, "text": "Doge", "kind": "link"}])],
                     kept_address={URL: SENS})
    assert (EID, "eventLink", SENS) in edge_set(edges)


def test_the_kept_frame_lists_the_addresses_it_absorbed():
    assert graph(url=SENS, also_at=[URL])[0][SENS]["also_at"] == [URL]
