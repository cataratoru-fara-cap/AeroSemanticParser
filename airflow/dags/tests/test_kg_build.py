"""Tests for kg/build.py — one entries doc -> (nodes, edges).

This module is the single producer: both the property-graph and the RDF
projections are generated from its output, so anything asserted here is
asserted about both representations at once.

``test_series_parent_is_not_also_a_relates_edge`` pins a real defect. The
RML exporter re-implemented this loop and started its per-entry ``seen``
set empty, where this module seeds it with ``series_parent``. The result
was 14,571 ``mk:relatesToMeme`` triples in the published kg_output.nt that
the property graph did not contain. The two projections now share this
code; the test stops the divergence coming back.

``OccurrenceTests`` pins the 4.0.0 rule: a node is something other things
can share. Page sections, body links and references are not nodes; what
was particular to one mention lives on the frame-level edge.

``FullRecordTests`` pins the scope against the real doge.html fixture:
every field the parser extracts lands somewhere in the graph, except the
Origin/Spread sections, which are deferred on purpose.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_build.py -v
"""
import os
import unittest
from collections import Counter
from datetime import datetime

from modules.kg import build

URL = "https://knowyourmeme.com/memes/doge"
PARENT = "https://knowyourmeme.com/memes/shiba-inu"
OTHER = "https://knowyourmeme.com/memes/cheems"
EXTERNAL = "https://en.wikipedia.org/wiki/Doge_(meme)"
IMG = "https://i.kym-cdn.com/photos/images/original/000/1.jpg"


def entry(**over) -> dict:
    base = {"url": URL, "title": "Doge", "category": "meme",
            "status": "confirmed"}
    base.update(over)
    return base


def section(kind="notable_examples", heading="Notable Examples", level=2,
            text=(), links=(), images=()):
    return {"kind": kind, "heading": heading, "level": level,
            "text": list(text), "links": list(links), "images": list(images)}


def nodes_by_id(nodes):
    """Later occurrences win, as the store's merge does."""
    out: dict[str, dict] = {}
    for n in nodes:
        out.setdefault(n["id"], {}).update(n)
    return out


def edge_set(edges):
    return {(e["src"], e["type"], e["dst"]) for e in edges}


def the_edge(edges, etype, dst):
    matches = [e for e in edges if e["type"] == etype and e["dst"] == dst]
    assert len(matches) == 1, f"{len(matches)} {etype} edges to {dst}"
    return matches[0]


class VocabularyTests(unittest.TestCase):
    def test_constants_are_exported(self):
        self.assertEqual(set(build.NODE_KINDS),
                         {"frame", "frame_stub", "entry_type_concept",
                          "tag_concept", "region_concept", "origin_concept",
                          "badge_concept", "external_ref", "image"})
        self.assertEqual(set(build.EDGE_TYPES),
                         {"hasEntryType", "hasTag", "hasRegion", "hasOrigin",
                          "hasBadge", "partOfSeries", "relatesToMeme",
                          "citesExternal", "hasImage"})
        self.assertLessEqual(set(build.OCCURRENCE_EDGE_TYPES), set(build.EDGE_TYPES))

    def test_version_is_stamped(self):
        self.assertEqual(build.KG_BUILD_VERSION, "5.0.1")

    def test_emitted_kinds_types_and_occurrence_fields_stay_in_the_vocabulary(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            entry_type=["meme"], tags=["shiba"], region=["Japan"],
            series_parent=PARENT, og_image=IMG,
            sections=[section(text=["t"], links=[{"url": OTHER, "text": "x"}],
                              images=[{"src": IMG, "alt": "a", "caption": "c"}])],
            additional_references=[{"url": EXTERNAL, "name": "Wikipedia"}],
            external_references=[{"url": EXTERNAL, "index": 1, "text": "w"}]))
        self.assertLessEqual({n["kind"] for n in nodes}, set(build.NODE_KINDS))
        self.assertLessEqual({e["type"] for e in edges}, set(build.EDGE_TYPES))
        for e in edges:
            if "occurrences" in e:
                self.assertIn(e["type"], build.OCCURRENCE_EDGE_TYPES)
                for occ in e["occurrences"]:
                    self.assertLessEqual(set(occ), set(build.OCCURRENCE_FIELDS))
                    self.assertTrue(occ)


class FrameNodeTests(unittest.TestCase):
    def test_frame_carries_its_attributes(self):
        nodes, _ = build.build_nodes_and_edges(entry(
            year=2013, origin="Tumblr", badges=["Sensitive"],
            aliases=["Shibe"], kym_added=1_300_000_000,
            kym_last_updated=1_700_000_000, corpus_status="ready",
            corpus_missing=[], parser_version="1.2.3",
            parsed_at=datetime(2026, 9, 1, 12, 0, 0),
            meta={"description": "Doge is a meme."}))
        frame = nodes_by_id(nodes)[URL]
        self.assertEqual(frame["kind"], "frame")
        self.assertEqual(frame["label"], "Doge")
        self.assertEqual((frame["category"], frame["status"]), ("meme", "confirmed"))
        self.assertEqual(frame["year"], 2013)
        self.assertEqual(frame["from"], "Tumblr")          # the infobox origin FIELD
        self.assertNotIn("badges", frame)     # 5.0.0: an edge now, not a literal
        self.assertEqual(frame["aliases"], ["Shibe"])
        self.assertEqual(frame["added"], "2011-03-13T07:06:40Z")
        self.assertEqual(frame["last_updated"], "2023-11-14T22:13:20Z")
        self.assertEqual(frame["parsed_at"], "2026-09-01T12:00:00Z")
        self.assertEqual(frame["description"], "Doge is a meme.")
        self.assertEqual(frame["corpus_status"], "ready")
        self.assertEqual(frame["parser_version"], "1.2.3")

    def test_absent_values_are_not_stored(self):
        # morph-kgc emits nothing for an empty cell; neither may any store.
        nodes, _ = build.build_nodes_and_edges(entry(badges=[], year=None))
        frame = nodes_by_id(nodes)[URL]
        for absent in ("badges", "year", "corpus_missing", "section_texts"):
            self.assertNotIn(absent, frame)

    def test_entry_without_url_yields_nothing(self):
        self.assertEqual(build.build_nodes_and_edges({"title": "x"}), ([], []))


class SectionTextTests(unittest.TestCase):
    def test_sections_are_frame_properties_not_nodes(self):
        nodes, edges = build.build_nodes_and_edges(entry(sections=[
            section(kind="about", heading="About", text=["one", "two"]),
            section(kind="other", heading="History", text=["p1", "", "p2"]),
            section(kind="origin", heading="Origin", text=["deferred"]),
            section(kind="various_examples", heading="Various Examples", text=[],
                    images=[{"src": IMG}]),
            section(kind="other", heading="Reception", text=["p3"]),
            section(kind="other", heading="", text=["headless"])]))
        frame = nodes_by_id(nodes)[URL]
        self.assertEqual(frame["about"], "one\n\ntwo")
        self.assertEqual(frame["section_texts"],
                         ["History\n\np1\n\np2", "Reception\n\np3", "headless"])
        self.assertEqual({n["kind"] for n in nodes}, {"frame", "image"})

    def test_about_is_not_repeated_in_section_texts(self):
        nodes, _ = build.build_nodes_and_edges(entry(sections=[
            section(kind="about", heading="About", text=["only here"])]))
        self.assertNotIn("section_texts", nodes_by_id(nodes)[URL])


class IsoUtcTests(unittest.TestCase):
    def test_every_input_shape_gives_one_lexical_form(self):
        expected = "2026-09-01T12:00:00Z"
        self.assertEqual(build.iso_utc(1788264000), expected)
        self.assertEqual(build.iso_utc(datetime(2026, 9, 1, 12)), expected)
        self.assertEqual(build.iso_utc("2026-09-01T12:00:00+00:00"), expected)
        self.assertEqual(build.iso_utc("2026-09-01T14:00:00+02:00"), expected)

    def test_garbage_is_none(self):
        for bad in (None, "", "yesterday", True, object()):
            self.assertIsNone(build.iso_utc(bad))


class ConceptTests(unittest.TestCase):
    def test_entry_types_become_prefixed_concepts(self):
        nodes, edges = build.build_nodes_and_edges(
            entry(entry_type=["exploitable", "image-macro"]))
        ids = nodes_by_id(nodes)
        self.assertEqual(ids["type:exploitable"]["kind"], "entry_type_concept")
        self.assertIn((URL, "hasEntryType", "type:exploitable"), edge_set(edges))

    def test_tags_are_lowercased_and_stripped(self):
        nodes, edges = build.build_nodes_and_edges(entry(tags=["  Shiba  "]))
        self.assertIn("tag:shiba", nodes_by_id(nodes))
        self.assertIn((URL, "hasTag", "tag:shiba"), edge_set(edges))

    def test_blank_tags_are_skipped(self):
        nodes, edges = build.build_nodes_and_edges(entry(tags=["   ", ""]))
        self.assertEqual([n for n in nodes if n["kind"] == "tag_concept"], [])
        self.assertEqual(edges, [])

    def test_regions_become_concepts(self):
        nodes, edges = build.build_nodes_and_edges(entry(region=[" Japan ", ""]))
        self.assertEqual(nodes_by_id(nodes)["region:Japan"]["kind"], "region_concept")
        self.assertEqual([e for e in edges if e["type"] == "hasRegion"],
                         [{"src": URL, "type": "hasRegion", "dst": "region:Japan"}])

    def test_badges_become_concepts(self):
        nodes, edges = build.build_nodes_and_edges(
            entry(badges=[" Sensitive ", ""]))
        self.assertEqual(nodes_by_id(nodes)["badge:sensitive"]["kind"], "badge_concept")
        self.assertEqual(nodes_by_id(nodes)["badge:sensitive"]["label"], "Sensitive")
        self.assertEqual([e for e in edges if e["type"] == "hasBadge"],
                         [{"src": URL, "type": "hasBadge", "dst": "badge:sensitive"}])

    def test_no_origin_edge_without_a_resolver(self):
        """The default (no origin_resolver given): back-compat with every
        existing caller that doesn't know about origin_concept."""
        nodes, edges = build.build_nodes_and_edges(entry(origin="Twitter"))
        self.assertEqual([n for n in nodes if n["kind"] == "origin_concept"], [])
        self.assertEqual([e for e in edges if e["type"] == "hasOrigin"], [])
        self.assertEqual(nodes_by_id(nodes)[URL]["from"], "Twitter")

    def test_origin_becomes_a_concept_via_the_resolver(self):
        nodes, edges = build.build_nodes_and_edges(
            entry(origin="Twitter"), origin_resolver=lambda raw: raw.lower())
        self.assertEqual(nodes_by_id(nodes)["origin:twitter"]["kind"], "origin_concept")
        self.assertEqual([e for e in edges if e["type"] == "hasOrigin"],
                         [{"src": URL, "type": "hasOrigin", "dst": "origin:twitter"}])
        # "from" (the raw literal) is untouched -- origin_concept is an
        # ADDED layer, not a replacement.
        self.assertEqual(nodes_by_id(nodes)[URL]["from"], "Twitter")

    def test_no_origin_edge_for_an_empty_origin(self):
        nodes, edges = build.build_nodes_and_edges(
            entry(origin=""), origin_resolver=lambda raw: raw.lower())
        self.assertEqual([e for e in edges if e["type"] == "hasOrigin"], [])

    def test_tags_are_plural_folded(self):
        nodes, edges = build.build_nodes_and_edges(
            entry(tags=["Catchphrases"]))
        self.assertIn("tag:catchphrase", nodes_by_id(nodes))
        self.assertIn((URL, "hasTag", "tag:catchphrase"), edge_set(edges))

    def test_tag_denylist_exempts_a_tag_from_folding(self):
        nodes, edges = build.build_nodes_and_edges(
            entry(tags=["News"]), tag_denylist=frozenset({"news"}))
        self.assertIn("tag:news", nodes_by_id(nodes))


class LinkClassificationTests(unittest.TestCase):
    """Classification is by the link's HOST, never by the field it came from."""

    def test_series_parent_becomes_a_stub_and_a_series_edge(self):
        nodes, edges = build.build_nodes_and_edges(entry(series_parent=PARENT))
        self.assertEqual(nodes_by_id(nodes)[PARENT]["kind"], "frame_stub")
        self.assertIn((URL, "partOfSeries", PARENT), edge_set(edges))

    def test_series_parent_is_not_also_a_relates_edge(self):
        # THE REGRESSION. The parent is linked in the body too, as it always
        # is on a real page; it must produce partOfSeries and nothing else.
        _, edges = build.build_nodes_and_edges(entry(
            series_parent=PARENT,
            sections=[section(links=[{"url": PARENT, "text": "Shiba"}])]))
        self.assertEqual({e["type"] for e in edges if e["dst"] == PARENT}, {"partOfSeries"})

    def test_kym_link_in_external_references_is_still_relatesToMeme(self):
        _, edges = build.build_nodes_and_edges(entry(external_references=[{"url": OTHER}]))
        self.assertIn((URL, "relatesToMeme", OTHER), edge_set(edges))

    def test_outside_link_in_a_body_section_is_still_citesExternal(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": EXTERNAL, "text": "wiki"}])]))
        self.assertIn((URL, "citesExternal", EXTERNAL), edge_set(edges))
        self.assertEqual(nodes_by_id(nodes)[EXTERNAL]["kind"], "external_ref")

    def test_self_link_is_not_an_edge(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": URL, "text": "Doge"}])]))
        self.assertEqual(edges, [])

    def test_all_link_bearing_fields_are_read(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": "https://a.example/1", "text": ""}])],
            additional_references=[{"url": "https://b.example/2", "name": "B"}],
            external_references=[{"url": "https://c.example/3"}]))
        self.assertEqual(len([e for e in edges if e["type"] == "citesExternal"]), 3)

    def test_links_in_deferred_sections_still_feed_frame_level_edges(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(kind="origin", links=[{"url": OTHER, "text": "x"}])]))
        e = the_edge(edges, "relatesToMeme", OTHER)
        self.assertNotIn("occurrences", e)             # anchor text is deferred too

    def test_www_host_counts_as_internal(self):
        www = "https://www.knowyourmeme.com/memes/pepe"
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": www, "text": "pepe"}])]))
        self.assertIn((URL, "relatesToMeme", www), edge_set(edges))


class OccurrenceTests(unittest.TestCase):
    def test_repeated_mentions_are_one_edge_with_every_occurrence(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(heading="About", links=[{"url": OTHER, "text": " Cheems "}]),
                      section(heading="Spread", kind="other",
                              links=[{"url": OTHER, "text": "the dog"}])],
            additional_references=[{"url": OTHER, "name": "KYM"}],
            external_references=[{"url": OTHER, "index": 4, "text": "Cheems – KYM"}]))
        e = the_edge(edges, "relatesToMeme", OTHER)
        self.assertEqual(e["occurrences"], [
            {"anchor_text": "Cheems", "in_section": "About"},
            {"anchor_text": "the dog", "in_section": "Spread"},
            {"site_name": "KYM"},
            {"citation_text": "Cheems – KYM", "citation_index": 4},
        ])

    def test_an_empty_mention_adds_no_occurrence(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(heading="", links=[{"url": EXTERNAL, "text": "  "}])]))
        self.assertNotIn("occurrences", the_edge(edges, "citesExternal", EXTERNAL))

    def test_no_link_or_reference_nodes(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": EXTERNAL, "text": "w"}])],
            external_references=[{"url": EXTERNAL, "index": 1}]))
        self.assertEqual(Counter(n["kind"] for n in nodes), {"frame": 1, "external_ref": 1})
        self.assertEqual([e["type"] for e in edges], ["citesExternal"])

    def test_image_shown_as_page_image_and_in_a_section_is_one_edge(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            og_image=IMG, template_image_url=IMG,
            meta={"og:image:width": "600", "og:image:height": "nope"},
            sections=[section(heading="Notable Examples",
                              images=[{"src": IMG, "alt": "doge", "caption": "wow"}]),
                      section(kind="origin", images=[{"src": IMG, "caption": "deferred"}])]))
        img = nodes_by_id(nodes)[f"image:{IMG}"]
        self.assertEqual(img, {"id": f"image:{IMG}", "kind": "image", "width": 600})
        e = the_edge(edges, "hasImage", f"image:{IMG}")
        self.assertEqual(e["occurrences"], [
            {"role": "page"},
            {"role": "section", "in_section": "Notable Examples",
             "alt_text": "doge", "caption": "wow"}])

    def test_caption_lives_on_the_edge_not_the_shared_image(self):
        # Two pages showing one file with different captions keep both.
        _, a = build.build_nodes_and_edges(entry(sections=[section(
            images=[{"src": IMG, "caption": "first"}])]))
        n, b = build.build_nodes_and_edges(entry(url=OTHER, sections=[section(
            images=[{"src": IMG, "caption": "second"}])]))
        self.assertNotIn("caption", nodes_by_id(n)[f"image:{IMG}"])
        self.assertEqual(a[0]["occurrences"][0]["caption"], "first")
        self.assertEqual(b[0]["occurrences"][0]["caption"], "second")

    def test_image_ids_cannot_collide_with_a_link_target(self):
        nodes, _ = build.build_nodes_and_edges(entry(sections=[
            section(links=[{"url": IMG, "text": "img"}], images=[{"src": IMG}])]))
        kinds = {n["id"]: n["kind"] for n in nodes}
        self.assertEqual(kinds[IMG], "external_ref")
        self.assertEqual(kinds[f"image:{IMG}"], "image")


class FullRecordTests(unittest.TestCase):
    """The real doge.html page, through the real parser."""

    @classmethod
    def setUpClass(cls):
        from modules.kym_parse import parse_entry
        path = os.path.join(os.path.dirname(__file__), "fixtures", "doge.html")
        with open(path, encoding="utf-8") as fh:
            parsed = parse_entry(fh.read())
        cls.doc = parsed.model_dump(mode="json", exclude_none=True)
        cls.nodes, cls.edges = build.build_nodes_and_edges(cls.doc)
        cls.ids = nodes_by_id(cls.nodes)
        cls.live = [s for s in cls.doc["sections"]
                    if s["kind"] not in build.DEFERRED_SECTION_KINDS]

    def occurrences(self, etype, **match):
        return [o for e in self.edges if e["type"] == etype
                for o in e.get("occurrences", [])
                if all(o.get(k) == v for k, v in match.items())]

    def test_every_non_about_section_with_text_is_kept(self):
        expected = [s for s in self.live if s["kind"] != "about" and any(s.get("text"))]
        self.assertTrue(expected)
        self.assertEqual(len(self.ids[self.doc["url"]]["section_texts"]), len(expected))

    def test_every_non_deferred_section_image_is_an_occurrence(self):
        self.assertEqual(len(self.occurrences("hasImage", role="section")),
                         sum(len(s.get("images", [])) for s in self.live))

    def test_every_reference_is_an_occurrence(self):
        refs = (self.occurrences("relatesToMeme") + self.occurrences("citesExternal"))
        cited = [o for o in refs if "citation_text" in o or "citation_index" in o]
        named = [o for o in refs if "site_name" in o]
        self.assertEqual(len(cited), len(self.doc.get("external_references", [])))
        self.assertEqual(len(named), len(self.doc.get("additional_references", [])))

    def test_frame_fields(self):
        frame = self.ids[self.doc["url"]]
        for field in ("label", "category", "status", "year", "from", "about",
                      "added", "last_updated"):
            self.assertIn(field, frame, field)

    def test_every_badge_becomes_an_edge(self):
        self.assertEqual(len([e for e in self.edges if e["type"] == "hasBadge"]),
                         len(self.doc.get("badges") or []))

    def test_origin_becomes_an_edge_when_a_resolver_is_supplied(self):
        # cls.edges (no resolver) already covers "from" staying a literal
        # (test_frame_fields) and no hasOrigin edge without one
        # (ConceptTests.test_no_origin_edge_without_a_resolver); this
        # covers the real fixture's actual origin value end to end.
        _, edges = build.build_nodes_and_edges(
            self.doc, origin_resolver=lambda raw: raw.lower())
        has_origin = [e for e in edges if e["type"] == "hasOrigin"]
        self.assertEqual(len(has_origin), 1)
        self.assertEqual(has_origin[0]["dst"], f"origin:{self.doc['origin'].lower()}")


class StubNodeTests(unittest.TestCase):
    def test_category_guessed_from_the_path(self):
        self.assertEqual(
            build.guess_stub_node("https://knowyourmeme.com/memes/people/x")["category"],
            "person")
        self.assertEqual(
            build.guess_stub_node("https://knowyourmeme.com/memes/events/x")["category"],
            "event")
        self.assertEqual(
            build.guess_stub_node("https://knowyourmeme.com/memes/x")["category"],
            "meme")

    def test_more_specific_prefix_wins(self):
        self.assertEqual(
            build.guess_stub_node(
                "https://knowyourmeme.com/memes/subcultures/x")["category"],
            "subculture")

    def test_unknown_path_guesses_nothing_rather_than_guessing_wrong(self):
        self.assertIsNone(
            build.guess_stub_node("https://knowyourmeme.com/photos/1")["category"])

    def test_shape_matches_what_build_emits_inline(self):
        nodes, _ = build.build_nodes_and_edges(entry(series_parent=PARENT))
        self.assertEqual(nodes_by_id(nodes)[PARENT], build.guess_stub_node(PARENT))


if __name__ == "__main__":
    unittest.main(verbosity=2)
