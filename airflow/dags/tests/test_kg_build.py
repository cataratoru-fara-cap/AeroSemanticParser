"""Tests for kg/build.py — one entries doc -> (nodes, edges).

This module is the single producer: both the property-graph and the RDF
projections are generated from its output, so anything asserted here is
asserted about both representations at once.

``test_series_parent_is_not_also_a_relates_edge`` pins a real defect. The
RML exporter re-implemented this loop and started its per-entry ``seen``
set empty, where this module seeds it with ``series_parent``. The result
was 14,571 ``mk:relatesToMeme`` triples in the published kg_output.nt that
the property graph did not contain (229,027 - 14,571 = 214,456, the Mongo
count exactly). The two projections now share this code; the test stops the
divergence coming back.

``FullRecordTests`` pins the 3.0.0 scope against the real doge.html
fixture: every field the parser extracts lands somewhere in the graph,
except the Origin/Spread sections, which are deferred on purpose.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_build.py -v
"""
import os
import unittest
from datetime import datetime

from modules.kg import build
from modules.mongo_base import url_doc_id

URL = "https://knowyourmeme.com/memes/doge"
PARENT = "https://knowyourmeme.com/memes/shiba-inu"
OTHER = "https://knowyourmeme.com/memes/cheems"
EXTERNAL = "https://en.wikipedia.org/wiki/Doge_(meme)"
IMG = "https://i.kym-cdn.com/photos/images/original/000/1.jpg"
BASE = f"{build.ATLAS_BASE}entry/{url_doc_id(URL)}/"

# The edges that existed before 3.0.0 and describe the frame as a whole.
FRAME_LEVEL = {"partOfSeries", "relatesToMeme", "citesExternal"}


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


def frame_level(edges):
    return [e for e in edges if e["type"] in FRAME_LEVEL]


class VocabularyTests(unittest.TestCase):
    def test_constants_are_exported(self):
        self.assertEqual(set(build.NODE_KINDS),
                         {"frame", "frame_stub", "entry_type_concept",
                          "tag_concept", "region_concept", "external_ref",
                          "section", "link", "reference", "image"})
        self.assertEqual(set(build.EDGE_TYPES),
                         {"hasEntryType", "hasTag", "hasRegion",
                          "partOfSeries", "relatesToMeme", "citesExternal",
                          "hasSection", "hasLink", "linksTo", "hasReference",
                          "refersTo", "hasImage"})

    def test_version_is_stamped(self):
        self.assertTrue(build.KG_BUILD_VERSION)

    def test_emitted_kinds_and_types_stay_inside_the_vocabulary(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            entry_type=["meme"], tags=["shiba"], region=["Japan"],
            series_parent=PARENT, og_image=IMG,
            sections=[section(links=[{"url": OTHER, "text": "x"}],
                              images=[{"src": IMG}])],
            additional_references=[{"url": EXTERNAL, "name": "Wikipedia"}],
            external_references=[{"url": EXTERNAL, "index": 1}]))
        self.assertLessEqual({n["kind"] for n in nodes}, set(build.NODE_KINDS))
        self.assertLessEqual({e["type"] for e in edges}, set(build.EDGE_TYPES))

    def test_entry_id_is_the_stores_id(self):
        # Section IRIs embed it so they join back to `entries` by _id.
        self.assertEqual(build.entry_id(URL), url_doc_id(URL))


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
        self.assertEqual(frame["category"], "meme")
        self.assertEqual(frame["status"], "confirmed")
        self.assertEqual(frame["year"], 2013)
        self.assertEqual(frame["from"], "Tumblr")          # the infobox origin FIELD
        self.assertEqual(frame["badges"], ["Sensitive"])
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
        self.assertNotIn("badges", frame)
        self.assertNotIn("year", frame)
        self.assertNotIn("corpus_missing", frame)

    def test_about_text_is_on_the_frame_not_the_section(self):
        nodes, _ = build.build_nodes_and_edges(entry(sections=[
            section(kind="about", heading="About", text=["one", "two"])]))
        ids = nodes_by_id(nodes)
        self.assertEqual(ids[URL]["about"], "one\n\ntwo")
        self.assertNotIn("text", ids[f"{BASE}section/0"])

    def test_entry_without_url_yields_nothing(self):
        self.assertEqual(build.build_nodes_and_edges({"title": "x"}), ([], []))


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


class LinkClassificationTests(unittest.TestCase):
    """Classification is by the link's HOST, never by the field it came from."""

    def test_series_parent_becomes_a_stub_and_a_series_edge(self):
        nodes, edges = build.build_nodes_and_edges(entry(series_parent=PARENT))
        self.assertEqual(nodes_by_id(nodes)[PARENT]["kind"], "frame_stub")
        self.assertIn((URL, "partOfSeries", PARENT), edge_set(edges))

    def test_series_parent_is_not_also_a_relates_edge(self):
        # THE REGRESSION. The parent is linked in the body too, as it always
        # is on a real page; it must produce partOfSeries and nothing else
        # at the frame level.
        _, edges = build.build_nodes_and_edges(entry(
            series_parent=PARENT,
            sections=[section(links=[{"url": PARENT, "text": "Shiba"}])]))
        types_to_parent = {e["type"] for e in frame_level(edges)
                           if e["dst"] == PARENT}
        self.assertEqual(types_to_parent, {"partOfSeries"})

    def test_kym_link_in_external_references_is_still_relatesToMeme(self):
        _, edges = build.build_nodes_and_edges(entry(
            external_references=[{"url": OTHER}]))
        self.assertIn((URL, "relatesToMeme", OTHER), edge_set(edges))

    def test_outside_link_in_a_body_section_is_still_citesExternal(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": EXTERNAL, "text": "wiki"}])]))
        self.assertIn((URL, "citesExternal", EXTERNAL), edge_set(edges))
        self.assertEqual(nodes_by_id(nodes)[EXTERNAL]["kind"], "external_ref")

    def test_self_link_is_not_a_frame_level_edge(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": URL, "text": "Doge"}])]))
        self.assertEqual(frame_level(edges), [])

    def test_repeated_link_within_one_entry_yields_one_frame_level_edge(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": OTHER, "text": "a"},
                                     {"url": OTHER, "text": "b"}])],
            additional_references=[{"url": OTHER, "name": "again"}]))
        self.assertEqual(len([e for e in frame_level(edges) if e["dst"] == OTHER]), 1)

    def test_all_link_bearing_fields_are_read(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": "https://a.example/1", "text": ""}])],
            additional_references=[{"url": "https://b.example/2", "name": "B"}],
            external_references=[{"url": "https://c.example/3"}]))
        self.assertEqual(len([e for e in edges if e["type"] == "citesExternal"]), 3)

    def test_links_in_deferred_sections_still_feed_frame_level_edges(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(kind="origin", links=[{"url": OTHER, "text": "x"}])]))
        self.assertIn((URL, "relatesToMeme", OTHER), edge_set(edges))

    def test_www_host_counts_as_internal(self):
        www = "https://www.knowyourmeme.com/memes/pepe"
        _, edges = build.build_nodes_and_edges(entry(
            sections=[section(links=[{"url": www, "text": "pepe"}])]))
        self.assertIn((URL, "relatesToMeme", www), edge_set(edges))


class BodyTests(unittest.TestCase):
    def test_section_node_and_its_links(self):
        nodes, edges = build.build_nodes_and_edges(entry(sections=[
            section(heading="Notable Examples", level=3, text=["p1", "", "p2"],
                    links=[{"url": OTHER, "text": " Cheems "},
                           {"url": EXTERNAL, "text": ""}])]))
        ids = nodes_by_id(nodes)
        sid = f"{BASE}section/0"
        self.assertEqual(ids[sid], {"id": sid, "kind": "section",
                                    "section_kind": "notable_examples",
                                    "heading": "Notable Examples",
                                    "position": 0, "level": 3,
                                    "text": "p1\n\np2"})
        self.assertEqual(ids[f"{sid}/link/0"]["anchor_text"], "Cheems")
        self.assertNotIn("anchor_text", ids[f"{sid}/link/1"])
        es = edge_set(edges)
        self.assertIn((URL, "hasSection", sid), es)
        self.assertIn((sid, "hasLink", f"{sid}/link/0"), es)
        self.assertIn((f"{sid}/link/0", "linksTo", OTHER), es)
        self.assertIn((f"{sid}/link/1", "linksTo", EXTERNAL), es)

    def test_deferred_sections_are_skipped_but_keep_positions_stable(self):
        nodes, edges = build.build_nodes_and_edges(entry(sections=[
            section(kind="origin", text=["o"], images=[{"src": IMG}]),
            section(kind="spread", text=["s"]),
            section(kind="other", text=["x"])]))
        sections = [n for n in nodes if n["kind"] == "section"]
        self.assertEqual([s["id"] for s in sections], [f"{BASE}section/2"])
        self.assertEqual(sections[0]["position"], 2)
        self.assertFalse(any(n["kind"] == "image" for n in nodes))
        self.assertEqual(build.DEFERRED_SECTION_KINDS, {"origin", "spread"})

    def test_section_images(self):
        nodes, edges = build.build_nodes_and_edges(entry(sections=[
            section(images=[{"src": IMG, "alt": "doge", "caption": "wow"}])]))
        img = nodes_by_id(nodes)[f"image:{IMG}"]
        self.assertEqual(img, {"id": f"image:{IMG}", "kind": "image",
                               "alt": "doge", "caption": "wow"})
        self.assertIn((f"{BASE}section/0", "hasImage", f"image:{IMG}"),
                      edge_set(edges))

    def test_page_image_carries_its_size(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            og_image=IMG, template_image_url=IMG,
            meta={"og:image:width": "600", "og:image:height": "nope"}))
        images = [n for n in nodes if n["kind"] == "image"]
        self.assertEqual(images, [{"id": f"image:{IMG}", "kind": "image",
                                   "width": 600}])
        self.assertEqual([e for e in edges if e["type"] == "hasImage"],
                         [{"src": URL, "type": "hasImage", "dst": f"image:{IMG}"}])

    def test_image_ids_cannot_collide_with_a_link_target(self):
        # A body link may point straight at an image file.
        nodes, _ = build.build_nodes_and_edges(entry(sections=[
            section(links=[{"url": IMG, "text": "img"}], images=[{"src": IMG}])]))
        kinds = {n["id"]: n["kind"] for n in nodes}
        self.assertEqual(kinds[IMG], "external_ref")
        self.assertEqual(kinds[f"image:{IMG}"], "image")


class ReferenceTests(unittest.TestCase):
    def test_external_references(self):
        nodes, edges = build.build_nodes_and_edges(entry(external_references=[
            {"index": 1, "text": "Wikipedia – Doge", "url": EXTERNAL},
            {"index": 2, "text": "no url"}]))
        ids = nodes_by_id(nodes)
        rid = f"{BASE}reference/0"
        self.assertEqual(ids[rid], {"id": rid, "kind": "reference",
                                    "ref_class": "ExternalReference",
                                    "index": 1,
                                    "citation_text": "Wikipedia – Doge"})
        self.assertNotIn(f"{BASE}reference/1", ids)
        self.assertIn((URL, "hasReference", rid), edge_set(edges))
        self.assertIn((rid, "refersTo", EXTERNAL), edge_set(edges))

    def test_additional_references_have_their_own_iri_space(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            external_references=[{"index": 1, "url": EXTERNAL}],
            additional_references=[{"name": "Wikipedia", "url": EXTERNAL}]))
        ids = nodes_by_id(nodes)
        rid = f"{BASE}additional-reference/0"
        self.assertEqual(ids[rid]["ref_class"], "AdditionalReference")
        self.assertEqual(ids[rid]["site_name"], "Wikipedia")
        self.assertIn(f"{BASE}reference/0", ids)

    def test_kym_reference_target_is_a_stub(self):
        nodes, _ = build.build_nodes_and_edges(entry(
            external_references=[{"url": OTHER}]))
        self.assertEqual(nodes_by_id(nodes)[OTHER]["kind"], "frame_stub")


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

    def test_every_non_deferred_section_is_a_node(self):
        expected = [i for i, s in enumerate(self.doc["sections"])
                    if s["kind"] not in build.DEFERRED_SECTION_KINDS]
        got = sorted(n["position"] for n in self.nodes if n["kind"] == "section")
        self.assertTrue(expected)
        self.assertEqual(got, expected)

    def test_every_non_deferred_link_and_image_is_carried(self):
        live = [s for s in self.doc["sections"]
                if s["kind"] not in build.DEFERRED_SECTION_KINDS]
        self.assertEqual(sum(e["type"] == "hasLink" for e in self.edges),
                         sum(len(s.get("links", [])) for s in live))
        section_images = sum(e["type"] == "hasImage"
                             and "/section/" in e["src"] for e in self.edges)
        self.assertEqual(section_images, sum(len(s.get("images", [])) for s in live))

    def test_every_reference_is_carried(self):
        self.assertEqual(sum(e["type"] == "hasReference" for e in self.edges),
                         len(self.doc.get("external_references", []))
                         + len(self.doc.get("additional_references", [])))

    def test_frame_fields(self):
        frame = self.ids[self.doc["url"]]
        for field in ("label", "category", "status", "year", "from", "about",
                      "added", "last_updated"):
            self.assertIn(field, frame, field)


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
        # /memes/subcultures/ must not be read as /memes/
        self.assertEqual(
            build.guess_stub_node(
                "https://knowyourmeme.com/memes/subcultures/x")["category"],
            "subculture")

    def test_unknown_path_guesses_nothing_rather_than_guessing_wrong(self):
        self.assertIsNone(
            build.guess_stub_node("https://knowyourmeme.com/photos/1")["category"])

    def test_shape_matches_what_build_emits_inline(self):
        nodes, _ = build.build_nodes_and_edges(entry(series_parent=PARENT))
        self.assertEqual(nodes_by_id(nodes)[PARENT],
                         build.guess_stub_node(PARENT))


if __name__ == "__main__":
    unittest.main(verbosity=2)
