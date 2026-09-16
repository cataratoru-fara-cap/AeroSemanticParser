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

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_build.py -v
"""
import unittest

from modules.kg import build

URL = "https://knowyourmeme.com/memes/doge"
PARENT = "https://knowyourmeme.com/memes/shiba-inu"
OTHER = "https://knowyourmeme.com/memes/cheems"
EXTERNAL = "https://en.wikipedia.org/wiki/Doge_(meme)"


def entry(**over) -> dict:
    base = {"url": URL, "title": "Doge", "category": "meme",
            "status": "confirmed"}
    base.update(over)
    return base


def nodes_by_id(nodes):
    return {n["id"]: n for n in nodes}


def edge_set(edges):
    return {(e["src"], e["type"], e["dst"]) for e in edges}


class VocabularyTests(unittest.TestCase):
    def test_constants_are_exported(self):
        self.assertEqual(set(build.NODE_KINDS),
                         {"frame", "frame_stub", "entry_type_concept",
                          "tag_concept", "external_ref"})
        self.assertEqual(set(build.EDGE_TYPES),
                         {"hasEntryType", "hasTag", "partOfSeries",
                          "relatesToMeme", "citesExternal"})

    def test_version_is_stamped(self):
        self.assertTrue(build.KG_BUILD_VERSION)

    def test_emitted_kinds_and_types_stay_inside_the_vocabulary(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            entry_type=["meme"], tags=["shiba"], series_parent=PARENT,
            external_references=[{"url": EXTERNAL}]))
        self.assertLessEqual({n["kind"] for n in nodes}, set(build.NODE_KINDS))
        self.assertLessEqual({e["type"] for e in edges}, set(build.EDGE_TYPES))


class FrameNodeTests(unittest.TestCase):
    def test_frame_carries_its_attributes(self):
        nodes, _ = build.build_nodes_and_edges(entry())
        frame = nodes_by_id(nodes)[URL]
        self.assertEqual(frame["kind"], "frame")
        self.assertEqual(frame["label"], "Doge")
        self.assertEqual(frame["category"], "meme")
        self.assertEqual(frame["status"], "confirmed")

    def test_entry_without_url_yields_nothing(self):
        self.assertEqual(build.build_nodes_and_edges({"title": "x"}), ([], []))


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
            sections=[{"links": [{"url": PARENT}]}]))
        types_to_parent = {e["type"] for e in edges if e["dst"] == PARENT}
        self.assertEqual(types_to_parent, {"partOfSeries"})

    def test_kym_link_in_external_references_is_still_relatesToMeme(self):
        _, edges = build.build_nodes_and_edges(entry(
            external_references=[{"url": OTHER}]))
        self.assertIn((URL, "relatesToMeme", OTHER), edge_set(edges))

    def test_outside_link_in_a_body_section_is_still_citesExternal(self):
        nodes, edges = build.build_nodes_and_edges(entry(
            sections=[{"links": [{"url": EXTERNAL}]}]))
        self.assertIn((URL, "citesExternal", EXTERNAL), edge_set(edges))
        self.assertEqual(nodes_by_id(nodes)[EXTERNAL]["kind"], "external_ref")

    def test_self_link_is_not_an_edge(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[{"links": [{"url": URL}]}]))
        self.assertEqual(edges, [])

    def test_repeated_link_within_one_entry_yields_one_edge(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[{"links": [{"url": OTHER}, {"url": OTHER}]}],
            additional_references=[{"url": OTHER, "name": "again"}]))
        self.assertEqual(len([e for e in edges if e["dst"] == OTHER]), 1)

    def test_all_link_bearing_fields_are_read(self):
        _, edges = build.build_nodes_and_edges(entry(
            sections=[{"links": [{"url": "https://a.example/1"}]}],
            additional_references=[{"url": "https://b.example/2"}],
            external_references=[{"url": "https://c.example/3"}]))
        self.assertEqual(len([e for e in edges if e["type"] == "citesExternal"]), 3)

    def test_www_host_counts_as_internal(self):
        www = "https://www.knowyourmeme.com/memes/pepe"
        _, edges = build.build_nodes_and_edges(entry(
            sections=[{"links": [{"url": www}]}]))
        self.assertIn((URL, "relatesToMeme", www), edge_set(edges))


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
