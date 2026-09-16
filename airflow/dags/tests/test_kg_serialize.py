"""Tests for kg/serialize.py — one build, every representation, one stream.

``ProjectionsAgreeTests`` is the point of the module. The exporters it
replaces derived the property-graph CSVs and the RML CSVs by two separate
loops, and those loops disagreed by 14,563 edges without anything noticing.
Every file here is a projection of one stream, and these tests assert the
projections describe the same edges.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_serialize.py -v
"""
import csv
import json
import os
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from modules.kg import ntdiff, rdf, serialize
from modules.kg.build import EDGE_TYPES
from modules.kg.taxonomy import CONCEPT_EDGE_TYPES

F1 = "https://knowyourmeme.com/memes/doge"
F2 = "https://knowyourmeme.com/memes/cheems"
STUB = "https://knowyourmeme.com/memes/shiba-inu"
EXT = "https://en.wikipedia.org/wiki/Doge"

NODES = [
    {"id": F1, "kind": "frame", "label": 'Doge "the" dog', "category": "meme",
     "status": "confirmed"},
    {"id": F2, "kind": "frame", "label": "Cheems", "category": "meme",
     "status": "confirmed"},
    {"id": STUB, "kind": "frame_stub", "label": None, "category": "meme",
     "status": None},
    {"id": "type:image-macro", "kind": "entry_type_concept", "label": "image-macro"},
    {"id": "type:meme", "kind": "entry_type_concept", "label": "meme"},
    {"id": "tag:doge", "kind": "tag_concept", "label": "doge"},
    {"id": "tag:dog", "kind": "tag_concept", "label": "dog"},
    {"id": EXT, "kind": "external_ref", "label": None},
]
EDGES = [
    {"src": F1, "type": "hasEntryType", "dst": "type:image-macro"},
    {"src": F1, "type": "hasTag", "dst": "tag:doge"},
    {"src": F1, "type": "hasTag", "dst": "tag:dog"},
    {"src": F2, "type": "hasTag", "dst": "tag:doge"},
    {"src": F1, "type": "partOfSeries", "dst": STUB},
    {"src": F1, "type": "relatesToMeme", "dst": F2},
    {"src": F1, "type": "citesExternal", "dst": EXT},
    {"src": "type:image-macro", "type": "broader", "dst": "type:meme"},
]


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class Built(unittest.TestCase):
    """Shared build in a temp dir."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.out = cls._tmp.name
        cls.manifest = serialize.write_build(
            lambda: iter(NODES), lambda: iter(EDGES), cls.out,
            build_id="kg_test", stamps={"kg_build_version": "2.0.0"})

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def rml(self, name):
        return read_csv(os.path.join(self.out, serialize.RML_DIR, name))


class LayoutTests(Built):
    def test_every_expected_file_exists(self):
        for name in ("graph.nt", "kg_view_nodes.csv", "kg_view_edges.csv",
                     "manifest.json"):
            self.assertTrue(Path(self.out, name).is_file(), name)
        for name, _ in serialize.EDGE_TYPE_TO_RML_FILE.values():
            self.assertTrue(Path(self.out, serialize.RML_DIR, name).is_file(), name)
        for name in serialize.RML_NODE_FILES:
            self.assertTrue(Path(self.out, serialize.RML_DIR, name).is_file(), name)

    def test_no_tmp_files_remain(self):
        leftovers = [p for p in Path(self.out).rglob("*.tmp")]
        self.assertEqual(leftovers, [])

    def test_manifest_hashes_match_the_files(self):
        for name, meta in self.manifest["files"].items():
            path = os.path.join(self.out, meta["path"])
            self.assertEqual(meta["sha256"], serialize._sha256(path), name)

    def test_manifest_records_view_filters_and_stamps(self):
        self.assertEqual(self.manifest["view_filters"],
                         {"exclude_kinds": [], "top_tags": 0})
        self.assertEqual(self.manifest["stamps"]["build_id"], "kg_test")
        self.assertEqual(self.manifest["stamps"]["kg_build_version"], "2.0.0")

    def test_load_manifest_round_trips(self):
        self.assertEqual(serialize.load_manifest(self.out), self.manifest)


class RmlValueMappingTests(Built):
    """The CSVs must hold exactly what kg_mapping.yarrrml.yml expects."""

    def test_frames_are_real_frames_only(self):
        rows = self.rml("frames.csv")
        self.assertEqual({r["url"] for r in rows}, {F1, F2})
        self.assertEqual(list(rows[0]), ["url", "title", "category", "status"])

    def test_types_strip_the_prefix_and_render_the_label(self):
        rows = {r["slug"]: r["label"] for r in self.rml("types.csv")}
        self.assertEqual(rows, {"image-macro": "image macro", "meme": "meme"})

    def test_entry_type_edges_strip_the_prefix(self):
        self.assertEqual(self.rml("entry_type_edges.csv"),
                         [{"url": F1, "slug": "image-macro"}])

    def test_tag_edges_strip_the_prefix(self):
        tags = {(r["url"], r["tag"]) for r in self.rml("tag_edges.csv")}
        self.assertEqual(tags, {(F1, "doge"), (F1, "dog"), (F2, "doge")})

    def test_broader_edges_strip_both_sides(self):
        self.assertEqual(self.rml("broader_edges.csv"),
                         [{"narrower": "image-macro", "broader": "meme"}])

    def test_url_edges_pass_through(self):
        self.assertEqual(self.rml("series_edges.csv"),
                         [{"url": F1, "parent_url": STUB}])
        self.assertEqual(self.rml("cites_edges.csv"),
                         [{"url": F1, "target_url": EXT}])


class ProjectionsAgreeTests(Built):
    """The drift regression: every representation, the same edges."""

    def test_rml_rows_equal_input_edges_per_type(self):
        for etype, (name, _) in serialize.EDGE_TYPE_TO_RML_FILE.items():
            expected = sum(1 for e in EDGES if e["type"] == etype)
            self.assertEqual(len(self.rml(name)), expected, etype)

    def test_property_graph_view_equals_input_edges_per_type(self):
        counts = Counter(r["type"] for r in
                         read_csv(os.path.join(self.out, "kg_view_edges.csv")))
        self.assertEqual(counts, Counter(e["type"] for e in EDGES))

    def test_rdf_edge_triples_equal_input_edges_per_type(self):
        _, _, preds = ntdiff.digest(os.path.join(self.out, "graph.nt"))
        for etype in set(EDGE_TYPES) | set(CONCEPT_EDGE_TYPES):
            expected = sum(1 for e in EDGES if e["type"] == etype)
            self.assertEqual(preds.get(etype, 0), expected, etype)

    def test_manifest_counts_match(self):
        self.assertEqual(self.manifest["counts"]["edges"], len(EDGES))
        self.assertEqual(self.manifest["counts"]["nodes"], len(NODES))
        self.assertEqual(self.manifest["counts"]["edges_by_type"]["hasTag"], 3)


class SetSemanticsTests(unittest.TestCase):
    def test_a_repeated_edge_appears_once_in_every_projection(self):
        dup = EDGES + [dict(EDGES[1])]
        with tempfile.TemporaryDirectory() as tmp:
            serialize.write_build(lambda: iter(NODES), lambda: iter(dup), tmp,
                                  build_id="b")
            tag_rows = read_csv(os.path.join(tmp, serialize.RML_DIR, "tag_edges.csv"))
            pg_rows = read_csv(os.path.join(tmp, "kg_view_edges.csv"))
            _, _, preds = ntdiff.digest(os.path.join(tmp, "graph.nt"))
        self.assertEqual(len(tag_rows), 3)
        self.assertEqual(sum(1 for r in pg_rows if r["type"] == "hasTag"), 3)
        self.assertEqual(preds["hasTag"], 3)


class ViewFilterTests(unittest.TestCase):
    def test_exclude_kinds_drops_nodes_and_their_edges_from_the_view_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = serialize.write_build(lambda: iter(NODES), lambda: iter(EDGES),
                                      tmp, build_id="b",
                                      exclude_kinds=["tag_concept"])
            pg_nodes = read_csv(os.path.join(tmp, "kg_view_nodes.csv"))
            pg_edges = read_csv(os.path.join(tmp, "kg_view_edges.csv"))
            tag_rml = read_csv(os.path.join(tmp, serialize.RML_DIR, "tag_edges.csv"))
        self.assertFalse(any(r["kind"] == "tag_concept" for r in pg_nodes))
        self.assertFalse(any(r["type"] == "hasTag" for r in pg_edges))
        self.assertEqual(len(tag_rml), 3)          # RML is never filtered
        self.assertEqual(m["view_filters"]["exclude_kinds"], ["tag_concept"])

    def test_top_tags_keeps_the_highest_degree_tags(self):
        with tempfile.TemporaryDirectory() as tmp:
            serialize.write_build(lambda: iter(NODES), lambda: iter(EDGES), tmp,
                                  build_id="b", top_tags=1)
            kept = {r["id"] for r in read_csv(os.path.join(tmp, "kg_view_nodes.csv"))
                    if r["kind"] == "tag_concept"}
        self.assertEqual(kept, {"tag:doge"})      # degree 2 beats degree 1

    def test_unknown_exclude_kind_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                serialize.write_build(lambda: iter(NODES), lambda: iter(EDGES),
                                      tmp, build_id="b", exclude_kinds=["nope"])


class AtomicityTests(unittest.TestCase):
    def test_a_failing_source_leaves_no_final_named_files(self):
        def bad_edges():
            yield EDGES[0]
            raise RuntimeError("cursor died")

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(RuntimeError):
                serialize.write_build(lambda: iter(NODES), bad_edges, tmp,
                                      build_id="b")
            finals = [p.name for p in Path(tmp).rglob("*")
                      if p.is_file() and not p.name.endswith(".tmp")]
            # The node-CSV pass completed before edges were touched, so
            # frames.csv/types.csv legitimately exist; nothing from the
            # failed pass onward may carry a final name.
            self.assertNotIn("manifest.json", finals)
            self.assertNotIn("graph.nt", finals)
            self.assertNotIn("kg_view_edges.csv", finals)
            self.assertFalse(any(n.endswith("_edges.csv") for n in finals))

    def test_output_is_byte_stable(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            ma = serialize.write_build(lambda: iter(NODES), lambda: iter(EDGES),
                                       a, build_id="same", stamps={"x": 1})
            mb = serialize.write_build(lambda: iter(NODES), lambda: iter(EDGES),
                                       b, build_id="same", stamps={"x": 1})
        for name in ma["files"]:
            self.assertEqual(ma["files"][name]["sha256"],
                             mb["files"][name]["sha256"], name)


class VocabularyTests(unittest.TestCase):
    def test_rml_table_covers_exactly_the_edge_vocabulary(self):
        self.assertEqual(set(serialize.EDGE_TYPE_TO_RML_FILE),
                         set(EDGE_TYPES) | set(CONCEPT_EDGE_TYPES))


if __name__ == "__main__":
    unittest.main(verbosity=2)
