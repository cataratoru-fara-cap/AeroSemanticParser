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
IMG = "https://i.kym-cdn.com/photos/images/original/000/1.jpg"
SEC = "https://meme4.science/atlas/entry/abc/section/2"
LNK = SEC + "/link/0"
REF = "https://meme4.science/atlas/entry/abc/reference/0"
AREF = "https://meme4.science/atlas/entry/abc/additional-reference/0"

NODES = [
    {"id": F1, "kind": "frame", "label": 'Doge "the" dog', "category": "meme",
     "status": "confirmed", "year": 2013, "from": "Tumblr",
     "about": "line one\n\nline\ttwo", "added": "2011-03-13T07:06:40Z",
     "badges": ["Sensitive"], "corpus_missing": ["region", "tags"]},
    {"id": F2, "kind": "frame", "label": "Cheems", "category": "meme",
     "status": "confirmed"},
    {"id": STUB, "kind": "frame_stub", "label": None, "category": "meme",
     "status": None},
    {"id": "type:image-macro", "kind": "entry_type_concept", "label": "image-macro"},
    {"id": "type:meme", "kind": "entry_type_concept", "label": "meme"},
    {"id": "tag:doge", "kind": "tag_concept", "label": "doge"},
    {"id": "tag:dog", "kind": "tag_concept", "label": "dog"},
    {"id": EXT, "kind": "external_ref", "label": None},
    {"id": "region:Japan", "kind": "region_concept", "label": "Japan"},
    {"id": SEC, "kind": "section", "section_kind": "other", "heading": "Notes",
     "position": 2, "level": 2, "text": "p1\n\np2"},
    {"id": LNK, "kind": "link", "anchor_text": "Cheems"},
    {"id": REF, "kind": "reference", "ref_class": "ExternalReference",
     "index": 1, "citation_text": "Wikipedia"},
    {"id": AREF, "kind": "reference", "ref_class": "AdditionalReference",
     "site_name": "Wikipedia"},
    {"id": "image:" + IMG, "kind": "image", "width": 600, "height": 400,
     "caption": "wow"},
]
EDGES = [
    {"src": F1, "type": "hasEntryType", "dst": "type:image-macro"},
    {"src": F1, "type": "hasTag", "dst": "tag:doge"},
    {"src": F1, "type": "hasTag", "dst": "tag:dog"},
    {"src": F2, "type": "hasTag", "dst": "tag:doge"},
    {"src": F1, "type": "partOfSeries", "dst": STUB},
    {"src": F1, "type": "relatesToMeme", "dst": F2},
    {"src": F1, "type": "citesExternal", "dst": EXT},
    {"src": "type:image-macro", "type": "subTypeOf", "dst": "type:meme"},
    {"src": F1, "type": "hasRegion", "dst": "region:Japan"},
    {"src": F1, "type": "hasSection", "dst": SEC},
    {"src": SEC, "type": "hasLink", "dst": LNK},
    {"src": LNK, "type": "linksTo", "dst": F2},
    {"src": F1, "type": "hasReference", "dst": REF},
    {"src": F1, "type": "hasReference", "dst": AREF},
    {"src": REF, "type": "refersTo", "dst": EXT},
    {"src": AREF, "type": "refersTo", "dst": EXT},
    {"src": F1, "type": "hasImage", "dst": "image:" + IMG},
    {"src": SEC, "type": "hasImage", "dst": "image:" + IMG},
]


ONTOLOGY = str(Path(__file__).resolve().parents[1] / "kg_config" / "memeatlas.ttl")


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
            build_id="kg_test", stamps={"kg_build_version": "2.0.0"},
            ontology_path=ONTOLOGY)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def rml(self, name):
        return read_csv(os.path.join(self.out, serialize.RML_DIR, name))


class LayoutTests(Built):
    def test_every_expected_file_exists(self):
        for name in ("graph.nt", "kg_view_nodes.csv", "kg_view_edges.csv",
                     "manifest.json", "ontology.ttl"):
            self.assertTrue(Path(self.out, name).is_file(), name)
        for name, _ in serialize.EDGE_TYPE_TO_RML_FILE.values():
            self.assertTrue(Path(self.out, serialize.RML_DIR, name).is_file(), name)
        written = {p.name for p in Path(self.out, serialize.RML_DIR).iterdir()}
        self.assertEqual(written, serialize.all_rml_files())

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
        self.assertEqual(list(rows[0]), [c for c, _ in serialize.RML_NODE_FILES["frames.csv"][1]])

    def test_frame_row_carries_imkg_fields_and_leaves_absent_ones_empty(self):
        rows = {r["url"]: r for r in self.rml("frames.csv")}
        self.assertEqual(rows[F1]["category_class"], "Meme")
        self.assertEqual(rows[F1]["year"], "2013")
        self.assertEqual(rows[F1]["about"], "line one\n\nline\ttwo")
        self.assertEqual(rows[F2]["year"], "")

    def test_list_properties_get_one_row_per_value(self):
        self.assertEqual(self.rml("frame_badges.csv"), [{"url": F1, "badge": "Sensitive"}])
        self.assertEqual({r["missing"] for r in self.rml("frame_corpus_missing.csv")},
                         {"region", "tags"})
        self.assertEqual(self.rml("frame_aliases.csv"), [])

    def test_body_rows_use_iris(self):
        self.assertEqual(self.rml("sections.csv")[0]["iri"], SEC)
        self.assertEqual(self.rml("images.csv")[0]["iri"], IMG)
        self.assertEqual(self.rml("image_edges.csv"),
                         [{"holder": F1, "image": IMG}, {"holder": SEC, "image": IMG}])
        refs = {r["iri"]: r for r in self.rml("references.csv")}
        self.assertEqual(refs[REF]["citation_index"], "1")
        self.assertEqual(refs[AREF]["site_name"], "Wikipedia")

    def test_region_edges_strip_the_prefix(self):
        self.assertEqual(self.rml("region_edges.csv"), [{"url": F1, "region": "Japan"}])

    def test_types_strip_the_prefix_and_render_the_label(self):
        rows = {r["slug"]: r["label"] for r in self.rml("types.csv")}
        self.assertEqual(rows, {"image-macro": "image macro", "meme": "meme"})

    def test_entry_type_edges_strip_the_prefix(self):
        self.assertEqual(self.rml("entry_type_edges.csv"),
                         [{"url": F1, "slug": "image-macro"}])

    def test_tag_edges_strip_the_prefix(self):
        tags = {(r["url"], r["tag"]) for r in self.rml("tag_edges.csv")}
        self.assertEqual(tags, {(F1, "doge"), (F1, "dog"), (F2, "doge")})

    def test_subtype_edges_strip_both_sides(self):
        self.assertEqual(self.rml("subtype_edges.csv"),
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

    def test_every_input_edge_is_in_the_rdf(self):
        graph = set(Path(self.out, "graph.nt").read_text(encoding="utf-8").splitlines())
        for edge in EDGES:
            expected = set(rdf.iter_triples([], [edge]))
            self.assertTrue(expected, edge)
            self.assertLessEqual(expected, graph, edge)

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
        self.assertEqual(preds["tag"], 3)


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
