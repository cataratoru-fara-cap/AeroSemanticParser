"""Tests for kg/rdf.py — (nodes, edges) -> canonical N-Triples.

Scope is pinned deliberately, because the RML validation path only emits
triples for rows that exist in one of its CSVs. If this module typed
``frame_stub`` nodes, every diff run would report ~9,500 phantom
divergences and the gate would be useless.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_rdf.py -v
"""
import hashlib
import tempfile
import unittest
from pathlib import Path

from modules.kg import rdf

FRAME = "https://knowyourmeme.com/memes/doge"
PARENT = "https://knowyourmeme.com/memes/shiba-inu"
EXTERNAL = "https://en.wikipedia.org/wiki/Doge"


def frame(nid=FRAME, label="Doge", category="meme", status="confirmed"):
    return {"id": nid, "kind": "frame", "label": label,
            "category": category, "status": status}


def triples(nodes, edges, **kw):
    return list(rdf.iter_triples(nodes, edges, **kw))


class EscapingTests(unittest.TestCase):
    def test_quote_and_backslash(self):
        # 1,141 real frame titles contain one of these.
        self.assertEqual(rdf.escape_literal('a "b" c'), 'a \\"b\\" c')
        self.assertEqual(rdf.escape_literal("a\\b"), "a\\\\b")

    def test_control_characters(self):
        self.assertEqual(rdf.escape_literal("a\nb\rc\td"), "a\\nb\\rc\\td")

    def test_escaped_title_round_trips_into_a_valid_literal(self):
        got = triples([frame(label='The "Best" Meme \\ Ever')], [])
        line = next(t for t in got if "rdf-schema#label" in t)
        self.assertIn('"The \\"Best\\" Meme \\\\ Ever"', line)
        self.assertTrue(all(t.endswith(" .") for t in got))


class NodeIriTests(unittest.TestCase):
    def test_entry_type_ids_map_to_the_types_namespace(self):
        self.assertEqual(rdf.node_iri("type:image-macro"),
                         rdf.TYPES_BASE + "image-macro")

    def test_urls_pass_through(self):
        self.assertEqual(rdf.node_iri(FRAME), FRAME)


class NodeTripleScopeTests(unittest.TestCase):
    def test_frame_gets_type_label_category_status(self):
        got = triples([frame()], [])
        self.assertEqual(len(got), 4)
        self.assertTrue(any("22-rdf-syntax-ns#type" in t
                            and "MemeFrame" in t for t in got))
        self.assertTrue(any("rdf-schema#label" in t and '"Doge"' in t
                            for t in got))

    def test_missing_attributes_emit_nothing(self):
        got = triples([frame(label=None, category=None, status=None)], [])
        self.assertEqual(len(got), 1)   # rdf:type only

    def test_stub_gets_no_triples(self):
        # The RML path has no CSV row for a stub, so neither may this.
        self.assertEqual(
            triples([{"id": PARENT, "kind": "frame_stub", "label": None,
                      "category": "meme", "status": None}], []), [])

    def test_tag_concept_gets_no_triples(self):
        self.assertEqual(
            triples([{"id": "tag:doge", "kind": "tag_concept",
                      "label": "doge"}], []), [])

    def test_external_ref_gets_no_triples(self):
        self.assertEqual(
            triples([{"id": EXTERNAL, "kind": "external_ref", "label": None}],
                    []), [])

    def test_entry_type_concept_gets_skos_treatment(self):
        got = triples([{"id": "type:exploitable", "kind": "entry_type_concept",
                        "label": "exploitable"}], [])
        # 3 for the concept (type, inScheme, prefLabel) + 2 declaring the
        # scheme itself. An skos:inScheme pointing at an undeclared resource
        # is incomplete SKOS, and the published graph declares it too.
        self.assertEqual(len(got), 5)
        self.assertTrue(any("inScheme" in t for t in got))
        self.assertTrue(any("prefLabel" in t for t in got))
        self.assertFalse(any("category" in t for t in got))

    def test_scheme_is_declared_exactly_once(self):
        got = triples([
            {"id": "type:a", "kind": "entry_type_concept", "label": "a"},
            {"id": "type:b", "kind": "entry_type_concept", "label": "b"},
            {"id": "type:c", "kind": "entry_type_concept", "label": "c"},
        ], [])
        declarations = [t for t in got if t.startswith(f"<{rdf.SCHEME_IRI}>")]
        self.assertEqual(len(declarations), 2)     # its type and its label

    def test_no_concepts_means_no_scheme_triples(self):
        got = triples([frame()], [])
        self.assertFalse(any(rdf.SCHEME_IRI in t for t in got))

    def test_pref_label_is_rendered_for_people(self):
        # types.csv does slug.replace("-", " "); 29 of the 119 slugs are
        # affected, and the published graph already carries the spaced form.
        self.assertEqual(rdf.concept_pref_label("ai-generated"), "ai generated")
        got = triples([{"id": "type:ai-generated", "kind": "entry_type_concept",
                        "label": "ai-generated"}], [])
        pref = next(t for t in got if "prefLabel" in t)
        self.assertIn('"ai generated"', pref)


class EdgeTripleTests(unittest.TestCase):
    def test_relations_become_iri_objects(self):
        got = triples([], [{"src": FRAME, "type": "partOfSeries",
                            "dst": PARENT}])
        self.assertEqual(got, [f"<{FRAME}> <{rdf.PREFIXES['mk']}partOfSeries> "
                               f"<{PARENT}> ."])

    def test_tags_become_literals_with_the_prefix_stripped(self):
        got = triples([], [{"src": FRAME, "type": "hasTag", "dst": "tag:doge"}])
        self.assertIn('"doge"', got[0])
        self.assertNotIn("tag:doge", got[0])

    def test_entry_type_edges_point_at_the_types_namespace(self):
        got = triples([], [{"src": FRAME, "type": "hasEntryType",
                            "dst": "type:exploitable"}])
        self.assertIn(f"<{rdf.TYPES_BASE}exploitable>", got[0])

    def test_broader_uses_the_skos_predicate(self):
        got = triples([], [{"src": "type:model", "type": "broader",
                            "dst": "type:influencer"}])
        self.assertIn(f"{rdf.PREFIXES['skos']}broader", got[0])

    def test_unknown_edge_type_is_skipped(self):
        self.assertEqual(triples([], [{"src": FRAME, "type": "nope",
                                       "dst": PARENT}]), [])

    def test_every_build_edge_type_has_a_predicate(self):
        from modules.kg import build, taxonomy
        for etype in set(build.EDGE_TYPES) | set(taxonomy.CONCEPT_EDGE_TYPES):
            self.assertIn(etype, rdf.EDGE_TYPE_TO_PRED)


class SetSemanticsTests(unittest.TestCase):
    """RDF is a set; the property-graph edge list is a bag."""

    def test_repeated_edge_yields_one_triple(self):
        e = {"src": FRAME, "type": "hasTag", "dst": "tag:doge"}
        self.assertEqual(len(triples([], [e, dict(e)])), 1)

    def test_repeated_node_yields_one_set_of_triples(self):
        self.assertEqual(len(triples([frame(), frame()], [])), 4)

    def test_two_frames_sharing_a_label_both_emit(self):
        # Dedup is per triple, not per literal value.
        got = triples([frame(), frame(nid=PARENT, label="Doge")], [])
        self.assertEqual(len(got), 8)


class ProvenanceTests(unittest.TestCase):
    def test_stamps_become_triples_about_the_build(self):
        got = triples([], [], stamps={"build_id": "kg_1", "snapshot_at": "t",
                                      "kg_build_version": "2.0.0",
                                      "taxonomy_version": "abc"})
        self.assertEqual(len(got), 4)
        self.assertTrue(all("currentBuild" in t for t in got))

    def test_absent_stamps_emit_nothing(self):
        self.assertEqual(triples([], [], stamps={}), [])


class WriteNtTests(unittest.TestCase):
    def test_writes_counts_and_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "graph.nt")
            got = rdf.write_nt([frame()], [], path)
            body = Path(path).read_bytes()
            self.assertEqual(got["triples"], 4)
            self.assertEqual(got["sha256"],
                             hashlib.sha256(body).hexdigest())
            self.assertEqual(body.decode().count("\n"), 4)

    def test_output_is_byte_stable_across_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = rdf.write_nt([frame()], [], str(Path(tmp) / "a.nt"))
            b = rdf.write_nt([frame()], [], str(Path(tmp) / "b.nt"))
            self.assertEqual(a["sha256"], b["sha256"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
