"""Tests for kg/rdf.py — (nodes, edges) -> canonical N-Triples, in IMKG's vocabulary.

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


M4S, MK, KYM = rdf.PREFIXES["m4s"], rdf.PREFIXES["mk"], rdf.PREFIXES["kym"]
RDF_TYPE = rdf.PREFIXES["rdf"] + "type"
XSD = rdf.PREFIXES["xsd"]


class ImkgFrameTests(unittest.TestCase):
    """A frame must read as an IMKG media frame, term for term."""

    def test_frame_gets_imkg_class_title_status_and_category_class(self):
        got = set(triples([frame()], []))
        self.assertEqual(got, {
            f"<{FRAME}> <{RDF_TYPE}> <{M4S}MediaFrame> .",
            f"<{FRAME}> <{RDF_TYPE}> <{KYM}Meme> .",
            f'<{FRAME}> <{M4S}title> "Doge" .',
            f'<{FRAME}> <{rdf.PREFIXES["rdfs"]}label> "Doge" .',
            f'<{FRAME}> <{M4S}status> "confirmed" .',
        })

    def test_imkg_literals_and_datatypes(self):
        got = set(triples([{
            "id": FRAME, "kind": "frame", "year": 2013, "from": "Tumblr",
            "about": "a\nb", "added": "2011-03-13T07:06:40Z",
            "last_updated": "2023-11-14T22:13:20Z"}], []))
        self.assertIn(f'<{FRAME}> <{M4S}year> "2013"^^<{XSD}integer> .', got)
        self.assertIn(f'<{FRAME}> <{M4S}from> "Tumblr" .', got)
        self.assertIn(f'<{FRAME}> <{M4S}added> '
                      f'"2011-03-13T07:06:40Z"^^<{XSD}dateTime> .', got)
        self.assertIn(f'<{FRAME}> <{M4S}last_update_source> '
                      f'"2023-11-14T22:13:20Z"^^<{XSD}dateTime> .', got)
        self.assertTrue(any(f"<{M4S}about>" in t for t in got))

    def test_list_properties_emit_one_triple_per_value(self):
        got = triples([{"id": FRAME, "kind": "frame",
                        "badges": ["Sensitive", "NSFW"], "aliases": ["Shibe"],
                        "corpus_missing": ["region"]}], [])
        self.assertEqual(sum(f"<{MK}badge>" in t for t in got), 2)
        self.assertEqual(sum("altLabel" in t for t in got), 1)
        self.assertEqual(sum(f"<{MK}corpusMissing>" in t for t in got), 1)

    def test_unknown_category_gets_no_category_class(self):
        self.assertIsNone(rdf.category_class("unknown"))
        self.assertIsNone(rdf.category_class(None))
        self.assertEqual(rdf.category_class("subculture"), "Subculture")

    def test_missing_attributes_emit_nothing(self):
        got = triples([frame(label=None, category=None, status=None)], [])
        self.assertEqual(got, [f"<{FRAME}> <{RDF_TYPE}> <{M4S}MediaFrame> ."])

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

    def test_entry_type_concept_is_a_class_and_a_skos_concept(self):
        got = triples([{"id": "type:exploitable", "kind": "entry_type_concept",
                        "label": "exploitable"}], [])
        # 4 for the concept (rdfs:Class, skos:Concept, inScheme, prefLabel)
        # + 2 declaring the scheme itself. An skos:inScheme pointing at an
        # undeclared resource is incomplete SKOS.
        self.assertEqual(len(got), 6)
        self.assertTrue(any("rdf-schema#Class" in t for t in got))
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


class BodyNodeTests(unittest.TestCase):
    SID = "https://meme4.science/atlas/entry/abc/section/3"

    def test_section(self):
        got = set(triples([{"id": self.SID, "kind": "section",
                            "section_kind": "other", "heading": "H",
                            "position": 3, "level": 2, "text": "p1\n\tp2"}], []))
        self.assertIn(f"<{self.SID}> <{RDF_TYPE}> <{MK}Section> .", got)
        self.assertIn(f'<{self.SID}> <{MK}position> "3"^^<{XSD}integer> .', got)
        self.assertIn(f'<{self.SID}> <{MK}headingLevel> "2"^^<{XSD}integer> .', got)
        self.assertIn(f'<{self.SID}> <{MK}text> "p1\\n\\tp2" .', got)

    def test_position_zero_is_a_value_not_an_absence(self):
        got = triples([{"id": self.SID, "kind": "section", "position": 0}], [])
        self.assertTrue(any(f"<{MK}position>" in t and '"0"' in t for t in got))

    def test_reference_class_comes_from_ref_class(self):
        rid = "https://meme4.science/atlas/entry/abc/reference/0"
        got = set(triples([{"id": rid, "kind": "reference",
                            "ref_class": "ExternalReference", "index": 1,
                            "citation_text": "Wikipedia"}], []))
        self.assertEqual(got, {
            f"<{rid}> <{RDF_TYPE}> <{MK}ExternalReference> .",
            f'<{rid}> <{MK}citationIndex> "1"^^<{XSD}integer> .',
            f'<{rid}> <{MK}citationText> "Wikipedia" .'})

    def test_image_iri_is_the_file_url(self):
        img = "https://i.kym-cdn.com/x.jpg"
        got = set(triples([{"id": f"image:{img}", "kind": "image",
                            "width": 600, "caption": "wow"}], []))
        self.assertEqual(got, {
            f"<{img}> <{RDF_TYPE}> <{MK}Image> .",
            f'<{img}> <{MK}width> "600"^^<{XSD}integer> .',
            f'<{img}> <{MK}caption> "wow" .'})

    def test_region_concept_gets_no_triples(self):
        self.assertEqual(triples([{"id": "region:Japan", "kind": "region_concept",
                                   "label": "Japan"}], []), [])


class EdgeTripleTests(unittest.TestCase):
    def test_series_is_skos_broader_with_its_inverse(self):
        # IMKG emits both directions; so do we.
        got = triples([], [{"src": FRAME, "type": "partOfSeries",
                            "dst": PARENT}])
        skos = rdf.PREFIXES["skos"]
        self.assertEqual(got, [f"<{FRAME}> <{skos}broader> <{PARENT}> .",
                               f"<{PARENT}> <{skos}narrower> <{FRAME}> ."])

    def test_relations_become_iri_objects(self):
        got = triples([], [{"src": FRAME, "type": "relatesToMeme",
                            "dst": PARENT}])
        self.assertEqual(got, [f"<{FRAME}> <{MK}relatesToMeme> <{PARENT}> ."])

    def test_entry_type_is_rdf_type(self):
        got = triples([], [{"src": FRAME, "type": "hasEntryType",
                            "dst": "type:exploitable"}])
        self.assertEqual(got, [f"<{FRAME}> <{RDF_TYPE}> <{rdf.TYPES_BASE}exploitable> ."])

    def test_region_is_a_literal(self):
        got = triples([], [{"src": FRAME, "type": "hasRegion", "dst": "region:Japan"}])
        self.assertEqual(got, [f'<{FRAME}> <{MK}region> "Japan" .'])

    def test_has_image_points_at_the_file(self):
        img = "https://i.kym-cdn.com/x.jpg"
        got = triples([], [{"src": FRAME, "type": "hasImage", "dst": f"image:{img}"}])
        self.assertEqual(got, [f"<{FRAME}> <{MK}hasImage> <{img}> ."])

    def test_tags_become_imkg_literals_with_the_prefix_stripped(self):
        got = triples([], [{"src": FRAME, "type": "hasTag", "dst": "tag:doge"}])
        self.assertEqual(got, [f'<{FRAME}> <{M4S}tag> "doge" .'])

    def test_entry_type_edges_point_at_the_types_namespace(self):
        got = triples([], [{"src": FRAME, "type": "hasEntryType",
                            "dst": "type:exploitable"}])
        self.assertIn(f"<{rdf.TYPES_BASE}exploitable>", got[0])

    def test_taxonomy_is_rdfs_subclassof_not_skos_broader(self):
        # skos:broader already means "part of a series" between frames in
        # IMKG; the entry-type hierarchy must not reuse it.
        got = triples([], [{"src": "type:model", "type": "subTypeOf",
                            "dst": "type:influencer"}])
        self.assertEqual(got, [f"<{rdf.TYPES_BASE}model> "
                               f"<{rdf.PREFIXES['rdfs']}subClassOf> "
                               f"<{rdf.TYPES_BASE}influencer> ."])

    def test_unknown_edge_type_is_skipped(self):
        self.assertEqual(triples([], [{"src": FRAME, "type": "nope",
                                       "dst": PARENT}]), [])

    def test_every_build_edge_type_has_a_predicate(self):
        from modules.kg import build, taxonomy
        for etype in set(build.EDGE_TYPES) | set(taxonomy.CONCEPT_EDGE_TYPES):
            self.assertIn(etype, rdf.EDGE_PREDICATES)

    def test_every_node_kind_is_either_classed_or_deliberately_not(self):
        from modules.kg import build
        unclassed = {"frame_stub", "tag_concept", "region_concept", "external_ref"}
        self.assertEqual(set(rdf.NODE_CLASSES) | unclassed, set(build.NODE_KINDS))


class SetSemanticsTests(unittest.TestCase):
    """RDF is a set; the property-graph edge list is a bag."""

    def test_repeated_edge_yields_one_triple(self):
        e = {"src": FRAME, "type": "hasTag", "dst": "tag:doge"}
        self.assertEqual(len(triples([], [e, dict(e)])), 1)

    def test_repeated_node_yields_one_set_of_triples(self):
        self.assertEqual(len(triples([frame(), frame()], [])), 5)

    def test_two_frames_sharing_a_label_both_emit(self):
        # Dedup is per triple, not per literal value.
        got = triples([frame(), frame(nid=PARENT, label="Doge")], [])
        self.assertEqual(len(got), 10)

    def test_dedupe_can_be_turned_off_for_unique_input(self):
        e = {"src": FRAME, "type": "hasTag", "dst": "tag:doge"}
        self.assertEqual(len(triples([], [e, dict(e)], dedupe=False)), 2)


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
            self.assertEqual(got["triples"], 5)
            self.assertEqual(got["sha256"],
                             hashlib.sha256(body).hexdigest())
            self.assertEqual(body.decode().count("\n"), 5)

    def test_output_is_byte_stable_across_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = rdf.write_nt([frame()], [], str(Path(tmp) / "a.nt"))
            b = rdf.write_nt([frame()], [], str(Path(tmp) / "b.nt"))
            self.assertEqual(a["sha256"], b["sha256"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
