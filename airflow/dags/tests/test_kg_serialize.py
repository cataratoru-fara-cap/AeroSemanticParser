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

NODES = [
    {"id": F1, "kind": "frame", "label": 'Doge "the" dog', "category": "meme",
     "status": "confirmed", "year": 2013, "from": "Tumblr",
     "about": "line one\n\nline\ttwo", "added": "2011-03-13T07:06:40Z",
     "badges": ["Sensitive"], "corpus_missing": ["region", "tags"],
     "section_texts": ["History\n\np1\n\np2", "Reception\n\np3"]},
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
    {"id": "image:" + IMG, "kind": "image", "width": 600, "height": 400},
    {"id": "origin:twitter", "kind": "origin_concept", "label": "twitter"},
    {"id": "origin:social-network", "kind": "origin_concept", "label": "social-network"},
    {"id": "badge:sensitive", "kind": "badge_concept", "label": "Sensitive"},
    # 6.0.0: an event, dated at day precision, with two actors and a
    # summary containing a quote (the escaping path).
    {"id": "event:abc123def456-0011223344", "kind": "event",
     "source_text": 'The photo "Kabosu" was posted to Tumblr on February 23rd, 2010. [3]',
     "source_section": "origin", "date": "2010-02-23",
     "date_precision": "day", "date_basis": "stated",
     "date_text": "February 23rd, 2010",
     "date_start": "2010-02-23T00:00:00Z", "date_end": "2010-02-23T23:59:59Z",
     "location": "Tumblr", "location_type": "platform",
     "certainty": "confirmed", "actors": ["Atsuko Sato", "u/kabosu"],
     "extraction_model": "ministral-3:14b", "extraction_version": "1.0.0"},
    # The embedded post's own node: in a real build the frame's citesExternal
    # edge to it creates this (build.py, parser 1.6.0 embeds).
    {"id": "https://www.tiktok.com/@a/video/1", "kind": "external_ref", "label": None},
    # ...and an undated one: no start/end, no actors.
    {"id": "event:abc123def456-5566778899", "kind": "event",
     "source_text": "Shortly afterwards it spread to 4chan.",
     "source_section": "spread", "date_precision": "none",
     "location": "4chan", "location_type": "platform",
     "certainty": "unconfirmed",
     "extraction_model": "ministral-3:14b", "extraction_version": "1.0.0"},
    # 6.1.0: two linked Wikidata items, one of them named from two fields.
    {"id": "wd:Q39315", "kind": "wikidata_entity", "qid": "Q39315",
     "label": "Shiba Inu", "description": "dog breed"},
    {"id": "wd:Q15894956", "kind": "wikidata_entity", "qid": "Q15894956",
     "label": 'Doge "meme"', "description": "Internet meme"},
]
EDGES = [
    {"src": F1, "type": "hasEntryType", "dst": "type:image-macro"},
    {"src": F1, "type": "hasTag", "dst": "tag:doge"},
    {"src": F1, "type": "hasTag", "dst": "tag:dog"},
    {"src": F2, "type": "hasTag", "dst": "tag:doge"},
    {"src": F1, "type": "partOfSeries", "dst": STUB},
    {"src": F1, "type": "relatesToMeme", "dst": F2, "occurrences": [
        {"anchor_text": "Cheems", "in_section": "Notes"}]},
    {"src": F1, "type": "citesExternal", "dst": EXT, "occurrences": [
        {"citation_text": 'The "Doge" article', "citation_index": 1},
        {"site_name": "Wikipedia"}]},
    {"src": "type:image-macro", "type": "subTypeOf", "dst": "type:meme"},
    {"src": F1, "type": "hasRegion", "dst": "region:Japan"},
    {"src": F1, "type": "hasImage", "dst": "image:" + IMG, "occurrences": [
        {"role": "page"},
        {"role": "section", "in_section": "Notes", "alt_text": "doge", "caption": "wow"}]},
    {"src": F1, "type": "hasOrigin", "dst": "origin:twitter"},
    {"src": F1, "type": "hasBadge", "dst": "badge:sensitive"},
    {"src": "origin:twitter", "type": "subTypeOf", "dst": "origin:social-network"},
    {"src": F1, "type": "hasEvent", "dst": "event:abc123def456-0011223344"},
    {"src": F1, "type": "hasEvent", "dst": "event:abc123def456-5566778899"},
    # extraction 2.0.0: what the dated event was attached to by position
    {"src": "event:abc123def456-0011223344", "type": "eventLink", "dst": F2},
    {"src": "event:abc123def456-0011223344", "type": "eventCitation", "dst": EXT},
    {"src": "event:abc123def456-0011223344", "type": "eventImage", "dst": "image:" + IMG},
    {"src": "event:abc123def456-5566778899", "type": "eventEmbed",
     "dst": "https://www.tiktok.com/@a/video/1"},
    {"src": "event:abc123def456-5566778899", "type": "eventDateAnchor",
     "dst": "event:abc123def456-0011223344"},
    # 6.1.0: one occurrence per mention; the NER label only where there is one.
    {"src": F1, "type": "fromTitle", "dst": "wd:Q15894956", "occurrences": [
        {"mention_text": "Doge", "link_score": 1.0, "link_method": "kym_id"}]},
    {"src": F1, "type": "fromTags", "dst": "wd:Q39315", "occurrences": [
        {"mention_text": "shiba inu", "link_score": 0.62, "link_method": "tag"}]},
    {"src": F1, "type": "fromAbout", "dst": "wd:Q39315", "occurrences": [
        {"mention_text": "Shiba Inus", "link_score": 0.83, "link_method": "ner",
         "ner_label": "ORG"},
        {"mention_text": "Shiba Inu", "link_score": 0.9, "link_method": "propn"}]},
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
        # badges left out: 5.0.0 moved it to hasBadge/badge_concept, not a
        # frame literal -- see ConceptFileTests.
        self.assertEqual({r["missing"] for r in self.rml("frame_corpus_missing.csv")},
                         {"region", "tags"})
        self.assertEqual(self.rml("frame_aliases.csv"), [])

    def test_images_are_files_with_their_size(self):
        self.assertEqual(self.rml("images.csv"), [{"iri": IMG, "width": "600", "height": "400"}])
        self.assertEqual(self.rml("image_edges.csv"), [{"url": F1, "image": IMG}])

    def test_section_texts_are_a_frame_list_file(self):
        self.assertEqual([r["text"] for r in self.rml("frame_section_texts.csv")],
                         ["History\n\np1\n\np2", "Reception\n\np3"])

    def test_one_occurrence_row_per_mention_with_empty_cells(self):
        cites = self.rml("cites_occurrences.csv")
        self.assertEqual(len(cites), 2)
        self.assertEqual((cites[0]["src"], cites[0]["dst"]), (F1, EXT))
        self.assertEqual((cites[0]["citation_text"], cites[0]["citation_index"],
                          cites[0]["site_name"]), ('The "Doge" article', "1", ""))
        self.assertEqual(cites[1]["site_name"], "Wikipedia")
        images = self.rml("image_occurrences.csv")
        self.assertEqual([(r["dst"], r["role"], r["caption"]) for r in images],
                         [(IMG, "page", ""), (IMG, "section", "wow")])
        self.assertEqual(self.rml("relates_occurrences.csv")[0]["anchor_text"], "Cheems")

    def test_occurrence_rows_are_counted_in_the_manifest(self):
        self.assertEqual(self.manifest["files"]["cites_occurrences.csv"]["rows"], 2)
        self.assertEqual(self.manifest["files"]["image_occurrences.csv"]["rows"], 2)

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
        # subTypeOf is one property-graph edge type split across TWO RML
        # files by id-prefix -- see serialize.py's ORIGIN_SUBTYPE_RML_FILE.
        # coOccursWith isn't in EDGE_TYPE_TO_RML_FILE at all (5.0.1: no
        # RDF representation for any pair), so this loop never sees it.
        # Every other type is a straight 1:1 count.
        for etype, (name, _) in serialize.EDGE_TYPE_TO_RML_FILE.items():
            if etype == "subTypeOf":
                expected = sum(1 for e in EDGES if e["type"] == etype
                              and not e["src"].startswith("origin:"))
            else:
                expected = sum(1 for e in EDGES if e["type"] == etype)
            self.assertEqual(len(self.rml(name)), expected, etype)

        origin_subtype_name = serialize.ORIGIN_SUBTYPE_RML_FILE[0]
        expected_origin_subtype = sum(
            1 for e in EDGES if e["type"] == "subTypeOf"
            and e["src"].startswith("origin:"))
        self.assertEqual(len(self.rml(origin_subtype_name)), expected_origin_subtype)

    def test_property_graph_view_equals_input_edges_per_type(self):
        counts = Counter(r["type"] for r in
                         read_csv(os.path.join(self.out, "kg_view_edges.csv")))
        self.assertEqual(counts, Counter(e["type"] for e in EDGES))

    def test_every_input_edge_and_occurrence_is_in_the_rdf(self):
        graph = set(Path(self.out, "graph.nt").read_text(encoding="utf-8").splitlines())
        annotations = [line for line in graph if line.startswith("<<")]
        # relates + cites + image values, then 6.1.0's entity mentions:
        # title 3, tags 3, About 4 + the second mention's 3 new values.
        self.assertEqual(len(annotations), 2 + 3 + 5 + 3 + 3 + 7)
        for edge in EDGES:
            expected = set(rdf.iter_triples([], [edge]))
            self.assertTrue(expected, edge)
            self.assertLessEqual(expected, graph, edge)

    def test_manifest_counts_match(self):
        self.assertEqual(self.manifest["counts"]["edges"], len(EDGES))
        self.assertEqual(self.manifest["counts"]["nodes"], len(NODES))
        self.assertEqual(self.manifest["counts"]["edges_by_type"]["hasTag"], 3)


class EventFileTests(Built):
    """6.0.0: what the event layer puts on disk for the RML path."""

    EV = "https://meme4.science/atlas/event/abc123def456-0011223344"
    UNDATED = "https://meme4.science/atlas/event/abc123def456-5566778899"

    def test_one_row_per_event_keyed_by_its_iri(self):
        rows = {r["iri"]: r for r in self.rml("events.csv")}
        self.assertEqual(set(rows), {self.EV, self.UNDATED})
        self.assertEqual(rows[self.EV]["date_start"], "2010-02-23T00:00:00Z")
        self.assertEqual(rows[self.EV]["certainty"], "confirmed")
        self.assertEqual(rows[self.EV]["date_basis"], "stated")

    def test_an_undated_event_leaves_the_interval_cells_empty(self):
        # An empty cell emits nothing in morph-kgc — same as rdf.py.
        row = {r["iri"]: r for r in self.rml("events.csv")}[self.UNDATED]
        self.assertEqual((row["date_start"], row["date_end"]), ("", ""))
        self.assertEqual(row["date_precision"], "none")

    def test_the_bare_date_is_not_a_column(self):
        # The one value pandas could read as a number inside morph-kgc.
        self.assertNotIn("date", self.rml("events.csv")[0])

    def test_one_actor_row_per_actor(self):
        rows = self.rml("event_actors.csv")
        self.assertEqual(sorted((r["iri"], r["actor"]) for r in rows),
                         [(self.EV, "Atsuko Sato"), (self.EV, "u/kabosu")])

    def test_the_edge_file_carries_the_bare_event_id(self):
        # The mapping templates it back onto https://meme4.science/atlas/event/.
        rows = self.rml("event_edges.csv")
        self.assertEqual(sorted(r["event"] for r in rows),
                         ["abc123def456-0011223344", "abc123def456-5566778899"])
        self.assertEqual({r["url"] for r in rows}, {F1})

    def test_attached_media_get_one_csv_each_keyed_by_bare_event_id(self):
        self.assertEqual(self.rml("event_link_edges.csv"),
                         [{"event": "abc123def456-0011223344", "target_url": F2}])
        self.assertEqual(self.rml("event_citation_edges.csv"),
                         [{"event": "abc123def456-0011223344", "target_url": EXT}])
        self.assertEqual(self.rml("event_image_edges.csv"),
                         [{"event": "abc123def456-0011223344", "image": IMG}])
        self.assertEqual(self.rml("event_embed_edges.csv"),
                         [{"event": "abc123def456-5566778899",
                           "target_url": "https://www.tiktok.com/@a/video/1"}])

    def test_graph_nt_carries_the_event_as_mk_event(self):
        with open(os.path.join(self.out, "graph.nt"), encoding="utf-8") as fh:
            nt = fh.read()
        mk = rdf.PREFIXES["mk"]
        self.assertIn(f"<{self.EV}> <{rdf.RDF_TYPE}> <{mk}Event> .", nt)
        self.assertIn(f"<{F1}> <{mk}hasEvent> <{self.EV}> .", nt)
        self.assertIn(f'<{self.EV}> <{mk}eventStart> '
                      f'"2010-02-23T00:00:00Z"^^<{rdf.XSD_DATETIME}> .', nt)
        self.assertNotIn(f"<{self.UNDATED}> <{mk}eventStart>", nt)
        self.assertIn(f"<{self.EV}> <{mk}eventImage> <{IMG}> .", nt)
        self.assertIn(f"<{self.EV}> <{mk}eventCitation> <{EXT}> .", nt)
        self.assertNotIn("eventSummary", nt)


class EntityFileTests(Built):
    """6.1.0: what the entity layer puts on disk for the RML path."""

    WD = "http://www.wikidata.org/entity/"

    def test_one_row_per_item_keyed_by_its_wikidata_iri(self):
        rows = {r["iri"]: r["label"] for r in self.rml("wikidata_entities.csv")}
        self.assertEqual(rows, {self.WD + "Q39315": "Shiba Inu",
                                self.WD + "Q15894956": 'Doge "meme"'})

    def test_each_field_has_its_own_edge_file_with_the_bare_qid(self):
        # The mapping templates it back onto http://www.wikidata.org/entity/.
        self.assertEqual(self.rml("entity_title_edges.csv"),
                         [{"url": F1, "qid": "Q15894956"}])
        self.assertEqual(self.rml("entity_tag_edges.csv"),
                         [{"url": F1, "qid": "Q39315"}])
        self.assertEqual(self.rml("entity_about_edges.csv"),
                         [{"url": F1, "qid": "Q39315"}])

    def test_one_occurrence_row_per_mention(self):
        rows = self.rml("entity_about_occurrences.csv")
        self.assertEqual([(r["mention_text"], r["link_score"], r["link_method"],
                           r["ner_label"]) for r in rows],
                         [("Shiba Inus", "0.83", "ner", "ORG"),
                          ("Shiba Inu", "0.9", "propn", "")])
        self.assertEqual({r["dst"] for r in rows}, {"Q39315"})

    def test_graph_nt_links_to_wikidata_with_imkgs_predicates(self):
        with open(os.path.join(self.out, "graph.nt"), encoding="utf-8") as fh:
            nt = fh.read()
        m4s, mk = rdf.PREFIXES["m4s"], rdf.PREFIXES["mk"]
        self.assertIn(f"<{F1}> <{m4s}fromAbout> <{self.WD}Q39315> .", nt)
        self.assertIn(f"<{F1}> <{m4s}fromTags> <{self.WD}Q39315> .", nt)
        self.assertIn(f"<{F1}> <{mk}fromTitle> <{self.WD}Q15894956> .", nt)
        self.assertIn(f'<< <{F1}> <{m4s}fromAbout> <{self.WD}Q39315> >> '
                      f'<{mk}linkScore> "0.9"^^<{rdf.XSD_DECIMAL}> .', nt)
        self.assertNotIn(f"<{self.WD}Q39315> <{rdf.RDF_TYPE}>", nt)


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


class ConceptFileTests(Built):
    """origin_concept/badge_concept RML files, the 3-row scheme.csv, and
    origin's platform-only subTypeOf edges routing to their own file."""

    def test_origin_and_badge_concepts_get_their_own_rml_files(self):
        origins = {r["slug"]: r["label"] for r in self.rml("origin_concepts.csv")}
        # label is rdf.concept_pref_label(slug): "-" -> " ", matching
        # entry_type's types.csv convention (rdf.py's iter_triples applies
        # the same transform for origin_concept, unlike badge_concept).
        self.assertEqual(origins, {"twitter": "twitter", "social-network": "social network"})
        badges = {r["slug"]: r["label"] for r in self.rml("badge_concepts.csv")}
        self.assertEqual(badges, {"sensitive": "Sensitive"})

    def test_scheme_csv_has_one_row_per_scheme(self):
        rows = {r["iri"]: r["label"] for r in self.rml("scheme.csv")}
        self.assertEqual(set(rows), {rdf.SCHEME_IRI, rdf.ORIGIN_SCHEME_IRI,
                                     rdf.BADGE_SCHEME_IRI})
        self.assertEqual(rows[rdf.ORIGIN_SCHEME_IRI], rdf.ORIGIN_SCHEME_LABEL)

    def test_has_origin_and_has_badge_edges(self):
        self.assertEqual(self.rml("origin_edges.csv"),
                         [{"url": F1, "origin": "twitter"}])
        self.assertEqual(self.rml("badge_edges.csv"),
                         [{"url": F1, "badge": "sensitive"}])

    def test_origin_subtype_routes_to_its_own_file_not_entry_types(self):
        self.assertEqual(self.rml("origin_subtype_edges.csv"),
                         [{"narrower": "twitter", "broader": "social-network"}])
        # subtype_edges.csv keeps only the entry_type pair from EDGES.
        self.assertEqual(self.rml("subtype_edges.csv"),
                         [{"narrower": "image-macro", "broader": "meme"}])


class CooccursRdfScopeTests(unittest.TestCase):
    """Isolated (not the shared Built fixture): a coOccursWith edge fails
    Built's generic "every edge produces some RDF" invariant by design
    (ProjectionsAgreeTests.test_every_input_edge_and_occurrence_is_in_the_
    rdf), so it is exercised here on its own instead. 5.0.1: this now
    applies to EVERY coOccursWith pair, not just tag ones -- entry_type's
    was removed, so tags are the only source and they never reach RDF."""

    def test_coOccursWith_is_always_property_graph_only(self):
        nodes = [{"id": "tag:meme", "kind": "tag_concept", "label": "meme"},
                {"id": "tag:dank-meme", "kind": "tag_concept", "label": "dank meme"}]
        edges = [{"src": "tag:meme", "type": "coOccursWith", "dst": "tag:dank-meme"}]
        with tempfile.TemporaryDirectory() as tmp:
            manifest = serialize.write_build(lambda: iter(nodes), lambda: iter(edges),
                                             tmp, build_id="b", assume_unique=True)
            # No RML file is ever created for coOccursWith at all now.
            self.assertFalse(os.path.exists(os.path.join(
                tmp, serialize.RML_DIR, "entry_type_cooccurs_edges.csv")))
            view_edges = read_csv(os.path.join(tmp, "kg_view_edges.csv"))
            self.assertEqual(view_edges, [{"source": "tag:meme", "target": "tag:dank-meme",
                                          "type": "coOccursWith"}])
            graph = Path(tmp, "graph.nt").read_text(encoding="utf-8")
            self.assertNotIn("coOccursWith", graph)
        # Still counted, even though absent from RML/RDF.
        self.assertEqual(manifest["counts"]["edges_by_type"]["coOccursWith"], 1)


class VocabularyTests(unittest.TestCase):
    def test_rml_table_covers_exactly_the_edge_vocabulary(self):
        # coOccursWith deliberately excluded (5.0.1): no RDF/RML
        # representation for any pair -- see EDGE_TYPE_TO_RML_FILE's
        # module-level assert in serialize.py for the same equality.
        self.assertEqual(set(serialize.EDGE_TYPE_TO_RML_FILE),
                         set(EDGE_TYPES) | set(CONCEPT_EDGE_TYPES))


if __name__ == "__main__":
    unittest.main(verbosity=2)
