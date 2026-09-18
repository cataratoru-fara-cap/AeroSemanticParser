"""Tests for kg/taxonomy.py — the curated entry_type taxonomy loader.

Two of these pin findings rather than behaviour, and should not be deleted
as redundant:

  * ``ConsistencyGuardTests`` — ``model -> influencer`` was encoded as a
    skos:broader triple in kg_output.nt (now rdfs:subClassOf) while the curated file listed it
    under ``contested`` ("sample before promoting"). The entries were
    sampled and the pair was then promoted by curator decision, so the
    record and the graph now agree. The guard is what stops them silently
    disagreeing again: the defect was never the edge itself, it was that a
    hand-typed constant could contradict the reviewed record unnoticed.
  * ``test_rationales_are_not_truncated`` — the file was flow-style YAML
    with unquoted prose, so a comma inside parentheses silently split a
    value in two and turned the remainder into a bogus key. Three
    rationales were cut mid-sentence before the repair.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_taxonomy.py -v
"""
import textwrap
import unittest
from dataclasses import replace
from pathlib import Path

from modules.kg import taxonomy as tx

CURATED = Path(__file__).resolve().parents[1] / "kg_config" / "entry_type_taxonomy.yaml"

# The edges the graph is expected to assert, transcribed from the curated
# file's two encodable buckets. ("model", "influencer") is here because the
# curator resolved it there — not because the old hand-typed constant had
# it. The distinction matters: the constant is gone, and this set is now
# derived from the reviewed record, which is the only source of truth.
EXPECTED_EDGES = {
    ("streamer", "creator"), ("fan-art", "fan-labor"), ("vlogger", "creator"),
    ("generator", "application"), ("ai-influencer", "influencer"),
    ("company", "organization"), ("song", "music"), ("album", "music"),
    ("flash-mob", "performance"), ("blockchain", "technology"),
    ("snowclone", "catchphrase"), ("creepypasta", "copypasta"),
    ("model", "influencer"),
}


def _write(tmpdir: str, body: str) -> str:
    path = Path(tmpdir) / "tax.yaml"
    path.write_text(textwrap.dedent(body))
    return str(path)


class LoadCuratedFileTests(unittest.TestCase):
    """Against the real file — these are the migration's proof."""

    @classmethod
    def setUpClass(cls):
        cls.tax = tx.load(str(CURATED))

    def test_encodes_exactly_the_reviewed_edges(self):
        self.assertEqual({e.pair for e in self.tax.edges}, EXPECTED_EDGES)

    def test_resolved_pair_is_encoded(self):
        # Promoted out of `contested` by curator decision after sampling.
        pairs = {tuple(sorted(e.pair)) for e in self.tax.edges}
        self.assertIn(("influencer", "model"), pairs)

    def test_still_contested_pairs_are_not_encoded(self):
        pairs = {tuple(sorted(e.pair)) for e in self.tax.edges}
        self.assertNotIn(("controversy", "viral-debate"), pairs)
        self.assertNotIn(("animal", "fauna"), pairs)

    def test_all_seven_buckets_are_parsed(self):
        self.assertEqual(set(self.tax.bucket_counts), set(tx.BUCKETS))
        self.assertEqual(self.tax.bucket_counts["broader_confirmed"], 5)
        self.assertEqual(self.tax.bucket_counts["contested"], 2)
        self.assertEqual(self.tax.bucket_counts["demoted"], 3)

    def test_withheld_pairs_are_carried_not_dropped(self):
        # contested(2) + demoted(3) + do_not_encode(5) = 10 pairs that exist
        # in the record but must never reach the graph.
        self.assertEqual(len(self.tax.withheld), 10)

    def test_rationales_are_not_truncated(self):
        for e in self.tax.edges:
            text = e.rationale or ""
            self.assertEqual(text.count("("), text.count(")"),
                             f"unbalanced parens in {e.pair}: {text!r}")

    def test_numeric_evidence_survives(self):
        snowclone = next(e for e in self.tax.edges if e.narrower == "snowclone")
        self.assertEqual(snowclone.cooccur, 181)
        self.assertEqual(snowclone.containment, 0.26)
        self.assertEqual(snowclone.pmi_bits, 1.0)

    def test_crosscutting_qualifiers_have_no_parent(self):
        self.assertEqual(self.tax.qualifiers,
                         ("ai-generated", "historical-figure", "shock-media"))
        for q in self.tax.qualifiers:
            self.assertNotIn(q, {e.narrower for e in self.tax.edges})

    def test_freestanding_note_is_not_mistaken_for_an_umbrella(self):
        self.assertEqual(len(self.tax.missing_umbrellas), 3)
        self.assertEqual(len(self.tax.notes), 1)

    def test_version_is_content_addressed(self):
        self.assertEqual(len(self.tax.version), 64)
        self.assertEqual(self.tax.version, tx.load(str(CURATED)).version)


class ConceptEdgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.edges = tx.load(str(CURATED)).concept_edges()

    def test_ids_match_kg_build_vocabulary(self):
        for e in self.edges:
            self.assertTrue(e["src"].startswith("type:"))
            self.assertTrue(e["dst"].startswith("type:"))

    def test_edge_type_is_subtypeof(self):
        # Not "broader": IMKG uses skos:broader for frame series, so the
        # type hierarchy is rdfs:subClassOf (kg/rdf.py) under its own name.
        self.assertEqual({e["type"] for e in self.edges}, {"subTypeOf"})
        self.assertEqual(set(tx.CONCEPT_EDGE_TYPES), {"subTypeOf"})

    def test_direction_is_narrower_to_broader(self):
        self.assertIn({"src": "type:streamer", "dst": "type:creator",
                       "type": "subTypeOf"}, self.edges)


class PrefixParameterTests(unittest.TestCase):
    """5.0.0: concept_edges()/encodable_edges() gained a prefix= parameter
    so kg/origin.py can reuse them for a different concept-id namespace.
    The default must stay byte-identical to pre-5.0.0 behaviour."""

    @classmethod
    def setUpClass(cls):
        cls.tax = tx.load(str(CURATED))

    def test_default_prefix_is_unchanged(self):
        self.assertEqual(self.tax.concept_edges(), self.tax.concept_edges(prefix="type:"))

    def test_custom_prefix_replaces_the_default(self):
        edges = self.tax.concept_edges(prefix="origin:")
        self.assertTrue(edges)
        for e in edges:
            self.assertTrue(e["src"].startswith("origin:"))
            self.assertTrue(e["dst"].startswith("origin:"))
            self.assertFalse(e["src"].startswith("type:"))

    def test_encodable_edges_default_prefix_is_unchanged(self):
        census = {"value_counts": {s: 1 for s in self.tax.slugs()}}
        default = tx.encodable_edges(self.tax, census)
        explicit = tx.encodable_edges(self.tax, census, prefix="type:")
        self.assertEqual(default, explicit)

    def test_encodable_edges_custom_prefix(self):
        census = {"value_counts": {s: 1 for s in self.tax.slugs()}}
        edges = tx.encodable_edges(self.tax, census, prefix="origin:")
        self.assertTrue(all(e["src"].startswith("origin:") for e in edges))


class ParseFromDictTests(unittest.TestCase):
    """``_parse`` is the file-I/O-free half of ``load()`` — kg/origin.py
    reuses it directly for the bucket section of its own curated file."""

    def test_parse_accepts_a_plain_dict(self):
        doc = {"broader_confirmed": [
            {"broader": "creator", "narrower": "streamer", "rationale": "x"}]}
        tax = tx._parse(doc, version="v1", path="<memory>")
        self.assertEqual(tax.version, "v1")
        self.assertEqual({e.pair for e in tax.edges}, {("streamer", "creator")})

    def test_parse_still_runs_check_consistency(self):
        doc = {"broader_confirmed": [
                  {"broader": "b", "narrower": "n", "rationale": "x"}],
              "contested": [{"pair": ["b", "n"]}]}
        with self.assertRaises(tx.TaxonomyError):
            tx._parse(doc, version="v1", path="<memory>")

    def test_load_and_parse_agree_on_the_real_file(self):
        via_load = tx.load(str(CURATED))
        with open(CURATED, "rb") as fh:
            raw = fh.read()
        import yaml
        doc = yaml.safe_load(raw.decode("utf-8"))
        via_parse = tx._parse(doc, version=via_load.version, path=str(CURATED))
        self.assertEqual(via_load.edges, via_parse.edges)


class ConsistencyGuardTests(unittest.TestCase):
    """The guards that make the shipped bug unrepresentable."""

    @classmethod
    def setUpClass(cls):
        cls.tax = tx.load(str(CURATED))

    def test_encoding_a_withheld_pair_is_rejected(self):
        bad = replace(self.tax, edges=self.tax.edges + (
            tx.BroaderEdge("viral-debate", "controversy",
                           "broader_semantic_only"),))
        with self.assertRaises(tx.TaxonomyError) as ctx:
            tx.check_consistency(bad)
        self.assertIn("contested", str(ctx.exception))

    def test_guard_is_order_insensitive(self):
        # contested lists [controversy, viral-debate]; encoding it the other
        # way round must still be caught.
        bad = replace(self.tax, edges=(
            tx.BroaderEdge("controversy", "viral-debate", "broader_confirmed"),))
        with self.assertRaises(tx.TaxonomyError):
            tx.check_consistency(bad)

    def test_cycle_is_rejected(self):
        # Three nodes, not two: a 2-cycle (a->b, b->a) normalises to the same
        # sorted key for both edges, so the duplicate-edge guard would fire
        # first and the cycle detector would never run.
        bad = replace(self.tax, edges=(
            tx.BroaderEdge("a", "b", "broader_confirmed"),
            tx.BroaderEdge("b", "c", "broader_confirmed"),
            tx.BroaderEdge("c", "a", "broader_confirmed")), withheld=())
        with self.assertRaises(tx.TaxonomyError) as ctx:
            tx.check_consistency(bad)
        self.assertIn("cycle", str(ctx.exception))

    def test_duplicate_edge_is_rejected(self):
        bad = replace(self.tax, edges=(
            tx.BroaderEdge("x", "y", "broader_confirmed"),
            tx.BroaderEdge("x", "y", "broader_semantic_only")), withheld=())
        with self.assertRaises(tx.TaxonomyError):
            tx.check_consistency(bad)


class MalformedFileTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        self.tmpdir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def test_unparseable_yaml_raises_rather_than_returning_empty(self):
        # The original file's failure mode. An empty taxonomy would quietly
        # produce a graph with no concept layer at all.
        path = _write(self.tmpdir, """
            broader_confirmed:
              - {broader: a, narrower: b, note: is this a key? no}
            """)
        with self.assertRaises(tx.TaxonomyError):
            tx.load(path)

    def test_unknown_bucket_is_rejected(self):
        path = _write(self.tmpdir, """
            broader_confirmd:
              - broader: creator
                narrower: streamer
            """)
        with self.assertRaises(tx.TaxonomyError) as ctx:
            tx.load(path)
        self.assertIn("unknown bucket", str(ctx.exception))

    def test_self_edge_is_rejected(self):
        path = _write(self.tmpdir, """
            broader_confirmed:
              - broader: creator
                narrower: creator
            """)
        with self.assertRaises(tx.TaxonomyError):
            tx.load(path)

    def test_item_without_a_pair_is_rejected(self):
        path = _write(self.tmpdir, """
            broader_confirmed:
              - evidence: agree
                rationale: "no slugs here"
            """)
        with self.assertRaises(tx.TaxonomyError):
            tx.load(path)


class ValidateAgainstCensusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tax = tx.load(str(CURATED))

    def test_missing_slugs_are_returned_as_data_not_printed(self):
        census = {"value_counts": {"streamer": 5, "creator": 9}}
        report = tx.validate(self.tax, census)
        self.assertEqual(report["edges_encoded"], 1)
        self.assertIn("snowclone", report["slugs_missing_from_census"])
        self.assertTrue(report["edges_dropped"])
        self.assertEqual(report["edges_dropped"][0].keys(),
                         {"narrower", "broader", "bucket", "missing"})

    def test_full_census_encodes_everything(self):
        census = {"value_counts": {s: 1 for s in self.tax.slugs()}}
        report = tx.validate(self.tax, census)
        self.assertEqual(report["slugs_missing_from_census"], [])
        self.assertEqual(report["edges_encoded"], 13)
        self.assertEqual(report["edges_dropped"], [])

    def test_encodable_edges_filters_to_the_corpus(self):
        census = {"value_counts": {"streamer": 5, "creator": 9}}
        edges = tx.encodable_edges(self.tax, census)
        self.assertEqual(edges, [{"src": "type:streamer", "dst": "type:creator",
                                  "type": "subTypeOf"}])

    def test_summary_shape_for_the_run_record(self):
        summary = self.tax.summary()
        self.assertEqual(summary["broader_edges"], 13)
        self.assertEqual(summary["withheld_pairs"], 10)
        self.assertEqual(summary["taxonomy_version"], self.tax.version)


if __name__ == "__main__":
    unittest.main(verbosity=2)
