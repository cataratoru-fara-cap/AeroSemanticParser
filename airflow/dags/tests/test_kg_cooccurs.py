"""Tests for kg/cooccurs.py -- coOccursWith edges from a census.

A separate module from kg/taxonomy.py on purpose (see the module
docstring): subTypeOf is curator judgement, coOccursWith is a threshold
applied to a statistic. This module doesn't know or care about the RDF
scope restriction (tag_concept has no RDF resource) -- that's applied
downstream in kg/rdf.py and kg/serialize.py, not here.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_cooccurs.py -v
"""
import unittest

from modules.kg import cooccurs


def census(pairs):
    return {"pair_cooccurrence": [{"a": a, "b": b, "count": c} for a, b, c in pairs]}


class EdgesFromCensusTests(unittest.TestCase):
    def test_canonical_ordering_is_preserved(self):
        c = census([("creator", "streamer", 23)])
        edges = cooccurs.edges_from_census(c, "type:")
        self.assertEqual(edges, [{"src": "type:creator", "dst": "type:streamer",
                                  "type": "coOccursWith"}])

    def test_prefix_is_applied_to_both_endpoints(self):
        c = census([("dog", "shiba-inu", 5)])
        edges = cooccurs.edges_from_census(c, "tag:")
        self.assertEqual(edges[0]["src"], "tag:dog")
        self.assertEqual(edges[0]["dst"], "tag:shiba-inu")

    def test_edge_type_is_coOccursWith(self):
        c = census([("a", "b", 1)])
        self.assertEqual(cooccurs.edges_from_census(c, "type:")[0]["type"], "coOccursWith")

    def test_min_count_filters_stricter_than_the_census(self):
        c = census([("a", "b", 10), ("c", "d", 2)])
        edges = cooccurs.edges_from_census(c, "type:", min_count=5)
        self.assertEqual(edges, [{"src": "type:a", "dst": "type:b", "type": "coOccursWith"}])

    def test_min_count_none_keeps_everything_the_census_already_returned(self):
        c = census([("a", "b", 10), ("c", "d", 2)])
        edges = cooccurs.edges_from_census(c, "type:")
        self.assertEqual(len(edges), 2)

    def test_empty_pair_cooccurrence_yields_no_edges(self):
        self.assertEqual(cooccurs.edges_from_census({"pair_cooccurrence": []}, "type:"), [])

    def test_missing_pair_cooccurrence_key_yields_no_edges(self):
        # origin's census never has this key populated with real pairs
        # (single-valued field) -- must not raise.
        self.assertEqual(cooccurs.edges_from_census({}, "origin:"), [])

    def test_multiple_pairs_all_get_edges(self):
        c = census([("a", "b", 5), ("b", "c", 5), ("a", "c", 5)])
        edges = cooccurs.edges_from_census(c, "type:")
        self.assertEqual(len(edges), 3)


class ConstantsTests(unittest.TestCase):
    def test_edge_type_is_a_one_tuple(self):
        self.assertEqual(cooccurs.COOCCURS_EDGE_TYPES, ("coOccursWith",))
        self.assertEqual(cooccurs.COOCCURS_EDGE_TYPE, "coOccursWith")


if __name__ == "__main__":
    unittest.main(verbosity=2)
