"""Tests for kg/siblings.py -- sharesSameSeries edges from partOfSeries.

Two frames are siblings when they have the same series parent. The
property graph keeps each pair once (src < dst) and RDF states it both
ways; the second half is pinned in test_kg_rdf.py and test_kg_serialize.py.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_siblings.py -v
"""
import random
import unittest

from modules.kg import siblings

KYM = "https://knowyourmeme.com/memes/"
SERIES = KYM + "italian-brainrot-ai-italian-animals"
OTHER_SERIES = KYM + "cheems"
A, B, C, D = (KYM + s for s in ("bombardiro-crocodilo", "brr-brr-patapim",
                                "tralalero-tralala", "tung-tung-tung-sahur"))


def series(child, parent):
    return {"src": child, "type": "partOfSeries", "dst": parent}


def pairs(edges):
    return [(e["src"], e["dst"]) for e in siblings.sibling_edges(edges)]


class SiblingEdgeTests(unittest.TestCase):
    def test_every_pair_of_a_series_once_in_id_order(self):
        got = list(siblings.sibling_edges([series(D, SERIES), series(A, SERIES),
                                           series(B, SERIES)]))
        self.assertEqual(got, [
            {"src": A, "dst": B, "type": "sharesSameSeries"},
            {"src": A, "dst": D, "type": "sharesSameSeries"},
            {"src": B, "dst": D, "type": "sharesSameSeries"}])

    def test_n_frames_give_n_choose_2_edges(self):
        edges = [series(f"{KYM}m{i:02d}", SERIES) for i in range(10)]
        got = pairs(edges)
        self.assertEqual(len(got), 45)
        self.assertEqual(len(set(got)), 45)
        self.assertTrue(all(a < b for a, b in got))

    def test_frames_of_different_series_are_not_siblings(self):
        got = pairs([series(A, SERIES), series(B, SERIES), series(C, OTHER_SERIES)])
        self.assertEqual(got, [(A, B)])

    def test_a_series_of_one_frame_has_no_siblings(self):
        self.assertEqual(pairs([series(A, SERIES)]), [])

    def test_the_parent_is_not_a_sibling_of_its_children(self):
        # SERIES is itself part of a series. Its children are siblings of
        # each other; SERIES is a sibling only of OTHER_SERIES's children.
        got = pairs([series(A, SERIES), series(B, SERIES),
                     series(SERIES, OTHER_SERIES), series(C, OTHER_SERIES)])
        self.assertEqual(sorted(got), sorted([(A, B), (SERIES, C)]))

    def test_other_edge_types_and_self_loops_are_ignored(self):
        got = pairs([series(A, SERIES), series(B, SERIES),
                     {"src": C, "type": "citesMediaFrame", "dst": SERIES},
                     series(SERIES, SERIES)])
        self.assertEqual(got, [(A, B)])

    def test_a_repeated_input_edge_does_not_repeat_a_pair(self):
        self.assertEqual(pairs([series(A, SERIES), series(A, SERIES),
                                series(B, SERIES)]), [(A, B)])

    def test_output_does_not_depend_on_input_order(self):
        edges = [series(f"{KYM}m{i:02d}", SERIES if i % 2 else OTHER_SERIES)
                 for i in range(12)]
        expected = pairs(edges)
        random.Random(7).shuffle(edges)
        self.assertEqual(pairs(edges), expected)

    def test_the_input_is_drained_before_the_first_pair(self):
        # kym_kg passes a Mongo cursor and then writes while iterating; the
        # cursor must be finished before the first write.
        read = []

        def cursor():
            for e in (series(A, SERIES), series(B, SERIES), series(C, SERIES)):
                read.append(e["src"])
                yield e

        it = siblings.sibling_edges(cursor())
        next(it)
        self.assertEqual(read, [A, B, C])

    def test_no_edges_in_no_edges_out(self):
        self.assertEqual(pairs([]), [])


class ConstantsTests(unittest.TestCase):
    def test_edge_type_is_a_one_tuple(self):
        self.assertEqual(siblings.SIBLING_EDGE_TYPES, ("sharesSameSeries",))
        self.assertEqual(siblings.SIBLING_EDGE_TYPE, "sharesSameSeries")


if __name__ == "__main__":
    unittest.main(verbosity=2)
