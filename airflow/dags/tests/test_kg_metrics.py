"""Tests for kg/metrics.py — graph statistics and integrity checks.

``ChainDepthTests.test_dense_dag_terminates`` pins a rewrite. The original
``_longest_chain`` pushed ``(child, path + [child], seen | {child})`` for
every child of every partial path — a full simple-path enumeration,
exponential in branching factor. It survived only because ``partOfSeries``
happens to be near-tree-shaped (19,158 edges, max depth 7); one popular
series parent whose children also chain would have hung the report. The
replacement is a memoised DAG longest-path, O(V + E).

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_metrics.py -v
"""
import time
import unittest
from pathlib import Path

from modules.kg import metrics


def node(nid, kind, label=None, category=None, status=None):
    return {"id": nid, "kind": kind, "label": label,
            "category": category, "status": status}


def edge(src, etype, dst):
    return {"src": src, "type": etype, "dst": dst}


def small_graph():
    """Three frames in a series chain ending at an unscraped stub."""
    nodes = {n["id"]: n for n in [
        node("f1", "frame", "One", "meme", "confirmed"),
        node("f2", "frame", "Two", "meme", "confirmed"),
        node("f3", "frame", "Three", "subculture", "submission"),
        node("s1", "frame_stub", None, "meme", None),
        node("type:meme", "entry_type_concept", "meme"),
        node("tag:doge", "tag_concept", "doge"),
        node("e1", "external_ref"),
    ]}
    edges = [
        edge("f1", "hasEntryType", "type:meme"),
        edge("f1", "hasTag", "tag:doge"),
        edge("f2", "hasTag", "tag:doge"),
        edge("f1", "partOfSeries", "f2"),
        edge("f2", "partOfSeries", "f3"),
        edge("f3", "partOfSeries", "s1"),
        edge("f1", "citesExternal", "e1"),
    ]
    return nodes, edges


class PureStdlibTests(unittest.TestCase):
    def test_module_imports_no_numpy(self):
        # The module docstring says "Pure stdlib: no numpy", but
        # requirements.txt credited numpy to it for months while the module
        # that actually needs numpy+scipy (semantics.py) went undeclared.
        source = Path(metrics.__file__).read_text()
        self.assertNotIn("import numpy", source)
        self.assertNotIn("import scipy", source)


class ChainDepthTests(unittest.TestCase):
    def test_straight_chain(self):
        depth, witness = metrics._longest_chain(
            {"a": ["b"], "b": ["c"], "c": ["d"]}, ["a"])
        self.assertEqual(depth, 3)
        self.assertEqual(witness, ["a", "b", "c", "d"])

    def test_empty_graph(self):
        self.assertEqual(metrics._longest_chain({}, []), (0, []))

    def test_single_node_has_depth_zero(self):
        self.assertEqual(metrics._longest_chain({}, ["solo"]), (0, ["solo"]))

    def test_deeper_of_two_branches_wins(self):
        depth, witness = metrics._longest_chain(
            {"x": ["m"], "y": ["n"], "n": ["m"], "m": ["top"]}, ["x", "y"])
        self.assertEqual(depth, 3)
        self.assertEqual(witness[0], "y")

    def test_diamond_takes_the_long_side(self):
        depth, _ = metrics._longest_chain(
            {"a": ["b", "c"], "b": ["d"], "c": ["e"], "e": ["d"]}, ["a"])
        self.assertEqual(depth, 3)

    def test_dense_dag_terminates(self):
        # 8 layers x 14 nodes, fully connected between adjacent layers.
        # Simple-path enumeration would visit 14**7 ~= 1e8 paths.
        layers, width = 8, 14
        adj = {f"l{i}n{j}": [f"l{i + 1}n{k}" for k in range(width)]
               for i in range(layers - 1) for j in range(width)}
        roots = [f"l0n{j}" for j in range(width)]
        started = time.perf_counter()
        depth, witness = metrics._longest_chain(adj, roots)
        elapsed = time.perf_counter() - started
        self.assertEqual(depth, layers - 1)
        self.assertEqual(len(witness), layers)
        self.assertLess(elapsed, 2.0, "longest-chain went superlinear again")


class CycleTests(unittest.TestCase):
    def test_cycle_is_found(self):
        adj = {"a": ["b"], "b": ["c"], "c": ["a"]}
        self.assertTrue(metrics._find_cycles(adj, ["a", "b", "c"]))

    def test_acyclic_graph_reports_none(self):
        self.assertEqual(metrics._find_cycles({"a": ["b"]}, ["a", "b"]), [])

    def test_depth_is_undefined_while_cyclic(self):
        nodes = {n["id"]: n for n in [node("f1", "frame"), node("f2", "frame")]}
        edges = [edge("f1", "partOfSeries", "f2"),
                 edge("f2", "partOfSeries", "f1")]
        m = metrics.compute_metrics(nodes, edges)
        self.assertTrue(m["integrity"]["series_cycles_found"])
        self.assertEqual(m["integrity"]["series_chain_max_depth"], -1)


class ReplicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = metrics.compute_metrics(*small_graph())

    def test_counts(self):
        rep = self.m["replication"]
        self.assertEqual(rep["nodes"], 7)
        self.assertEqual(rep["edges"], 7)
        self.assertEqual(rep["frames"], 3)
        self.assertEqual(rep["rel_types"], 4)

    def test_nodes_by_kind(self):
        self.assertEqual(self.m["replication"]["nodes_by_kind"]["frame"], 3)
        self.assertEqual(self.m["replication"]["nodes_by_kind"]["frame_stub"], 1)

    def test_property_graph_is_the_default_counting_mode(self):
        self.assertEqual(self.m["replication"]["counting_mode"], "property_graph")

    def test_triple_equivalent_counts_attributes_too(self):
        te = metrics.compute_metrics(*small_graph(), triple_equivalent=True)
        self.assertEqual(te["replication"]["counting_mode"], "triple_equivalent")
        self.assertGreater(te["replication"]["edges"],
                           self.m["replication"]["edges"])


class IntegrityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.integrity = metrics.compute_metrics(*small_graph())["integrity"]

    def test_unresolved_series_parents_counts_stubs(self):
        self.assertEqual(self.integrity["unresolved_series_parents"], 1)

    def test_series_chain_depth(self):
        self.assertEqual(self.integrity["series_chain_max_depth"], 3)

    def test_frames_missing_semantics_are_counted(self):
        self.assertEqual(self.integrity["frames_without_entry_type"], 2)
        self.assertEqual(self.integrity["frames_without_tags"], 1)

    def test_clean_graph_has_no_dangling_or_duplicates(self):
        self.assertEqual(self.integrity["dangling_edge_targets"], 0)
        self.assertEqual(self.integrity["duplicate_edges"], 0)
        self.assertEqual(self.integrity["self_loops"], 0)

    def test_distributions_describe_frames_only(self):
        # A stub carries a guessed category under the same key a real frame
        # uses, so counting it here would inflate the corpus profile.
        self.assertEqual(self.integrity["category_distribution"],
                         {"meme": 2, "subculture": 1})
        self.assertEqual(sum(self.integrity["category_distribution"].values()), 3)


class DefectDetectionTests(unittest.TestCase):
    def test_dangling_target_is_reported(self):
        nodes = {"f1": node("f1", "frame")}
        m = metrics.compute_metrics(nodes, [edge("f1", "hasTag", "tag:ghost")])
        self.assertEqual(m["integrity"]["dangling_edge_targets"], 1)

    def test_duplicate_edge_is_reported(self):
        nodes = {n["id"]: n for n in [node("f1", "frame"), node("f2", "frame")]}
        e = edge("f1", "relatesToMeme", "f2")
        m = metrics.compute_metrics(nodes, [e, dict(e)])
        self.assertEqual(m["integrity"]["duplicate_edges"], 1)

    def test_self_loop_is_reported(self):
        nodes = {"f1": node("f1", "frame")}
        m = metrics.compute_metrics(nodes, [edge("f1", "relatesToMeme", "f1")])
        self.assertEqual(m["integrity"]["self_loops"], 1)

    def test_isolated_node_is_reported(self):
        nodes = {n["id"]: n for n in [node("f1", "frame"), node("lonely", "frame")]}
        m = metrics.compute_metrics(nodes, [])
        self.assertEqual(m["integrity"]["isolated_nodes"], 2)


class ReportTests(unittest.TestCase):
    def test_format_report_renders_without_error(self):
        text = metrics.format_report(metrics.compute_metrics(*small_graph()))
        self.assertIsInstance(text, str)
        self.assertTrue(text.strip())


if __name__ == "__main__":
    unittest.main(verbosity=2)
