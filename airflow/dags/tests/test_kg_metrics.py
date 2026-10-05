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

    def test_unresolved_series_parents_are_parents_that_are_not_frames(self):
        # f2, f3 and s1 are parents; only s1 is not a scraped frame.
        self.assertEqual(self.integrity["series_parents"], 3)
        self.assertEqual(self.integrity["unresolved_series_parents"], 1)
        self.assertEqual(self.integrity["unresolved_series_parent_pct"], 33.33)
        self.assertEqual(self.integrity["unresolved_series_parents_sample"], ["s1"])

    def test_a_stub_that_is_only_linked_is_not_an_unresolved_parent(self):
        # Until 2026-10-05 every frame_stub counted as an unresolved series
        # parent: 9,576 in 6.5.0, of which 1,061 were parents. A stub that a
        # page merely links to is counted as a stub, not as a parent.
        nodes, edges = small_graph()
        nodes["s2"] = node("s2", "frame_stub", None, "meme", None)
        edges.append(edge("f1", "citesMediaFrame", "s2"))
        integrity = metrics.compute_metrics(nodes, edges)["integrity"]
        self.assertEqual(integrity["unresolved_series_parents"], 1)
        self.assertEqual(integrity["frame_stubs"], 2)

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
        e = edge("f1", "citesMediaFrame", "f2")
        m = metrics.compute_metrics(nodes, [e, dict(e)])
        self.assertEqual(m["integrity"]["duplicate_edges"], 1)

    def test_self_loop_is_reported(self):
        nodes = {"f1": node("f1", "frame")}
        m = metrics.compute_metrics(nodes, [edge("f1", "citesMediaFrame", "f1")])
        self.assertEqual(m["integrity"]["self_loops"], 1)

    def test_isolated_node_is_reported(self):
        nodes = {n["id"]: n for n in [node("f1", "frame"), node("lonely", "frame")]}
        m = metrics.compute_metrics(nodes, [])
        self.assertEqual(m["integrity"]["isolated_nodes"], 2)


def layered_graph():
    """small_graph plus the 6.x layers: f1 has a title link, a template that
    shows an item, and two events; f2 a template that shows nothing; f3
    nothing. wd:Q2 is reached from text AND an image, wd:Q3 from an image only."""
    nodes, edges = small_graph()
    nodes.update({n["id"]: n for n in [
        node("wd:Q1", "wikidata_entity", "one"),
        node("wd:Q2", "wikidata_entity", "two"),
        node("wd:Q3", "wikidata_entity", "three"),
        node("t1", "template", "T one"),
        node("t2", "template", "T two"),
        node("ev1", "event"), node("ev2", "event"),
    ]})
    edges += [
        edge("f1", "fromTitle", "wd:Q1"),
        edge("f1", "fromAbout", "wd:Q2"),
        edge("f2", "fromTags", "wd:Q2"),
        edge("f1", "hasTemplate", "t1"),
        edge("f2", "hasTemplate", "t1"),
        edge("f2", "hasTemplate", "t2"),
        edge("t1", "fromImage", "wd:Q2"),
        edge("t1", "fromImage", "wd:Q3"),
        edge("f1", "hasEvent", "ev1"),
        edge("f1", "hasEvent", "ev2"),
    ]
    return nodes, edges


class LayerTests(unittest.TestCase):
    def setUp(self):
        self.lay = metrics.layer_metrics(*layered_graph())

    def test_frame_coverage(self):
        lay = self.lay
        self.assertEqual(lay["frames"], 3)
        self.assertEqual(lay["frames_with_entity_link"], 2)
        self.assertEqual(lay["frames_with_entity_link_by_field"],
                         {"fromTitle": 1, "fromTags": 1, "fromAbout": 1})
        self.assertEqual(lay["frames_with_template"], 2)
        self.assertEqual(lay["frames_with_template_entity"], 2)   # both keep t1
        self.assertEqual(lay["frames_with_events"], 1)
        self.assertEqual(lay["frames_with_all_three"], 1)
        self.assertEqual(lay["frames_with_none"], 1)              # f3
        self.assertEqual(lay["events_per_frame_mean"], 2.0)
        self.assertEqual(lay["templates_per_frame_mean"], 1.5)

    def test_entities_by_source(self):
        lay = self.lay
        self.assertEqual(lay["wikidata_entities"], 3)
        self.assertEqual((lay["entities_from_text_only"], lay["entities_from_images_only"],
                          lay["entities_from_both"]), (1, 1, 1))

    def test_templates(self):
        lay = self.lay
        self.assertEqual(lay["templates"], 2)
        self.assertEqual(lay["templates_with_entity"], 1)
        self.assertEqual(lay["templates_shared_by_frames"], 1)
        self.assertEqual(lay["top_entities_by_frames"][0],
                         {"id": "wd:Q2", "label": "two", "frames": 2})

    def test_a_frames_own_image_and_statements(self):
        nodes, edges = layered_graph()
        nodes["wd:Q5"] = node("wd:Q5", "wikidata_entity", "human")
        edges += [edge("f3", "fromImage", "wd:Q3"),
                  edge("f3", "fromImage", "wd:Q1"),
                  edge("wd:Q2", "P31", "wd:Q5"),
                  edge("wd:Q3", "P31", "wd:Q5"),
                  edge("wd:Q3", "P21", "wd:Q1")]
        lay = metrics.layer_metrics(nodes, edges)
        self.assertEqual(lay["frames_with_image_entity"], 1)               # f3
        self.assertEqual(lay["templates_with_entity"], 1)                  # still t1 only
        self.assertEqual(lay["top_entities_by_templates"][0]["templates"], 1)
        self.assertEqual(lay["top_entities_by_frame_images"][0],
                         {"id": "wd:Q1", "label": "one", "frames": 1})
        self.assertEqual((lay["entities_from_text_only"], lay["entities_from_images_only"],
                          lay["entities_from_both"]), (0, 1, 2))
        self.assertEqual((lay["wikidata_statements"], lay["wikidata_statement_properties"],
                          lay["wikidata_items_with_statements"]), (3, 2, 2))
        self.assertEqual(lay["top_statement_properties"][0],
                         {"property": "P31", "statements": 2})

    def test_a_core_graph_has_no_layers(self):
        lay = metrics.layer_metrics(*small_graph())
        self.assertEqual((lay["frames_with_entity_link"], lay["templates"],
                          lay["frames_with_events"], lay["frames_with_none"]), (0, 0, 0, 3))


class ScopeTests(unittest.TestCase):
    def test_core_drops_origin_hierarchy_and_new_layers(self):
        self.assertTrue(metrics.in_scope(edge("type:a", "subTypeOf", "type:b"), "core"))
        self.assertFalse(metrics.in_scope(edge("origin:a", "subTypeOf", "origin:b"), "core"))
        self.assertFalse(metrics.in_scope(edge("f1", "fromTitle", "wd:Q1"), "core"))
        self.assertFalse(metrics.in_scope(edge("f1", "hasTemplate", "t1"), "core"))

    def test_core_counts_a_node_only_through_a_core_edge(self):
        nodes, edges = small_graph()
        nodes["https://imgflip.com/meme/1"] = node("https://imgflip.com/meme/1", "external_ref")
        nodes["f4"] = node("f4", "frame", "Lonely")
        core = metrics.restrict_to_scope(nodes, edges, "core")
        self.assertNotIn("https://imgflip.com/meme/1", core)   # a template's page
        self.assertIn("e1", core)                              # cited by a frame
        self.assertIn("f4", core)                              # frames always count
        self.assertEqual(metrics.compute_metrics(core, edges)["integrity"]["isolated_nodes"], 1)
        self.assertEqual(len(metrics.restrict_to_scope(nodes, edges, "full")), len(nodes))

    def test_full_keeps_everything(self):
        for e in layered_graph()[1] + [edge("origin:a", "subTypeOf", "origin:b")]:
            self.assertTrue(metrics.in_scope(e, "full"))


class ReportTests(unittest.TestCase):
    def test_format_report_renders_without_error(self):
        text = metrics.format_report(metrics.compute_metrics(*small_graph()))
        self.assertIsInstance(text, str)
        self.assertTrue(text.strip())
        self.assertNotIn("LAYERS", text)

    def test_the_full_scope_report_shows_the_layers(self):
        g = layered_graph()
        m = metrics.compute_metrics(*g)
        m.update(scope="full", build_id="kg_x", layers=metrics.layer_metrics(*g))
        text = metrics.format_report(m)
        self.assertIn("scope: full   build: kg_x", text)
        self.assertIn("LAYERS", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
