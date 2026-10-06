"""kg/metrics.py: graph statistics and integrity checks. The dense-DAG test pins
a rewrite: _longest_chain once enumerated every simple path (exponential in the
branching), surviving only because partOfSeries is near-tree-shaped; it is a
memoised DAG longest path now, O(V + E)."""
import time
from pathlib import Path

import pytest

from modules.kg import metrics


def node(nid, kind, label=None, category=None, status=None):
    return {"id": nid, "kind": kind, "label": label, "category": category, "status": status}


def edge(src, etype, dst):
    return {"src": src, "type": etype, "dst": dst}


def small_graph():
    """Three frames in a series chain ending at an unscraped stub."""
    nodes = {n["id"]: n for n in [
        node("f1", "frame", "One", "meme", "confirmed"), node("f2", "frame", "Two", "meme", "confirmed"),
        node("f3", "frame", "Three", "subculture", "submission"), node("s1", "frame_stub", None, "meme", None),
        node("type:meme", "entry_type_concept", "meme"), node("tag:doge", "tag_concept", "doge"),
        node("e1", "external_ref")]}
    edges = [edge("f1", "hasEntryType", "type:meme"), edge("f1", "hasTag", "tag:doge"), edge("f2", "hasTag", "tag:doge"),
             edge("f1", "partOfSeries", "f2"), edge("f2", "partOfSeries", "f3"), edge("f3", "partOfSeries", "s1"),
             edge("f1", "citesExternal", "e1")]
    return nodes, edges


def layered_graph():
    """small_graph plus the 6.x layers: f1 has a title link, a template that
    shows an item and two events; f2 a template that shows nothing; f3 nothing.
    wd:Q2 is reached from text AND an image, wd:Q3 from an image only."""
    nodes, edges = small_graph()
    nodes.update({n["id"]: n for n in [
        node("wd:Q1", "wikidata_entity", "one"), node("wd:Q2", "wikidata_entity", "two"),
        node("wd:Q3", "wikidata_entity", "three"), node("t1", "template", "T one"), node("t2", "template", "T two"),
        node("ev1", "event"), node("ev2", "event")]})
    edges += [edge("f1", "fromTitle", "wd:Q1"), edge("f1", "fromAbout", "wd:Q2"), edge("f2", "fromTags", "wd:Q2"),
              edge("f1", "hasTemplate", "t1"), edge("f2", "hasTemplate", "t1"), edge("f2", "hasTemplate", "t2"),
              edge("t1", "fromImage", "wd:Q2"), edge("t1", "fromImage", "wd:Q3"), edge("f1", "hasEvent", "ev1"),
              edge("f1", "hasEvent", "ev2")]
    return nodes, edges


def test_the_module_is_pure_stdlib():
    # requirements.txt credited numpy to it for months while semantics.py, which needs it, went undeclared
    source = Path(metrics.__file__).read_text()
    assert "import numpy" not in source and "import scipy" not in source


# -- chains and cycles ------------------------------------------------------------------

@pytest.mark.parametrize("adj, roots, depth, witness", [
    ({"a": ["b"], "b": ["c"], "c": ["d"]}, ["a"], 3, ["a", "b", "c", "d"]),
    ({}, [], 0, []), ({}, ["solo"], 0, ["solo"]),
    ({"x": ["m"], "y": ["n"], "n": ["m"], "m": ["top"]}, ["x", "y"], 3, None),   # the deeper branch, from y
    ({"a": ["b", "c"], "b": ["d"], "c": ["e"], "e": ["d"]}, ["a"], 3, None),     # a diamond's long side
])
def test_longest_chain(adj, roots, depth, witness):
    got_depth, got_witness = metrics._longest_chain(adj, roots)
    assert got_depth == depth and (witness is None or got_witness == witness)


def test_the_deeper_of_two_branches_is_the_witness():
    assert metrics._longest_chain({"x": ["m"], "y": ["n"], "n": ["m"], "m": ["top"]}, ["x", "y"])[1][0] == "y"


def test_a_dense_dag_terminates():
    # 8 layers x 14 nodes fully connected: path enumeration would visit 14**7 ~ 1e8 paths
    layers, width = 8, 14
    adj = {f"l{i}n{j}": [f"l{i + 1}n{k}" for k in range(width)] for i in range(layers - 1) for j in range(width)}
    started = time.perf_counter()
    depth, witness = metrics._longest_chain(adj, [f"l0n{j}" for j in range(width)])
    assert (depth, len(witness)) == (layers - 1, layers)
    assert time.perf_counter() - started < 2.0, "longest-chain went superlinear again"


def test_cycles_are_found_and_make_depth_undefined():
    assert metrics._find_cycles({"a": ["b"], "b": ["c"], "c": ["a"]}, ["a", "b", "c"])
    assert metrics._find_cycles({"a": ["b"]}, ["a", "b"]) == []
    m = metrics.compute_metrics({n["id"]: n for n in [node("f1", "frame"), node("f2", "frame")]},
                                [edge("f1", "partOfSeries", "f2"), edge("f2", "partOfSeries", "f1")])
    assert m["integrity"]["series_cycles_found"] and m["integrity"]["series_chain_max_depth"] == -1


# -- replication and integrity ------------------------------------------------------------

def test_replication_counts():
    rep = metrics.compute_metrics(*small_graph())["replication"]
    assert (rep["nodes"], rep["edges"], rep["frames"], rep["rel_types"]) == (7, 7, 3, 4)
    assert (rep["nodes_by_kind"]["frame"], rep["nodes_by_kind"]["frame_stub"]) == (3, 1)
    assert rep["counting_mode"] == "property_graph"                      # the default
    te = metrics.compute_metrics(*small_graph(), triple_equivalent=True)["replication"]
    assert te["counting_mode"] == "triple_equivalent" and te["edges"] > rep["edges"]   # attributes count too


def test_integrity_of_a_clean_graph():
    i = metrics.compute_metrics(*small_graph())["integrity"]
    # f2, f3 and s1 are parents; only s1 is not a scraped frame
    assert (i["series_parents"], i["unresolved_series_parents"], i["unresolved_series_parent_pct"],
            i["unresolved_series_parents_sample"]) == (3, 1, 33.33, ["s1"])
    assert (i["series_chain_max_depth"], i["frames_without_entry_type"], i["frames_without_tags"]) == (3, 2, 1)
    assert (i["dangling_edge_targets"], i["duplicate_edges"], i["self_loops"]) == (0, 0, 0)
    # a stub carries a guessed category under a frame's key: frames only
    assert i["category_distribution"] == {"meme": 2, "subculture": 1}


def test_a_stub_that_is_only_linked_is_not_an_unresolved_parent():
    # until 2026-10-05 every stub counted: 9,576 in 6.5.0, of which 1,061 were parents
    nodes, edges = small_graph()
    nodes["s2"] = node("s2", "frame_stub", None, "meme", None)
    i = metrics.compute_metrics(nodes, edges + [edge("f1", "citesMediaFrame", "s2")])["integrity"]
    assert (i["unresolved_series_parents"], i["frame_stubs"]) == (1, 2)


@pytest.mark.parametrize("nodes, edges, key, n", [
    ([node("f1", "frame")], [edge("f1", "hasTag", "tag:ghost")], "dangling_edge_targets", 1),
    ([node("f1", "frame"), node("f2", "frame")], [edge("f1", "citesMediaFrame", "f2")] * 2, "duplicate_edges", 1),
    ([node("f1", "frame")], [edge("f1", "citesMediaFrame", "f1")], "self_loops", 1),
    ([node("f1", "frame"), node("lonely", "frame")], [], "isolated_nodes", 2),
])
def test_defects_are_reported(nodes, edges, key, n):
    assert metrics.compute_metrics({x["id"]: x for x in nodes}, [dict(e) for e in edges])["integrity"][key] == n


# -- layers ----------------------------------------------------------------------------------

def test_layer_coverage():
    lay = metrics.layer_metrics(*layered_graph())
    assert {k: lay[k] for k in (
        "frames", "frames_with_entity_link", "frames_with_entity_link_by_field", "frames_with_template",
        "frames_with_template_entity", "frames_with_events", "frames_with_all_three", "frames_with_none",
        "events_per_frame_mean", "templates_per_frame_mean", "wikidata_entities", "entities_from_text_only",
        "entities_from_images_only", "entities_from_both", "templates", "templates_with_entity",
        "templates_shared_by_frames")} == {
        "frames": 3, "frames_with_entity_link": 2,
        "frames_with_entity_link_by_field": {"fromTitle": 1, "fromTags": 1, "fromAbout": 1},
        "frames_with_template": 2, "frames_with_template_entity": 2,       # both keep t1
        "frames_with_events": 1, "frames_with_all_three": 1, "frames_with_none": 1,   # f3
        "events_per_frame_mean": 2.0, "templates_per_frame_mean": 1.5, "wikidata_entities": 3,
        "entities_from_text_only": 1, "entities_from_images_only": 1, "entities_from_both": 1,
        "templates": 2, "templates_with_entity": 1, "templates_shared_by_frames": 1}
    assert lay["top_entities_by_frames"][0] == {"id": "wd:Q2", "label": "two", "frames": 2}


def test_a_frames_own_image_and_statements():
    nodes, edges = layered_graph()
    nodes["wd:Q5"] = node("wd:Q5", "wikidata_entity", "human")
    lay = metrics.layer_metrics(nodes, edges + [
        edge("f3", "fromImage", "wd:Q3"), edge("f3", "fromImage", "wd:Q1"), edge("wd:Q2", "P31", "wd:Q5"),
        edge("wd:Q3", "P31", "wd:Q5"), edge("wd:Q3", "P21", "wd:Q1")])
    assert (lay["frames_with_image_entity"], lay["templates_with_entity"]) == (1, 1)     # f3; still t1 only
    assert lay["top_entities_by_templates"][0]["templates"] == 1
    assert lay["top_entities_by_frame_images"][0] == {"id": "wd:Q1", "label": "one", "frames": 1}
    assert (lay["entities_from_text_only"], lay["entities_from_images_only"], lay["entities_from_both"]) == (0, 1, 2)
    assert (lay["wikidata_statements"], lay["wikidata_statement_properties"],
            lay["wikidata_items_with_statements"]) == (3, 2, 2)
    assert lay["top_statement_properties"][0] == {"property": "P31", "statements": 2}


def test_a_core_graph_has_no_layers():
    lay = metrics.layer_metrics(*small_graph())
    assert (lay["frames_with_entity_link"], lay["templates"], lay["frames_with_events"], lay["frames_with_none"]) == \
        (0, 0, 0, 3)


# -- scope ------------------------------------------------------------------------------------

@pytest.mark.parametrize("e, core", [
    (edge("type:a", "subTypeOf", "type:b"), True), (edge("origin:a", "subTypeOf", "origin:b"), False),
    (edge("f1", "fromTitle", "wd:Q1"), False), (edge("f1", "hasTemplate", "t1"), False),
])
def test_the_core_scope_drops_the_origin_hierarchy_and_the_new_layers(e, core):
    assert metrics.in_scope(e, "core") is core and metrics.in_scope(e, "full")


def test_core_counts_a_node_only_through_a_core_edge():
    nodes, edges = small_graph()
    nodes["https://imgflip.com/meme/1"] = node("https://imgflip.com/meme/1", "external_ref")
    nodes["f4"] = node("f4", "frame", "Lonely")
    core = metrics.restrict_to_scope(nodes, edges, "core")
    assert "https://imgflip.com/meme/1" not in core                         # a template's page
    assert {"e1", "f4"} <= set(core)                                        # cited by a frame; frames always
    assert metrics.compute_metrics(core, edges)["integrity"]["isolated_nodes"] == 1
    assert len(metrics.restrict_to_scope(nodes, edges, "full")) == len(nodes)
    assert all(metrics.in_scope(e, "full") for e in layered_graph()[1])


def test_the_report():
    text = metrics.format_report(metrics.compute_metrics(*small_graph()))
    assert isinstance(text, str) and text.strip() and "LAYERS" not in text
    g = layered_graph()
    m = {**metrics.compute_metrics(*g), "scope": "full", "build_id": "kg_x", "layers": metrics.layer_metrics(*g)}
    text = metrics.format_report(m)
    assert "scope: full   build: kg_x" in text and "LAYERS" in text
