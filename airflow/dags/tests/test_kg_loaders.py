"""Tests for kg/loaders.py — Neo4j and Fuseki, against stubs.

No test here touches a real server (the house rule). The Neo4j driver is a
stub that records every Cypher statement and its parameters; the Fuseki
"session" is a stub recording HTTP calls. What is asserted is the CONTRACT:
uid/build_id tagging, batching, the vocabulary as relationship types, the
pointer, and that prune never deletes the published generation.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_loaders.py -v
"""
import os
import tempfile
import unittest

from modules.kg import loaders as L
from modules.kg.build import EDGE_TYPES
from modules.kg.cooccurs import COOCCURS_EDGE_TYPES
from modules.kg.taxonomy import CONCEPT_EDGE_TYPES

BUILD = "kg_20260916T134720Z_manual"
F1 = "https://knowyourmeme.com/memes/doge"
F2 = "https://knowyourmeme.com/memes/cheems"


# --- Neo4j stubs -------------------------------------------------------------

class _Record:
    def __init__(self, d): self._d = d
    def data(self): return self._d
    def __getitem__(self, k): return self._d[k]


class _Session:
    def __init__(self, driver): self.driver = driver
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def run(self, cypher, **params):
        self.driver.calls.append((" ".join(cypher.split()), params))
        return [_Record(d) for d in (self.driver.responses.pop(0)
                                     if self.driver.responses else [])]


class StubDriver:
    def __init__(self, responses=None):
        self.calls: list[tuple[str, dict]] = []
        self.responses: list[list[dict]] = list(responses or [])
    def session(self, database=None):
        self.database = database
        return _Session(self)


CFG = L.Neo4jConfig(password="x", batch=2)


def node(nid, kind, label=None):
    return {"id": nid, "kind": kind, "label": label, "category": "meme",
            "status": None}


class LabelTests(unittest.TestCase):
    def test_kinds_become_pascal_case_labels(self):
        self.assertEqual(L.label_for_kind("frame_stub"), "FrameStub")
        self.assertEqual(L.label_for_kind("entry_type_concept"), "EntryTypeConcept")
        self.assertEqual(L.label_for_kind("frame"), "Frame")


class Neo4jLoadTests(unittest.TestCase):
    def test_nodes_are_tagged_with_uid_and_build_id(self):
        d = StubDriver()
        L.neo4j_load(d, CFG, BUILD, [node(F1, "frame", "Doge")], [])
        cypher, params = d.calls[0]
        self.assertIn("MERGE (n:KGNode:`Frame` {uid: r.uid})", cypher)
        self.assertEqual(params["bid"], BUILD)
        self.assertIn("SET n += r.props, n.build_id = $bid", cypher)
        self.assertEqual(params["rows"][0]["uid"], f"{BUILD}|{F1}")
        self.assertEqual(params["rows"][0]["props"]["label"], "Doge")

    def test_every_parsed_property_reaches_neo4j_and_nulls_do_not(self):
        d = StubDriver()
        n = {"id": F1, "kind": "frame", "label": "Doge", "about": "text",
             "badges": ["Sensitive"], "year": 2013, "status": None,
             "aliases": [], "_id": "x", "build_id": "y", "node_id": F1}
        L.neo4j_load(d, CFG, BUILD, [n], [])
        props = d.calls[0][1]["rows"][0]["props"]
        self.assertEqual(props, {"id": F1, "kind": "frame", "label": "Doge",
                                 "about": "text", "badges": ["Sensitive"],
                                 "year": 2013})

    def test_one_statement_per_kind_and_batches_respected(self):
        d = StubDriver()
        nodes = [node(f"u{i}", "frame") for i in range(5)] + \
                [node("type:meme", "entry_type_concept", "meme")]
        counts = L.neo4j_load(d, CFG, BUILD, nodes, [])
        self.assertEqual(counts["nodes"], 6)
        frame_calls = [c for c in d.calls if "`Frame`" in c[0]]
        self.assertEqual(len(frame_calls), 3)          # 5 rows / batch 2
        self.assertTrue(any("`EntryTypeConcept`" in c[0] for c in d.calls))

    def test_edges_use_the_vocabulary_as_relationship_types(self):
        d = StubDriver()
        L.neo4j_load(d, CFG, BUILD, [],
                     [{"src": F1, "type": "relatesToMeme", "dst": F2},
                      {"src": "type:model", "type": "subTypeOf", "dst": "type:influencer"}])
        types = {c[0].split("[e:`")[1].split("`")[0] for c in d.calls if "[e:`" in c[0]}
        self.assertEqual(types, {"relatesToMeme", "subTypeOf"})
        cypher, params = [c for c in d.calls if "relatesToMeme" in c[0]][0]
        self.assertEqual(params["rows"][0], {"suid": f"{BUILD}|{F1}",
                                             "duid": f"{BUILD}|{F2}", "props": {}})
        self.assertIn("SET e += r.props", cypher)

    def test_occurrences_become_index_aligned_relationship_lists(self):
        # Neo4j cannot store a list of maps; position i of every list is
        # the same mention, with "" / -1 where that mention lacks the field.
        props = L.edge_properties({"src": F1, "type": "citesExternal", "dst": F2,
                                   "occurrences": [
                                       {"anchor_text": "wiki", "in_section": "About"},
                                       {"citation_text": "Doge", "citation_index": 3}]})
        self.assertEqual(props, {"occurrence_count": 2,
                                 "anchor_texts": ["wiki", ""],
                                 "in_sections": ["About", ""],
                                 "citation_texts": ["", "Doge"],
                                 "citation_indexes": [-1, 3]})
        self.assertEqual(L.edge_properties({"src": F1, "type": "hasTag", "dst": F2}), {})

    def test_edge_properties_reach_the_statement(self):
        d = StubDriver()
        L.neo4j_load(d, CFG, BUILD, [], [{"src": F1, "type": "hasImage", "dst": F2,
                                          "occurrences": [{"role": "page"}]}])
        _, params = [c for c in d.calls if "hasImage" in c[0]][0]
        self.assertEqual(params["rows"][0]["props"], {"occurrence_count": 1, "roles": ["page"]})
        self.assertEqual(params["bid"], BUILD)

    def test_every_vocabulary_type_is_accepted(self):
        d = StubDriver()
        edges = [{"src": F1, "type": t, "dst": F2}
                 for t in set(EDGE_TYPES) | set(CONCEPT_EDGE_TYPES) | set(COOCCURS_EDGE_TYPES)]
        self.assertEqual(L.neo4j_load(d, CFG, BUILD, [], edges)["edges"], len(edges))

    def test_cooccurs_with_loads_for_both_type_and_tag_prefixed_uids(self):
        # No RDF restriction in the property graph -- kg/rdf.py's tag:
        # exclusion is an RDF-path-only guard, not a loader-level one.
        d = StubDriver()
        edges = [{"src": "type:streamer", "type": "coOccursWith", "dst": "type:creator"},
                {"src": "tag:meme", "type": "coOccursWith", "dst": "tag:dank-meme"}]
        counts = L.neo4j_load(d, CFG, BUILD, [], edges)
        self.assertEqual(counts["edges"], 2)
        cooccurs_calls = [c for c in d.calls if "coOccursWith" in c[0]]
        uids = {r["suid"] for c in cooccurs_calls for r in c[1]["rows"]} |               {r["duid"] for c in cooccurs_calls for r in c[1]["rows"]}
        self.assertEqual(uids, {f"{BUILD}|type:streamer", f"{BUILD}|type:creator",
                               f"{BUILD}|tag:meme", f"{BUILD}|tag:dank-meme"})

    def test_unknown_kind_or_type_is_refused(self):
        with self.assertRaises(L.LoaderError):
            L.neo4j_load(StubDriver(), CFG, BUILD, [node("x", "mystery")], [])
        with self.assertRaises(L.LoaderError):
            L.neo4j_load(StubDriver(), CFG, BUILD, [],
                         [{"src": F1, "type": "knows", "dst": F2}])

    def test_schema_is_community_safe(self):
        d = StubDriver()
        L.neo4j_ensure_schema(d, CFG)
        joined = " ".join(c[0] for c in d.calls)
        self.assertIn("REQUIRE n.uid IS UNIQUE", joined)
        self.assertIn("IF NOT EXISTS", joined)
        self.assertNotIn("NODE KEY", joined)            # Enterprise-only
        self.assertEqual(d.database, "neo4j")


class Neo4jPointerTests(unittest.TestCase):
    def test_publish_returns_previous_and_sets_pointer(self):
        d = StubDriver(responses=[[{"bid": "kg_old"}]])
        prev = L.neo4j_publish(d, CFG, BUILD)
        self.assertEqual(prev, "kg_old")
        cypher, params = d.calls[-1]
        self.assertIn("MERGE (p:KGPointer {name: 'current'})", cypher)
        self.assertEqual(params["bid"], BUILD)

    def test_current_is_none_before_first_publish(self):
        self.assertIsNone(L.neo4j_current(StubDriver(responses=[[]]), CFG))

    def test_prune_never_deletes_the_published_generation(self):
        # current lookup -> published; doomed lookup -> only the old one
        d = StubDriver(responses=[[{"bid": BUILD}], [{"bid": "kg_old"}],
                                  [{"n": 7}], [{"n": 3}]])
        out = L.neo4j_prune(d, CFG, keep=["kg_other"])
        doomed_call = [c for c in d.calls if "RETURN DISTINCT n.build_id" in c[0]][0]
        self.assertIn(BUILD, doomed_call[1]["keep"])       # published added to keep
        self.assertIn("kg_other", doomed_call[1]["keep"])
        self.assertEqual(out, {"generations_pruned": 1, "nodes_deleted": 3,
                               "edges_deleted": 7})

    def test_prune_deletes_relationships_before_nodes_in_small_batches(self):
        # DETACH DELETE per node batch sized transactions by node degree and
        # ran Neo4j out of transaction memory on the first real prune.
        d = StubDriver(responses=[[], [{"bid": "kg_old"}], [{"n": 0}], [{"n": 0}]])
        L.neo4j_prune(d, CFG, keep=[])
        deletes = [c for c in d.calls if "IN TRANSACTIONS" in c[0]]
        self.assertEqual(len(deletes), 2)
        self.assertIn("-[r]->()", deletes[0][0])
        self.assertIn("DELETE r", deletes[0][0])
        self.assertIn("DETACH DELETE n", deletes[1][0])
        for cypher, params in deletes:
            self.assertIn(f"IN TRANSACTIONS OF {L.PRUNE_BATCH} ROWS", cypher)
            self.assertEqual(params["bid"], "kg_old")
        self.assertLessEqual(L.PRUNE_BATCH, 5_000)


# --- Fuseki stubs -------------------------------------------------------------

class _Resp:
    def __init__(self, status=200, text="", payload=None):
        self.status_code, self.text, self._payload = status, text, payload
    def json(self): return self._payload


class StubHttp:
    def __init__(self, responses=None):
        self.calls: list[tuple[str, str, dict]] = []
        self.responses = list(responses or [])
    def _take(self, method, url, kw):
        body = kw.get("data")
        if hasattr(body, "read"):                  # streamed file, record size
            kw = {**kw, "data": f"<stream {os.fstat(body.fileno()).st_size}b>"}
        self.calls.append((method, url, kw))
        return self.responses.pop(0) if self.responses else _Resp(200)
    def put(self, url, **kw): return self._take("PUT", url, kw)
    def post(self, url, **kw): return self._take("POST", url, kw)
    def delete(self, url, **kw): return self._take("DELETE", url, kw)


FCFG = L.FusekiConfig(base_url="http://fuseki:3030", password="pw")


def _nt(tmp, n=3):
    p = os.path.join(tmp, "graph.nt")
    with open(p, "w") as fh:
        for i in range(n):
            fh.write(f"<u{i}> <p> <o> .\n")
    return p


class FusekiLoadTests(unittest.TestCase):
    def test_load_puts_into_the_build_graph_streaming(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = StubHttp()
            out = L.fuseki_load(h, FCFG, BUILD, _nt(tmp))
        method, url, kw = h.calls[0]
        self.assertEqual((method, url), ("PUT", "http://fuseki:3030/kg/data"))
        self.assertEqual(kw["params"], {"graph": f"urn:memeatlas:build:{BUILD}"})
        self.assertEqual(kw["headers"]["Content-Type"], "application/n-triples")
        self.assertTrue(kw["data"].startswith("<stream"))
        self.assertEqual(kw["auth"], ("admin", "pw"))
        self.assertEqual(out["graph"], L.fuseki_graph_iri(BUILD))

    def test_publish_puts_the_same_file_into_the_default_graph(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = StubHttp()
            L.fuseki_publish(h, FCFG, BUILD, _nt(tmp))
        _, _, kw = h.calls[0]
        self.assertEqual(kw["params"], {"default": ""})

    def test_ontology_goes_into_its_own_named_graph_as_turtle(self):
        with tempfile.TemporaryDirectory() as tmp:
            ttl = os.path.join(tmp, "memeatlas.ttl")
            with open(ttl, "w") as fh:
                fh.write("@prefix mk: <https://meme4.science/atlas/> .\n")
            h = StubHttp()
            out = L.fuseki_load_ontology(h, FCFG, ttl)
        _, url, kw = h.calls[0]
        self.assertEqual(url, "http://fuseki:3030/kg/data")
        self.assertEqual(kw["params"], {"graph": "urn:memeatlas:ontology"})
        self.assertEqual(kw["headers"]["Content-Type"], "text/turtle")
        self.assertEqual(out["graph"], L.ONTOLOGY_GRAPH)

    def test_http_error_raises_loader_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = StubHttp(responses=[_Resp(401, "Unauthorized")])
            with self.assertRaises(L.LoaderError) as ctx:
                L.fuseki_load(h, FCFG, BUILD, _nt(tmp))
        self.assertIn("401", str(ctx.exception))


class FusekiReadTests(unittest.TestCase):
    def _bindings(self, var, values):
        return _Resp(200, payload={"results": {"bindings": [
            {var: {"value": v}} for v in values]}})

    def test_current_reads_the_provenance_triple(self):
        h = StubHttp(responses=[self._bindings("b", [BUILD])])
        self.assertEqual(L.fuseki_current(h, FCFG), BUILD)
        method, url, kw = h.calls[0]
        self.assertEqual((method, url), ("POST", "http://fuseki:3030/kg/query"))
        self.assertIn("currentBuild", kw["data"]["query"])
        self.assertIn("buildId", kw["data"]["query"])

    def test_current_is_none_on_an_empty_store(self):
        h = StubHttp(responses=[self._bindings("b", [])])
        self.assertIsNone(L.fuseki_current(h, FCFG))

    def test_count_default_and_named_graph(self):
        h = StubHttp(responses=[self._bindings("n", ["808687"]),
                                self._bindings("n", ["5"])])
        self.assertEqual(L.fuseki_count(h, FCFG), 808687)
        self.assertEqual(L.fuseki_count(h, FCFG, "urn:memeatlas:build:x"), 5)
        self.assertIn("GRAPH <urn:memeatlas:build:x>", h.calls[1][2]["data"]["query"])

    def test_prune_deletes_only_stale_build_graphs(self):
        graphs = [f"urn:memeatlas:build:{b}" for b in (BUILD, "kg_old", "kg_keep")]
        h = StubHttp(responses=[self._bindings("b", [BUILD]),      # current
                                self._bindings("g", graphs + [L.ONTOLOGY_GRAPH])])
        out = L.fuseki_prune(h, FCFG, keep=["kg_keep"])
        deletes = [c for c in h.calls if c[0] == "DELETE"]
        self.assertEqual([c[2]["params"]["graph"] for c in deletes],
                         ["urn:memeatlas:build:kg_old"])
        self.assertEqual(out, {"graphs_pruned": 1, "pruned": ["kg_old"]})


class ConfigTests(unittest.TestCase):
    def test_neo4j_requires_a_password(self):
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(L.LoaderError):
                L.Neo4jConfig.from_env()

    def test_fuseki_urls_tolerate_a_trailing_slash(self):
        c = L.FusekiConfig(base_url="http://h:3030/", dataset="kg")
        self.assertEqual(c.data_url, "http://h:3030/kg/data")
        self.assertEqual(c.query_url, "http://h:3030/kg/query")
        self.assertIsNone(c.auth)                       # no password -> no auth


import unittest.mock  # noqa: E402  (used above)

if __name__ == "__main__":
    unittest.main(verbosity=2)
