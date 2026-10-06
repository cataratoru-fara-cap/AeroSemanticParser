"""kg/loaders.py: Neo4j and Fuseki against stubs (no real server, the house
rule). The Neo4j driver records every Cypher statement and its parameters; the
Fuseki session records HTTP calls. Pinned is the contract: uid/build_id
tagging, batching, the vocabulary as relationship types, the pointer, and that
prune never deletes the published generation."""
import os
from unittest import mock

import pytest

from modules.kg import loaders as L
from modules.kg.build import EDGE_TYPES
from modules.kg.cooccurs import COOCCURS_EDGE_TYPES
from modules.kg.siblings import SIBLING_EDGE_TYPES
from modules.kg.taxonomy import CONCEPT_EDGE_TYPES

BUILD = "kg_20260916T134720Z_manual"
F1, F2 = "https://knowyourmeme.com/memes/doge", "https://knowyourmeme.com/memes/cheems"
CFG = L.Neo4jConfig(password="x", batch=2)
FCFG = L.FusekiConfig(base_url="http://fuseki:3030", password="pw")


class _Record(dict):
    def data(self):
        return dict(self)


class StubDriver:
    def __init__(self, responses=None):
        self.calls, self.responses = [], list(responses or [])

    def session(self, database=None):
        self.database = database
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def run(self, cypher, **params):
        self.calls.append((" ".join(cypher.split()), params))
        return [_Record(d) for d in (self.responses.pop(0) if self.responses else [])]

    def matching(self, text):
        return [c for c in self.calls if text in c[0]]


def node(nid, kind, label=None):
    return {"id": nid, "kind": kind, "label": label, "category": "meme", "status": None}


def load(nodes=(), edges=(), driver=None):
    d = driver or StubDriver()
    return d, L.neo4j_load(d, CFG, BUILD, list(nodes), list(edges))


# -- labels and properties (generic: a new layer needs no loader change) ----------

@pytest.mark.parametrize("kind, label", [("frame_stub", "FrameStub"), ("entry_type_concept", "EntryTypeConcept"),
                                         ("frame", "Frame"), ("event", "Event"), ("wikidata_entity", "WikidataEntity")])
def test_kinds_become_pascal_case_labels(kind, label):
    assert L.label_for_kind(kind) == label


def test_node_properties_keep_every_value_and_no_null():
    props = L.node_properties({"id": "event:x-y", "kind": "event", "summary": "s", "date": "2010",
                               "date_precision": "year", "locations": [], "actors": ["Atsuko Sato"],
                               "certainty": "confirmed"})
    # `date` has no RDF triple but is a property-graph property
    assert (props["date"], props["actors"]) == ("2010", ["Atsuko Sato"]) and "locations" not in props
    d, _ = load([{"id": F1, "kind": "frame", "label": "Doge", "about": "text", "badges": ["Sensitive"], "year": 2013,
                  "status": None, "aliases": [], "_id": "x", "build_id": "y", "node_id": F1}])
    assert d.calls[0][1]["rows"][0]["props"] == {"id": F1, "kind": "frame", "label": "Doge", "about": "text",
                                                 "badges": ["Sensitive"], "year": 2013}


@pytest.mark.parametrize("occurrences, props", [
    # Neo4j cannot store a list of maps: position i of every list is one mention,
    # "" / -1 where it lacks the field, and a float stand-in for a float list
    ([{"anchor_text": "wiki", "in_section": "About"}, {"citation_text": "Doge", "citation_index": 3}],
     {"occurrence_count": 2, "anchor_texts": ["wiki", ""], "in_sections": ["About", ""],
      "citation_texts": ["", "Doge"], "citation_indexes": [-1, 3]}),
    ([{"mention_text": "Shiba Inus", "link_score": 0.83, "link_method": "ner", "ner_label": "ORG"},
      {"mention_text": "Shiba Inu", "link_method": "propn"}],
     {"occurrence_count": 2, "mention_texts": ["Shiba Inus", "Shiba Inu"], "link_scores": [0.83, -1.0],
      "link_methods": ["ner", "propn"], "ner_labels": ["ORG", ""]}),
    (None, {}),
])
def test_occurrences_become_index_aligned_relationship_lists(occurrences, props):
    edge = {"src": F1, "type": "citesExternal", "dst": F2}
    assert L.edge_properties({**edge, "occurrences": occurrences} if occurrences else edge) == props


# -- loading -----------------------------------------------------------------------

def test_nodes_are_tagged_with_uid_and_build_id_one_statement_per_kind_in_batches():
    d, counts = load([node(F1, "frame", "Doge")] + [node(f"u{i}", "frame") for i in range(4)]
                     + [node("type:meme", "entry_type_concept", "meme")])
    cypher, params = d.calls[0]
    assert "MERGE (n:KGNode:`Frame` {uid: r.uid})" in cypher and "SET n += r.props, n.build_id = $bid" in cypher
    assert (params["bid"], params["rows"][0]["uid"], params["rows"][0]["props"]["label"]) == \
        (BUILD, f"{BUILD}|{F1}", "Doge")
    assert counts["nodes"] == 6
    assert len(d.matching("`Frame`")) == 3 and d.matching("`EntryTypeConcept`")      # 5 rows / batch 2


def test_edges_use_the_vocabulary_as_relationship_types():
    d, _ = load(edges=[{"src": F1, "type": "citesMediaFrame", "dst": F2},
                       {"src": "type:model", "type": "subTypeOf", "dst": "type:influencer"},
                       {"src": F1, "type": "hasImage", "dst": F2, "occurrences": [{"role": "page"}]}])
    assert {c[0].split("[e:`")[1].split("`")[0] for c in d.calls if "[e:`" in c[0]} == \
        {"citesMediaFrame", "subTypeOf", "hasImage"}
    [(cypher, params)] = d.matching("citesMediaFrame")
    assert params["rows"][0] == {"suid": f"{BUILD}|{F1}", "duid": f"{BUILD}|{F2}", "props": {}}
    assert "SET e += r.props" in cypher
    [(_, params)] = d.matching("hasImage")
    assert (params["rows"][0]["props"], params["bid"]) == ({"occurrence_count": 1, "roles": ["page"]}, BUILD)


def test_every_vocabulary_type_loads_and_cooccurs_with_takes_any_prefix():
    edges = [{"src": F1, "type": t, "dst": F2}
             for t in set(EDGE_TYPES) | set(CONCEPT_EDGE_TYPES) | set(COOCCURS_EDGE_TYPES) | set(SIBLING_EDGE_TYPES)]
    assert load(edges=edges)[1]["edges"] == len(edges)
    # the RDF path's tag: exclusion is not a loader rule
    d, counts = load(edges=[{"src": "type:streamer", "type": "coOccursWith", "dst": "type:creator"},
                            {"src": "tag:meme", "type": "coOccursWith", "dst": "tag:dank-meme"}])
    rows = [r for c in d.matching("coOccursWith") for r in c[1]["rows"]]
    assert counts["edges"] == 2
    assert {r["suid"] for r in rows} | {r["duid"] for r in rows} == {
        f"{BUILD}|type:streamer", f"{BUILD}|type:creator", f"{BUILD}|tag:meme", f"{BUILD}|tag:dank-meme"}


def test_a_wikidata_statement_is_a_relationship_named_by_its_property():
    # 7.1.0: the paper's (person)-[:P31]->(:Q5) reads as Cypher
    d, _ = load(edges=[{"src": "wd:Q22686", "type": "P31", "dst": "wd:Q5"}])
    [(cypher, _)] = d.matching("`P31`")
    assert "MERGE (a)-[e:`P31`" in cypher


@pytest.mark.parametrize("nodes, edges", [
    ([node("x", "mystery")], []),
    *[([], [{"src": F1, "type": bad, "dst": F2}])        # spliced into Cypher: only the exact shape passes
      for bad in ("knows", "P31`]->() DETACH DELETE (a", "P0", "Px", "p31")],
])
def test_an_unknown_kind_or_type_is_refused(nodes, edges):
    with pytest.raises(L.LoaderError):
        load(nodes, edges)


def test_the_schema_is_community_safe():
    d = StubDriver()
    L.neo4j_ensure_schema(d, CFG)
    joined = " ".join(c[0] for c in d.calls)
    assert "REQUIRE n.uid IS UNIQUE" in joined and "IF NOT EXISTS" in joined
    assert "NODE KEY" not in joined and d.database == "neo4j"      # NODE KEY is Enterprise-only


# -- the Neo4j pointer and prune ------------------------------------------------------

def test_publish_returns_the_previous_and_sets_the_pointer():
    d = StubDriver(responses=[[{"bid": "kg_old"}]])
    assert L.neo4j_publish(d, CFG, BUILD) == "kg_old"
    cypher, params = d.calls[-1]
    assert "MERGE (p:KGPointer {name: 'current'})" in cypher and params["bid"] == BUILD
    assert L.neo4j_current(StubDriver(responses=[[]]), CFG) is None


def test_prune_never_deletes_the_published_generation():
    d = StubDriver(responses=[[{"bid": BUILD}], [{"bid": "kg_old"}], [{"n": 7}], [{"n": 3}]])
    out = L.neo4j_prune(d, CFG, keep=["kg_other"])
    [(_, params)] = d.matching("RETURN DISTINCT n.build_id")
    assert {BUILD, "kg_other"} <= set(params["keep"])          # the published one is added
    assert out == {"generations_pruned": 1, "nodes_deleted": 3, "edges_deleted": 7}


def test_prune_deletes_relationships_before_nodes_in_small_batches():
    # DETACH DELETE per node batch sized transactions by degree: the first real
    # prune ran Neo4j out of transaction memory
    d = StubDriver(responses=[[], [{"bid": "kg_old"}], [{"n": 0}], [{"n": 0}]])
    L.neo4j_prune(d, CFG, keep=[])
    deletes = d.matching("IN TRANSACTIONS")
    assert len(deletes) == 2 and "-[r]->()" in deletes[0][0] and "DELETE r" in deletes[0][0]
    assert "DETACH DELETE n" in deletes[1][0]
    assert all(f"IN TRANSACTIONS OF {L.PRUNE_BATCH} ROWS" in c and p["bid"] == "kg_old" for c, p in deletes)
    assert L.PRUNE_BATCH <= 5_000


# -- Fuseki -----------------------------------------------------------------------------

class _Resp:
    def __init__(self, status=200, text="", payload=None):
        self.status_code, self.text, self._payload = status, text, payload

    def json(self):
        return self._payload


class StubHttp:
    def __init__(self, responses=None):
        self.calls, self.responses = [], list(responses or [])

    def _take(self, method, url, kw):
        body = kw.get("data")
        if hasattr(body, "read"):                    # a streamed file: record its size
            kw = {**kw, "data": f"<stream {os.fstat(body.fileno()).st_size}b>"}
        self.calls.append((method, url, kw))
        return self.responses.pop(0) if self.responses else _Resp(200)

    def put(self, url, **kw):
        return self._take("PUT", url, kw)

    def post(self, url, **kw):
        return self._take("POST", url, kw)

    def get(self, url, **kw):
        return self._take("GET", url, kw)

    def delete(self, url, **kw):
        return self._take("DELETE", url, kw)


@pytest.fixture
def graph_nt(tmp_path):
    p = tmp_path / "graph.nt"
    p.write_text("".join(f"<u{i}> <p> <o> .\n" for i in range(3)))
    return str(p)


def bindings(var, values):
    return _Resp(200, payload={"results": {"bindings": [{var: {"value": v}} for v in values]}})


def test_load_streams_into_the_build_graph_and_publish_into_the_default(graph_nt):
    h = StubHttp()
    out = L.fuseki_load(h, FCFG, BUILD, graph_nt)
    method, url, kw = h.calls[0]
    assert (method, url, kw["params"], kw["headers"]["Content-Type"], kw["auth"]) == \
        ("PUT", "http://fuseki:3030/kg/data", {"graph": f"urn:memeatlas:build:{BUILD}"}, "application/n-triples",
         ("admin", "pw"))
    assert kw["data"].startswith("<stream") and out["graph"] == L.fuseki_graph_iri(BUILD)
    h = StubHttp()
    L.fuseki_publish(h, FCFG, BUILD, graph_nt)
    assert h.calls[0][2]["params"] == {"default": ""}
    with pytest.raises(L.LoaderError, match="401"):
        L.fuseki_load(StubHttp(responses=[_Resp(401, "Unauthorized")]), FCFG, BUILD, graph_nt)


def test_the_ontology_goes_into_its_own_named_graph_as_turtle(tmp_path):
    ttl = tmp_path / "memeatlas.ttl"
    ttl.write_text("@prefix mk: <https://meme4.science/atlas/> .\n")
    h = StubHttp()
    out = L.fuseki_load_ontology(h, FCFG, str(ttl))
    _, url, kw = h.calls[0]
    assert (url, kw["params"], kw["headers"]["Content-Type"], out["graph"]) == \
        ("http://fuseki:3030/kg/data", {"graph": "urn:memeatlas:ontology"}, "text/turtle", L.ONTOLOGY_GRAPH)


def test_current_and_counts_read_by_query():
    h = StubHttp(responses=[bindings("b", [BUILD])])
    assert L.fuseki_current(h, FCFG) == BUILD
    method, url, kw = h.calls[0]
    assert (method, url) == ("POST", "http://fuseki:3030/kg/query")
    assert "currentBuild" in kw["data"]["query"] and "buildId" in kw["data"]["query"]
    assert L.fuseki_current(StubHttp(responses=[bindings("b", [])]), FCFG) is None
    h = StubHttp(responses=[bindings("n", ["808687"]), bindings("n", ["5"])])
    assert (L.fuseki_count(h, FCFG), L.fuseki_count(h, FCFG, "urn:memeatlas:build:x")) == (808687, 5)
    assert "GRAPH <urn:memeatlas:build:x>" in h.calls[1][2]["data"]["query"]


def test_fuseki_prune_deletes_only_stale_build_graphs():
    graphs = [f"urn:memeatlas:build:{b}" for b in (BUILD, "kg_old", "kg_keep")]
    h = StubHttp(responses=[bindings("b", [BUILD]), bindings("g", graphs + [L.ONTOLOGY_GRAPH])])
    assert L.fuseki_prune(h, FCFG, keep=["kg_keep"]) == {"graphs_pruned": 1, "pruned": ["kg_old"]}
    assert [c[2]["params"]["graph"] for c in h.calls if c[0] == "DELETE"] == ["urn:memeatlas:build:kg_old"]


# gap 04: TDB2 keeps replaced and deleted triples on disk until compacted
def compact(responses, **kw):
    h, naps, ticks = StubHttp(responses=responses), [], iter(range(0, 10_000, 5))
    return h, naps, L.fuseki_compact(h, FCFG, poll_s=5, sleep=naps.append, clock=lambda: next(ticks), **kw)


def test_compaction_deletes_the_old_generation_and_waits_for_the_task():
    started = {"task": "Compact", "taskId": "7", "started": "2026-09-30T16:00:00Z"}
    done = _Resp(200, payload={**started, "finished": "2026-09-30T16:03:10Z", "success": True})
    h, naps, out = compact([_Resp(202, payload={"taskId": "7", "requestId": 3}), _Resp(200, payload=started),
                            _Resp(200, payload=started), done])
    method, url, kw = h.calls[0]
    assert (method, url, kw["params"], kw["auth"]) == \
        ("POST", "http://fuseki:3030/$/compact/kg", {"deleteOld": "true"}, ("admin", "pw"))
    assert [c[1] for c in h.calls[1:]] == ["http://fuseki:3030/$/tasks/7"] * 3 and naps == [5, 5]
    assert (out["task_id"], out["delete_old"], out["finished"]) == ("7", True, "2026-09-30T16:03:10Z")


@pytest.mark.parametrize("responses, kw, message", [
    ([_Resp(202, payload={"taskId": "8"}), _Resp(200, payload={"taskId": "8", "finished": "x", "success": False})],
     {}, "failed"),
    ([_Resp(202, payload={"taskId": "9"})] + [_Resp(200, payload={"taskId": "9", "started": "x"})] * 10,
     {"timeout_s": 12}, "still running"),
    ([_Resp(403, "Forbidden")], {}, None),
])
def test_a_compaction_that_fails_refuses_or_hangs_is_an_error(responses, kw, message):
    with pytest.raises(L.LoaderError, match=message):
        compact(responses, **kw)


def test_config():
    with mock.patch.dict(os.environ, {}, clear=True), pytest.raises(L.LoaderError):
        L.Neo4jConfig.from_env()                          # a password is required
    c = L.FusekiConfig(base_url="http://h:3030/", dataset="kg")
    assert (c.data_url, c.query_url, c.auth) == ("http://h:3030/kg/data", "http://h:3030/kg/query", None)
