"""
kg/loaders.py — generational load / publish / prune for Neo4j and Fuseki
==========================================================================
Driver calls only: no Mongo, no Airflow. The DAG hands this module a
build_id and the store's node/edge iterators (or the build's graph.nt) and
it puts that generation into the two external graph stores using the SAME
scheme kg_store.py uses for Mongo — so there is one mental model for all
four stores, not three.

The scheme
----------
Every write is tagged with its build_id and lands in a namespace nothing is
reading yet; publishing is one small pointer write; readers resolve the
pointer first. Concretely:

    Neo4j    every node carries ``build_id`` and ``uid = "<build_id>|<id>"``;
             every relationship carries ``build_id``. Publishing sets
             ``(:KGPointer {name:'current'}).build_id``. Readers MATCH the
             pointer, then filter on build_id.
    Fuseki   each build is PUT into the named graph
             ``urn:memeatlas:build:<build_id>``; publishing PUTs the same
             file into the DEFAULT graph, one TDB2 write transaction, so a
             SPARQL client querying the default graph sees the old build and
             then the new one, never a mixture. The build's own provenance
             triples (rdf.py) are inside the file, so the default graph says
             which build it is.

Neo4j Community constraints, and why the design looks like this
---------------------------------------------------------------
Community Edition has exactly one user database, so "load into a
build-scoped database and swap" is not available. Node-key and composite
constraints are Enterprise too, so uniqueness is a plain constraint on the
synthetic ``uid`` string — the same trick as Mongo's ``_id``. Labels and
relationship types cannot be parameterised in Cypher, so there is one
UNWIND per node kind and per edge type; the type strings are kg/build.py's
edge vocabulary verbatim, backticked, so a Cypher pattern reads
``(a)-[:hasTag]->(b)`` with the same word the RDF predicate and the CSV
header use.

Publish order and what a crash leaves behind
--------------------------------------------
kym_kg publishes files -> Fuseki -> Neo4j -> Mongo, least authoritative
first. A crash between two of these leaves a store AHEAD of the authority,
never behind it, and every publish here is set-to-value (PUT replaces, SET
assigns), so re-running converges. This is bounded eventual agreement, not
a distributed transaction; kg_store.py's docstring says the same.

Environment
-----------
    NEO4J_URI        (default bolt://neo4j:7687)
    NEO4J_USER       (default neo4j)
    NEO4J_PASSWORD   required to connect
    FUSEKI_URL       (default http://fuseki:3030)
    FUSEKI_DATASET   (default kg)
    FUSEKI_USER / FUSEKI_PASSWORD   basic auth for writes
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

from modules.kg.build import EDGE_TYPES, NODE_KINDS
from modules.kg.rdf import PREFIXES
from modules.kg.taxonomy import CONCEPT_EDGE_TYPES

__all__ = [
    "Neo4jConfig", "FusekiConfig", "LoaderError",
    "neo4j_driver", "neo4j_ensure_schema", "neo4j_load", "neo4j_publish",
    "neo4j_current", "neo4j_counts", "neo4j_prune", "label_for_kind",
    "fuseki_graph_iri", "CURRENT_GRAPH", "fuseki_load", "fuseki_publish",
    "fuseki_count", "fuseki_current", "fuseki_prune", "fuseki_graphs",
]


class LoaderError(RuntimeError):
    """A store refused or mangled a write. Raised, not returned: a build
    that is not fully in a store must not be published as if it were."""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Neo4jConfig:
    uri: str = "bolt://neo4j:7687"
    user: str = "neo4j"
    password: str = ""
    database: str = "neo4j"           # the ONE database Community allows
    batch: int = 10_000

    @classmethod
    def from_env(cls) -> "Neo4jConfig":
        pw = os.getenv("NEO4J_PASSWORD", "")
        if not pw:
            raise LoaderError("NEO4J_PASSWORD is not set")
        return cls(uri=os.getenv("NEO4J_URI", cls.uri),
                   user=os.getenv("NEO4J_USER", cls.user), password=pw,
                   database=os.getenv("NEO4J_DATABASE", cls.database),
                   batch=int(os.getenv("NEO4J_BATCH", cls.batch)))


@dataclass(frozen=True)
class FusekiConfig:
    base_url: str = "http://fuseki:3030"
    dataset: str = "kg"
    user: str = "admin"
    password: str = ""
    timeout_s: float = 600.0          # a 120 MB PUT on a slow disk

    def __post_init__(self) -> None:
        # A trailing slash on base_url produced "http://host//kg/data". The
        # dataclass is frozen, so normalise through object.__setattr__.
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))

    @classmethod
    def from_env(cls) -> "FusekiConfig":
        return cls(base_url=os.getenv("FUSEKI_URL", cls.base_url).rstrip("/"),
                   dataset=os.getenv("FUSEKI_DATASET", cls.dataset),
                   user=os.getenv("FUSEKI_USER", cls.user),
                   password=os.getenv("FUSEKI_PASSWORD", ""),
                   timeout_s=float(os.getenv("FUSEKI_TIMEOUT_S", cls.timeout_s)))

    @property
    def data_url(self) -> str:        # Graph Store Protocol endpoint
        return f"{self.base_url}/{self.dataset}/data"

    @property
    def query_url(self) -> str:
        return f"{self.base_url}/{self.dataset}/query"

    @property
    def update_url(self) -> str:
        return f"{self.base_url}/{self.dataset}/update"

    @property
    def auth(self):
        return (self.user, self.password) if self.password else None


# ---------------------------------------------------------------------------
# Neo4j
# ---------------------------------------------------------------------------

_LABEL_RE = re.compile(r"[^A-Za-z0-9]+")


def label_for_kind(kind: str) -> str:
    """``frame_stub`` -> ``FrameStub``. Labels are identifiers, so the
    snake_case kind becomes PascalCase; every node also carries ``KGNode``."""
    return "".join(p.capitalize() for p in _LABEL_RE.split(kind) if p)


def neo4j_driver(cfg: Neo4jConfig):
    from neo4j import GraphDatabase     # lazy: importing this module must
    return GraphDatabase.driver(cfg.uri, auth=(cfg.user, cfg.password))


def _run(driver, cfg: Neo4jConfig, cypher: str, **params) -> list[dict]:
    with driver.session(database=cfg.database) as session:
        return [r.data() for r in session.run(cypher, **params)]


def neo4j_ensure_schema(driver, cfg: Neo4jConfig) -> None:
    """Idempotent. Plain constraints/indexes only — Community-safe."""
    _run(driver, cfg, "CREATE CONSTRAINT kgnode_uid IF NOT EXISTS "
                      "FOR (n:KGNode) REQUIRE n.uid IS UNIQUE")
    _run(driver, cfg, "CREATE INDEX kgnode_build IF NOT EXISTS "
                      "FOR (n:KGNode) ON (n.build_id)")
    _run(driver, cfg, "CREATE INDEX kgnode_id IF NOT EXISTS "
                      "FOR (n:KGNode) ON (n.id)")
    _run(driver, cfg, "CREATE CONSTRAINT kgpointer_name IF NOT EXISTS "
                      "FOR (p:KGPointer) REQUIRE p.name IS UNIQUE")


def _batches(items: Iterable[dict], size: int) -> Iterator[list[dict]]:
    batch: list[dict] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def neo4j_load(driver, cfg: Neo4jConfig, build_id: str,
               nodes: Iterable[dict], edges: Iterable[dict]) -> dict[str, int]:
    """Load one generation. Idempotent: MERGE on uid / on (a,b,type,build)."""
    counts = {"nodes": 0, "edges": 0}
    by_kind: dict[str, list[dict]] = {k: [] for k in NODE_KINDS}

    def flush_nodes(kind: str, rows: list[dict]) -> None:
        if not rows:
            return
        label = label_for_kind(kind)
        _run(driver, cfg, f"""
            UNWIND $rows AS r
            MERGE (n:KGNode:`{label}` {{uid: r.uid}})
            SET n.build_id = $bid, n.id = r.id, n.kind = r.kind,
                n.label = r.label, n.category = r.category, n.status = r.status
            """, rows=rows, bid=build_id)
        counts["nodes"] += len(rows)
        # No rows.clear() here: the list was just handed to the driver, and
        # mutating it afterwards aliases the parameters of a query that may
        # not have been sent yet. Callers start a fresh list instead.

    for node in nodes:
        kind = node.get("kind")
        if kind not in by_kind:
            raise LoaderError(f"node kind {kind!r} outside NODE_KINDS")
        by_kind[kind].append({
            "uid": f"{build_id}|{node['id']}", "id": node["id"], "kind": kind,
            "label": node.get("label"), "category": node.get("category"),
            "status": node.get("status")})
        if len(by_kind[kind]) >= cfg.batch:
            flush_nodes(kind, by_kind[kind])
            by_kind[kind] = []
    for kind, rows in by_kind.items():
        flush_nodes(kind, rows)

    allowed = set(EDGE_TYPES) | set(CONCEPT_EDGE_TYPES)
    by_type: dict[str, list[dict]] = {t: [] for t in allowed}

    def flush_edges(etype: str, rows: list[dict]) -> None:
        if not rows:
            return
        _run(driver, cfg, f"""
            UNWIND $rows AS r
            MATCH (a:KGNode {{uid: r.suid}}), (b:KGNode {{uid: r.duid}})
            MERGE (a)-[e:`{etype}` {{build_id: $bid}}]->(b)
            """, rows=rows, bid=build_id)
        counts["edges"] += len(rows)

    for edge in edges:
        etype = edge.get("type")
        if etype not in by_type:
            raise LoaderError(f"edge type {etype!r} outside the vocabulary")
        by_type[etype].append({"suid": f"{build_id}|{edge['src']}",
                               "duid": f"{build_id}|{edge['dst']}"})
        if len(by_type[etype]) >= cfg.batch:
            flush_edges(etype, by_type[etype])
            by_type[etype] = []
    for etype, rows in by_type.items():
        flush_edges(etype, rows)
    return counts


def neo4j_counts(driver, cfg: Neo4jConfig, build_id: str) -> dict[str, Any]:
    nodes = _run(driver, cfg, "MATCH (n:KGNode {build_id: $bid}) "
                              "RETURN n.kind AS kind, count(*) AS n", bid=build_id)
    edges = _run(driver, cfg, "MATCH (:KGNode {build_id: $bid})-[e {build_id: $bid}]->() "
                              "RETURN type(e) AS type, count(*) AS n", bid=build_id)
    by_kind = {r["kind"]: r["n"] for r in nodes}
    by_type = {r["type"]: r["n"] for r in edges}
    return {"nodes": sum(by_kind.values()), "edges": sum(by_type.values()),
            "nodes_by_kind": by_kind, "edges_by_type": by_type}


def neo4j_publish(driver, cfg: Neo4jConfig, build_id: str) -> str | None:
    """Single-node write; returns the previously published build id."""
    previous = neo4j_current(driver, cfg)
    _run(driver, cfg, "MERGE (p:KGPointer {name: 'current'}) "
                      "SET p.build_id = $bid, p.published_at = datetime()",
         bid=build_id)
    return previous


def neo4j_current(driver, cfg: Neo4jConfig) -> str | None:
    rows = _run(driver, cfg, "MATCH (p:KGPointer {name: 'current'}) "
                             "RETURN p.build_id AS bid")
    return rows[0]["bid"] if rows else None


def neo4j_prune(driver, cfg: Neo4jConfig, keep: Iterable[str]) -> dict[str, int]:
    """Delete every generation not in ``keep``, in transactional batches
    (CALL {...} IN TRANSACTIONS is Community-available in 5.x)."""
    keep = list(keep)
    current = neo4j_current(driver, cfg)
    if current and current not in keep:
        keep.append(current)              # never delete the published build
    doomed = [r["bid"] for r in _run(
        driver, cfg, "MATCH (n:KGNode) WHERE NOT n.build_id IN $keep "
                     "RETURN DISTINCT n.build_id AS bid", keep=keep)]
    deleted = 0
    for bid in doomed:
        with driver.session(database=cfg.database) as session:
            result = session.run(
                "MATCH (n:KGNode {build_id: $bid}) "
                "CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS "
                "RETURN count(*) AS n", bid=bid)
            deleted += sum(r["n"] for r in result)
    return {"generations_pruned": len(doomed), "nodes_deleted": deleted}


# ---------------------------------------------------------------------------
# Fuseki
# ---------------------------------------------------------------------------

CURRENT_GRAPH = "urn:memeatlas:current"      # documentary; the DEFAULT graph is what is served
_BUILD_GRAPH = "urn:memeatlas:build:"
_MK = PREFIXES["mk"]


def fuseki_graph_iri(build_id: str) -> str:
    return _BUILD_GRAPH + build_id


def _check(resp, what: str) -> None:
    if resp.status_code >= 400:
        raise LoaderError(f"Fuseki {what} -> {resp.status_code}: "
                          f"{(resp.text or '')[:300]}")


def fuseki_load(session, cfg: FusekiConfig, build_id: str, nt_path: str) -> dict[str, Any]:
    """PUT graph.nt into this build's named graph. PUT replaces, so a retry
    converges; the file is streamed, not read into memory."""
    with open(nt_path, "rb") as fh:
        resp = session.put(cfg.data_url, params={"graph": fuseki_graph_iri(build_id)},
                           data=fh, headers={"Content-Type": "application/n-triples"},
                           auth=cfg.auth, timeout=cfg.timeout_s)
    _check(resp, f"load {build_id}")
    return {"graph": fuseki_graph_iri(build_id), "status": resp.status_code,
            "bytes": os.path.getsize(nt_path)}


def fuseki_publish(session, cfg: FusekiConfig, build_id: str, nt_path: str) -> dict[str, Any]:
    """PUT the same file into the DEFAULT graph — one TDB2 write transaction.

    A reader on the default graph sees the previous build until the commit,
    then this one. Copying the file rather than ADD/MOVE between graphs
    keeps this a single request with a single, obvious failure mode, and the
    provenance triples inside the file mean the default graph names its own
    build (see fuseki_current).
    """
    with open(nt_path, "rb") as fh:
        resp = session.put(cfg.data_url, params={"default": ""}, data=fh,
                           headers={"Content-Type": "application/n-triples"},
                           auth=cfg.auth, timeout=cfg.timeout_s)
    _check(resp, f"publish {build_id}")
    return {"status": resp.status_code}


def _select(session, cfg: FusekiConfig, query: str) -> list[dict]:
    resp = session.post(cfg.query_url, data={"query": query},
                        headers={"Accept": "application/sparql-results+json"},
                        timeout=cfg.timeout_s)
    _check(resp, "query")
    return resp.json()["results"]["bindings"]


def fuseki_count(session, cfg: FusekiConfig, graph_iri: str | None = None) -> int:
    where = (f"GRAPH <{graph_iri}> {{ ?s ?p ?o }}" if graph_iri else "?s ?p ?o")
    rows = _select(session, cfg, f"SELECT (COUNT(*) AS ?n) WHERE {{ {where} }}")
    return int(rows[0]["n"]["value"]) if rows else 0


def fuseki_current(session, cfg: FusekiConfig) -> str | None:
    """Which build the default graph holds — read from the provenance triple
    rdf.py writes into every graph.nt, so the store describes itself."""
    rows = _select(session, cfg,
                   f"SELECT ?b WHERE {{ <{_MK}currentBuild> <{_MK}buildId> ?b }}")
    return rows[0]["b"]["value"] if rows else None


def fuseki_graphs(session, cfg: FusekiConfig) -> list[str]:
    rows = _select(session, cfg, "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }")
    return sorted(r["g"]["value"] for r in rows)


def fuseki_prune(session, cfg: FusekiConfig, keep: Iterable[str]) -> dict[str, Any]:
    """DELETE every build graph not in ``keep``. Never touches the default
    graph — that is the published one — nor the build it currently holds."""
    keep = set(keep)
    current = fuseki_current(session, cfg)
    if current:
        keep.add(current)
    removed = []
    for g in fuseki_graphs(session, cfg):
        if not g.startswith(_BUILD_GRAPH):
            continue
        bid = g[len(_BUILD_GRAPH):]
        if bid in keep:
            continue
        resp = session.delete(cfg.data_url, params={"graph": g}, auth=cfg.auth,
                              timeout=cfg.timeout_s)
        if resp.status_code not in (200, 204, 404):
            _check(resp, f"prune {g}")
        removed.append(bid)
    return {"graphs_pruned": len(removed), "pruned": removed}
