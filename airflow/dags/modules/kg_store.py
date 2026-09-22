"""
kg_store.py — MongoDB persistence for the KG build stage
=========================================================
The ONLY place the KG DAG touches the database (dom_store rule, applied to
the KG stage). Knows nothing about how a graph is derived — kg/build.py
knows nothing about Mongo; the DAG glues them.

Collections
-----------
``entries``   (owned by the parse stage, read-only here)
    The source corpus. Selection freezes a ``snapshot_at`` and every read
    filters ``parsed_at <= snapshot_at``, so a build sees one consistent
    generation of the corpus even while the parse stage keeps writing.

``events``    (owned by the event stage, read-only here, via event_store)
    Extracted events, one doc per (frame, section). Read per build chunk
    through ``events_for`` and summarised into the staleness stamps through
    ``extraction_stamps`` — both deliberate re-exports of event_store, so
    the KG DAG imports exactly one store (the same rule parse_store follows
    for `doms` with iter_html). Frozen at the build's snapshot by
    ``extracted_at <= snapshot_at``, the events analogue of parsed_at.

``kg_nodes`` / ``kg_edges``  (owned by this module) — GENERATIONAL
    _id        "<build_id>|<node_id>"  /  "<build_id>|<src>|<type>|<dst>"
    build_id   which build wrote this document
    ...        the node/edge fields kg/build.py emits

    A writer only ever inserts documents carrying its own ``build_id``; it
    never touches the published generation. That is the whole atomicity
    mechanism — a reader cannot observe a half-written graph because the
    half-written graph lives in a namespace nobody is reading yet.

``kg_builds``  (owned by this module)
    _id        the build_id, plus one pointer document with _id "current"
    state      building | verified | published | superseded | failed
    stamps     versions + snapshot_at, for the staleness gate
    manifest   per-file sha256 and counts, written at publish time

    ``{_id: "current"}`` is the authority. Flipping it is a single-document
    write, which is atomic in Mongo even on a standalone server — and this
    deployment IS standalone (no replica set), so multi-document
    transactions are unavailable and nothing here relies on them.

No query against kg_nodes/kg_edges is valid without a ``build_id`` filter.
Every index leads on ``build_id`` so an unfiltered scan is visibly wrong in
explain() rather than quietly returning several generations at once.

Never-downgrade, by construction
--------------------------------
A frame must never be replaced by a ``frame_stub`` for the same url. The
previous implementation defended that with a find_one before every node
write — 348,753 nodes, so ~700k round trips per build. Here ``save_graph``
DROPS stub nodes entirely and a single ``materialize_stubs`` pass afterwards
creates one for each edge target that ended up with no node in this build.
Order between mapped tasks stops mattering, and the invariant becomes
unrepresentable instead of enforced.

Connection settings come from the environment (docker-compose):
    MONGODB_URI                     (default: mongodb://localhost:27017)
    MONGODB_DB                      (default: memes)
    MONGODB_ENTRIES_COLLECTION      (default: entries)
    MONGODB_KG_NODES_COLLECTION     (default: kg_nodes)
    MONGODB_KG_EDGES_COLLECTION     (default: kg_edges)
    MONGODB_KG_BUILDS_COLLECTION    (default: kg_builds)
    MONGODB_EVENTS_COLLECTION       (default: events; read via event_store)
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, Iterator

from modules.mongo_base import MongoStoreBase, as_utc, now_utc

log = logging.getLogger("kg_store")

__all__ = [
    "KGStore", "get_store", "CURRENT", "BULK_BATCH",
    "snapshot", "published_stamps", "begin_build", "save_graph",
    "materialize_stubs", "save_concept_edges", "iter_entries", "iter_field",
    "iter_nodes", "iter_edges", "graph_counts", "publish_build",
    "current_build", "current_build_id", "prune_builds", "fail_build",
    "mark_verified", "record_validation", "events_for", "extraction_stamps",
    "EVENT_STAMP_KEYS",
]

# The pointer document's _id. A build_id can never collide with it because
# build ids are prefixed "kg_".
CURRENT = "current"

# Ops per bulk_write. Large enough that 1M documents is ~1k round trips,
# small enough that a failure loses little and memory stays flat.
BULK_BATCH = 1000

# Since KG build 3.0.0 the graph carries the whole parsed record (section
# text, images, references), so the projection EXCLUDES instead of listing
# what to include: a field the parser adds reaches kg/build.py without an
# edit here. What is excluded is pipeline bookkeeping kg/build.py
# deliberately does not model. Streaming still matters — a chunk of full
# entries is several MB, which is why iter_entries yields rather than lists
# (the OOM warnings in parse_store.py).
ENTRY_PROJECTION = {
    "_id": 0, "dom_content_sha256": 0, "corpus_policy_version": 0,
    "schema_version": 0,
}

# id prefix -> node kind, for materialize_stubs: a concept referenced only
# by a concept-to-concept edge (kg/taxonomy.py's subTypeOf, kg/origin.py's,
# kg/cooccurs.py's coOccursWith) still needs its correct kind, not the
# generic external_ref every other dangling target gets.
_CONCEPT_KIND_FOR_PREFIX = {
    "type:": "entry_type_concept", "tag:": "tag_concept",
    "region:": "region_concept", "origin:": "origin_concept",
    "badge:": "badge_concept",
    # Defensive, not load-bearing: build.py always emits an event node
    # alongside its hasEvent edge. But if one ever went missing, this stops
    # materialize_stubs minting an external_ref whose id is "event:..." and
    # writing a bogus <event:...> row into the view CSV.
    "event:": "event",
}

# The event layer's staleness stamps (6.0.0), compared by is_stale like
# every other key. events_total is carried as well as events_units because
# a re-extraction can change what the events SAY without changing how many
# units exist, and that must rebuild the graph too.
EVENT_STAMP_KEYS: tuple[str, ...] = (
    "events_units", "events_total", "events_prompt_versions",
    "events_extraction_versions", "events_schema_shas",
    "events_max_extracted_at",
)


class KGStore(MongoStoreBase):
    """Owner of ``kg_nodes``, ``kg_edges`` and ``kg_builds``; reads ``entries``."""

    def _configure(self) -> None:
        self.entries = self.collection("MONGODB_ENTRIES_COLLECTION", "entries")
        self.nodes = self.collection("MONGODB_KG_NODES_COLLECTION", "kg_nodes")
        self.edges = self.collection("MONGODB_KG_EDGES_COLLECTION", "kg_edges")
        self.builds = self.collection("MONGODB_KG_BUILDS_COLLECTION", "kg_builds")

        # Every index leads on build_id — see the module docstring.
        #
        # PARTIAL, not plain, unique: the live kg_nodes/kg_edges still hold
        # the previous implementation's documents (348,753 + 712,785), none
        # of which carry build_id or node_id. Mongo indexes a missing field
        # as null, so a plain unique index over them fails to build with a
        # DuplicateKeyError and the store could never connect. Restricting
        # the constraint to documents that HAVE a build_id lets the legacy
        # rows sit harmlessly — invisible to every build-scoped query here —
        # while uniqueness is still enforced on every generational document.
        self.nodes.create_index(
            [("build_id", 1), ("node_id", 1)], unique=True,
            partialFilterExpression={"build_id": {"$exists": True}})
        self.nodes.create_index([("build_id", 1), ("kind", 1)])
        self.edges.create_index([("build_id", 1), ("type", 1)])
        self.edges.create_index([("build_id", 1), ("dst", 1)])
        self.edges.create_index([("build_id", 1), ("src", 1)])
        self.builds.create_index([("state", 1), ("created_at", -1)])

    # -- ids ----------------------------------------------------------------

    @staticmethod
    def node_key(build_id: str, node_id: str) -> str:
        return f"{build_id}|{node_id}"

    @staticmethod
    def edge_key(build_id: str, edge: dict) -> str:
        return f'{build_id}|{edge["src"]}|{edge["type"]}|{edge["dst"]}'

    # -- selection ----------------------------------------------------------

    def snapshot(self, namespaces: Iterable[str] | None = None,
                 ready_only: bool = False, limit: int = 0) -> dict[str, Any]:
        """Freeze the corpus generation this build will read.

        ``parse_store.build_entry_doc`` stamps ``parsed_at`` at write time,
        so anything written after this instant necessarily has a later
        ``parsed_at`` and is invisible to this build — and its newer stamp
        makes the next build pick it up. That is what makes the snapshot
        consistent without a transaction.
        """
        snapshot_at = now_utc()
        query: dict[str, Any] = {"parsed_at": {"$lte": snapshot_at}}
        if ready_only:
            query["corpus_status"] = "ready"

        cursor = self.entries.find(query, {"_id": 1, "parser_version": 1,
                                           "corpus_policy_version": 1,
                                           "parsed_at": 1})
        if limit:
            cursor = cursor.limit(limit)

        entry_ids: list[str] = []
        parser_versions: set[str] = set()
        policy_versions: set[str] = set()
        max_parsed_at = None
        for doc in cursor:
            entry_ids.append(doc["_id"])
            if doc.get("parser_version"):
                parser_versions.add(doc["parser_version"])
            if doc.get("corpus_policy_version"):
                policy_versions.add(doc["corpus_policy_version"])
            parsed = doc.get("parsed_at")
            if parsed is not None and (max_parsed_at is None or parsed > max_parsed_at):
                max_parsed_at = parsed

        return {
            "snapshot_at": snapshot_at,
            "entry_ids": entry_ids,
            "entries_count": len(entry_ids),
            "parser_versions": sorted(parser_versions),
            "corpus_policy_versions": sorted(policy_versions),
            # The newest parse in the snapshot: if it moved, the corpus did.
            "max_parsed_at": as_utc(max_parsed_at),
            "ready_only": ready_only,
        }

    def iter_entries(self, entry_ids: list[str], snapshot_at) -> Iterator[dict]:
        """Stream the projected entries for one chunk.

        Streaming is load-bearing here for the same reason it is in the
        parse stage: do NOT materialise this into a list.
        """
        if not entry_ids:
            return
        yield from self.entries.find(
            {"_id": {"$in": entry_ids}, "parsed_at": {"$lte": snapshot_at}},
            ENTRY_PROJECTION)

    def iter_field(self, field: str, snapshot_at) -> Iterator[dict]:
        """Stream one field across the whole snapshot, for the census."""
        yield from self.entries.find(
            {"parsed_at": {"$lte": snapshot_at}}, {"_id": 0, field: 1})

    # -- build lifecycle ----------------------------------------------------

    def begin_build(self, build_id: str, stamps: dict[str, Any]) -> None:
        now = now_utc()
        self.builds.update_one(
            {"_id": build_id},
            {"$set": {"build_id": build_id, "state": "building",
                      "stamps": stamps, "updated_at": now},
             "$setOnInsert": {"created_at": now}},
            upsert=True)
        log.info("Begin KG build %s", build_id)

    def published_stamps(self) -> dict[str, Any] | None:
        """The stamps of the currently published build, or None."""
        pointer = self.builds.find_one({"_id": CURRENT})
        if not pointer or not pointer.get("build_id"):
            return None
        doc = self.builds.find_one({"_id": pointer["build_id"]})
        return (doc or {}).get("stamps")

    @staticmethod
    def is_stale(stamps: dict[str, Any], published: dict[str, Any] | None,
                 force: bool = False) -> tuple[bool, str]:
        """Does anything require a rebuild? Returns (stale, reason).

        The unit of staleness is the BUILD, not the node: a taxonomy edit
        changes concept edges corpus-wide, so a per-node stamp would be a
        lie. Compared fields are the ones that can change the output.
        """
        if force:
            return True, "force_rebuild"
        if published is None:
            return True, "no published build"
        for key in ("kg_build_version", "taxonomy_version",
                    "origin_taxonomy_version", "tag_denylist_version",
                    "entries_count", "parser_versions",
                    "corpus_policy_versions", "max_parsed_at",
                    *EVENT_STAMP_KEYS):
            if stamps.get(key) != published.get(key):
                return True, (f"{key} changed: "
                              f"{published.get(key)!r} -> {stamps.get(key)!r}")
        return False, "nothing changed since the published build"

    def fail_build(self, build_id: str, reason: str) -> None:
        self.builds.update_one(
            {"_id": build_id},
            {"$set": {"state": "failed", "failed_reason": str(reason)[:2000],
                      "updated_at": now_utc()}})
        log.warning("KG build %s failed: %s", build_id, reason)

    def mark_verified(self, build_id: str) -> None:
        """verify() passed. Tells a build that is complete but deliberately
        unpublished (publish=False) apart from one still being written —
        without this, both read as "building"."""
        now = now_utc()
        self.builds.update_one(
            {"_id": build_id},
            {"$set": {"state": "verified", "verified_at": now, "updated_at": now}})

    def record_validation(self, build_id: str, result: dict[str, Any]) -> None:
        """Attach the RDF diff gate's verdict to a build.

        Never touches ``state`` or the pointer: a published graph that later
        fails validation is FLAGGED, not yanked. The in-process path is the
        product; retroactively un-publishing it would be worse than showing
        red on the dashboard. The dashboard reads ``validation.equal``.
        """
        self.builds.update_one(
            {"_id": build_id},
            {"$set": {"validation": result, "updated_at": now_utc()}})

    # -- writes -------------------------------------------------------------

    def save_graph(self, build_id: str, nodes: Iterable[dict],
                   edges: Iterable[dict]) -> dict[str, int]:
        """Persist one chunk's nodes and edges into this build's generation.

        ``frame_stub`` nodes are dropped: materialize_stubs creates them
        once, afterwards, for edge targets that have no node. See the
        module docstring.

        Nodes are MERGED, not replaced. One node id can be emitted several
        times with different properties — the same image file is an entry's
        og:image (with its size) on one page and a captioned section image
        on another — so each write ``$set``s only the values it has, within
        a chunk and across chunks. On a real conflict (two captions for one
        file) the later write wins, which is still one value per property,
        and every store is fed from this one merged document.
        """
        from pymongo import ReplaceOne, UpdateOne

        edge_ops: list[Any] = []
        written = {"nodes_written": 0, "edges_written": 0, "stubs_deferred": 0}
        merged: dict[str, dict] = {}
        seen_edges: set[str] = set()

        def flush(coll, ops, key):
            if ops:
                coll.bulk_write(ops, ordered=False)
                written[key] += len(ops)
                ops.clear()

        # A chunk's distinct nodes are bounded by the chunk size (hundreds of
        # entries), so merging them in memory first is cheap.
        for node in nodes:
            if node.get("kind") == "frame_stub":
                written["stubs_deferred"] += 1
                continue
            into = merged.setdefault(node["id"], {})
            into.update({k: v for k, v in node.items()
                         if v not in (None, "", [])})

        node_ops: list[Any] = []
        for node_id, node in merged.items():
            key = self.node_key(build_id, node_id)
            node_ops.append(UpdateOne(
                {"_id": key},
                {"$set": {**node, "build_id": build_id, "node_id": node_id}},
                upsert=True))
            if len(node_ops) >= BULK_BATCH:
                flush(self.nodes, node_ops, "nodes_written")

        for edge in edges:
            key = self.edge_key(build_id, edge)
            if key in seen_edges:
                continue
            seen_edges.add(key)
            doc = {**edge, "_id": key, "build_id": build_id}
            edge_ops.append(ReplaceOne({"_id": key}, doc, upsert=True))
            if len(edge_ops) >= BULK_BATCH:
                flush(self.edges, edge_ops, "edges_written")

        flush(self.nodes, node_ops, "nodes_written")
        flush(self.edges, edge_ops, "edges_written")
        return written

    def materialize_stubs(self, build_id: str) -> dict[str, int]:
        """Create a node for every edge target that has none in this build.

        Two projected cursors and a set difference, deliberately not
        ``distinct()`` (712k targets would risk the 16MB BSON cap) and not
        ``$lookup`` (mongomock's support is too thin to test against).
        """
        from pymongo import ReplaceOne
        from modules.kg.build import guess_stub_node, _is_kym_url

        have: set[str] = {d["node_id"] for d in self.nodes.find(
            {"build_id": build_id}, {"_id": 0, "node_id": 1})}
        wanted: set[str] = set()
        for d in self.edges.find({"build_id": build_id},
                                 {"_id": 0, "src": 1, "dst": 1}):
            wanted.add(d["dst"])
            wanted.add(d["src"])

        missing = sorted(wanted - have)
        ops = []
        for node_id in missing:
            concept_kind = next((k for p, k in _CONCEPT_KIND_FOR_PREFIX.items()
                                 if node_id.startswith(p)), None)
            if concept_kind is not None:
                # A concept referenced only by a concept-to-concept edge —
                # e.g. kg/origin.py's synthetic umbrella parents
                # ("social-network", "imageboard", ...), which no raw
                # origin value ever aliases to directly, so build_chunk
                # never creates them. Typed correctly (not external_ref)
                # so it still gets its skos:Concept/scheme/prefLabel
                # triples in kg/rdf.py.
                node = {"id": node_id, "kind": concept_kind,
                       "label": node_id.split(":", 1)[1]}
            elif node_id.startswith("image:") or not _is_kym_url(node_id):
                # Not a KYM page: an outbound citation target.
                node = {"id": node_id, "kind": "external_ref", "label": None}
            else:
                node = guess_stub_node(node_id)
            doc = {**node, "_id": self.node_key(build_id, node_id),
                   "build_id": build_id, "node_id": node_id}
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))

        for start in range(0, len(ops), BULK_BATCH):
            self.nodes.bulk_write(ops[start:start + BULK_BATCH], ordered=False)

        log.info("Materialized %d stub nodes for build %s", len(ops), build_id)
        return {"stubs_materialized": len(ops)}

    def save_concept_edges(self, build_id: str,
                           edges: Iterable[dict]) -> dict[str, int]:
        """Concept-to-concept edges (subTypeOf -> rdfs:subClassOf)."""
        return {"concept_edges_written":
                self.save_graph(build_id, (), edges)["edges_written"]}

    # -- reads --------------------------------------------------------------

    def iter_nodes(self, build_id: str, kinds: Iterable[str] | None = None,
                   fields: Iterable[str] | None = None) -> Iterator[dict]:
        """One build's nodes, optionally only some kinds and some fields.

        ``fields`` matters for consumers that need the graph's shape but not
        its text (metrics): section text and about narratives are most of a
        node document's bytes. ``id`` and ``kind`` are always returned.
        """
        query: dict[str, Any] = {"build_id": build_id}
        if kinds is not None:
            query["kind"] = {"$in": list(kinds)}
        if fields is None:
            projection: dict[str, int] = {"_id": 0, "build_id": 0, "node_id": 0}
        else:
            projection = {"_id": 0, "id": 1, "kind": 1, **{f: 1 for f in fields}}
        yield from self.nodes.find(query, projection)

    def iter_edges(self, build_id: str, types: Iterable[str] | None = None,
                   occurrences: bool = True) -> Iterator[dict]:
        """One build's edges. ``occurrences=False`` leaves out the per-mention
        lists (anchor and citation texts, captions) for consumers that need
        only the graph's shape — the metrics task holds every edge in
        memory at once."""
        query: dict[str, Any] = {"build_id": build_id}
        if types is not None:
            query["type"] = {"$in": list(types)}
        projection = {"_id": 0, "src": 1, "dst": 1, "type": 1}
        if occurrences:
            projection["occurrences"] = 1
        yield from self.edges.find(query, projection)

    def counts(self, build_id: str) -> dict[str, Any]:
        """Per-kind and per-type counts, aggregated server-side."""
        def group(coll, field):
            return {(d["_id"] or "(none)"): d["n"] for d in coll.aggregate([
                {"$match": {"build_id": build_id}},
                {"$group": {"_id": f"${field}", "n": {"$sum": 1}}},
                {"$sort": {"n": -1}},
            ])}

        by_kind = group(self.nodes, "kind")
        by_type = group(self.edges, "type")
        return {
            "nodes": sum(by_kind.values()),
            "edges": sum(by_type.values()),
            "frames": by_kind.get("frame", 0),
            "nodes_by_kind": by_kind,
            "edges_by_type": by_type,
        }

    # -- the pointer --------------------------------------------------------

    def publish(self, build_id: str, manifest: dict[str, Any] | None = None) -> str:
        """Flip the authority to this build. One single-document write.

        Called LAST, after the files, Fuseki and Neo4j followers have been
        pointed at the same build — so once the authority says published,
        every other store is already serving it. Idempotent: re-running
        converges rather than double-applying.
        """
        now = now_utc()
        previous = self.current_build_id()
        if previous and previous != build_id:
            self.builds.update_one({"_id": previous},
                                   {"$set": {"state": "superseded",
                                             "updated_at": now}})
        self.builds.update_one(
            {"_id": build_id},
            {"$set": {"state": "published", "published_at": now,
                      "manifest": manifest or {}, "updated_at": now}})
        self.builds.update_one(
            {"_id": CURRENT},
            {"$set": {"build_id": build_id, "published_at": now}},
            upsert=True)
        log.info("Published KG build %s (was %s)", build_id, previous)
        return build_id

    def current_build_id(self) -> str | None:
        doc = self.builds.find_one({"_id": CURRENT})
        return (doc or {}).get("build_id")

    def current_build(self) -> dict[str, Any] | None:
        build_id = self.current_build_id()
        if not build_id:
            return None
        doc = self.builds.find_one({"_id": build_id})
        if doc:
            doc["published_at"] = as_utc(doc.get("published_at"))
            doc["created_at"] = as_utc(doc.get("created_at"))
        return doc

    def prune(self, keep: int = 2) -> dict[str, int]:
        """Drop generations beyond the newest ``keep``, never the published one."""
        current = self.current_build_id()
        builds = [d["_id"] for d in self.builds.find(
            {"_id": {"$ne": CURRENT}}, {"_id": 1}).sort("created_at", -1)]
        survivors = set(builds[:max(keep, 1)]) | ({current} if current else set())
        doomed = [b for b in builds if b not in survivors]
        for build_id in doomed:
            self.nodes.delete_many({"build_id": build_id})
            self.edges.delete_many({"build_id": build_id})
            self.builds.delete_one({"_id": build_id})
        if doomed:
            log.info("Pruned %d old KG build(s): %s", len(doomed), doomed)
        return {"pruned": len(doomed), "kept": len(survivors)}


def get_store(uri: str | None = None, db_name: str | None = None) -> KGStore:
    return KGStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facade functions — the only calls the KG DAG makes (dom_store style)
# ---------------------------------------------------------------------------

def snapshot(namespaces=None, ready_only: bool = False, limit: int = 0) -> dict:
    with get_store() as store:
        snap = store.snapshot(namespaces=namespaces, ready_only=ready_only,
                              limit=limit)
        log.info("KG snapshot: %d entries at %s",
                 snap["entries_count"], snap["snapshot_at"])
        return snap


def published_stamps() -> dict | None:
    with get_store() as store:
        return store.published_stamps()


def begin_build(build_id: str, stamps: dict) -> None:
    with get_store() as store:
        store.begin_build(build_id, stamps)


def save_graph(build_id: str, nodes, edges) -> dict[str, int]:
    with get_store() as store:
        return store.save_graph(build_id, nodes, edges)


def materialize_stubs(build_id: str) -> dict[str, int]:
    with get_store() as store:
        return store.materialize_stubs(build_id)


def save_concept_edges(build_id: str, edges) -> dict[str, int]:
    with get_store() as store:
        return store.save_concept_edges(build_id, edges)


def iter_entries(entry_ids: list[str], snapshot_at):
    """Stream projected entries. A generator on purpose — see iter_html."""
    with get_store() as store:
        yield from store.iter_entries(entry_ids, snapshot_at)


def iter_field(field: str, snapshot_at):
    with get_store() as store:
        yield from store.iter_field(field, snapshot_at)


def events_for(entry_ids: list[str], snapshot_at) -> dict[str, list[dict]]:
    """{frame_url: [event, ...]} for one build chunk, frozen at the snapshot.

    A deliberate re-export of event_store.events_for: `events` belongs to
    the event stage, so the KG DAG reaches it through this module — one
    store import per stage (the rule parse_store.iter_html follows).
    Keyed by URL because ENTRY_PROJECTION excludes _id: the entries this
    DAG streams have nothing else to join on.
    """
    from modules import event_store
    return event_store.events_for(entry_ids, extracted_at_lte=snapshot_at)


def extraction_stamps(snapshot_at) -> dict[str, Any]:
    """The event layer's staleness stamps over the same frozen generation
    the build reads — computed over events the build cannot see, the gate
    would record a count the build never saw. See EVENT_STAMP_KEYS."""
    from modules import event_store
    return event_store.extraction_stamps(extracted_at_lte=snapshot_at)


def iter_nodes(build_id: str, kinds=None, fields=None):
    """Stream one build's nodes. kg/serialize.py calls this several times
    through a factory — one fresh cursor per pass, nothing buffered."""
    with get_store() as store:
        yield from store.iter_nodes(build_id, kinds=kinds, fields=fields)


def iter_edges(build_id: str, types=None, occurrences: bool = True):
    with get_store() as store:
        yield from store.iter_edges(build_id, types=types, occurrences=occurrences)


def graph_counts(build_id: str) -> dict:
    with get_store() as store:
        return store.counts(build_id)


def publish_build(build_id: str, manifest: dict | None = None) -> str:
    with get_store() as store:
        return store.publish(build_id, manifest)


def current_build() -> dict | None:
    with get_store() as store:
        return store.current_build()


def current_build_id() -> str | None:
    with get_store() as store:
        return store.current_build_id()


def prune_builds(keep: int = 2) -> dict[str, int]:
    with get_store() as store:
        return store.prune(keep=keep)


def fail_build(build_id: str, reason: str) -> None:
    with get_store() as store:
        store.fail_build(build_id, reason)


def mark_verified(build_id: str) -> None:
    with get_store() as store:
        store.mark_verified(build_id)


def record_validation(build_id: str, result: dict) -> None:
    with get_store() as store:
        store.record_validation(build_id, result)
