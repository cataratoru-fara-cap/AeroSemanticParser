"""
kym_kg_dag.py — Airflow DAG over modules/kg/* + modules/kg_store.py
=====================================================================
Top-level orchestration ONLY. All graph logic lives in modules/kg/ (pure: no
Mongo, no Airflow); all persistence in modules/kg_store.py (the only place
this DAG touches MongoDB) and modules/kg/loaders.py (Neo4j and Fuseki, driver
calls only). State between tasks lives in the stores and in the build
directory; XCom carries only entry ids, small dicts, and a manifest of file
hashes.

What a run produces
-------------------
One BUILD — a generation of the knowledge graph derived from one consistent
snapshot of `entries` — in every representation at once:

    Mongo    kg_nodes / kg_edges documents tagged with this build_id
    files    data/kg/builds/<build_id>/{graph.nt, rml_data/*.csv,
             kg_view_*.csv, ontology.ttl, manifest.json}
    Fuseki   named graph urn:memeatlas:build:<build_id>      (RDF)
             + the vocabulary in urn:memeatlas:ontology
    Neo4j    nodes/relationships carrying build_id            (property graph)

and then, if verification passes, flips every pointer to it.

The graph is MemeAtlas as an extension of IMKG: IMKG's terms where IMKG
models a thing, mk: terms (kg_config/memeatlas.ttl) for the rest of the
parsed record — see kg/rdf.py and kg_config/MODEL.md.

Atomicity without transactions
------------------------------
Mongo here is standalone, so multi-document transactions are unavailable —
and no transaction could span four stores anyway. Instead every writer
inserts only into its own build_id namespace and never touches the
published generation; a reader cannot see a half-built graph because it is
in a namespace nobody reads yet. Each pointer flip is one small write.

Publish order is files -> Fuseki -> Neo4j -> Mongo: the authority moves
LAST, so once it says "published", every follower is already serving that
build. A crash between two flips leaves a follower AHEAD of the authority,
never behind it; every flip is set-to-value, so re-running `publish`
converges, and `reconcile` re-points any follower that drifted. `publish`
reads each pointer back after flipping and fails loudly on disagreement.
This is bounded eventual agreement, not 2PC.

`reconcile` runs on EVERY run, including one the staleness gate skips.
It was briefly downstream of the gate, which meant a follower that drifted
during a quiet period — exactly when nobody is looking — would stay drifted
until the corpus happened to change. It needs only Mongo's pointer, so it
has no reason to wait for a build to start.

Followers are optional. A store whose password is not configured is
skipped, so the stage degrades to Mongo + files rather than failing.

Pipeline:
    snapshot            freeze the corpus generation; compute staleness stamps
    reconcile           re-point any follower that disagrees with Mongo's
                        current — ABOVE the gate, so it still runs on a run
                        the gate skips
    gate                skip the rest if nothing that affects the graph moved
    chunk_entries       split into mapped workloads
    build_chunk         (mapped) stream entries + their extracted events
                        (6.0.0, kym_events) + their Wikidata links (6.1.0,
                        kym_entities) -> kg/build.py -> kg_store.save_graph
    materialize_stubs   one pass: a stub node for every edge target with no node
    census              entry_type frequency + co-occurrence for the taxonomy
    load_taxonomy       kg_config/entry_type_taxonomy.yaml, validated against it
    write_concept_edges subTypeOf edges (rdfs:subClassOf) into the same generation
    census_origin        raw `origin` values (5.0.0) -- for origin's taxonomy only
    load_origin_taxonomy kg_config/origin_taxonomy.yaml -- canonicalization +
                         a platform-only subClassOf hierarchy (kg/origin.py)
    write_origin_concept_edges  origin's subTypeOf edges into the same generation
    census_tags          plural-folded tag frequency + co-occurrence (5.0.0)
    write_cooccurs_edges statistical coOccursWith edges (kg/cooccurs.py), tags only
                         (5.0.1: removed for entry_type -- needless alongside its
                         curated subTypeOf). Property-graph-only always
                         (tag_concept has no RDF resource)
    write_exports       kg/serialize.py: one stream -> every file representation
    load_fuseki         graph.nt -> the build's named graph; ontology -> its own
    load_neo4j          nodes/edges -> Neo4j, tagged with build_id
    verify              Mongo == manifest == RML rows == Fuseki == Neo4j
    publish             followers first, Mongo last; read every pointer back
    prune               drop generations beyond keep_builds, in every store
    compute_metrics     kg/metrics.py over the published build's IMKG-comparable
                        core (memory-heavy, so AFTER publish: it can never
                        block a verified graph)
    summarize / record_summary   -> run_summaries, stage="kg"

The RDF diff gate (morph-kgc re-derivation vs graph.nt) is its own DAG,
kym_kg_validate.

Trigger-time params:
    batch_size     entries this run (0 = whole snapshot)
    chunk_size     entries per mapped build task
    ready_only     restrict to corpus_status == "ready" (default False —
                   "nothing is discarded for being incomplete" applies here)
    force_rebuild  ignore the staleness gate
    keep_builds    generations to retain after publish
    top_tags / exclude_kinds   shape the property-graph VIEW csvs only
    publish        False = build + verify, leave every pointer where it is
"""

from __future__ import annotations

import logging
import os
import re
import shutil
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowSkipException

from modules import kg_store as store
from modules.kg import build as kg_build
from modules.kg import loaders

log = logging.getLogger(__name__)

# Building is CPU + Mongo-bound, no outbound HTTP; parse runs 4 too.
MAX_PARALLEL_BUILD_TASKS = 4

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

KG_DATA_DIR = os.getenv("KG_DATA_DIR", "/opt/airflow/data/kg")
KG_CONFIG_DIR = os.getenv("KG_CONFIG_DIR", "/opt/airflow/dags/kg_config")
TAXONOMY_PATH = os.path.join(KG_CONFIG_DIR, "entry_type_taxonomy.yaml")
ORIGIN_TAXONOMY_PATH = os.path.join(KG_CONFIG_DIR, "origin_taxonomy.yaml")
TAG_DENYLIST_PATH = os.path.join(KG_CONFIG_DIR, "tag_normalization_exceptions.yaml")
ONTOLOGY_PATH = os.path.join(KG_CONFIG_DIR, "memeatlas.ttl")

# The subgraph kg/metrics.py measures: the shape IMKG publishes numbers for
# (frames, their types, tags, series and cross-links), so MemeAtlas's figures
# stay comparable with IMKG's. The page body — sections, links, references,
# images — would multiply node counts without saying anything about meme
# connectivity, and would not fit in the worker's memory besides. Every
# node a core edge touches is a core kind (kg/build.py emits frame-level
# edges for every body link and reference), so the core is closed.
METRICS_NODE_KINDS = ("frame", "frame_stub", "entry_type_concept",
                      "tag_concept", "external_ref")
METRICS_EDGE_TYPES = ("hasEntryType", "hasTag", "partOfSeries",
                      "relatesToMeme", "citesExternal", "subTypeOf")
METRICS_NODE_FIELDS = ("label", "category", "status")


def _build_dir(build_id: str) -> str:
    return os.path.join(KG_DATA_DIR, "builds", build_id)


def _graph_nt(build_id: str) -> str:
    return os.path.join(_build_dir(build_id), "graph.nt")


def _slug(run_id: str) -> str:
    """The run TYPE, not the whole run id: Airflow run ids look like
    ``manual__2026-09-16T13:37:08.762494+00:00``, and the build id already
    carries its own timestamp, so repeating this one produced
    ``kg_…Z_manual_2026_09_16T13_37_08_762494_00_00``. ``kg_…Z_manual`` says
    the same thing."""
    kind = run_id.split("__", 1)[0] if run_id else "manual"
    return re.sub(r"[^A-Za-z0-9]+", "_", kind).strip("_")[:24] or "manual"


# -- followers ---------------------------------------------------------------

def _fuseki_enabled() -> bool:
    return bool(os.getenv("FUSEKI_PASSWORD"))


def _neo4j_enabled() -> bool:
    return bool(os.getenv("NEO4J_PASSWORD"))


def _http():
    import requests
    return requests.Session()


def _point_files_at(build_id: str) -> dict[str, str]:
    """Atomically repoint data/kg/current and data/kg/CURRENT.

    rename(2) over a symlink path is atomic; the CURRENT text file is for
    tools that do not follow symlinks. Both are set-to-value, so re-running
    converges.
    """
    target = os.path.join("builds", build_id)
    link = os.path.join(KG_DATA_DIR, "current")
    tmp_link = link + ".tmp"
    if os.path.lexists(tmp_link):
        os.remove(tmp_link)
    os.symlink(target, tmp_link)
    os.replace(tmp_link, link)

    pointer = os.path.join(KG_DATA_DIR, "CURRENT")
    with open(pointer + ".tmp", "w", encoding="utf-8") as fh:
        fh.write(build_id + "\n")
    os.replace(pointer + ".tmp", pointer)
    return {"current": link, "CURRENT": pointer}


def _files_current() -> str | None:
    pointer = os.path.join(KG_DATA_DIR, "CURRENT")
    if not os.path.exists(pointer):
        return None
    with open(pointer, encoding="utf-8") as fh:
        return fh.read().strip() or None


@dag(
    dag_id="kym_kg",
    schedule=None,  # triggered by kym_events (parse -> entities -> events -> kg)
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "kym", "kg"],
    params={
        "batch_size": Param(0, type="integer", minimum=0,
                            description="Entries this run (0 = whole snapshot)"),
        "chunk_size": Param(500, type="integer", minimum=1,
                            description="Entries per mapped build task"),
        "ready_only": Param(False, type="boolean",
                            description="Only corpus_status == ready entries"),
        "force_rebuild": Param(False, type="boolean",
                               description="Ignore the staleness gate"),
        "keep_builds": Param(2, type="integer", minimum=1,
                             description="Generations to keep after publish"),
        "top_tags": Param(0, type="integer", minimum=0,
                          description="View CSVs only: keep N highest-degree tags (0 = all)"),
        "exclude_kinds": Param([], type="array", items={"type": "string"},
                               description="View CSVs only: node kinds to drop"),
        "publish": Param(True, type="boolean",
                         description="False = build and verify, do not move any pointer"),
    },
)
def kym_kg_dag():

    # -- Phase 1: what generation of the corpus are we building from? -------
    @task
    def snapshot(params: dict | None = None, run_id: str | None = None) -> dict:
        import hashlib
        from modules.kg import origin, taxonomy

        p = params or {}
        snap = store.snapshot(ready_only=p.get("ready_only", False),
                              limit=p.get("batch_size", 0))
        tax = taxonomy.load(TAXONOMY_PATH)
        origin_tax = origin.load(ORIGIN_TAXONOMY_PATH)
        with open(TAG_DENYLIST_PATH, "rb") as fh:
            tag_denylist_version = hashlib.sha256(fh.read()).hexdigest()
        stamps = {
            "kg_build_version": kg_build.KG_BUILD_VERSION,
            "taxonomy_version": tax.version,
            "origin_taxonomy_version": origin_tax.version,
            "tag_denylist_version": tag_denylist_version,
            "entries_count": snap["entries_count"],
            "parser_versions": snap["parser_versions"],
            "corpus_policy_versions": snap["corpus_policy_versions"],
            "max_parsed_at": (snap["max_parsed_at"].isoformat()
                              if snap["max_parsed_at"] else None),
            "snapshot_at": snap["snapshot_at"].isoformat(),
            "ready_only": snap["ready_only"],
            # 6.0.0: what the event stage has extracted, frozen at the same
            # instant — a re-extraction moves these and rebuilds the graph.
            **store.extraction_stamps(snap["snapshot_at"]),
            # 6.1.0: likewise for the entity stage — a re-link (new text, a
            # new lexicon, a new linker) moves these.
            **store.linking_stamps(snap["snapshot_at"]),
        }
        stale, reason = store.KGStore.is_stale(
            stamps, store.published_stamps(), force=p.get("force_rebuild", False))
        build_id = (f"kg_{snap['snapshot_at']:%Y%m%dT%H%M%SZ}_"
                    f"{_slug(run_id or 'manual')}")
        log.info("Snapshot: %d entries at %s; stale=%s (%s); build_id=%s",
                 snap["entries_count"], stamps["snapshot_at"], stale, reason,
                 build_id)
        return {"build_id": build_id, "stamps": stamps, "stale": stale,
                "reason": reason, "entry_ids": snap["entry_ids"]}

    @task
    def gate(snap: dict) -> dict:
        """Skip everything downstream when nothing that affects the graph
        changed. summarize() still runs (none_failed) and records why."""
        if not snap["stale"]:
            raise AirflowSkipException(f"KG up to date: {snap['reason']}")
        store.begin_build(snap["build_id"], snap["stamps"])
        return {"build_id": snap["build_id"],
                "snapshot_at": snap["stamps"]["snapshot_at"]}

    @task
    def reconcile(snap: dict) -> dict:
        """Make every follower agree with Mongo — on every run, gate or no.

        Takes ``snap`` rather than ``gate``'s output purely for sequencing:
        it reads Mongo's published pointer and nothing from this build, so
        gating it behind staleness would leave a drifted follower unattended
        for as long as the corpus sat still.

        A crash mid-publish can leave a follower ahead of the authority. Each
        follower is set-to-value, so re-pointing it at Mongo's current build
        is safe — provided the follower actually HOLDS that generation (a
        store added after builds already existed will not; that case is
        logged and left for this run's fresh build to fill).
        """
        authority = store.current_build_id()
        out: dict = {"authority": authority, "repointed": []}
        if not authority:
            return out
        if _files_current() != authority:
            _point_files_at(authority)
            out["repointed"].append("files")
        if _fuseki_enabled():
            cfg = loaders.FusekiConfig.from_env()
            with _http() as http:
                if loaders.fuseki_current(http, cfg) != authority:
                    if loaders.fuseki_count(http, cfg, loaders.fuseki_graph_iri(authority)):
                        loaders.fuseki_publish(http, cfg, authority, _graph_nt(authority))
                        out["repointed"].append("fuseki")
                    else:
                        log.warning("Fuseki has no graph for %s; this build fills it", authority)
        if _neo4j_enabled():
            cfg = loaders.Neo4jConfig.from_env()
            driver = loaders.neo4j_driver(cfg)
            try:
                loaders.neo4j_ensure_schema(driver, cfg)
                if loaders.neo4j_current(driver, cfg) != authority:
                    if loaders.neo4j_counts(driver, cfg, authority)["nodes"]:
                        loaders.neo4j_publish(driver, cfg, authority)
                        out["repointed"].append("neo4j")
                    else:
                        log.warning("Neo4j has no generation %s; this build fills it", authority)
            finally:
                driver.close()
        if out["repointed"]:
            log.warning("Reconciled followers to %s: %s", authority, out["repointed"])
        return out

    @task
    def chunk_entries(snap: dict, proceed: dict, reconciled: dict,
                      params: dict | None = None) -> list[list[str]]:
        size = (params or {}).get("chunk_size", 500)
        ids = snap["entry_ids"]
        chunks = [ids[i:i + size] for i in range(0, len(ids), size)]
        log.info("Split %d entries into %d chunks of ≤%d", len(ids), len(chunks), size)
        return chunks

    # -- Phase 2: build into this generation (one mapped task per chunk) ------
    @task(
        max_active_tis_per_dagrun=MAX_PARALLEL_BUILD_TASKS,
        execution_timeout=timedelta(minutes=30),
        retries=2,
    )
    def build_chunk(chunk: list[str], proceed: dict) -> dict:
        if not chunk:
            return {"entries": 0, "nodes_written": 0, "edges_written": 0,
                    "stubs_deferred": 0, "frames_with_events": 0,
                    "frames_with_entities": 0}
        import functools
        from modules.kg import origin, tag_normalize
        snapshot_at = datetime.fromisoformat(proceed["snapshot_at"])
        # Loaded fresh per chunk (cheap: two small YAML files, no Mongo) —
        # same pattern as load_taxonomy loading entry_type's YAML fresh
        # rather than threading it through XCom.
        origin_tax = origin.load(ORIGIN_TAXONOMY_PATH)
        origin_resolver = functools.partial(origin.resolve, aliases=origin_tax.aliases)
        tag_denylist = tag_normalize.load_denylist(TAG_DENYLIST_PATH)
        # iter_entries streams projected docs one at a time; a chunk's nodes
        # and edges (~500 entries -> ~20k small dicts) are buffered so
        # save_graph can bulk_write them in 1000-op batches. Re-running the
        # chunk is idempotent: every _id is build-scoped and upserted.
        # 6.0.0: this chunk's events in ONE query, keyed by frame url (the
        # streamed entries carry no _id — see kg_store.events_for). Small:
        # a few KB per entry, unlike the entries themselves.
        events_by_url = store.events_for(chunk, snapshot_at)
        # 6.1.0: and its Wikidata links, the same way — one query, keyed by
        # url, only the fields build.py uses.
        entities_by_url = store.entity_links_for(chunk, snapshot_at)
        nodes: list[dict] = []
        edges: list[dict] = []
        seen = 0
        for entry in store.iter_entries(chunk, snapshot_at):
            seen += 1
            n, e = kg_build.build_nodes_and_edges(
                entry, origin_resolver=origin_resolver, tag_denylist=tag_denylist,
                events=events_by_url.get(entry.get("url"), ()),
                entities=entities_by_url.get(entry.get("url"), ()))
            nodes.extend(n)
            edges.extend(e)
        written = store.save_graph(proceed["build_id"], nodes, edges)
        written["entries"] = seen
        written["frames_with_events"] = len(events_by_url)
        written["frames_with_entities"] = len(entities_by_url)
        log.info("Chunk done — %s", written)
        return written

    @task
    def materialize_stubs(proceed: dict, chunk_stats: list[dict]) -> dict:
        return store.materialize_stubs(proceed["build_id"])

    # -- Phase 3: the concept layer -------------------------------------------
    @task
    def census(proceed: dict) -> dict:
        from modules.kg import census as kg_census
        snapshot_at = datetime.fromisoformat(proceed["snapshot_at"])
        return kg_census.run_census(
            store.iter_field("entry_type", snapshot_at), "entry_type")

    @task
    def load_taxonomy(proceed: dict, entry_type_census: dict) -> dict:
        from modules.kg import taxonomy
        tax = taxonomy.load(TAXONOMY_PATH)
        report = taxonomy.validate(tax, entry_type_census)
        if report["slugs_missing_from_census"]:
            # Data, not stdout: it reaches the summary and the dashboard.
            log.warning("Taxonomy slugs absent from the corpus: %s",
                        report["slugs_missing_from_census"])
        return {**report, **tax.summary(),
                "edges": taxonomy.encodable_edges(tax, entry_type_census)}

    @task
    def write_concept_edges(proceed: dict, tax: dict) -> dict:
        return store.save_concept_edges(proceed["build_id"], tax["edges"])

    @task
    def census_origin(proceed: dict) -> dict:
        """Raw origin values — normalize=None, so curators (and
        origin.validate's canonical_census) see the actual fragmentation,
        not a pre-cleaned view. Always empty pair_cooccurrence: origin is
        one string per frame (kg/census.py's module docstring)."""
        from modules.kg import census as kg_census
        snapshot_at = datetime.fromisoformat(proceed["snapshot_at"])
        return kg_census.run_census(
            store.iter_field("origin", snapshot_at), "origin")

    @task
    def load_origin_taxonomy(proceed: dict, origin_census: dict) -> dict:
        from modules.kg import origin
        tax = origin.load(ORIGIN_TAXONOMY_PATH)
        report = origin.validate(tax, origin_census)
        if report["slugs_missing_from_census"]:
            log.warning("Origin hierarchy narrower slugs absent from the "
                       "corpus: %s", report["slugs_missing_from_census"])
        return {**report, "edges": origin.encodable_edges(tax, origin_census)}

    @task
    def write_origin_concept_edges(proceed: dict, origin_tax: dict) -> dict:
        return store.save_concept_edges(proceed["build_id"], origin_tax["edges"])

    @task
    def census_tags(proceed: dict) -> dict:
        """Same folded normalize kg/build.py's tag-minting loop uses
        (kg/tag_normalize.py, same curated denylist), so a coOccursWith
        edge's endpoints are exactly the tag_concept node ids the graph
        actually has — not the raw, unfolded strings census.py's own
        default FIELDS["tags"].normalize (curator-facing) would give."""
        from modules.kg import census as kg_census, tag_normalize
        snapshot_at = datetime.fromisoformat(proceed["snapshot_at"])
        denylist = tag_normalize.load_denylist(TAG_DENYLIST_PATH)
        def normalize(v: str) -> str:
            return tag_normalize.fold(v.strip().lower(), denylist)
        return kg_census.run_census(
            store.iter_field("tags", snapshot_at), "tags", normalize=normalize)

    @task
    def write_cooccurs_edges(proceed: dict, tags_census: dict) -> dict:
        """Tags only (5.0.1) — entry_type's coOccursWith was removed:
        needless alongside its curated subTypeOf hierarchy. Tags keep it;
        they have no curated alternative. Property-graph-only either way
        (tag_concept has no RDF resource) — see kg/rdf.py."""
        from modules.kg import cooccurs
        edges = cooccurs.edges_from_census(tags_census, "tag:")
        return store.save_concept_edges(proceed["build_id"], edges)

    # -- Phase 4: every file representation, from the same stream ------------
    @task(execution_timeout=timedelta(minutes=30))
    def write_exports(snap: dict, proceed: dict, stubs: dict, concepts: dict,
                      origin_concepts: dict, cooccurs_written: dict,
                      params: dict | None = None) -> dict:
        from modules.kg import serialize
        p = params or {}
        bid = proceed["build_id"]
        out_dir = _build_dir(bid)
        os.makedirs(out_dir, exist_ok=True)
        # assume_unique: the store keys every node and edge by a unique _id,
        # so serialize's dedupe sets (millions of entries at this size)
        # would only cost memory.
        manifest = serialize.write_build(
            lambda: store.iter_nodes(bid), lambda: store.iter_edges(bid),
            out_dir, build_id=bid, stamps=snap["stamps"],
            exclude_kinds=p.get("exclude_kinds") or (),
            top_tags=p.get("top_tags", 0),
            ontology_path=ONTOLOGY_PATH, assume_unique=True)
        log.info("Exports written to %s: %s", out_dir, manifest["counts"])
        return manifest

    # -- Phase 5: the external graph stores, generationally --------------------
    @task(execution_timeout=timedelta(minutes=30))
    def load_fuseki(proceed: dict, manifest: dict) -> dict:
        if not _fuseki_enabled():
            raise AirflowSkipException("FUSEKI_PASSWORD unset — Fuseki follower disabled")
        bid = proceed["build_id"]
        cfg = loaders.FusekiConfig.from_env()
        with _http() as http:
            loaded = loaders.fuseki_load(http, cfg, bid, _graph_nt(bid))
            triples = loaders.fuseki_count(http, cfg, loaders.fuseki_graph_iri(bid))
            # The vocabulary is not part of any one build; loading it with
            # each one keeps the endpoint's copy equal to the tracked file.
            loaders.fuseki_load_ontology(http, cfg, ONTOLOGY_PATH)
        log.info("Fuseki: %s triples in %s", triples, loaded["graph"])
        return {**loaded, "triples": triples,
                "ontology_graph": loaders.ONTOLOGY_GRAPH}

    @task(execution_timeout=timedelta(minutes=45))
    def load_neo4j(proceed: dict, stubs: dict, concepts: dict,
                   origin_concepts: dict, cooccurs_written: dict) -> dict:
        if not _neo4j_enabled():
            raise AirflowSkipException("NEO4J_PASSWORD unset — Neo4j follower disabled")
        bid = proceed["build_id"]
        cfg = loaders.Neo4jConfig.from_env()
        driver = loaders.neo4j_driver(cfg)
        try:
            loaders.neo4j_ensure_schema(driver, cfg)
            loaded = loaders.neo4j_load(driver, cfg, bid,
                                        store.iter_nodes(bid), store.iter_edges(bid))
            counts = loaders.neo4j_counts(driver, cfg, bid)
        finally:
            driver.close()
        log.info("Neo4j: loaded %s, now holds %s", loaded, counts)
        return counts

    # -- Phase 6: verify, then flip -------------------------------------------
    @task(trigger_rule="none_failed")
    def verify(proceed: dict, manifest: dict, fuseki: dict | None = None,
               neo4j: dict | None = None) -> dict:
        """The counts that must agree before anything is published.

        Mongo is what was written; the manifest is what was serialized; the
        RML rows are what morph-kgc will read; Fuseki and Neo4j are what the
        followers hold. If any pair disagrees, the representations describe
        different graphs — precisely the drift this stage exists to make
        impossible — and every pointer stays put.
        """
        from modules.kg import serialize
        bid = proceed["build_id"]
        counts = store.graph_counts(bid)
        problems: list[str] = []
        mc = manifest["counts"]

        if counts["nodes"] != mc["nodes"]:
            problems.append(f"nodes: mongo={counts['nodes']} manifest={mc['nodes']}")
        if counts["edges"] != mc["edges"]:
            problems.append(f"edges: mongo={counts['edges']} manifest={mc['edges']}")
        files = manifest["files"]
        for etype, (name, _) in serialize.EDGE_TYPE_TO_RML_FILE.items():
            if etype == "subTypeOf":
                # Also one property-graph edge type, two id namespaces
                # (type:/origin:) — but unlike coOccursWith, BOTH reach RDF,
                # just through two different files with two different
                # subject/object templates (kymt:<slug> vs
                # mk:origin/<slug>) — see serialize.py's
                # ORIGIN_SUBTYPE_RML_FILE. The row count to compare against
                # Mongo's total is the sum of both files, not just
                # subtype_edges.csv's entry_type share.
                mongo_n = counts["edges_by_type"].get(etype, 0)
                origin_subtype_name = serialize.ORIGIN_SUBTYPE_RML_FILE[0]
                rml_n = (files.get(name, {}).get("rows", 0)
                        + files.get(origin_subtype_name, {}).get("rows", 0))
            else:
                mongo_n = counts["edges_by_type"].get(etype, 0)
                rml_n = files.get(name, {}).get("rows", 0)
            if mongo_n != rml_n:
                problems.append(f"{etype}: mongo={mongo_n} rml_rows={rml_n}")
        if files["frames.csv"]["rows"] != counts["frames"]:
            problems.append(f"frames: mongo={counts['frames']} "
                            f"frames.csv={files['frames.csv']['rows']}")
        if files["graph.nt"]["triples"] <= 0:
            problems.append("graph.nt is empty")
        if fuseki and fuseki.get("triples") != mc["triples"]:
            problems.append(f"fuseki: {fuseki.get('triples')} triples vs manifest {mc['triples']}")
        if neo4j:
            if neo4j.get("nodes") != counts["nodes"]:
                problems.append(f"neo4j nodes: {neo4j.get('nodes')} vs mongo {counts['nodes']}")
            if neo4j.get("edges") != counts["edges"]:
                problems.append(f"neo4j edges: {neo4j.get('edges')} vs mongo {counts['edges']}")

        if problems:
            store.fail_build(bid, "verify: " + "; ".join(problems))
            raise RuntimeError("KG verification failed: " + "; ".join(problems))
        store.mark_verified(bid)
        log.info("Verified build %s: %s (fuseki=%s, neo4j=%s)", bid, counts,
                 bool(fuseki), bool(neo4j))
        return {"build_id": bid, "counts": counts,
                "stores": {"fuseki": bool(fuseki), "neo4j": bool(neo4j)}}

    @task(retries=0)
    def publish(proceed: dict, manifest: dict, verified: dict,
                params: dict | None = None) -> dict:
        """Followers first, authority last; then read every pointer back.

        retries=0 — a torn publish must be loud — and every step is
        set-to-value, so a manual re-run converges rather than double-applies.
        """
        bid = proceed["build_id"]
        if not (params or {}).get("publish", True):
            log.info("publish=False: build %s verified, no pointer moved", bid)
            return {"build_id": bid, "published": False}

        flipped: dict[str, str | None] = {}
        flipped["files"] = _point_files_at(bid)["CURRENT"]
        if verified["stores"]["fuseki"]:
            cfg = loaders.FusekiConfig.from_env()
            with _http() as http:
                loaders.fuseki_publish(http, cfg, bid, _graph_nt(bid))
                flipped["fuseki"] = loaders.fuseki_current(http, cfg)
        if verified["stores"]["neo4j"]:
            cfg = loaders.Neo4jConfig.from_env()
            driver = loaders.neo4j_driver(cfg)
            try:
                loaders.neo4j_publish(driver, cfg, bid)
                flipped["neo4j"] = loaders.neo4j_current(driver, cfg)
            finally:
                driver.close()
        store.publish_build(bid, manifest)                 # the authority, last
        flipped["mongo"] = store.current_build_id()
        flipped["files"] = _files_current()

        torn = {k: v for k, v in flipped.items() if v != bid}
        if torn:
            raise RuntimeError(f"publish of {bid} is torn — pointers disagree: {torn}")
        log.info("Published build %s in %s", bid, sorted(flipped))
        return {"build_id": bid, "published": True, "pointers": flipped}

    @task(retries=0)
    def prune(published: dict, verified: dict, params: dict | None = None) -> dict:
        keep = (params or {}).get("keep_builds", 2)
        result = store.prune_builds(keep=keep)
        live = {d["_id"] for d in store.get_store().builds.find(
            {"_id": {"$ne": store.CURRENT}}, {"_id": 1})}
        current = store.current_build_id()

        builds_dir = os.path.join(KG_DATA_DIR, "builds")
        removed = []
        if os.path.isdir(builds_dir):
            for name in sorted(os.listdir(builds_dir)):
                if name not in live and name != current:
                    shutil.rmtree(os.path.join(builds_dir, name), ignore_errors=True)
                    removed.append(name)
        result["dirs_removed"] = removed

        if verified["stores"]["fuseki"]:
            cfg = loaders.FusekiConfig.from_env()
            with _http() as http:
                result["fuseki"] = loaders.fuseki_prune(http, cfg, live)
        if verified["stores"]["neo4j"]:
            cfg = loaders.Neo4jConfig.from_env()
            driver = loaders.neo4j_driver(cfg)
            try:
                result["neo4j"] = loaders.neo4j_prune(driver, cfg, live)
            finally:
                driver.close()
        return result

    # -- Phase 7: measure the published graph ----------------------------------
    @task(execution_timeout=timedelta(minutes=30), retries=1)
    def compute_metrics(published: dict, pruned: dict) -> dict:
        """kg/metrics.py needs its graph in memory (~0.6 GB for the core:
        ~350k nodes / ~713k edges), which is why it runs after publish: a
        metrics OOM must never block a verified graph from going live. Only
        the core subgraph (METRICS_*) and only the fields metrics reads are
        loaded — section text alone would not fit."""
        import json
        from modules.kg import metrics
        bid = published["build_id"]
        nodes = {n["id"]: n for n in store.iter_nodes(
            bid, kinds=METRICS_NODE_KINDS, fields=METRICS_NODE_FIELDS)}
        # subTypeOf is ONE property-graph edge type shared by entry_type's
        # curated hierarchy (in the core) and origin's (5.0.0, NOT part of
        # the IMKG-comparable core — origin_concept isn't in
        # METRICS_NODE_KINDS). types=METRICS_EDGE_TYPES can't tell the two
        # apart by type name alone, so origin:-prefixed subTypeOf edges are
        # dropped here — otherwise they'd show up as dangling (their
        # origin_concept endpoints were never loaded into `nodes`) and
        # inflate edge/degree counts the core is supposed to stay stable
        # against.
        edges = [e for e in store.iter_edges(bid, types=METRICS_EDGE_TYPES, occurrences=False)
                if not (e["type"] == "subTypeOf" and e["src"].startswith("origin:"))]
        m = metrics.compute_metrics(nodes, edges)
        del nodes, edges
        out = os.path.join(_build_dir(bid), "metrics.json")
        with open(out + ".tmp", "w", encoding="utf-8") as fh:
            json.dump(m, fh, indent=2, default=str)
        os.replace(out + ".tmp", out)
        return {"replication": m["replication"], "integrity": {
            k: v for k, v in m["integrity"].items()
            if not k.endswith("_sample") and k != "series_chain_witness"}}

    # -- Phase 8: the run record ------------------------------------------------
    @task(trigger_rule="none_failed")
    def summarize(snap: dict, chunk_stats: list[dict] | None = None,
                  stubs: dict | None = None, tax: dict | None = None,
                  manifest: dict | None = None, fuseki: dict | None = None,
                  neo4j: dict | None = None, verified: dict | None = None,
                  published: dict | None = None,
                  metrics: dict | None = None,
                  origin_tax: dict | None = None,
                  cooccurs_written: dict | None = None) -> dict:
        if not snap["stale"]:
            summary = {"skipped": True, "reason": snap["reason"],
                       "build": {"build_id": None, "stamps": snap["stamps"]}}
            log.info("KG RUN SKIPPED — %s", snap["reason"])
            return summary

        run_totals: dict[str, int] = {}
        for s in chunk_stats or []:
            for k, v in s.items():
                run_totals[k] = run_totals.get(k, 0) + v
        run_totals["chunks"] = len(chunk_stats or [])
        run_totals.update(stubs or {})

        rep = (metrics or {}).get("replication", {})
        # The whole graph's counts come from verify (they are Mongo's, which
        # every other store was checked against); metrics measures the
        # IMKG-comparable core only, reported under "core".
        counts = (verified or {}).get("counts", {})
        summary = {
            "run": run_totals,
            "build": {"build_id": snap["build_id"],
                      "published": bool((published or {}).get("published")),
                      "pointers": (published or {}).get("pointers"),
                      **snap["stamps"]},
            "graph": {
                "nodes": counts.get("nodes"), "edges": counts.get("edges"),
                "frames": counts.get("frames"),
                "rel_types": len(counts.get("edges_by_type", {})) or None,
                "triples": (manifest or {}).get("counts", {}).get("triples"),
                "nodes_by_kind": counts.get("nodes_by_kind", {}),
                "edges_by_type": counts.get("edges_by_type", {}),
                "core": {
                    "nodes": rep.get("nodes"), "edges": rep.get("edges"),
                    "frames": rep.get("frames"), "rel_types": rep.get("rel_types"),
                    "avg_degree": rep.get("avg_degree"),
                },
            },
            "integrity": (metrics or {}).get("integrity", {}),
            "taxonomy": {k: (tax or {}).get(k) for k in (
                "taxonomy_version", "broader_edges", "edges_encoded",
                "slugs_missing_from_census", "withheld_pairs", "buckets")},
            "origin_taxonomy": {k: (origin_tax or {}).get(k) for k in (
                "taxonomy_version", "aliases_declared", "edges_declared",
                "edges_encoded", "slugs_missing_from_census", "buckets")},
            "cooccurs": {"edges_written":
                        (cooccurs_written or {}).get("concept_edges_written")},
            "stores": {
                "fuseki": ({"triples": fuseki.get("triples"), "graph": fuseki.get("graph")}
                           if fuseki else None),
                "neo4j": ({"nodes": neo4j.get("nodes"), "edges": neo4j.get("edges")}
                          if neo4j else None),
            },
            "exports": {
                "dir": _build_dir(snap["build_id"]),
                "triples": (manifest or {}).get("counts", {}).get("triples"),
                "view_filters": (manifest or {}).get("view_filters"),
                "files": {k: {kk: vv for kk, vv in v.items() if kk != "path"}
                          for k, v in (manifest or {}).get("files", {}).items()},
            },
        }
        log.info("KG RUN COMPLETE — build=%s graph=%s stores=%s",
                 snap["build_id"], summary["graph"], summary["stores"])
        return summary

    @task(trigger_rule="none_failed")
    def record_summary(summary: dict, run_id: str | None = None) -> str:
        """Give this run's stats a durable, queryable home in
        `run_summaries` — the collection the dashboard reads."""
        from modules import summary_store
        return summary_store.save_summary(
            stage="kg", dag_id="kym_kg",
            run_id=run_id or "manual", summary=summary)

    snap = snapshot()
    reconciled = reconcile(snap)
    proceed = gate(snap)
    chunks = chunk_entries(snap, proceed, reconciled)
    built = build_chunk.partial(proceed=proceed).expand(chunk=chunks)
    stubs = materialize_stubs(proceed, built)
    cen = census(proceed)
    tax = load_taxonomy(proceed, cen)
    concepts = write_concept_edges(proceed, tax)
    origin_cen = census_origin(proceed)
    origin_tax = load_origin_taxonomy(proceed, origin_cen)
    origin_concepts = write_origin_concept_edges(proceed, origin_tax)
    tags_cen = census_tags(proceed)
    cooccurs_written = write_cooccurs_edges(proceed, tags_cen)
    manifest = write_exports(snap, proceed, stubs, concepts, origin_concepts,
                             cooccurs_written)
    fus = load_fuseki(proceed, manifest)
    neo = load_neo4j(proceed, stubs, concepts, origin_concepts, cooccurs_written)
    verified = verify(proceed, manifest, fus, neo)
    published = publish(proceed, manifest, verified)
    pruned = prune(published, verified)
    measured = compute_metrics(published, pruned)
    summary = summarize(snap, built, stubs, tax, manifest, fus, neo, verified,
                        published, measured, origin_tax=origin_tax,
                        cooccurs_written=cooccurs_written)
    record_summary(summary)


kym_kg_dag()
