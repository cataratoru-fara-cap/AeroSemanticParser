"""
kym_events_dag.py — Airflow DAG over modules/kg/events.py + modules/event_store.py
===================================================================================
Top-level orchestration ONLY. All extraction logic lives in
modules/kg/events.py (no Mongo, no Airflow); all persistence in
modules/event_store.py (the only place this DAG touches the database).
State between tasks lives in MongoDB; XCom carries only unit ids and small
stats dicts.

What a run produces
-------------------
For every (frame, SECTION) unit that is pending, one LLM call per unit, and
its result in two places at once:

    files   data/kg/events/<extract_id>/chunk-<map_index>.jsonl
            one line per unit, appended and flushed as it lands
    Mongo   one `events` doc per unit, written in the same loop

Mongo is the authority — it is what the KG build reads and what decides
what is still pending. The JSONL is the artifact: reviewable by hand,
greppable, and replayable into Mongo with no model
(``event_store.load_from_jsonl``) if the collection is ever lost.

Why this is its own DAG, and not a task inside kym_kg
-----------------------------------------------------
Four reasons, in order of how much they hurt:

  * **Wall clock.** ~36,500 units, one at a time (the hosts are FIFO — see
    MAX_PARALLEL_EVENT_TASKS), is days for the first full pass. Inside kym_kg, every graph build
    would be hostage to a GPU queue, and the staleness gate — whose entire
    job is to make a no-op run cheap — could never skip.
  * **Failure domain.** kym_kg makes no outbound HTTP except to Fuseki
    over the Docker network. Folding in a contended, 50 s-capped lab host
    means a GPU outage fails the GRAPH BUILD.
  * **Cadence.** kym_kg rebuilds when the taxonomy or the tag denylist is
    edited. Re-running 36,500 LLM calls because someone edited
    tag_normalization_exceptions.yaml would be absurd. Extraction is
    invalidated by entry TEXT; the taxonomy is not.
  * It is the shape the repo already has: one stage, one collection, one
    owner module — exactly as kym_parse owns `entries`.

During the initial backfill every kym_events run moves the KG's event
stamps, so kym_kg rebuilds the whole graph afterwards. That is correct but
expensive — and kym_kg publishes by default — so the backfill should run
with ``trigger_kg=false`` and build once at the end. Outside a backfill
(parse -> entities -> events -> kg on new pages) the default stays on.

Pipeline:
    select_units     entries -> pending (frame, section) units
    chunk_units      split into mapped workloads (unit IDS, not bodies)
    extract_chunk    (mapped) re-filter, then one LLM call per unit ->
                     JSONL line + Mongo doc, per unit, as it lands
    summarize / record_summary   -> run_summaries, stage="events"

Trigger-time params:
    batch_size        units this run (0 = everything pending)
    chunk_size        units per mapped task
    sections          which narrative sections to extract from
    ready_only        restrict to corpus_status == "ready"
    force_reextract   ignore staleness; re-ask for everything selected
    trigger_kg        trigger kym_kg at the end (turn OFF for backfill batches)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from modules import event_store as store
from modules.kg import events as kg_events

log = logging.getLogger(__name__)

# ONE extraction at a time, deliberately. The lab hosts (pagoda) have no
# load balancer and serve requests FIFO: a second concurrent call does not
# run alongside the first, it waits behind it — so parallel tasks buy no
# throughput, only longer per-call waits (which count against ollama-ui's
# 50 s proxy cut and our read timeout) and a longer queue for everyone else
# using the lab GPU. Throughput is the host's service rate; the only way to
# raise it is a faster call (kg/events.py v2 asks for far less output), not
# more callers. Measured 2026-09-18: four parallel tasks stalled a host for
# minutes while a single CLI stream ran at ~15 s per section.
MAX_PARALLEL_EVENT_TASKS = 1

EVENTS_DIR = os.path.join(os.getenv("KG_DATA_DIR", "/opt/airflow/data/kg"),
                          "events")
SCHEMA_PATH = os.path.join(os.getenv("KG_CONFIG_DIR", "/opt/airflow/dags/kg_config"),
                           "event_extraction_schema.json")

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _slug(run_id: str) -> str:
    """The run TYPE, not the whole run id — same convention as kym_kg's
    build_id, whose comment explains why (the id already has a timestamp)."""
    import re
    kind = run_id.split("__", 1)[0] if run_id else "manual"
    return re.sub(r"[^A-Za-z0-9]+", "_", kind).strip("_")[:24] or "manual"


@dag(
    dag_id="kym_events",
    schedule=None,          # triggered by kym_entities (parse -> entities -> events)
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "kym", "events"],
    params={
        "batch_size": Param(500, type="integer", minimum=0,
                            description="Units this run (0 = all pending). "
                                        "One unit ≈ one LLM call."),
        "chunk_size": Param(50, type="integer", minimum=1,
                            description="Units per mapped task."),
        "sections": Param(list(kg_events.SOURCE_SECTIONS), type="array",
                          items={"type": "string"},
                          description="Narrative sections to extract from."),
        "ready_only": Param(False, type="boolean",
                            description="Only corpus_status == 'ready' entries."),
        "force_reextract": Param(False, type="boolean",
                                 description="Re-ask even for up-to-date units."),
        "trigger_kg": Param(True, type="boolean",
                            description="Trigger kym_kg when done. Turn OFF "
                                        "for backfill batches: every batch moves "
                                        "the event stamps, so each would force a "
                                        "full graph rebuild (and publish)."),
    },
)
def kym_events_dag():

    # -- Phase 1: what still needs extracting? -------------------------------
    @task
    def select_units(params: dict | None = None,
                     run_id: str | None = None) -> dict:
        p = params or {}
        _schema, schema_sha = kg_events.load_schema(SCHEMA_PATH)
        units = store.pending_units(
            sections=p.get("sections") or kg_events.SOURCE_SECTIONS,
            ready_only=p.get("ready_only", False),
            prompt_version=kg_events.PROMPT_VERSION,
            extraction_version=kg_events.EXTRACTION_VERSION,
            schema_sha=schema_sha,
            force=p.get("force_reextract", False),
            limit=p.get("batch_size", 0),
        )
        extract_id = (f"ev_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_"
                      f"{_slug(run_id or 'manual')}")
        log.info("Selected %d units to extract (prompt=%s extraction=%s "
                 "schema=%s) into %s", len(units), kg_events.PROMPT_VERSION,
                 kg_events.EXTRACTION_VERSION, schema_sha, extract_id)
        # Only the IDS travel on XCom: a chunk of unit bodies is section
        # text, and the rule every DAG here states is that XCom carries ids
        # and small dicts. extract_chunk re-reads the bodies from Mongo.
        return {"extract_id": extract_id, "schema_sha": schema_sha,
                "selected_at": datetime.now(timezone.utc).isoformat(),
                "force": bool(p.get("force_reextract", False)),
                "unit_ids": [u["unit_id"] for u in units]}

    @task
    def chunk_units(selected: dict, params: dict | None = None) -> list[list[str]]:
        size = (params or {}).get("chunk_size", 50)
        ids = selected["unit_ids"]
        chunks = [ids[i:i + size] for i in range(0, len(ids), size)]
        log.info("Split %d units into %d chunks of ≤%d", len(ids), len(chunks), size)
        return chunks

    # -- Phase 2: extract (one mapped task per chunk) -------------------------
    @task(
        max_active_tis_per_dagrun=MAX_PARALLEL_EVENT_TASKS,
        execution_timeout=timedelta(hours=2),
        retries=1,
    )
    def extract_chunk(chunk: list[str], selected: dict,
                      params: dict | None = None, ti=None) -> dict:
        if not chunk:
            return {"units": 0, "events": 0, "failed": 0, "skipped": 0}

        from modules.openwebui_client import LLMConfig, OpenWebUIClient

        p = params or {}
        sections = p.get("sections") or kg_events.SOURCE_SECTIONS
        units = store.units_for(chunk, sections=sections)

        # Re-filter against Mongo first (the house rule), so a retried chunk
        # re-buys nothing already paid for — which here is GPU minutes, not
        # milliseconds. A FORCED run needs a different question: every unit
        # it selected is up to date by its stamps, so select_pending would
        # drop them all and the force would do nothing. For it, "done" means
        # written since this run selected its units.
        with store.get_store() as st:
            if selected.get("force"):
                todo = st.not_extracted_since(units, selected["selected_at"])
            else:
                todo = st.select_pending(
                    units, prompt_version=kg_events.PROMPT_VERSION,
                    extraction_version=kg_events.EXTRACTION_VERSION,
                    schema_sha=selected["schema_sha"])
        skipped = len(units) - len(todo)
        if not todo:
            log.info("Chunk already complete — %d units skipped", skipped)
            return {"units": 0, "events": 0, "failed": 0, "skipped": skipped}

        # One file per mapped task, named by map_index: a retried task
        # reopens the same file in append mode. NOT one shared file —
        # O_APPEND does not make multi-KB line writes atomic, so four
        # workers would interleave and corrupt it.
        map_index = getattr(ti, "map_index", 0) if ti is not None else 0
        out_path = os.path.join(EVENTS_DIR, selected["extract_id"],
                                f"chunk-{max(map_index, 0):05d}.jsonl")

        # Configuration is read here, where it is used — never at import.
        client = OpenWebUIClient(LLMConfig.from_env())
        # The model POLICY, not a name: KG_EVENTS_* plus "never a reasoning
        # model" (kg/events.py, "Model policy").
        request = kg_events.model_request()

        written = {"units": 0, "events": 0}

        def persist(record: dict) -> None:
            """Durable in Mongo the moment it is durable on disk."""
            tally = store.save_extraction([record])
            written["units"] += tally["units"]
            written["events"] += tally["events"]

        summary = kg_events.extract(
            client, todo, out_path, request, schema_path=SCHEMA_PATH,
            progress=lambda line: log.info("%s", line),
            on_record=persist)

        if summary["failed"]:
            store.save_failures(summary["failed"],
                                prompt_version=kg_events.PROMPT_VERSION,
                                extraction_version=kg_events.EXTRACTION_VERSION,
                                schema_sha=selected["schema_sha"])

        tallies = {
            "units": written["units"], "events": written["events"],
            "failed": summary["failed_count"], "skipped": skipped,
            "zero_event_units": summary["zero_event_units"],
            "dated_events": summary["dated_events"],
        }
        log.info("Chunk done — %s", tallies)
        return tallies

    # -- Phase 3: corpus-level summary ---------------------------------------
    @task(trigger_rule="none_failed")
    def summarize(selected: dict, chunk_stats: list[dict]) -> dict:
        run_totals: dict[str, int] = {}
        for s in chunk_stats or []:
            for k, v in s.items():
                run_totals[k] = run_totals.get(k, 0) + v
        corpus = store.event_stats()
        log.info("EVENTS RUN COMPLETE — run=%s corpus=%s", run_totals, corpus)
        return {"run": run_totals, "corpus": corpus,
                "extract_id": selected["extract_id"],
                "schema_sha": selected["schema_sha"],
                "prompt_version": kg_events.PROMPT_VERSION,
                "extraction_version": kg_events.EXTRACTION_VERSION,
                "selected": len(selected["unit_ids"])}

    # -- Phase 4: persist the summary for the dashboard ----------------------
    @task(trigger_rule="none_failed")
    def record_summary(summary: dict, run_id: str | None = None) -> str:
        """Give this run's stats a durable, queryable home in
        `run_summaries` — the collection the dashboard reads. summarize()
        already logged them; logs rotate, this does not."""
        from modules import summary_store
        return summary_store.save_summary(
            stage="events", dag_id="kym_events",
            run_id=run_id or "manual", summary=summary)

    @task.short_circuit(trigger_rule="none_failed")
    def should_trigger_kg(params: dict | None = None) -> bool:
        """The backfill switch.

        Every kym_events run that lands anything moves the KG's event
        stamps, so kym_kg's gate cannot skip it: a full rebuild — and, with
        kym_kg's defaults, a PUBLISH — per batch. During the initial
        backfill that is tens of rebuilds, and a published graph where a
        few percent of frames have events, which a consumer cannot tell
        apart from "those frames have no events". Run the batches with
        trigger_kg=false and let the last one (or a manual kym_kg) build.

        none_failed, NOT all_done: a run with a failed extraction chunk must
        LOOK failed. With all_done here and on the trigger, the only leaf
        task was skipped or succeeded, so Airflow called a run "success"
        while 21 of 60 sections had not been extracted (seen 2026-09-21) —
        exactly the silent coverage hole the 100% target exists to prevent.
        What DID land is durable in Mongo and the next run resumes from it,
        so nothing is lost by failing loudly and re-running.
        """
        wanted = bool((params or {}).get("trigger_kg", True))
        if not wanted:
            log.info("trigger_kg=false — leaving kym_kg alone this run")
        return wanted

    trigger_kg = TriggerDagRunOperator(
        task_id="trigger_kym_kg",
        trigger_dag_id="kym_kg",
        wait_for_completion=False,
        # none_failed: the graph is built from a COMPLETE extraction run.
        # A failed chunk leaves a hole in coverage that a later kym_kg
        # build would silently bake in; fix it and re-run (the sections
        # that landed are durable, so a re-run only redoes the rest).
        trigger_rule="none_failed",
    )

    selected = select_units()
    chunks = chunk_units(selected)
    stats = extract_chunk.partial(selected=selected).expand(chunk=chunks)
    summary = summarize(selected, stats)
    record_summary(summary) >> should_trigger_kg() >> trigger_kg


kym_events_dag()
