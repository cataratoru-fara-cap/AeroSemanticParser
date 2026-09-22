"""
kym_entities_dag.py — Airflow DAG over modules/kg/entities.py + modules/entity_store.py
=========================================================================================
Top-level orchestration ONLY. All recognition and linking logic lives in
modules/kg/entities.py and modules/kg/wikidata.py (no Mongo, no Airflow);
all persistence in modules/entity_store.py (the only place this DAG touches
the database). XCom carries only frame ids and small stats dicts.

What a run produces
-------------------
For every frame whose linking is missing or stale, one ``entities`` doc:
the named entities (and common-noun concepts) spaCy recognised in its
title and About section, and its tags, each linked to a Wikidata item from
the local lexicon — grounded to the characters it was read from.

The lexicon is a FILE, not a service: ``WIKIDATA_LEXICON`` (default
data/wikidata/lexicon.sqlite), built from a downloaded Wikidata dump by
``python -m modules.kg.wikidata build`` (see kg/wikidata.py). When it is not
there, this DAG links nothing and says so — and still triggers the next
stage, so a missing lexicon never blocks the event layer or the graph.

Why this is its own DAG, and not a task inside kym_kg
-----------------------------------------------------
The event layer's reasons, most of which still hold at a smaller scale:

  * **Cadence.** Links are invalidated by entry TEXT, the lexicon and the
    linker — not by the taxonomy or the tag denylist, which rebuild the
    graph. Re-running NLP over the corpus on a taxonomy edit is waste.
  * **Failure domain.** A missing or half-copied lexicon must not fail a
    graph build; here it only skips the linking.
  * **Curation needs the rows.** The `entities` collection keeps every
    link's features, and the NER spans nothing was found for — the input
    the planned curation step (gap 09) works from, which a graph build
    would throw away.

Pipeline (kym_parse -> kym_entities -> kym_events -> kym_kg):
    select_frames    entries -> frames whose linking is missing or stale
    chunk_frames     split into mapped workloads (frame IDS, not bodies)
    link_chunk       (mapped) re-filter, then spaCy + lexicon -> Mongo
    summarize / record_summary   -> run_summaries, stage="entities"
    trigger_kym_events           the cascade, unless trigger_events=false

Trigger-time params:
    batch_size      frames this run (0 = everything pending)
    chunk_size      frames per mapped task
    ready_only      restrict to corpus_status == "ready"
    force_relink    ignore staleness; re-link everything selected
    trigger_events  trigger kym_events at the end
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from modules import entity_store as store
from modules.kg import entities as kg_entities

log = logging.getLogger(__name__)

# CPU-bound and local: spaCy's small English model and a read-only SQLite
# file, no outbound HTTP. The worker has 3 GiB; each task holds one model
# (~150 MB) and the lexicon's page cache.
MAX_PARALLEL_LINK_TASKS = 4

LEXICON_PATH = os.getenv("WIKIDATA_LEXICON",
                         "/opt/airflow/data/wikidata/lexicon.sqlite")

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="kym_entities",
    schedule=None,          # triggered by kym_parse
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "kym", "entities"],
    params={
        "batch_size": Param(0, type="integer", minimum=0,
                            description="Frames this run (0 = all pending)."),
        "chunk_size": Param(1000, type="integer", minimum=1,
                            description="Frames per mapped task."),
        "ready_only": Param(False, type="boolean",
                            description="Only corpus_status == 'ready' entries."),
        "force_relink": Param(False, type="boolean",
                              description="Re-link even up-to-date frames."),
        "trigger_events": Param(True, type="boolean",
                                description="Trigger kym_events when done."),
    },
)
def kym_entities_dag():

    @task
    def select_frames(params: dict | None = None) -> dict:
        from modules.kg.wikidata import Lexicon

        p = params or {}
        selected_at = datetime.now(timezone.utc).isoformat()
        if not os.path.exists(LEXICON_PATH):
            # Degrade, don't fail: see the module docstring.
            log.warning("No Wikidata lexicon at %s — linking nothing this run. "
                        "Build one with `python -m modules.kg.wikidata build`.",
                        LEXICON_PATH)
            return {"frame_ids": [], "stamps": None, "lexicon": None,
                    "selected_at": selected_at, "force": False}
        with Lexicon(LEXICON_PATH) as lexicon:
            stamps = {"linker_version": kg_entities.LINKER_VERSION,
                      "lexicon_version": lexicon.version,
                      "nlp_model": kg_entities.model_stamp()}
            lexicon_meta = {k: lexicon.meta.get(k) for k in (
                "version", "dump", "dump_newest_modified", "entities", "built_at")}
        units = store.pending_units(stamps=stamps,
                                    ready_only=p.get("ready_only", False),
                                    force=p.get("force_relink", False),
                                    limit=p.get("batch_size", 0))
        log.info("Selected %d frames to link against lexicon %s (%s)",
                 len(units), stamps["lexicon_version"], lexicon_meta)
        return {"frame_ids": [u["unit_id"] for u in units], "stamps": stamps,
                "lexicon": lexicon_meta, "selected_at": selected_at,
                "force": bool(p.get("force_relink", False))}

    @task
    def chunk_frames(selected: dict, params: dict | None = None) -> list[list[str]]:
        size = (params or {}).get("chunk_size", 1000)
        ids = selected["frame_ids"]
        chunks = [ids[i:i + size] for i in range(0, len(ids), size)]
        log.info("Split %d frames into %d chunks of ≤%d", len(ids), len(chunks), size)
        return chunks

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_LINK_TASKS,
          execution_timeout=timedelta(hours=1))
    def link_chunk(chunk: list[str], selected: dict) -> dict:
        if not chunk:
            return {"units": 0, "mentions": 0, "skipped": 0}
        from modules.kg.wikidata import Lexicon

        units = store.units_for(chunk)
        # Re-filter first (the house rule), so a retried chunk redoes
        # nothing already written. A forced run asks "written since this run
        # selected?" instead — every forced frame is up to date by its stamps.
        with store.get_store() as st:
            if selected.get("force"):
                todo = st.not_linked_since(units, selected["selected_at"])
            else:
                todo = st.select_pending(units, stamps=selected["stamps"])
        skipped = len(units) - len(todo)
        if not todo:
            return {"units": 0, "mentions": 0, "skipped": skipped}

        with Lexicon(LEXICON_PATH) as lexicon:
            linker = kg_entities.Linker(lexicon, kg_entities.load_nlp())
            if linker.stamps != selected["stamps"]:
                # The lexicon or the model changed between select and link
                # (a rebuild landed mid-run). Linking now would stamp frames
                # with versions this run did not select for — fail, re-run.
                raise RuntimeError(f"stamps moved since selection: "
                                   f"{selected['stamps']} -> {linker.stamps}")
            buffer: list[dict] = []

            def persist(record: dict) -> None:
                buffer.append(record)
                if len(buffer) >= store.BULK_BATCH:
                    store.save_links(buffer)
                    buffer.clear()

            summary = kg_entities.link_units(linker, todo, on_record=persist)
            if buffer:
                store.save_links(buffer)
        tallies = {"units": summary["units"], "mentions": summary["mentions"],
                   "skipped": skipped, "nil": summary["nil"],
                   "rejected": summary["rejected"],
                   "frames_with_links": summary["frames_with_links"],
                   "self_links": summary["self_links"]}
        log.info("Chunk done — %s by field %s by method %s", tallies,
                 summary["by_field"], summary["by_method"])
        return tallies

    @task(trigger_rule="none_failed")
    def summarize(selected: dict, chunk_stats: list[dict] | None = None) -> dict:
        run_totals: dict[str, int] = {}
        for s in chunk_stats or []:
            for k, v in s.items():
                run_totals[k] = run_totals.get(k, 0) + v
        corpus = store.entity_stats()
        log.info("ENTITIES RUN COMPLETE — run=%s corpus=%s", run_totals, corpus)
        return {"run": run_totals, "corpus": corpus,
                "stamps": selected["stamps"], "lexicon": selected["lexicon"],
                "selected": len(selected["frame_ids"])}

    @task(trigger_rule="none_failed")
    def record_summary(summary: dict, run_id: str | None = None) -> str:
        from modules import summary_store
        return summary_store.save_summary(
            stage="entities", dag_id="kym_entities",
            run_id=run_id or "manual", summary=summary)

    @task.short_circuit(trigger_rule="none_failed")
    def should_trigger_events(params: dict | None = None) -> bool:
        """The cascade switch, as kym_parse's and kym_events' are. none_failed:
        a run with a failed chunk must LOOK failed (kym_events explains the
        silent-success trap), and what did land is durable either way."""
        wanted = bool((params or {}).get("trigger_events", True))
        if not wanted:
            log.info("trigger_events=false — leaving kym_events alone this run")
        return wanted

    trigger_events = TriggerDagRunOperator(
        task_id="trigger_kym_events",
        trigger_dag_id="kym_events",
        wait_for_completion=False,
        trigger_rule="none_failed",
    )

    selected = select_frames()
    chunks = chunk_frames(selected)
    stats = link_chunk.partial(selected=selected).expand(chunk=chunks)
    summary = summarize(selected, stats)
    record_summary(summary) >> should_trigger_events() >> trigger_events


kym_entities_dag()
