"""
kym_templates_dag.py — Airflow DAG over kg/templates.py + modules/template_store.py
====================================================================================
Top-level orchestration ONLY. What a template search, a relevance score, a
duplicate and a selection ARE lives in modules/kg/templates.py and
modules/kg/visual.py (no Mongo, no Airflow); the fetching in
modules/imgflip_client.py; the wiring in modules/template_search.py; all
persistence in modules/template_store.py. XCom carries only frame ids,
template ids and small stats dicts.

What a run produces
-------------------
For every eligible KYM frame — category meme, plus any frame that links to
imgflip or has a KYM Template section — a ``frame_templates`` doc: the
imgflip templates that fit it, 0 when none does (with the reason), else 1
to 10, most varied first, with near-identical uploads merged into one
(the others are kept in ``imgflip_templates`` as its duplicates, and never
reach the graph). Kept templates get their /memetemplate details and their
full-size image, which kym_template_entities reads.

Why this is its own DAG
-----------------------
  * **It talks to imgflip.** Its cadence and failure domain are an outside
    site's, like kym_scrape's, not the corpus'.
  * **Selection is global.** One picture's representative must be the same
    upload for every frame that selects it, so ``assign`` runs once, over
    every searched frame, after the mapped searches — offline, from what is
    stored, so a new threshold re-selects without a single request.
  * **Entity extraction is a separate DAG** (kym_template_entities), on the
    lab's GPU hosts at one request at a time. A lab outage must never fail
    a search, and a new lexicon re-links without re-running the VLM.

Pipeline:
    select_frames      eligible frames whose search is missing or stale,
                       known imgflip links first
    chunk_frames       frame IDS, not bodies
    search_chunk       (mapped, 2 at a time) imgflip search, thumbnails,
                       hashes, the frame's KYM image hashes -> Mongo
    assign             ONE task: score, dedup across all frames, select
    fetch_details      (mapped, 2 at a time) /memetemplate page + full image
                       of each newly kept template
    summarize / record_summary     -> run_summaries, stage="templates"
    trigger_kym_template_entities  only if trigger_template_entities=true

In the monthly chain kym_events triggers this DAG with batch_size 0,
research_after_days 0 (a frame is searched again only when its text or a
version stamp changed), trigger_template_entities and trigger_kg.

Trigger-time params:
    batch_size                 frames searched this run (0 = all pending)
    chunk_size                 frames per mapped task
    research_after_days        re-search a frame searched longer ago (0 = never)
    force_research             search even up-to-date frames
    reparse_only               re-derive from archived pages; fetch nothing new
    details_limit              kept templates to fetch details for (0 = all)
    sample_seed                non-zero: a random sample stratified by priority
    trigger_template_entities  trigger kym_template_entities when done (all
                               pending templates)
    trigger_kg                 ...and have it trigger kym_kg in turn (the chain's
                               end; off for a manual search batch)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from modules import template_store as store
from modules.kg import templates as kg_templates

log = logging.getLogger(__name__)

# imgflip is an outside site: two tasks at a time, each at one page every
# two seconds (imgflip_client's default), is about one request a second.
MAX_PARALLEL_FETCH_TASKS = 2

# Search pages older than this are fetched again.
PAGE_MAX_AGE_DAYS = 180

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="kym_templates",
    schedule=None,          # triggered by kym_events (the monthly chain), or by hand
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "imgflip", "templates"],
    params={
        "batch_size": Param(500, type="integer", minimum=0,
                            description="Frames searched this run (0 = all pending)."),
        "chunk_size": Param(50, type="integer", minimum=1,
                            description="Frames per mapped task."),
        "research_after_days": Param(180, type="integer", minimum=0,
                                     description="Re-search frames searched longer "
                                                 "ago than this (0 = never)."),
        "force_research": Param(False, type="boolean",
                                description="Search even up-to-date frames."),
        "reparse_only": Param(False, type="boolean",
                              description="Use archived pages only; fetch nothing."),
        "details_limit": Param(0, type="integer", minimum=0,
                               description="Kept templates to fetch details for "
                                           "(0 = all)."),
        "sample_seed": Param(0, type="integer", minimum=0,
                             description="Non-zero: search a random sample of batch_size "
                                         "frames, stratified by priority, instead of the "
                                         "highest-priority ones (to measure the pool)."),
        "trigger_template_entities": Param(False, type="boolean",
                                           description="Trigger kym_template_entities "
                                                       "when done."),
        "trigger_kg": Param(False, type="boolean",
                            description="With trigger_template_entities: have it "
                                        "trigger kym_kg when done (the chain's end)."),
    },
)
def kym_templates_dag():

    @task
    def select_frames(params: dict | None = None) -> dict:
        p = params or {}
        stamps = kg_templates.stamps()
        units = store.pending_units(stamps=stamps,
                                    research_after_days=p.get("research_after_days", 180),
                                    force=p.get("force_research", False),
                                    limit=p.get("batch_size", 500),
                                    sample_seed=p.get("sample_seed", 0))
        by_priority: dict[int, int] = {}
        for u in units:
            by_priority[u["priority"]] = by_priority.get(u["priority"], 0) + 1
        log.info("Selected %d frames to search (by priority %s)", len(units),
                 dict(sorted(by_priority.items())))
        return {"frame_ids": [u["unit_id"] for u in units], "stamps": stamps,
                "selected_at": datetime.now(timezone.utc).isoformat(),
                "force": bool(p.get("force_research", False)),
                "reparse_only": bool(p.get("reparse_only", False)),
                "by_priority": {str(k): v for k, v in sorted(by_priority.items())}}

    @task
    def chunk_frames(selected: dict, params: dict | None = None) -> list[list[str]]:
        size = (params or {}).get("chunk_size", 50)
        ids = selected["frame_ids"]
        chunks = [ids[i:i + size] for i in range(0, len(ids), size)]
        log.info("Split %d frames into %d chunks of ≤%d", len(ids), len(chunks), size)
        return chunks

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_FETCH_TASKS,
          execution_timeout=timedelta(hours=3))
    def search_chunk(chunk: list[str], selected: dict) -> dict:
        if not chunk:
            return {"frames": 0}
        from modules import imgflip_client, template_search

        with store.get_store() as st:
            units = st.units_for(chunk)
            # Re-filter first (the house rule): a retried chunk redoes nothing
            # already written.
            if selected.get("force"):
                todo = st.not_searched_since(units, selected["selected_at"])
            else:
                todo = st.select_pending(units, stamps=selected["stamps"])
            if kg_templates.stamps() != selected["stamps"]:
                raise RuntimeError(f"stamps moved since selection: {selected['stamps']} "
                                   f"-> {kg_templates.stamps()}")
            io = template_search.StoreIO(imgflip_client.make_client(), st,
                                         template_search.data_dir(),
                                         max_page_age_days=PAGE_MAX_AGE_DAYS,
                                         reparse_only=selected.get("reparse_only", False))
            tally = template_search.search_units(todo, io, selected["stamps"])
        tally["skipped"] = len(units) - len(todo)
        log.info("Chunk done — %s", tally)
        return tally

    @task(trigger_rule="all_done", execution_timeout=timedelta(hours=1))
    def assign(run_id: str | None = None) -> dict:
        """Global: every searched frame, not only this run's — a newly seen
        upload can become the representative of a picture other frames
        already selected. all_done: what the chunks that succeeded stored
        is selected even when one failed (the run still LOOKS failed,
        because that chunk did)."""
        from modules import template_search

        with store.get_store() as st:
            counts = template_search.run_assignment(st, run_id or "manual")
        log.info("Assignment — %s", counts)
        return counts

    @task
    def chunk_details(_assigned: dict, params: dict | None = None) -> list[list[int]]:
        limit = (params or {}).get("details_limit", 0)
        ids = store.templates_needing_details(limit)
        size = 100
        chunks = [ids[i:i + size] for i in range(0, len(ids), size)]
        log.info("%d kept templates need details, in %d chunks", len(ids), len(chunks))
        return chunks

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_FETCH_TASKS,
          execution_timeout=timedelta(hours=2))
    def fetch_details(chunk: list[int]) -> dict:
        if not chunk:
            return {"templates": 0}
        from modules import imgflip_client, template_search

        with store.get_store() as st:
            io = template_search.StoreIO(imgflip_client.make_client(), st,
                                         template_search.data_dir(),
                                         max_page_age_days=PAGE_MAX_AGE_DAYS)
            tally = template_search.fetch_details(chunk, io)
        return {**tally, "html_requests": io.client.requests["html"],
                "image_requests": io.client.requests["image"]}

    @task(trigger_rule="none_failed")
    def summarize(selected: dict, assigned: dict | None = None,
                  search_stats: list[dict] | None = None,
                  detail_stats: list[dict] | None = None) -> dict:
        def total(stats: list[dict] | None) -> dict[str, int]:
            out: dict[str, int] = {}
            for s in stats or []:
                for k, v in s.items():
                    if isinstance(v, (int, float)):
                        out[k] = out.get(k, 0) + v
            return out

        corpus = store.template_stats()
        summary = {"selected": len(selected["frame_ids"]),
                   "by_priority": selected["by_priority"], "stamps": selected["stamps"],
                   "search": total(search_stats), "assignment": assigned or {},
                   "details": total(detail_stats), "corpus": corpus}
        log.info("TEMPLATES RUN COMPLETE — %s", summary)
        return summary

    @task(trigger_rule="none_failed")
    def record_summary(summary: dict, run_id: str | None = None) -> str:
        from modules import summary_store
        return summary_store.save_summary(
            stage="templates", dag_id="kym_templates",
            run_id=run_id or "manual", summary=summary)

    @task.short_circuit(trigger_rule="none_failed")
    def should_trigger_template_entities(params: dict | None = None) -> bool:
        wanted = bool((params or {}).get("trigger_template_entities", False))
        if not wanted:
            log.info("trigger_template_entities=false — leaving the entity stage alone")
        return wanted

    @task
    def template_entities_conf(params: dict | None = None) -> dict:
        """Every pending template; the KG only when this run is the chain's
        (trigger_kg). A manual search batch feeds the reader as it finishes
        (kym_template_entities runs one at a time, so triggered runs queue)
        and leaves publishing to a separate decision."""
        return {"batch_size": 0, "trigger_kg": bool((params or {}).get("trigger_kg", False))}

    entities_conf = template_entities_conf()
    trigger_entities = TriggerDagRunOperator(
        task_id="trigger_kym_template_entities",
        trigger_dag_id="kym_template_entities",
        conf=entities_conf,     # an XComArg: resolved to the dict at run time
        wait_for_completion=False,
        trigger_rule="none_failed",
    )

    selected = select_frames()
    chunks = chunk_frames(selected)
    searched = search_chunk.partial(selected=selected).expand(chunk=chunks)
    assigned = assign()
    searched >> assigned
    detail_chunks = chunk_details(assigned)
    details = fetch_details.expand(chunk=detail_chunks)
    summary = summarize(selected, assigned, searched, details)
    record_summary(summary) >> should_trigger_template_entities() >> entities_conf


kym_templates_dag()
