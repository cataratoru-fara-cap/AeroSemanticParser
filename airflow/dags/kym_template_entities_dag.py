"""
kym_template_entities_dag.py — Airflow DAG over kg/template_entities.py
========================================================================
Top-level orchestration ONLY. What a template shows and how it is read is
modules/kg/template_entities.py (the vision model's prompt, the grounding,
the blind audit, the linking); persistence is
modules/template_entity_store.py. XCom carries only template ids and small
stats dicts.

What a run produces
-------------------
For every template some frame keeps (kym_templates), one
``template_entities`` doc: what the lab's vision model sees in the image —
named people and characters, animals, objects, logos, artworks, printed
text — each with its box, and each linked to a Wikidata item from the
local lexicon. The JSONL artifact under
``data/kg/templates/entities/<run>/chunk-<n>.jsonl`` is written first, so
a lost collection can be rebuilt without a single model call.

Why this is its own DAG
-----------------------
  * **The GPU.** One call at a time on the lab's shared hosts (the
    kym_events reasoning): ~30 s a template with qwen3-vl:32b, so a full
    pass is days, and must never hold up a search or a graph build.
  * **Two levels of staleness.** A new prompt, schema, model or image
    re-reads a template; a new lexicon or linker only re-LINKS it — minutes
    of CPU, no GPU.

Pipeline:
    select_templates    kept templates whose reading is missing or stale
    chunk_templates
    detect_chunk        (mapped, 1 at a time) vision model -> JSONL -> Mongo
    select_links        templates whose links are missing or stale
    link_chunk          (mapped, 4 at a time) lexicon -> Mongo
    summarize / record_summary   -> run_summaries, stage="template_entities"
    trigger_kym_kg      only if trigger_kg=true

Trigger-time params:
    batch_size       templates read this run (0 = all pending)
    chunk_size       templates per mapped task
    force_redetect   re-read even up-to-date templates
    link_only        skip the model; only (re)link what is already read
    trigger_kg       trigger kym_kg when done
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from modules import template_entity_store as store
from modules.kg import template_entities as kg_te

log = logging.getLogger(__name__)

# Readers at a time, one per host that serves the model: each task starts
# on a different host (rotated host priority; the model is pinned by digest,
# so a host without it passes the task to one that has it). Measured
# 2026-09-29: two readers on ollama-ccdd alone give the same 11.8 templates
# a minute as one (the host serves qwen3-vl:32b one request at a time), so
# the default is 1; set KG_TEMPLATES_VLM_PARALLEL=2 once ollama-ui serves it.
MAX_PARALLEL_DETECT_TASKS = max(1, int(os.getenv("KG_TEMPLATES_VLM_PARALLEL", "1") or 1))
MAX_PARALLEL_LINK_TASKS = 4

ENTITIES_DIR = os.path.join(os.getenv("KG_DATA_DIR", "/opt/airflow/data/kg"),
                            "templates", "entities")
SCHEMA_PATH = os.path.join(os.getenv("KG_CONFIG_DIR", "/opt/airflow/dags/kg_config"),
                           "template_entity_schema.json")
LEXICON_PATH = os.getenv("WIKIDATA_LEXICON", "/opt/airflow/data/wikidata/lexicon.sqlite")
SENSES_PATH = os.path.join(os.getenv("KG_CONFIG_DIR", "/opt/airflow/dags/kg_config"),
                           "entity_senses.yaml")

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="kym_template_entities",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "imgflip", "templates", "entities", "llm"],
    params={
        "batch_size": Param(200, type="integer", minimum=0,
                            description="Templates read this run (0 = all pending)."),
        "chunk_size": Param(25, type="integer", minimum=1,
                            description="Templates per mapped task."),
        "force_redetect": Param(False, type="boolean",
                                description="Re-read even up-to-date templates."),
        "link_only": Param(False, type="boolean",
                           description="Skip the vision model; only (re)link."),
        "trigger_kg": Param(False, type="boolean",
                            description="Trigger kym_kg when done."),
    },
)
def kym_template_entities_dag():

    @task
    def select_templates(params: dict | None = None) -> dict:
        p = params or {}
        _schema, schema_sha = kg_te.load_schema(SCHEMA_PATH)
        request = kg_te.model_request()
        stamps = {"extractor_version": kg_te.EXTRACTOR_VERSION,
                  "prompt_version": kg_te.PROMPT_VERSION, "schema_sha": schema_sha,
                  "requested_model": request.model}
        selected_at = datetime.now(timezone.utc)
        if p.get("link_only"):
            units = []
        else:
            units = store.pending_detection(stamps=stamps,
                                            force=p.get("force_redetect", False),
                                            limit=p.get("batch_size", 200))
        log.info("Selected %d templates to read with %s", len(units), request.model)
        return {"template_ids": [u["template_id"] for u in units], "stamps": stamps,
                "selected_at": selected_at.isoformat(),
                "run_dir": selected_at.strftime("%Y%m%dT%H%M%SZ"),
                "force": bool(p.get("force_redetect", False))}

    @task
    def chunk_templates(selected: dict, params: dict | None = None) -> list[list[int]]:
        size = (params or {}).get("chunk_size", 25)
        ids = selected["template_ids"]
        return [ids[i:i + size] for i in range(0, len(ids), size)]

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_DETECT_TASKS,
          execution_timeout=timedelta(hours=3))
    def detect_chunk(chunk: list[int], selected: dict, ti=None) -> dict:
        if not chunk:
            return {"templates": 0}
        from modules.openwebui_client import LLMConfig, OpenWebUIClient

        with store.get_store() as st:
            units = st.units_for(chunk)
            todo = (st.not_detected_since(units, selected["selected_at"])
                    if selected.get("force") else st.select_pending(units, selected["stamps"]))
        schema, schema_sha = kg_te.load_schema(SCHEMA_PATH)
        if schema_sha != selected["stamps"]["schema_sha"]:
            raise RuntimeError("the entity schema changed since selection — re-run")
        map_index = getattr(ti, "map_index", 0) if ti is not None else 0
        out_path = os.path.join(ENTITIES_DIR, selected["run_dir"],
                                f"chunk-{max(map_index, 0):05d}.jsonl")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        client = OpenWebUIClient(LLMConfig.from_env().rotated(max(map_index, 0)))
        request = kg_te.model_request()
        tally = {"templates": 0, "regions": 0, "failed": 0, "no_image": 0,
                 "skipped": len(units) - len(todo), "blind": 0}
        for unit in todo:
            image = store.read_image(unit)
            if image is None:
                tally["no_image"] += 1
                continue
            record = kg_te.detect(client, request, unit, image, schema=schema,
                                  schema_sha=schema_sha)
            with open(out_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            if record["ok"]:
                store.save_detection(record)
                tally["templates"] += 1
                tally["regions"] += len(record["regions"])
                tally["blind"] += "blind" in record
            else:
                store.save_failure(record)
                tally["failed"] += 1
                log.warning("template %s: %s: %s", unit["template_id"],
                            record.get("error_kind"), record.get("error"))
        log.info("Chunk done — %s", tally)
        return tally

    @task(trigger_rule="all_done")
    def select_links(_detected=None) -> list[list[int]]:
        """all_done: templates read before a failed chunk are linked anyway."""
        from modules.kg import entities as kg_entities
        from modules.kg.wikidata import Lexicon

        if not os.path.exists(LEXICON_PATH):
            log.warning("No Wikidata lexicon at %s — linking nothing", LEXICON_PATH)
            return []
        with Lexicon(LEXICON_PATH) as lexicon:
            stamps = {"linker_version": kg_entities.LINKER_VERSION,
                      "lexicon_version": lexicon.version,
                      "nlp_model": kg_entities.model_stamp(),
                      "senses_version": kg_entities.load_senses(SENSES_PATH).version,
                      "template_link_version": kg_te.TEMPLATE_LINK_VERSION}
        ids = store.pending_linking(stamps)
        log.info("%d templates to (re)link", len(ids))
        return [ids[i:i + 200] for i in range(0, len(ids), 200)]

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_LINK_TASKS,
          execution_timeout=timedelta(hours=1))
    def link_chunk(chunk: list[int]) -> dict:
        if not chunk:
            return {"templates": 0}
        from modules.kg import entities as kg_entities
        from modules.kg.wikidata import Lexicon

        units = store.link_units_for(chunk)
        records = []
        with Lexicon(LEXICON_PATH) as lexicon:
            linker = kg_entities.Linker(lexicon, kg_entities.load_nlp(),
                                        senses=kg_entities.load_senses(SENSES_PATH))
            for u in units:
                records.append(kg_te.link_detection(
                    linker, u["detection"], context_text=u["context_text"],
                    prefer=u["prefer"], link_context_sha=u["link_context_sha"]))
        store.save_links(records)
        return {"templates": len(records),
                "mentions": sum(r["mention_count"] for r in records),
                "in_graph": sum(r["in_graph_count"] for r in records),
                "nil": sum(len(r["nil"]) for r in records)}

    @task(trigger_rule="none_failed")
    def summarize(selected: dict, detected: list[dict] | None = None,
                  linked: list[dict] | None = None) -> dict:
        def total(stats):
            out: dict[str, int] = {}
            for s in stats or []:
                for k, v in s.items():
                    out[k] = out.get(k, 0) + v
            return out

        summary = {"selected": len(selected["template_ids"]), "stamps": selected["stamps"],
                   "detect": total(detected), "link": total(linked),
                   "corpus": store.entity_stats()}
        log.info("TEMPLATE ENTITIES RUN COMPLETE — %s", summary)
        return summary

    @task(trigger_rule="none_failed")
    def record_summary(summary: dict, run_id: str | None = None) -> str:
        from modules import summary_store
        return summary_store.save_summary(
            stage="template_entities", dag_id="kym_template_entities",
            run_id=run_id or "manual", summary=summary)

    @task.short_circuit(trigger_rule="none_failed")
    def should_trigger_kg(params: dict | None = None) -> bool:
        wanted = bool((params or {}).get("trigger_kg", False))
        if not wanted:
            log.info("trigger_kg=false — the graph is built and published by hand")
        return wanted

    trigger_kg = TriggerDagRunOperator(
        task_id="trigger_kym_kg",
        trigger_dag_id="kym_kg",
        wait_for_completion=False,
        trigger_rule="none_failed",
    )

    selected = select_templates()
    chunks = chunk_templates(selected)
    detected = detect_chunk.partial(selected=selected).expand(chunk=chunks)
    link_chunks = select_links(detected)
    linked = link_chunk.expand(chunk=link_chunks)
    summary = summarize(selected, detected, linked)
    record_summary(summary) >> should_trigger_kg() >> trigger_kg


kym_template_entities_dag()
