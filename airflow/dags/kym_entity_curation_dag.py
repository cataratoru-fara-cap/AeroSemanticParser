"""
kym_entity_curation_dag.py — Airflow DAG over kg/curation.py (gap 09)
======================================================================
Top-level orchestration ONLY. What makes a linked entity relevant is
modules/kg/curation.py (the rules, the curated lists in
kg_config/entity_curation.yaml, the judge's prompt); persistence is
modules/entity_curation_store.py. XCom carries only frame ids and small
stats dicts.

What a run produces
-------------------
For every frame with Wikidata links (kym_entities), one ``entity_curation``
doc: a keep/drop decision per link, with its basis — a rule (title,
own_item, platform, title_agrees, tag_and_text, deny_item, deny_class) or
the LLM judge. Only KEPT links reach the graph (kg_store.entity_links_for);
until the judge has read a frame, only its rule-kept links do.

Why its own DAG
---------------
  * **The judge is a model on the lab's hosts**: ~20k frames, a judge
    call (ministral-3:14b) and, for About-only keeps, a confirming call
    to the same model with another prompt; two chunks at a time.
  * **Two cadences**: the rules re-run over the whole corpus in seconds
    whenever the links or the lists change; the judge only re-asks when its
    prompt, schema or model changes, or new items appear.

Pipeline:
    select_rules      frames whose curation is missing or stale
    rules_chunk       (mapped, 4 at a time) the lexicon's classes + the lists
    select_judge      frames with items the judge has not answered
    judge_chunk       (mapped, 2 at a time) the LLMs -> JSONL -> Mongo
    summarize / record_summary   -> run_summaries, stage="entity_curation"
    trigger_kym_events  the monthly chain (kym_entities -> here -> kym_events),
                        unless trigger_events=false

Trigger-time params:
    judge          ask the judge (false = rules only)
    judge_limit    frames judged this run (0 = all pending)
    chunk_size     frames per judge task
    force_rules    re-run the rules on every frame
    force_judge    re-ask the judge about every rule-pending item
    trigger_events trigger kym_events when done (all pending sections)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from modules import entity_curation_store as store
from modules.kg import curation as kc

log = logging.getLogger(__name__)

MAX_PARALLEL_RULE_TASKS = 4
# Two: calls are short (ministral-3:14b, ~1 s) and the host serves several
# at once; 22 -> 30 frames a minute going from one task to two (2026-09-29).
MAX_PARALLEL_JUDGE_TASKS = 2

CONFIG_DIR = os.getenv("KG_CONFIG_DIR", "/opt/airflow/dags/kg_config")
LISTS_PATH = os.path.join(CONFIG_DIR, "entity_curation.yaml")
SCHEMA_PATH = os.path.join(CONFIG_DIR, "entity_curation_schema.json")
LEXICON_PATH = os.getenv("WIKIDATA_LEXICON", "/opt/airflow/data/wikidata/lexicon.sqlite")
OUT_DIR = os.path.join(os.getenv("KG_DATA_DIR", "/opt/airflow/data/kg"), "entity_curation")

DEFAULT_ARGS = {"owner": "gabi", "retries": 1, "retry_delay": timedelta(minutes=10)}


def _judge_stamps() -> dict[str, str]:
    _schema, sha = kc.load_schema(SCHEMA_PATH)
    return kc.judge_stamps(kc.model_request(), sha, kc.confirm_request())


@dag(
    dag_id="kym_entity_curation",
    schedule=None,          # triggered by kym_entities (the monthly chain), or by hand
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "entities", "curation", "llm"],
    params={
        "judge": Param(True, type="boolean", description="Ask the LLM judge (false = rules only)."),
        "judge_limit": Param(0, type="integer", minimum=0,
                             description="Frames judged this run (0 = all pending)."),
        "chunk_size": Param(200, type="integer", minimum=1,
                            description="Frames per judge task."),
        "force_rules": Param(False, type="boolean", description="Re-run the rules on every frame."),
        "force_judge": Param(False, type="boolean",
                             description="Re-ask the judge about every rule-pending item."),
        "trigger_events": Param(True, type="boolean",
                                description="Trigger kym_events when done (the monthly chain)."),
    },
)
def kym_entity_curation_dag():

    @task
    def select_rules(params: dict | None = None) -> dict:
        from modules.kg.wikidata import Lexicon

        p = params or {}
        if not os.path.exists(LEXICON_PATH):
            # As kym_entities: say so and go on, so the chain is never blocked.
            log.warning("No lexicon at %s — the rules wait for it this run", LEXICON_PATH)
            return {"stamps": None, "chunks": []}
        lists = kc.load_lists(LISTS_PATH)
        with Lexicon(LEXICON_PATH) as lexicon:
            stamps = kc.rule_stamps(lists, lexicon.version)
        ids = store.pending_rules(stamps, force=p.get("force_rules", False))
        log.info("%d frames need the rules (%s)", len(ids), stamps)
        return {"stamps": stamps, "chunks": [ids[i:i + 2000] for i in range(0, len(ids), 2000)]}

    @task
    def rule_chunks(selected: dict) -> list[list[str]]:
        return selected["chunks"]

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_RULE_TASKS,
          execution_timeout=timedelta(hours=1))
    def rules_chunk(chunk: list[str], selected: dict) -> dict:
        from modules.kg.wikidata import Lexicon

        lists = kc.load_lists(LISTS_PATH)
        with Lexicon(LEXICON_PATH) as lexicon:
            if kc.rule_stamps(lists, lexicon.version) != selected["stamps"]:
                raise RuntimeError("the curation lists or the lexicon changed since selection")
            classes = kc.ClassIndex(lexicon)
            results = [(r, kc.apply_rules(r, lists, classes)) for r in store.records_for(chunk)]
        tally = {"frames": len(results)}
        for _r, rules in results:
            for d in rules:
                tally[d["basis"]] = tally.get(d["basis"], 0) + 1
        tally.update(store.save_rules(results, selected["stamps"]))
        return tally

    @task(trigger_rule="all_done")
    def select_judge(_rules=None, params: dict | None = None) -> dict:
        p = params or {}
        if not p.get("judge", True):
            log.info("judge=false — rules only this run")
            return {"chunks": [], "stamps": None}
        stamps = _judge_stamps()
        ids = store.pending_judge(stamps, force=p.get("force_judge", False),
                                  limit=p.get("judge_limit", 0))
        size = p.get("chunk_size", 200)
        log.info("%d frames for the judge (%s)", len(ids), stamps)
        return {"stamps": stamps, "chunks": [ids[i:i + size] for i in range(0, len(ids), size)],
                "run_dir": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}

    @task
    def judge_chunks(selected: dict) -> list[list[str]]:
        return selected["chunks"]

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_JUDGE_TASKS,
          execution_timeout=timedelta(hours=3))
    def judge_chunk(chunk: list[str], selected: dict, ti=None) -> dict:
        from modules.openwebui_client import LLMConfig, OpenWebUIClient

        stamps = _judge_stamps()
        if stamps != selected["stamps"]:
            raise RuntimeError(f"judge stamps moved: {selected['stamps']} -> {stamps}")
        schema, _sha = kc.load_schema(SCHEMA_PATH)
        client = OpenWebUIClient(LLMConfig.from_env())
        request, confirm = kc.model_request(), kc.confirm_request()
        map_index = getattr(ti, "map_index", 0) if ti is not None else 0
        out_path = os.path.join(OUT_DIR, selected["run_dir"], f"chunk-{max(map_index, 0):05d}.jsonl")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        tally = {"frames": 0, "items": 0, "kept": 0, "dropped": 0, "failed": 0}
        for unit in store.judge_units_for(chunk, stamps):
            result = kc.judge_frame(client, request, unit["context"], unit["items"],
                                    schema=schema, confirm=confirm)
            with open(out_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"frame_id": unit["frame_id"], **stamps,
                                     "items": unit["items"], **result},
                                    ensure_ascii=False) + "\n")
            if result["ok"]:
                got = store.save_judge(unit["frame_id"], result, stamps,
                                       merge=unit["stamps_current"])
                tally["frames"] += 1
                tally["items"] += len(unit["items"])
                tally["kept"] += got["kept"]
                tally["dropped"] += got["dropped"]
            else:
                store.save_failure(unit["frame_id"], result, stamps)
                tally["failed"] += 1
                log.warning("%s: %s: %s", unit["frame_id"], result["error_kind"], result["error"])
        log.info("Chunk done — %s", tally)
        return tally

    @task(trigger_rule="none_failed")
    def summarize(rules: list[dict] | None = None, judged: list[dict] | None = None) -> dict:
        def total(stats):
            out: dict[str, int] = {}
            for s in stats or []:
                for k, v in s.items():
                    out[k] = out.get(k, 0) + v
            return out

        summary = {"rules": total(rules), "judge": total(judged),
                   "corpus": store.curation_stats()}
        log.info("ENTITY CURATION RUN COMPLETE — %s", summary)
        return summary

    @task(trigger_rule="none_failed")
    def record_summary(summary: dict, run_id: str | None = None) -> str:
        from modules import summary_store
        return summary_store.save_summary(stage="entity_curation", dag_id="kym_entity_curation",
                                          run_id=run_id or "manual", summary=summary)

    @task.short_circuit(trigger_rule="none_failed")
    def should_trigger_events(params: dict | None = None) -> bool:
        wanted = bool((params or {}).get("trigger_events", True))
        if not wanted:
            log.info("trigger_events=false — leaving kym_events alone this run")
        return wanted

    # batch_size 0: a chained run extracts every pending section, not
    # kym_events' manual default of 500. Pending means new or changed text
    # (or a new prompt/schema), so nothing already extracted is re-asked.
    trigger_events = TriggerDagRunOperator(task_id="trigger_kym_events",
                                           trigger_dag_id="kym_events",
                                           conf={"batch_size": 0},
                                           wait_for_completion=False,
                                           trigger_rule="none_failed")

    selected = select_rules()
    ruled = rules_chunk.partial(selected=selected).expand(chunk=rule_chunks(selected))
    to_judge = select_judge(ruled)
    judged = judge_chunk.partial(selected=to_judge).expand(chunk=judge_chunks(to_judge))
    summary = summarize(ruled, judged)
    record_summary(summary) >> should_trigger_events() >> trigger_events


kym_entity_curation_dag()
