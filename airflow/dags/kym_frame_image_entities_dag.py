"""
kym_frame_image_entities_dag.py — Airflow DAG over kg/frame_images.py
=====================================================================
Top-level orchestration ONLY. How an entry's own image is read is
modules/kg/frame_images.py (the frame prompt; grounding, audit and linking
are kg/template_entities.py's); persistence is modules/frame_image_store.py.
XCom carries only frame URLs and small stats dicts.

What a run produces
-------------------
For every frame with an image (its og:image, KYM's entry icon), one
``frame_image_entities`` doc: what the lab's vision model sees in it —
named people and characters, animals, objects, logos, artworks, printed
text — each with its box and linked to Wikidata. kym_kg turns the kept
links into ``m4s:fromImage`` from the FRAME, which is what IMKG's paper
queries ask (KG 7.1.0). The JSONL under
``data/kg/frame_images/entities/<run>/chunk-<n>.jsonl`` is written first,
so a lost collection can be rebuilt without a model call.

Its own DAG, after kym_template_entities, for the template reader's
reasons: one call at a time on the lab's shared GPU (~5 s an image, so
~34 h for the 24,291 frames of 7.0.0), and two levels of staleness — a new
prompt, schema, model or image re-reads; a new lexicon or linker re-links.

Pipeline:
    select_frames       frames whose reading is missing or stale
    chunk_frames
    detect_chunk        (mapped) download the image if not cached, vision
                        model -> JSONL -> Mongo
    select_links / link_chunk   lexicon -> Mongo
    summarize / record_summary  -> run_summaries, stage="frame_images"
    trigger_kym_wikidata_statements   only if trigger_kg=true (the monthly
                        chain passes it): the statements of the items just
                        linked, then kym_kg

Trigger-time params: batch_size (0 = all pending), chunk_size,
force_redetect, link_only, trigger_kg — as kym_template_entities.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from modules import frame_image_store as store
from modules.kg import frame_images as kg_fi
from modules.kg import template_entities as kg_te

log = logging.getLogger(__name__)

# One reader per host that serves the model (kym_template_entities' reasoning).
MAX_PARALLEL_DETECT_TASKS = max(1, int(os.getenv("KG_TEMPLATES_VLM_PARALLEL", "1") or 1))
MAX_PARALLEL_LINK_TASKS = 4

ENTITIES_DIR = os.path.join(os.getenv("KG_DATA_DIR", "/opt/airflow/data/kg"),
                            "frame_images", "entities")
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
    dag_id="kym_frame_image_entities",
    schedule=None,          # triggered by kym_template_entities (the monthly chain), or by hand
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "kym", "frames", "entities", "llm"],
    params={
        "batch_size": Param(200, type="integer", minimum=0,
                            description="Frames read this run (0 = all pending)."),
        "chunk_size": Param(25, type="integer", minimum=1,
                            description="Frames per mapped task."),
        "force_redetect": Param(False, type="boolean",
                                description="Re-read even up-to-date frames."),
        "link_only": Param(False, type="boolean",
                           description="Skip the vision model; only (re)link."),
        "trigger_kg": Param(False, type="boolean",
                            description="Carry the chain on to kym_kg (through "
                                        "kym_wikidata_statements)."),
    },
)
def kym_frame_image_entities_dag():

    @task
    def select_frames(params: dict | None = None) -> dict:
        p = params or {}
        _schema, schema_sha = kg_te.load_schema(SCHEMA_PATH)
        request = kg_fi.model_request()
        stamps = {"reader_version": kg_fi.READER_VERSION,
                  "prompt_version": kg_fi.PROMPT_VERSION, "schema_sha": schema_sha,
                  "requested_model": request.model}
        selected_at = datetime.now(timezone.utc)
        units = [] if p.get("link_only") else store.pending_detection(
            stamps=stamps, force=p.get("force_redetect", False),
            limit=p.get("batch_size", 200))
        log.info("Selected %d frames to read with %s", len(units), request.model)
        return {"frame_urls": [u["frame_url"] for u in units], "stamps": stamps,
                "selected_at": selected_at.isoformat(),
                "run_dir": selected_at.strftime("%Y%m%dT%H%M%SZ"),
                "force": bool(p.get("force_redetect", False))}

    @task
    def chunk_frames(selected: dict, params: dict | None = None) -> list[list[str]]:
        size = (params or {}).get("chunk_size", 25)
        urls = selected["frame_urls"]
        return [urls[i:i + size] for i in range(0, len(urls), size)]

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_DETECT_TASKS,
          execution_timeout=timedelta(hours=3))
    def detect_chunk(chunk: list[str], selected: dict, ti=None) -> dict:
        if not chunk:
            return {"frames": 0}
        from modules import imgflip_client
        from modules.openwebui_client import LLMConfig, OpenWebUIClient

        with store.get_store() as st:
            units = st.units_for(chunk)
            todo = (st.not_detected_since(units, selected["selected_at"])
                    if selected.get("force") else st.select_pending(units, selected["stamps"]))
        schema, schema_sha = kg_te.load_schema(SCHEMA_PATH)
        if schema_sha != selected["stamps"]["schema_sha"]:
            raise RuntimeError("the entity schema changed since selection — re-run")
        map_index = max(getattr(ti, "map_index", 0) if ti is not None else 0, 0)
        out_path = os.path.join(ENTITIES_DIR, selected["run_dir"], f"chunk-{map_index:05d}.jsonl")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        client = OpenWebUIClient(LLMConfig.from_env().rotated(map_index))
        request = kg_fi.model_request()
        images = imgflip_client.make_client()
        tally = {"frames": 0, "regions": 0, "failed": 0, "no_image": 0, "downloaded": 0,
                 "skipped": len(units) - len(todo), "blind": 0}
        for unit in todo:
            image = store.read_image(unit)
            if image is None:
                got = images.fetch_image(unit["image_url"])
                if not got.ok:
                    # Not dead-lettered: a download can fail for reasons that
                    # pass (the CDN, the network); the next run tries again.
                    tally["no_image"] += 1
                    log.warning("frame %s: image %s: %s", unit["frame_url"],
                                unit["image_url"], got.error)
                    continue
                store.write_image(unit["image_url"], got.content)
                image = got.content
                tally["downloaded"] += 1
            record = kg_fi.detect(client, request, unit, image, schema=schema,
                                  schema_sha=schema_sha)
            with open(out_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            if record["ok"]:
                store.save_detection(record)
                tally["frames"] += 1
                tally["regions"] += len(record["regions"])
                tally["blind"] += "blind" in record
            else:
                store.save_failure(record)
                tally["failed"] += 1
                log.warning("frame %s: %s: %s", unit["frame_url"],
                            record.get("error_kind"), record.get("error"))
        log.info("Chunk done — %s", tally)
        return tally

    @task(trigger_rule="all_done")
    def select_links(_detected=None) -> list[list[str]]:
        """all_done: frames read before a failed chunk are linked anyway."""
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
        urls = store.pending_linking(stamps)
        log.info("%d frames to (re)link", len(urls))
        return [urls[i:i + 200] for i in range(0, len(urls), 200)]

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_LINK_TASKS,
          execution_timeout=timedelta(hours=1))
    def link_chunk(chunk: list[str]) -> dict:
        if not chunk:
            return {"frames": 0}
        from modules.kg import entities as kg_entities
        from modules.kg.wikidata import Lexicon

        units = store.link_units_for(chunk)
        records = []
        with Lexicon(LEXICON_PATH) as lexicon:
            linker = kg_entities.Linker(lexicon, kg_entities.load_nlp(),
                                        senses=kg_entities.load_senses(SENSES_PATH))
            for u in units:
                records.append(kg_fi.link_detection(
                    linker, u["detection"], context_text=u["context_text"],
                    prefer=u["prefer"], link_context_sha=u["link_context_sha"]))
        store.save_links(records)
        return {"frames": len(records),
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

        summary = {"selected": len(selected["frame_urls"]), "stamps": selected["stamps"],
                   "detect": total(detected), "link": total(linked),
                   "corpus": store.entity_stats()}
        log.info("FRAME IMAGE ENTITIES RUN COMPLETE — %s", summary)
        return summary

    @task(trigger_rule="none_failed")
    def record_summary(summary: dict, run_id: str | None = None) -> str:
        from modules import summary_store
        return summary_store.save_summary(
            stage="frame_images", dag_id="kym_frame_image_entities",
            run_id=run_id or "manual", summary=summary)

    @task.short_circuit(trigger_rule="none_failed")
    def should_trigger_kg(params: dict | None = None) -> bool:
        wanted = bool((params or {}).get("trigger_kg", False))
        if not wanted:
            log.info("trigger_kg=false — leaving kym_kg alone this run")
        return wanted

    trigger_kg = TriggerDagRunOperator(
        task_id="trigger_kym_wikidata_statements",
        trigger_dag_id="kym_wikidata_statements",
        conf={"trigger_kg": True},
        wait_for_completion=False,
        trigger_rule="none_failed",
    )

    selected = select_frames()
    chunks = chunk_frames(selected)
    detected = detect_chunk.partial(selected=selected).expand(chunk=chunks)
    link_chunks = select_links(detected)
    linked = link_chunk.expand(chunk=link_chunks)
    summary = summarize(selected, detected, linked)
    record_summary(summary) >> should_trigger_kg() >> trigger_kg


kym_frame_image_entities_dag()
