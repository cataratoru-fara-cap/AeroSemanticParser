"""
kym_wikidata_statements_dag.py — Airflow DAG over kg/wikidata_statements.py
===========================================================================
Top-level orchestration ONLY: what a statement is lives in
modules/kg/wikidata_statements.py, persistence in
modules/wikidata_statement_store.py.

What a run produces
-------------------
For every Wikidata item a frame's text, a template's image or a frame's
image links to (32,486 on 2026-10-05), its truthy item-valued statements,
read from the same dated dump the lexicon was built from. kym_kg turns
them into edges named by their property (``P31``; RDF ``wdt:P31``), the
statements IMKG imported and its paper's queries read (KG 7.1.0).

Only what is missing is read: an item already extracted from this dump
under this rule is never read again, so a monthly run reads the dump only
when new items were linked. Two passes when needed, in one task:
    1. the wanted items' statements, labels, and every property's label;
    2. the labels of the statement VALUES the lexicon cannot name (items
       with no Wikipedia article), so every node in the graph has a label.
Each pass is ~1 h for the 156 GB dump.

Pipeline:
    select              items without statements under (dump, rule)
    extract             the two passes -> Mongo
    summarize / record_summary   -> run_summaries, stage="wikidata_statements"
    trigger_kym_kg      only if trigger_kg=true (the monthly chain passes it)
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from modules import wikidata_statement_store as store
from modules.kg import wikidata_statements as kg_ws

log = logging.getLogger(__name__)

LEXICON_PATH = os.getenv("WIKIDATA_LEXICON", "/opt/airflow/data/wikidata/lexicon.sqlite")

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _dump_and_stamps() -> tuple[str, dict[str, str]]:
    """The dump the lexicon was built from, and the extraction stamps."""
    from modules.kg.wikidata import Lexicon

    with Lexicon(LEXICON_PATH) as lexicon:
        dump = lexicon.meta["dump"]
    path = os.path.join(os.path.dirname(LEXICON_PATH), dump)
    return path, {"dump": dump, "statements_version": kg_ws.STATEMENTS_VERSION}


@dag(
    dag_id="kym_wikidata_statements",
    schedule=None,          # triggered by kym_frame_image_entities (the monthly chain), or by hand
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "wikidata", "kg"],
    params={
        "trigger_kg": Param(False, type="boolean", description="Trigger kym_kg when done."),
        "limit_lines": Param(0, type="integer", minimum=0,
                             description="Dump lines to read (0 = all; for a sample run)."),
    },
)
def kym_wikidata_statements_dag():

    @task
    def select() -> dict:
        path, stamps = _dump_and_stamps()
        if not os.path.exists(path):
            raise FileNotFoundError(f"the lexicon's dump {path} is not on disk")
        qids = store.pending(stamps)
        log.info("%d linked items need statements from %s", len(qids), stamps["dump"])
        return {"qids": qids, "stamps": stamps, "dump_path": path}

    @task(execution_timeout=timedelta(hours=5), retries=0)
    def extract(selected: dict, params: dict | None = None) -> dict:
        from modules.kg.wikidata import Lexicon

        qids, stamps, path = selected["qids"], selected["stamps"], selected["dump_path"]
        limit = (params or {}).get("limit_lines", 0)
        if not qids:
            return {"items": 0, "statements": 0, "value_labels": 0}
        first = kg_ws.scan(path, [int(q[1:]) for q in qids], statements=True,
                           limit_lines=limit)
        found = first["items"]
        # An item the dump no longer has is saved empty, so it is not read again.
        rows = [found.get(q) or {"id": q, "statements": []} for q in qids]
        store.save_items(rows, stamps)
        store.save_labels([{"id": p, "label": label}
                           for p, label in first["properties"].items()], stamps["dump"])
        values = sorted({f"Q{v}" for r in found.values() for _p, v in r["statements"]},
                        key=lambda q: int(q[1:]))
        with Lexicon(LEXICON_PATH) as lexicon:
            unnamed = [q for q in values if lexicon.entity(q) is None]
        unnamed = [q for q in unnamed if q not in store.labels_known(unnamed, stamps["dump"])]
        second = {"items": {}, "seconds": 0}
        if unnamed:
            log.info("%d statement values have no lexicon label; reading them", len(unnamed))
            second = kg_ws.scan(path, [int(q[1:]) for q in unnamed], statements=False,
                                limit_lines=limit)
            store.save_labels(second["items"].values(), stamps["dump"])
        return {"items": len(found), "missing_from_dump": len(qids) - len(found),
                "statements": sum(len(r["statements"]) for r in found.values()),
                "properties": len(first["properties"]), "values": len(values),
                "value_labels": len(second["items"]), "unnamed_values": len(unnamed),
                "lines": first["lines"], "seconds": first["seconds"] + second["seconds"]}

    @task(trigger_rule="none_failed")
    def summarize(selected: dict, extracted: dict | None = None) -> dict:
        summary = {"selected": len(selected["qids"]), "stamps": selected["stamps"],
                   "extract": extracted or {}, "corpus": store.stats()}
        log.info("WIKIDATA STATEMENTS RUN COMPLETE — %s", summary)
        return summary

    @task(trigger_rule="none_failed")
    def record_summary(summary: dict, run_id: str | None = None) -> str:
        from modules import summary_store
        return summary_store.save_summary(
            stage="wikidata_statements", dag_id="kym_wikidata_statements",
            run_id=run_id or "manual", summary=summary)

    @task.short_circuit(trigger_rule="none_failed")
    def should_trigger_kg(params: dict | None = None) -> bool:
        wanted = bool((params or {}).get("trigger_kg", False))
        if not wanted:
            log.info("trigger_kg=false — leaving kym_kg alone this run")
        return wanted

    trigger_kg = TriggerDagRunOperator(
        task_id="trigger_kym_kg",
        trigger_dag_id="kym_kg",
        wait_for_completion=False,
        trigger_rule="none_failed",
    )

    selected = select()
    extracted = extract(selected)
    summary = summarize(selected, extracted)
    record_summary(summary) >> should_trigger_kg() >> trigger_kg


kym_wikidata_statements_dag()
