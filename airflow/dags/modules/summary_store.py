"""
summary_store.py — MongoDB persistence for per-run DAG summaries
=================================================================
Self-contained like dom_store.py, and the ONLY place any stage touches
the ``run_summaries`` collection. Every summarize task already computes
a small stats dict; this module gives that dict a durable home so the
dashboard can draw *trends across runs* instead of being limited to a
single-run snapshot. XCom keeps carrying the same small dict — this is
an additional sink, not a replacement.

Ownership note: three DAGs write here, but only through this module and
each under its own ``stage`` value — the "one owner module per
collection" rule holds, the owner just happens to serve every stage.

Collection
----------
``run_summaries``  (owned by this module)
    _id           "<stage>:<run_id>" — a retried summarize task upserts
                  its own document instead of duplicating it
    stage         'discovery' | 'scrape' | 'parse' | ... (free-form)
    dag_id        the DAG that produced the summary
    run_id        Airflow run id
    created_at    first write for this (stage, run_id); kept on retries
    updated_at    last write
    summary_json  the summary dict as a JSON string. A string, not a
                  sub-document, because summary keys (namespace paths,
                  error type names) are arbitrary strings, and Mongo
                  field names are not a safe home for arbitrary strings.

Connection settings come from the environment (docker-compose), same
variable names as the other stores:
    MONGODB_URI                       (default: mongodb://localhost:27017)
    MONGODB_DB                        (default: memes)
    MONGODB_RUN_SUMMARIES_COLLECTION  (default: run_summaries)
"""

from __future__ import annotations

import json
import logging
from typing import Any

from modules.mongo_base import MongoStoreBase, as_utc, now_utc

log = logging.getLogger("summary_store")


class SummaryStore(MongoStoreBase):
    """Owner of the ``run_summaries`` collection."""

    def _configure(self) -> None:
        self.summaries = self.collection(
            "MONGODB_RUN_SUMMARIES_COLLECTION", "run_summaries")
        self.summaries.create_index([("stage", 1), ("created_at", 1)])

    # -- writes -------------------------------------------------------------

    def save(self, stage: str, dag_id: str, run_id: str,
             summary: dict[str, Any]) -> str:
        """Upsert one summary document; returns its _id."""
        doc_id = f"{stage}:{run_id}"
        now = now_utc()
        self.summaries.update_one(
            {"_id": doc_id},
            {"$set": {
                "stage": stage,
                "dag_id": dag_id,
                "run_id": run_id,
                "summary_json": json.dumps(summary, default=str),
                "updated_at": now,
             },
             "$setOnInsert": {"created_at": now}},
            upsert=True)
        return doc_id

    # -- reads --------------------------------------------------------------

    def history(self, stage: str, limit: int = 300) -> list[dict[str, Any]]:
        """
        The last ``limit`` summaries for a stage, oldest -> newest:
        [{"run_id", "created_at", "summary"}, ...]. Chronological order is
        what the dashboard's trend charts consume directly.

        ``_id`` is the tiebreaker because two runs can land in the same
        millisecond (a backfill, or a retried summarize task): sorting on
        created_at alone left their relative order up to Mongo, so the
        same history could come back in a different order twice.
        """
        cur = (self.summaries
               .find({"stage": stage},
                     {"_id": 0, "run_id": 1, "created_at": 1,
                      "summary_json": 1})
               .sort([("created_at", -1), ("_id", -1)])
               .limit(limit))
        rows = [{"run_id": d.get("run_id"),
                 "created_at": as_utc(d.get("created_at")),
                 "summary": json.loads(d.get("summary_json") or "{}")}
                for d in cur]
        rows.reverse()
        return rows


def get_store(uri: str | None = None,
              db_name: str | None = None) -> SummaryStore:
    return SummaryStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facade functions — the only calls the DAGs make (dom_store style)
# ---------------------------------------------------------------------------

def save_summary(stage: str, dag_id: str, run_id: str,
                 summary: dict[str, Any]) -> str:
    with get_store() as store:
        doc_id = store.save(stage, dag_id, run_id, summary)
        log.info("Saved run summary %s", doc_id)
        return doc_id


def load_history(stage: str, limit: int = 300) -> list[dict[str, Any]]:
    with get_store() as store:
        rows = store.history(stage, limit=limit)
        log.info("Loaded %d historical summaries for stage=%s",
                 len(rows), stage)
        return rows