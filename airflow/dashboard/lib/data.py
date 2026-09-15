"""
data.py — read-only Mongo access for the dashboard.

Deliberately NOT importing the pipeline's store modules. Three reasons:

  1. Those stores call ``create_index`` on connect. A dashboard has no
     business issuing DDL against the pipeline's collections.
  2. They are written for a write path (upserts, staleness stamps); the
     dashboard only ever projects and aggregates.
  3. Keeping them out means the dashboard image does not need the Airflow
     tree mounted, so the two containers version independently.

What IS shared is the configuration: the same ``MONGODB_*`` environment
variable names the stores use, so collection names stay single-sourced in
docker-compose.

Two kinds of read, deliberately distinguished:

  * **current state** — aggregated live from ``urls``/``doms``/``entries``/
    ``parse_failures``. Always up to date, including work done since the
    last DAG run finished.
  * **history** — read from ``run_summaries``, one document per stage per
    run. This is the only source of *trend*; the live collections know
    what is true now, not what was true in July.

Both are cached (TTL 60s) so panning around the dashboard does not hammer
Mongo — the underlying data changes on DAG-run cadence, not per second.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import streamlit as st
from pymongo import MongoClient

CACHE_TTL = 60  # seconds

UNKNOWN_NAMESPACE = "unknown"


@st.cache_resource
def _client() -> MongoClient:
    """One pooled client per Streamlit process (cache_resource, not
    cache_data — a socket is not a serialisable value)."""
    return MongoClient(
        os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        serverSelectionTimeoutMS=5000,
        appname="kym-dashboard",
    )


def _db():
    return _client()[os.getenv("MONGODB_DB", "memes")]


def _coll(env_var: str, default: str):
    return _db()[os.getenv(env_var, default)]


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def ping() -> tuple[bool, str]:
    """(reachable, message). Called once at the top of every page so a
    connection problem shows up as a message rather than a stack trace."""
    try:
        _client().admin.command("ping")
        return True, ""
    except Exception as exc:
        return False, str(exc)


# ---------------------------------------------------------------------------
# Current state
# ---------------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL)
def discovery_state() -> dict[str, Any]:
    urls = _coll("MONGODB_URLS_COLLECTION", "urls")
    by_ns_confirmed: dict[str, dict[str, int]] = {}
    for d in urls.aggregate([
        {"$group": {"_id": {"ns": "$namespace", "c": "$Confirmed"},
                    "n": {"$sum": 1}}}
    ]):
        ns = d["_id"].get("ns") or UNKNOWN_NAMESPACE
        bucket = by_ns_confirmed.setdefault(ns, {"confirmed": 0, "unconfirmed": 0})
        bucket["confirmed" if d["_id"].get("c") else "unconfirmed"] += d["n"]

    total = sum(v["confirmed"] + v["unconfirmed"] for v in by_ns_confirmed.values())
    confirmed = sum(v["confirmed"] for v in by_ns_confirmed.values())
    return {
        "total": total,
        "confirmed": confirmed,
        "unconfirmed": total - confirmed,
        "lastmod_null": urls.count_documents(
            {"$or": [{"lastmod": None}, {"lastmod": {"$exists": False}}]}),
        "by_namespace": by_ns_confirmed,
    }


@st.cache_data(ttl=CACHE_TTL)
def scrape_state() -> dict[str, Any]:
    urls = _coll("MONGODB_URLS_COLLECTION", "urls")
    doms = _coll("MONGODB_DOMS_COLLECTION", "doms")
    ok = doms.count_documents({"scrape_status": "ok"})
    failed = doms.count_documents({"scrape_status": "failed"})
    permanent = doms.count_documents(
        {"scrape_status": "failed", "last_error_kind": "permanent"})
    by_error: dict[str, int] = {}
    for d in doms.aggregate([
        {"$match": {"scrape_status": "failed"}},
        {"$group": {"_id": "$last_error_kind", "n": {"$sum": 1}}},
    ]):
        by_error[d["_id"] or "unclassified"] = d["n"]

    size = list(doms.aggregate([
        {"$match": {"scrape_status": "ok"}},
        {"$group": {"_id": None, "bytes": {"$sum": "$content_length"},
                    "avg": {"$avg": "$content_length"}}},
    ]))
    return {
        "urls_confirmed": urls.count_documents({"Confirmed": True}),
        "doms_total": ok + failed,
        "doms_ok": ok,
        "doms_failed": failed,
        "failed_permanent": permanent,
        "failed_retryable": failed - permanent,
        "by_error_kind": by_error,
        "content_bytes": (size[0]["bytes"] if size else 0) or 0,
        "content_avg": (size[0]["avg"] if size else 0) or 0,
    }


@st.cache_data(ttl=CACHE_TTL)
def parse_state() -> dict[str, Any]:
    entries = _coll("MONGODB_ENTRIES_COLLECTION", "entries")
    failures = _coll("MONGODB_PARSE_FAILURES_COLLECTION", "parse_failures")

    total = entries.count_documents({})
    ready = entries.count_documents({"corpus_status": "ready"})

    def _counts(coll, field: str) -> dict[str, int]:
        """{value: count} for a scalar field, descending. Null/missing
        folds into 'unknown' — the same label discovery uses, so a
        namespace with no value reads the same everywhere."""
        return {(d["_id"] if d["_id"] is not None else UNKNOWN_NAMESPACE): d["n"]
                for d in coll.aggregate([
                    {"$group": {"_id": f"${field}", "n": {"$sum": 1}}},
                    {"$sort": {"n": -1}},
                ])}

    missing = {d["_id"]: d["n"] for d in entries.aggregate([
        {"$unwind": "$corpus_missing"},
        {"$group": {"_id": "$corpus_missing", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
    ])}

    return {
        "entries_total": total,
        "entries_ready": ready,
        "entries_incomplete": total - ready,
        "parse_failures": failures.count_documents({}),
        "missing_field_counts": missing,
        "failure_type_counts": _counts(failures, "error_type"),
        "failure_namespace_counts": _counts(failures, "namespace"),
        "by_category": _counts(entries, "category"),
        "by_status": _counts(entries, "status"),
        "parser_versions": _counts(entries, "parser_version"),
    }


@st.cache_data(ttl=CACHE_TTL)
def failure_samples(limit: int = 50) -> list[dict[str, Any]]:
    """A page of dead-letter records, newest first — the "what actually
    broke" drill-down behind the parse failure charts."""
    failures = _coll("MONGODB_PARSE_FAILURES_COLLECTION", "parse_failures")
    return [
        {"url": d.get("url"),
         "namespace": d.get("namespace") or UNKNOWN_NAMESPACE,
         "error_type": d.get("error_type"),
         "attempts": d.get("attempts"),
         "failed_at": _as_utc(d.get("failed_at")),
         "error": (d.get("error") or "")[:400]}
        for d in failures.find({}, {"_id": 0}).sort("failed_at", -1).limit(limit)
    ]


# ---------------------------------------------------------------------------
# History (run_summaries) — the only source of trend
# ---------------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL)
def run_history(stage: str | None = None, limit: int = 300) -> list[dict[str, Any]]:
    """Runs oldest -> newest. ``_id`` breaks created_at ties so the order
    is stable across calls (two runs can share a millisecond)."""
    coll = _coll("MONGODB_RUN_SUMMARIES_COLLECTION", "run_summaries")
    query = {"stage": stage} if stage else {}
    rows = [
        {"stage": d.get("stage"),
         "dag_id": d.get("dag_id"),
         "run_id": d.get("run_id"),
         "created_at": _as_utc(d.get("created_at")),
         "summary": json.loads(d.get("summary_json") or "{}")}
        for d in coll.find(query, {"_id": 0}).sort(
            [("created_at", -1), ("run_id", -1)]).limit(limit)
    ]
    rows.reverse()
    return rows


@st.cache_data(ttl=CACHE_TTL)
def stage_freshness() -> dict[str, dict[str, Any]]:
    """{stage: {run_id, created_at, age_hours}} for the most recent run of
    each stage — the "is this pipeline actually running" panel."""
    coll = _coll("MONGODB_RUN_SUMMARIES_COLLECTION", "run_summaries")
    out: dict[str, dict[str, Any]] = {}
    for stage in coll.distinct("stage"):
        doc = coll.find_one({"stage": stage},
                            {"_id": 0, "run_id": 1, "created_at": 1,
                             "dag_id": 1},
                            sort=[("created_at", -1)])
        if not doc:
            continue
        created = _as_utc(doc.get("created_at"))
        out[stage] = {
            "run_id": doc.get("run_id"),
            "dag_id": doc.get("dag_id"),
            "created_at": created,
            "age_hours": ((datetime.now(timezone.utc) - created).total_seconds()
                          / 3600) if created else None,
        }
    return out


def scalar_series(rows: list[dict[str, Any]], path: str) -> list[tuple[datetime, float]]:
    """Pull one dotted path (``corpus.entries_ready``) out of a run history
    as (time, value) points. Missing keys become gaps, not zeros — a stage
    that did not report a field in July did not report *zero*."""
    out: list[tuple[datetime, float]] = []
    for row in rows:
        node: Any = row.get("summary") or {}
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                node = None
                break
            node = node[part]
        if isinstance(node, (int, float)) and not isinstance(node, bool):
            out.append((row["created_at"], float(node)))
    return out
