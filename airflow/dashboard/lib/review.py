"""
review.py — the ONE place the dashboard writes.

Everywhere else the dashboard is strictly read-only, and lib/data.py says
why: a viewer has no business issuing DDL or mutating pipeline state.
This module is the documented exception, and it is narrow on purpose:

  * it touches ``event_reviews`` and nothing else — a collection that
    holds no pipeline state, only human verdicts;
  * it creates no indexes and no collections (``review_store.py`` on the
    Airflow side owns those, and draws the sample);
  * it reads whole documents rather than joining: a drawn section already
    carries its numbered sentences and its events, precisely so the
    dashboard needs none of the pipeline's code to render it.

A review tool is data entry, not a dashboard. It lives here because this
is the surface the reviewer can already reach (the campus firewall leaves
one port open, and the proxy on it serves this app).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import streamlit as st
from pymongo import MongoClient, ReturnDocument

SAMPLES = ("representative", "relative", "unconfirmed")
FIELDS = ("event", "date", "location", "actors", "certainty")
VERDICTS = {
    "event": ("ok", "not-an-event", "wrong-span"),
    "date": ("ok", "wrong", "missing"),
    "location": ("ok", "wrong", "missing"),
    "actors": ("ok", "wrong", "incomplete"),
    "certainty": ("ok", "wrong"),
}


@st.cache_resource
def _client() -> MongoClient:
    return MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
                       serverSelectionTimeoutMS=5000, appname="kym-review")


def _coll():
    db = _client()[os.getenv("MONGODB_DB", "memes")]
    return db[os.getenv("MONGODB_EVENT_REVIEWS_COLLECTION", "event_reviews")]


def progress() -> dict[str, Any]:
    coll = _coll()
    out: dict[str, Any] = {"by_sample": {}}
    for sample in SAMPLES:
        out["by_sample"][sample] = {
            "sections": coll.count_documents({"samples": sample}),
            "done": coll.count_documents({"samples": sample, "status": "done"}),
        }
    out["sections_total"] = coll.count_documents({})
    out["sections_done"] = coll.count_documents({"status": "done"})
    out["events_total"] = sum(
        len(d.get("events") or ()) for d in coll.find({}, {"events": 1}))
    return out


def queue(sample: str | None = None, include_done: bool = False) -> list[dict]:
    """Section ids in reading order, so the reviewer can move about."""
    query: dict[str, Any] = {} if include_done else {"status": "pending"}
    if sample and sample != "all":
        query["samples"] = sample
    return list(_coll().find(query, {"frame_url": 1, "source_section": 1,
                                     "samples": 1, "status": 1}).sort("_id", 1))


def section(unit_id: str) -> dict | None:
    return _coll().find_one({"_id": unit_id})


def save(unit_id: str, *, verdicts: dict, missed_sentences, note: str,
         reviewer: str, elapsed_s: float | None) -> dict | None:
    """Record a verdict. Idempotent: re-reviewing a section overwrites it,
    which is how a second pass over the same 20 sections works."""
    return _coll().find_one_and_update(
        {"_id": unit_id},
        {"$set": {"verdicts": verdicts,
                  "missed_sentences": sorted({int(i) for i in missed_sentences}),
                  "note": note or "", "reviewer": reviewer or "",
                  "status": "done", "elapsed_s": elapsed_s,
                  "reviewed_at": datetime.now(timezone.utc)}},
        return_document=ReturnDocument.AFTER)
