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
from datetime import datetime, timedelta, timezone
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
def event_state() -> dict[str, Any]:
    """Current state of the event layer (kym_events), live from `events`
    and `event_failures`. Re-implements event_store.stats() rather than
    importing it, for the reasons at the top of this module.

    `events` holds one doc per (frame, section), each with an `events`
    array; per-event breakdowns unwind it.
    """
    events = _coll("MONGODB_EVENTS_COLLECTION", "events")
    failures = _coll("MONGODB_EVENT_FAILURES_COLLECTION", "event_failures")
    entries = _coll("MONGODB_ENTRIES_COLLECTION", "entries")

    def _unit_counts(coll, field: str) -> dict[str, int]:
        return {(d["_id"] if d["_id"] is not None else "unknown"): d["n"]
                for d in coll.aggregate([
                    {"$group": {"_id": f"${field}", "n": {"$sum": 1}}},
                    {"$sort": {"n": -1}},
                ])}

    def _event_counts(field: str) -> dict[str, int]:
        return {(d["_id"] if d["_id"] is not None else "unknown"): d["n"]
                for d in events.aggregate([
                    {"$unwind": "$events"},
                    {"$group": {"_id": f"$events.{field}", "n": {"$sum": 1}}},
                    {"$sort": {"n": -1}},
                ])}

    total = list(events.aggregate([
        {"$group": {"_id": None, "n": {"$sum": "$event_count"},
                    "latest": {"$max": "$extracted_at"}}}]))
    # Entries that HAVE an origin or spread section — the reachable
    # denominator. Coverage against all 23,882 entries would count the
    # ~23% with neither section as "not yet extracted" forever.
    with_narrative = entries.count_documents(
        {"sections.kind": {"$in": ["origin", "spread"]}})
    # The 100%-coverage denominator: every origin/spread section with text.
    possible = list(entries.aggregate([
        {"$unwind": "$sections"},
        {"$match": {"sections.kind": {"$in": ["origin", "spread"]},
                    "sections.text": {"$elemMatch": {"$regex": r"\S"}}}},
        {"$group": {"_id": {"e": "$_id", "k": "$sections.kind"}}},
        {"$count": "n"}]))
    return {
        "units_total": events.count_documents({}),
        "units_by_section": _unit_counts(events, "source_section"),
        "events_total": total[0]["n"] if total else 0,
        "last_extracted_at": _as_utc(total[0]["latest"]) if total else None,
        "zero_event_units": events.count_documents({"event_count": 0}),
        "frames_covered": len(events.distinct("entry_id")),
        "frames_with_narrative": with_narrative,
        "units_possible": possible[0]["n"] if possible else 0,
        "events_by_certainty": _event_counts("certainty"),
        "events_by_precision": _event_counts("date_precision"),
        "events_by_location_type": _event_counts("location_type"),
        "models_in_use": _unit_counts(events, "model"),
        "failures_total": failures.count_documents({}),
        "failure_kind_counts": _unit_counts(failures, "error_kind"),
    }


@st.cache_data(ttl=CACHE_TTL)
def event_places(ev: dict[str, Any]) -> list[str]:
    """An event's places: ``locations`` since extraction 3.0.0, a single
    ``location`` before it — both are in Mongo until the backfill replaces
    every section, and the review snapshots keep the old shape for good."""
    if ev.get("locations"):
        return list(ev["locations"])
    return [ev["location"]] if ev.get("location") else []


def event_samples(limit: int = 40) -> list[dict[str, Any]]:
    """The most recently extracted events, flattened for a table — the
    page's way of letting a reader check the model's work against the
    quote it cites (gap 08)."""
    events = _coll("MONGODB_EVENTS_COLLECTION", "events")
    rows: list[dict[str, Any]] = []
    for doc in events.find({"event_count": {"$gt": 0}},
                           {"frame_url": 1, "source_section": 1, "events": 1,
                            "model": 1}).sort("extracted_at", -1).limit(limit):
        for ev in doc.get("events") or []:
            rows.append({
                "entry": (doc.get("frame_url") or "").rsplit("/", 1)[-1],
                "section": doc.get("source_section"),
                "date": ev.get("date") or "—",
                "precision": ev.get("date_precision"),
                "evidence (verbatim)": ev.get("source_text"),
                "date words": ev.get("date_text") or "—",
                "where": " · ".join(event_places(ev)) or "—",
                "who": ", ".join(ev.get("actors") or []) or "—",
                "certainty": ev.get("certainty"),
                "links": len(ev.get("links") or []),
                "photos": len(ev.get("images") or []),
                "embeds": len(ev.get("embeds") or []),
                "model": doc.get("model"),
            })
            if len(rows) >= limit:
                return rows
    return rows


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


def _version_key(v: str | None) -> tuple:
    """Order "3.2.0" < "4.0.0" < "10.0.0" (a string sort would not)."""
    parts = []
    for bit in str(v or "").split("."):
        parts.append(int(bit) if bit.isdigit() else -1)
    return tuple(parts)


@st.cache_data(ttl=CACHE_TTL)
def event_progress() -> dict[str, Any]:
    """How far a re-extraction has got: sections by extraction version,
    the newest version's pace over the last hours, and when the rest will
    be done at that pace. Live from `events` (current state, not history)."""
    events = _coll("MONGODB_EVENTS_COLLECTION", "events")
    by_version = {(d["_id"] or "unknown"): d["n"] for d in events.aggregate([
        {"$group": {"_id": "$extraction_version", "n": {"$sum": 1}}}])}
    if not by_version:
        return {"by_version": {}, "current": None}
    current = max(by_version, key=_version_key)
    now = datetime.now(timezone.utc)
    pace = {h: events.count_documents({"extraction_version": current,
                                        "extracted_at": {"$gte": now - timedelta(hours=h)}})
            for h in (1, 6)}
    remaining = sum(n for v, n in by_version.items() if v != current)
    per_hour = pace[6] / 6
    return {"by_version": by_version, "current": current,
            "current_units": by_version[current], "remaining": remaining,
            "last_hour": pace[1], "last_6h": pace[6],
            "eta_hours": (remaining / per_hour) if per_hour and remaining else None}


def event_cards(limit: int = 12, section: str = "any", certainty: str = "any",
                with_values: bool = True) -> list[dict[str, Any]]:
    """The newest events of the newest extraction, for reading against
    their evidence: each with its quote and what was extracted from it.
    Not cached — the filters change per click and the query is small."""
    events = _coll("MONGODB_EVENTS_COLLECTION", "events")
    newest = event_progress().get("current")
    query: dict[str, Any] = {"event_count": {"$gt": 0}}
    if newest:
        query["extraction_version"] = newest
    if section != "any":
        query["source_section"] = section
    out: list[dict[str, Any]] = []
    for doc in events.find(query, {"frame_url": 1, "source_section": 1, "events": 1,
                                   "model": 1, "extraction_version": 1}
                           ).sort("extracted_at", -1).limit(limit * 6):
        for ev in doc.get("events") or []:
            if certainty != "any" and ev.get("certainty") != certainty:
                continue
            if with_values and not (ev.get("date_text") or ev.get("locations")
                                    or ev.get("location") or ev.get("actors")):
                continue
            out.append({**ev, "frame_url": doc.get("frame_url"),
                        "source_section": doc.get("source_section"),
                        "model": doc.get("model")})
            if len(out) >= limit:
                return out
    return out


# ---------------------------------------------------------------------------
# Entities: Wikidata links (kym_entities) and their curation (gap 09)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL)
def entity_state() -> dict[str, Any]:
    """The linker's output, live from `entities` (one doc per frame)."""
    ents = _coll("MONGODB_ENTITIES_COLLECTION", "entities")
    head = list(ents.aggregate([{"$group": {
        "_id": None, "frames": {"$sum": 1},
        "linked": {"$sum": {"$cond": [{"$gt": ["$mention_count", 0]}, 1, 0]}},
        "mentions": {"$sum": "$mention_count"},
        "own_item": {"$sum": {"$cond": [{"$ifNull": ["$self_qid", False]}, 1, 0]}},
        "latest": {"$max": "$linked_at"}}}]))
    head = head[0] if head else {}
    facets = list(ents.aggregate([
        {"$project": {"mentions.field": 1, "mentions.method": 1, "mentions.qid": 1}},
        {"$unwind": "$mentions"},
        {"$facet": {
            "field": [{"$group": {"_id": "$mentions.field", "n": {"$sum": 1}}}],
            "method": [{"$group": {"_id": "$mentions.method", "n": {"$sum": 1}}}],
            "items": [{"$group": {"_id": "$mentions.qid"}}, {"$count": "n"}]}},
    ], allowDiskUse=True))
    f = facets[0] if facets else {"field": [], "method": [], "items": []}

    def versions(field: str) -> dict[str, int]:
        return {(d["_id"] or "unknown"): d["n"] for d in ents.aggregate([
            {"$group": {"_id": f"${field}", "n": {"$sum": 1}}}])}
    return {
        "frames": head.get("frames", 0), "frames_linked": head.get("linked", 0),
        "mentions": head.get("mentions", 0), "own_item": head.get("own_item", 0),
        "last_linked_at": _as_utc(head.get("latest")),
        "by_field": {d["_id"] or "unknown": d["n"] for d in f["field"]},
        "by_method": {d["_id"] or "unknown": d["n"] for d in f["method"]},
        "distinct_items": f["items"][0]["n"] if f["items"] else 0,
        "linker_versions": versions("linker_version"),
        "senses_versions": versions("senses_version"),
        "lexicon_versions": versions("lexicon_version"),
    }


KEEP_BASES_ORDER = ("title", "own_item", "platform", "format", "title_agrees",
                    "tag_and_text", "tag_named", "judge")
KEEP_ROLES = ("subject", "source", "format", "platform")


@st.cache_data(ttl=CACHE_TTL)
def curation_state() -> dict[str, Any]:
    """Which links curation keeps and why, live from `entity_curation`.
    Keep/drop counts are MENTIONS (what the graph carries); the judge's
    roles are ITEMS per frame (what it was asked)."""
    cur = _coll("MONGODB_ENTITY_CURATION_COLLECTION", "entity_curation")
    fails = _coll("MONGODB_ENTITY_CURATION_FAILURES_COLLECTION", "entity_curation_failures")
    decisions = {(d["_id"]["b"], d["_id"]["k"]): d["n"] for d in cur.aggregate([
        {"$project": {"decisions.basis": 1, "decisions.keep": 1}},
        {"$unwind": "$decisions"},
        {"$group": {"_id": {"b": "$decisions.basis", "k": "$decisions.keep"},
                    "n": {"$sum": 1}}}], allowDiskUse=True)}
    kept = {b: n for (b, k), n in decisions.items() if k is True}
    dropped = {b: n for (b, k), n in decisions.items() if k is False}
    pending = sum(n for (b, k), n in decisions.items() if k is None)
    roles = {d["_id"]: d["n"] for d in cur.aggregate([
        {"$match": {"judge.roles": {"$exists": True}}},
        {"$project": {"r": {"$objectToArray": "$judge.roles"}}},
        {"$unwind": "$r"}, {"$group": {"_id": "$r.v", "n": {"$sum": 1}}}])}
    # The confirming reading: About-only items the judge kept, asked again
    # with the other prompt. Kept only if both readings keep them.
    confirm = {d["_id"]: d["n"] for d in cur.aggregate([
        {"$match": {"judge.confirm_roles": {"$exists": True}}},
        {"$project": {"r": {"$objectToArray": "$judge.confirm_roles"}}},
        {"$unwind": "$r"},
        {"$group": {"_id": {"$in": ["$r.v", list(KEEP_ROLES)]}, "n": {"$sum": 1}}}])}
    models = {f"{d['_id'].get('m') or '?'} · prompt {d['_id'].get('p') or '?'}": d["n"]
              for d in cur.aggregate([
                  {"$match": {"judge": {"$exists": True}}},
                  {"$group": {"_id": {"m": "$judge.judge_model",
                                      "p": "$judge.judge_prompt_version"},
                              "n": {"$sum": 1}}}])}
    latest = list(cur.aggregate([{"$group": {"_id": None, "t": {"$max": "$curated_at"}}}]))
    return {
        "frames": cur.count_documents({}),
        "frames_judged": cur.count_documents({"judge": {"$exists": True}}),
        "frames_waiting": cur.count_documents({"pending_qids.0": {"$exists": True}}),
        "frames_none_kept": cur.count_documents({"in_graph_count": 0}),
        "failures": fails.count_documents({}),
        "kept": kept, "dropped": dropped, "pending": pending,
        "kept_total": sum(kept.values()), "dropped_total": sum(dropped.values()),
        "judge_roles": roles,
        "confirm_agreed": confirm.get(True, 0), "confirm_overturned": confirm.get(False, 0),
        "judge_models": models,
        "versions": {(d["_id"] or "unknown"): d["n"] for d in cur.aggregate([
            {"$group": {"_id": "$curation_version", "n": {"$sum": 1}}}])},
        "last_curated_at": _as_utc(latest[0]["t"]) if latest else None,
    }


@st.cache_data(ttl=CACHE_TTL)
def curation_items(limit: int = 20) -> list[dict[str, Any]]:
    """The most-linked items (by About and tag mentions — titles are always
    kept) and how curation split them: which frequent items survive."""
    cur = _coll("MONGODB_ENTITY_CURATION_COLLECTION", "entity_curation")
    ents = _coll("MONGODB_ENTITIES_COLLECTION", "entities")
    rows = list(cur.aggregate([
        {"$project": {"decisions.qid": 1, "decisions.keep": 1, "decisions.field": 1}},
        {"$unwind": "$decisions"},
        {"$match": {"decisions.field": {"$ne": "title"}}},
        {"$group": {"_id": "$decisions.qid", "total": {"$sum": 1},
                    "kept": {"$sum": {"$cond": ["$decisions.keep", 1, 0]}}}},
        {"$sort": {"total": -1}}, {"$limit": limit}], allowDiskUse=True))
    qids = [r["_id"] for r in rows]
    labels = {d["_id"]: d["label"] for d in ents.aggregate([
        {"$match": {"mentions.qid": {"$in": qids}}},
        {"$project": {"mentions.qid": 1, "mentions.label": 1}},
        {"$unwind": "$mentions"}, {"$match": {"mentions.qid": {"$in": qids}}},
        {"$group": {"_id": "$mentions.qid", "label": {"$first": "$mentions.label"}}}])}
    return [{"item": f"{labels.get(r['_id'], r['_id'])} ({r['_id']})",
             "kept": r["kept"], "dropped": r["total"] - r["kept"], "mentions": r["total"]}
            for r in rows]


# ---------------------------------------------------------------------------
# Templates: imgflip search (kym_templates) and reading (kym_template_entities)
# ---------------------------------------------------------------------------

TEMPLATE_PRIORITY = {1: "own imgflip link", 2: "template-type meme", 3: "other meme",
                     4: "other entry"}
TEMPLATE_STATUS = ("selected", "below_threshold", "no_results")


@st.cache_data(ttl=CACHE_TTL)
def template_state() -> dict[str, Any]:
    """The template pool, live from `frame_templates` and `imgflip_templates`."""
    frames = _coll("MONGODB_FRAME_TEMPLATES_COLLECTION", "frame_templates")
    templates = _coll("MONGODB_IMGFLIP_TEMPLATES_COLLECTION", "imgflip_templates")
    by_priority: dict[str, dict[str, int]] = {}
    for d in frames.aggregate([{"$group": {"_id": {"p": "$priority", "s": "$status"},
                                           "n": {"$sum": 1}}}]):
        label = TEMPLATE_PRIORITY.get(d["_id"].get("p"), str(d["_id"].get("p")))
        by_priority.setdefault(label, {})[d["_id"].get("s") or "not searched"] = d["n"]
    per_frame = {d["_id"]: d["n"] for d in frames.aggregate([
        {"$match": {"status": "selected"}},
        {"$group": {"_id": {"$size": "$selected"}, "n": {"$sum": 1}}}])}
    methods = {(d["_id"] or "unknown"): d["n"] for d in frames.aggregate([
        {"$match": {"status": "selected"}}, {"$unwind": "$selected"},
        {"$group": {"_id": "$selected.method", "n": {"$sum": 1}}}])}
    kept = frames.distinct("selected.template_id", {"status": "selected"})
    with_image = templates.count_documents({"_id": {"$in": kept}, "blank_path": {"$exists": True}})
    image_failed = templates.count_documents({"_id": {"$in": kept}, "blank_error": {"$exists": True},
                                              "blank_path": {"$exists": False}})
    latest = list(frames.aggregate([{"$group": {"_id": None, "t": {"$max": "$searched_at"}}}]))
    return {
        "frames": frames.count_documents({}),
        "by_priority": by_priority,
        "by_status": {s: sum(v.get(s, 0) for v in by_priority.values()) for s in TEMPLATE_STATUS},
        "per_frame": per_frame,
        "links": sum(k * n for k, n in per_frame.items()),
        "methods": methods,
        "kept": len(kept), "with_image": with_image, "image_failed": image_failed,
        "seen": templates.estimated_document_count(),
        "merged": templates.count_documents({"leader": {"$exists": True},
                                             "$expr": {"$ne": ["$leader", "$_id"]}}),
        "last_searched_at": _as_utc(latest[0]["t"]) if latest else None,
    }


def _iou(a: list[float], b: list[float]) -> float:
    ix = max(0.0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    union = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / union if union > 0 else 0.0


@st.cache_data(ttl=CACHE_TTL)
def template_reading() -> dict[str, Any]:
    """What the vision model has read, live from `template_entities`."""
    frames = _coll("MONGODB_FRAME_TEMPLATES_COLLECTION", "frame_templates")
    templates = _coll("MONGODB_IMGFLIP_TEMPLATES_COLLECTION", "imgflip_templates")
    read = _coll("MONGODB_TEMPLATE_ENTITIES_COLLECTION", "template_entities")
    fails = _coll("MONGODB_TEMPLATE_ENTITY_FAILURES_COLLECTION", "template_entity_failures")
    kept = frames.distinct("selected.template_id", {"status": "selected"})
    readable = [d["_id"] for d in templates.find(
        {"_id": {"$in": kept}, "blank_path": {"$exists": True}}, {"_id": 1})]
    read_kept = read.count_documents({"_id": {"$in": readable}})
    failed_kept = fails.count_documents({"_id": {"$in": readable}})
    now = datetime.now(timezone.utc)
    pace = {h: read.count_documents({"detected_at": {"$gte": now - timedelta(hours=h)}})
            for h in (1, 6)}
    remaining = max(0, len(readable) - read_kept - failed_kept)
    per_hour = pace[6] / 6
    regions = list(read.aggregate([
        {"$project": {"r": "$detection.regions"}}, {"$unwind": "$r"},
        {"$facet": {"kind": [{"$group": {"_id": "$r.kind", "n": {"$sum": 1}}}],
                    "named": [{"$group": {"_id": "$r.named", "n": {"$sum": 1}}}]}}],
        allowDiskUse=True))
    rg = regions[0] if regions else {"kind": [], "named": []}
    links = {(d["_id"].get("s") or "?", bool(d["_id"].get("g"))): d["n"] for d in read.aggregate([
        {"$project": {"m": "$links.mentions"}}, {"$unwind": "$m"},
        {"$group": {"_id": {"s": "$m.source", "g": "$m.in_graph"}, "n": {"$sum": 1}}}])}
    failure_kinds: dict[str, int] = {}
    for d in fails.find({}, {"error": 1, "error_kind": 1}):
        err = str(d.get("error") or "")
        if "token repeat limit" in err:
            what = "stuck repeating itself"
        elif "not JSON" in err:
            what = "answer cut off"
        else:
            what = d.get("error_kind") or "other"
        failure_kinds[what] = failure_kinds.get(what, 0) + 1
    # The blind audit: of the names read WITH context, how many the blind
    # reading also found (same name, or same kind in an overlapping box).
    named = confirmed = 0
    for d in read.find({"detection.blind.regions": {"$exists": True}},
                       {"detection.regions": 1, "detection.blind.regions": 1}):
        blind = d["detection"]["blind"]["regions"]
        for r in d["detection"].get("regions") or []:
            if not r.get("named"):
                continue
            named += 1
            confirmed += any(
                b["name"].casefold() == r["name"].casefold()
                or (b["kind"] == r["kind"] and _iou(b["box"], r["box"]) >= 0.5)
                for b in blind)
    return {
        "readable": len(readable), "read": read_kept, "read_total": read.count_documents({}),
        "failed": failed_kept, "remaining": remaining,
        "last_hour": pace[1], "last_6h": pace[6],
        "eta_hours": (remaining / per_hour) if per_hour and remaining else None,
        "regions_by_kind": {(d["_id"] or "other"): d["n"] for d in rg["kind"]},
        "regions_named": sum(d["n"] for d in rg["named"] if d["_id"]),
        "regions_total": sum(d["n"] for d in rg["named"]),
        "links": links,
        "links_in_graph": sum(n for (s, g), n in links.items() if g),
        "failure_kinds": failure_kinds,
        "blind_named": named, "blind_confirmed": confirmed,
    }


def recent_templates(limit: int = 8) -> list[dict[str, Any]]:
    """The templates read most recently, with what was read and linked —
    for looking at the model's work. Not cached: it should move."""
    read = _coll("MONGODB_TEMPLATE_ENTITIES_COLLECTION", "template_entities")
    templates = _coll("MONGODB_IMGFLIP_TEMPLATES_COLLECTION", "imgflip_templates")
    docs = list(read.find({"detection.ok": True},
                          {"detection.regions": 1, "links.mentions": 1, "detected_at": 1}
                          ).sort("detected_at", -1).limit(limit))
    meta = {d["_id"]: d for d in templates.find(
        {"_id": {"$in": [d["_id"] for d in docs]}},
        {"name": 1, "thumb_url": 1, "url": 1})}
    out = []
    for d in docs:
        m = meta.get(d["_id"], {})
        by_region: dict[int, list[str]] = {}
        for mention in (d.get("links") or {}).get("mentions") or []:
            idx = (mention.get("region") or {}).get("index")
            tag = mention.get("label") or mention.get("qid")
            if mention.get("in_graph"):
                tag += " ✓"
            by_region.setdefault(idx, []).append(tag)
        out.append({
            "template_id": d["_id"], "name": m.get("name") or str(d["_id"]),
            "thumb_url": m.get("thumb_url"), "url": m.get("url"),
            "read_at": _as_utc(d.get("detected_at")),
            "regions": [{"name": r.get("name"), "kind": r.get("kind"),
                         "named": bool(r.get("named")), "text": r.get("text"),
                         "links": by_region.get(r.get("index"), [])}
                        for r in (d.get("detection") or {}).get("regions") or []],
        })
    return out


@st.cache_data(ttl=CACHE_TTL)
def derived_state() -> dict[str, Any]:
    """The Overview's one line per derived layer — plain counts only, so the
    landing page stays fast; each layer's own page has the breakdowns."""
    ents = _coll("MONGODB_ENTITIES_COLLECTION", "entities")
    cur = _coll("MONGODB_ENTITY_CURATION_COLLECTION", "entity_curation")
    frames = _coll("MONGODB_FRAME_TEMPLATES_COLLECTION", "frame_templates")
    templates = _coll("MONGODB_IMGFLIP_TEMPLATES_COLLECTION", "imgflip_templates")
    read = _coll("MONGODB_TEMPLATE_ENTITIES_COLLECTION", "template_entities")
    linked = list(ents.aggregate([{"$group": {"_id": None, "m": {"$sum": "$mention_count"},
                                              "f": {"$sum": {"$cond": [
                                                  {"$gt": ["$mention_count", 0]}, 1, 0]}}}}]))
    kept = list(cur.aggregate([{"$group": {"_id": None, "k": {"$sum": "$in_graph_count"}}}]))
    kept_ids = frames.distinct("selected.template_id", {"status": "selected"})
    readable = [d["_id"] for d in templates.find(
        {"_id": {"$in": kept_ids}, "blank_path": {"$exists": True}}, {"_id": 1})]
    return {
        "frames_total": ents.estimated_document_count(),
        "frames_linked": linked[0]["f"] if linked else 0,
        "mentions": linked[0]["m"] if linked else 0,
        "frames_curated": cur.count_documents({}),
        "frames_waiting": cur.count_documents({"pending_qids.0": {"$exists": True}}),
        "links_kept": kept[0]["k"] if kept else 0,
        "templates_kept": len(kept_ids),
        "templates_readable": len(readable),
        "templates_read": read.count_documents({"_id": {"$in": readable}}),
    }


# ---------------------------------------------------------------------------
# Knowledge graph (kg_builds / kg_nodes / kg_edges) — build-scoped reads only
# ---------------------------------------------------------------------------
# The KG collections are GENERATIONAL: several builds coexist, each tagged
# with a build_id, and ``kg_builds/{_id:"current"}`` says which one is
# published. No read against kg_nodes/kg_edges is meaningful without a
# build_id filter, so everything here resolves the pointer first. The two
# other stores (Neo4j, Fuseki) are deliberately NOT read from the dashboard —
# it would need two more drivers — their agreement is recorded in the run
# summary at publish time and shown from there.

def _kg_group(coll, build_id: str, field: str) -> dict[str, int]:
    return {(d["_id"] or "(none)"): d["n"] for d in coll.aggregate([
        {"$match": {"build_id": build_id}},
        {"$group": {"_id": f"${field}", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
    ])}


@st.cache_data(ttl=CACHE_TTL)
def kg_state() -> dict[str, Any]:
    nodes = _coll("MONGODB_KG_NODES_COLLECTION", "kg_nodes")
    edges = _coll("MONGODB_KG_EDGES_COLLECTION", "kg_edges")
    builds = _coll("MONGODB_KG_BUILDS_COLLECTION", "kg_builds")

    pointer = builds.find_one({"_id": "current"}) or {}
    bid = pointer.get("build_id")
    out: dict[str, Any] = {
        "build_id": bid,
        "published_at": _as_utc(pointer.get("published_at")),
        "state": None, "stamps": {}, "manifest_counts": {}, "validation": None,
        "nodes": 0, "edges": 0, "frames": 0,
        "nodes_by_kind": {}, "edges_by_type": {},
        # Documents from the previous, non-generational implementation carry
        # no build_id. They are invisible to every build-scoped read and are
        # never pruned — shown so their dead weight is a known quantity.
        "legacy_docs": (
            nodes.estimated_document_count()
            - nodes.count_documents({"build_id": {"$exists": True}})
            + edges.estimated_document_count()
            - edges.count_documents({"build_id": {"$exists": True}})),
        "generations": builds.count_documents({"_id": {"$ne": "current"}}),
    }
    if not bid:
        return out
    doc = builds.find_one({"_id": bid}) or {}
    out["state"] = doc.get("state")
    out["stamps"] = doc.get("stamps") or {}
    out["manifest_counts"] = (doc.get("manifest") or {}).get("counts") or {}
    out["validation"] = doc.get("validation")
    out["nodes_by_kind"] = _kg_group(nodes, bid, "kind")
    out["edges_by_type"] = _kg_group(edges, bid, "type")
    out["nodes"] = sum(out["nodes_by_kind"].values())
    out["edges"] = sum(out["edges_by_type"].values())
    out["frames"] = out["nodes_by_kind"].get("frame", 0)
    return out


def _allocated(path: str) -> int:
    """Bytes allocated under ``path``, as ``du`` counts them: TDB2's files
    are sparse, so their apparent size overstates the disk they take."""
    total = 0
    for entry in os.scandir(path):
        try:
            if entry.is_dir(follow_symlinks=False):
                total += _allocated(entry.path)
            else:
                total += entry.stat(follow_symlinks=False).st_blocks * 512
        except OSError:
            continue                     # a file TDB2 removed mid-walk
    return total


@st.cache_data(ttl=600)
def fuseki_disk() -> dict[str, Any] | None:
    """Fuseki's TDB2 databases on disk, per generation directory
    (``Data-NNNN``), and the free space of the filesystem they sit on (the
    host's). None when the volume is not mounted in this container (gap 04:
    the store grew unseen until someone ran ``du``)."""
    import shutil
    root = os.getenv("FUSEKI_DATA_DIR", "")
    databases = os.path.join(root, "databases") if root else ""
    if not databases or not os.path.isdir(databases):
        return None
    gens = []
    for ds in sorted(os.scandir(databases), key=lambda e: e.name):
        if not ds.is_dir():
            continue
        for gen in sorted(os.scandir(ds.path), key=lambda e: e.name):
            if gen.is_dir() and gen.name.startswith("Data-"):
                gens.append({"dataset": ds.name, "generation": gen.name,
                             "bytes": _allocated(gen.path)})
    usage = shutil.disk_usage(databases)
    return {"generations": gens, "bytes": sum(g["bytes"] for g in gens),
            "disk_total": usage.total, "disk_free": usage.free}


@st.cache_data(ttl=CACHE_TTL)
def kg_builds() -> list[dict[str, Any]]:
    """Every retained generation, newest first — the drill-down behind the
    pointer. ``current`` marks the published one."""
    builds = _coll("MONGODB_KG_BUILDS_COLLECTION", "kg_builds")
    current = (builds.find_one({"_id": "current"}) or {}).get("build_id")
    rows = []
    for d in builds.find({"_id": {"$ne": "current"}}).sort("created_at", -1):
        val = d.get("validation") or {}
        rows.append({
            "build_id": d["_id"],
            "state": d.get("state"),
            "published": d["_id"] == current,
            "created_at": _as_utc(d.get("created_at")),
            "triples": ((d.get("manifest") or {}).get("counts") or {}).get("triples"),
            "rdf_diff": (None if not val else
                         ("agrees" if val.get("equal") else
                          f"DIVERGES: {', '.join(val.get('content_divergence') or [])}")),
        })
    return rows


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
