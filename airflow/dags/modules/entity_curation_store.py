"""
entity_curation_store.py — MongoDB persistence for entity curation (gap 09)
============================================================================
The ONLY place kym_entity_curation touches the database. kg/curation.py
decides; this module keeps.

Collections
-----------
``entities``, ``entries``    (read-only) the links, and the frame's text for
                             the judge

``entity_curation``          ONE DOC PER FRAME (``_id`` = the entities _id)
    frame_url, mentions_sha  the links it was computed over (kg/curation
                             .mentions_sha): a re-link makes it stale
    rules                    kg/curation.apply_rules' decision per mention —
                             ``keep`` None where the judge must decide
    rule stamps              curation_version, curation_lists_version,
                             lexicon_version (RULE_STAMP_KEYS)
    judge                    {verdicts {qid: keep}, roles {qid: role},
                             confirm_roles (the second model's, for the
                             About-only items the first kept),
                             judge_prompt_version,
                             judge_schema_sha, judge_model, digest, host,
                             judged_at} — PER ITEM, so a rule edit re-uses
                             them and only a new prompt/schema/model re-asks
    decisions                rules resolved with the judge's verdicts: what
                             the KG reads. A pending one stays out of the
                             graph (Gabi: rule-kept only until judged)
    pending_qids             items still waiting for the judge
    in_graph_count, curated_at

``entity_curation_failures`` dead letters, one per frame: a frame whose
    judge call failed is not asked again under the same stamps and the
    same pending items; a later success clears it.

The ``entities`` collection is never written: the template layer hashes a
frame's mentions (template_entity_store.link_context), and curation must
not look like a re-link.

Environment: MONGODB_URI, MONGODB_DB, MONGODB_ENTITIES_COLLECTION,
MONGODB_ENTRIES_COLLECTION, MONGODB_ENTITY_CURATION_COLLECTION
(entity_curation), MONGODB_ENTITY_CURATION_FAILURES_COLLECTION
(entity_curation_failures).
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from modules.kg import curation as kc
from modules.mongo_base import MongoStoreBase, as_utc, now_utc

log = logging.getLogger("entity_curation_store")

__all__ = [
    "EntityCurationStore", "get_store", "pending_rules", "records_for",
    "save_rules", "pending_judge", "judge_units_for", "save_judge", "save_failure",
    "decisions_for", "curation_stamps", "curation_stats",
    "RULE_STAMP_KEYS", "JUDGE_STAMP_KEYS",
]

RULE_STAMP_KEYS = ("curation_version", "curation_lists_version", "lexicon_version")
JUDGE_STAMP_KEYS = ("judge_prompt_version", "judge_schema_sha", "judge_model",
                    "judge_confirm_model")
MENTION_KEY_FIELDS = {"mentions.field": 1, "mentions.tag_index": 1, "mentions.start": 1,
                      "mentions.end": 1, "mentions.qid": 1}
RULE_FIELDS = {"frame_url": 1, "self_qid": 1, "mentions.field": 1, "mentions.tag_index": 1,
               "mentions.start": 1, "mentions.end": 1, "mentions.qid": 1,
               "mentions.method": 1, "mentions.text": 1, "mentions.label": 1,
               "mentions.description": 1}
LOOKUP_BATCH = 1000
BULK_BATCH = 500


class EntityCurationStore(MongoStoreBase):
    """Owner of entity_curation and entity_curation_failures."""

    def _configure(self) -> None:
        self.entities = self.collection("MONGODB_ENTITIES_COLLECTION", "entities")
        self.entries = self.collection("MONGODB_ENTRIES_COLLECTION", "entries")
        self.curation = self.collection("MONGODB_ENTITY_CURATION_COLLECTION",
                                        "entity_curation")
        self.failures = self.collection("MONGODB_ENTITY_CURATION_FAILURES_COLLECTION",
                                        "entity_curation_failures")
        self.curation.create_index("frame_url")
        self.curation.create_index("curated_at")
        self.curation.create_index("pending_qids")

    # -- the rules --------------------------------------------------------------

    def pending_rules(self, stamps: dict[str, str], *, force: bool = False,
                      limit: int = 0) -> list[str]:
        """Frames whose curation is missing, or stale by its links or its
        rule stamps."""
        have = {d["_id"]: d for d in self.curation.find(
            {}, {"mentions_sha": 1, **{k: 1 for k in RULE_STAMP_KEYS}})}
        out: list[str] = []
        for e in self.entities.find({}, MENTION_KEY_FIELDS):
            cur = have.get(e["_id"])
            if force or cur is None or cur.get("mentions_sha") != kc.mentions_sha(
                    e.get("mentions") or []) or any(
                    cur.get(k) != stamps.get(k) for k in RULE_STAMP_KEYS):
                out.append(e["_id"])
                if limit and len(out) >= limit:
                    break
        return out

    def records_for(self, frame_ids: Iterable[str]) -> list[dict]:
        ids = list(frame_ids)
        out: list[dict] = []
        for i in range(0, len(ids), LOOKUP_BATCH):
            out.extend(self.entities.find({"_id": {"$in": ids[i:i + LOOKUP_BATCH]}},
                                          RULE_FIELDS))
        return out

    def save_rules(self, results: Iterable[tuple[dict, list[dict]]],
                   stamps: dict[str, str]) -> dict[str, int]:
        """(entities doc, rule decisions) pairs -> curation docs, re-using
        the judge's stored verdicts (whatever they were given under: a newer
        judgement replaces them when kym_entity_curation re-judges)."""
        from pymongo import UpdateOne

        results = list(results)
        judges = {d["_id"]: d.get("judge") or {} for d in self.curation.find(
            {"_id": {"$in": [r[0]["_id"] for r in results]}}, {"judge": 1})}
        now = now_utc()
        ops, frames, pending = [], 0, 0
        for record, rules in results:
            verdicts = judges.get(record["_id"], {}).get("verdicts") or {}
            decisions = kc.resolve(rules, verdicts)
            waiting = sorted({d["qid"] for d in decisions if d["keep"] is None})
            ops.append(UpdateOne({"_id": record["_id"]}, {"$set": {
                "frame_url": record.get("frame_url"),
                "mentions_sha": kc.mentions_sha(record.get("mentions") or []),
                "rules": rules, "decisions": decisions, "pending_qids": waiting,
                "in_graph_count": sum(1 for d in decisions if d["keep"]),
                **{k: stamps[k] for k in RULE_STAMP_KEYS},
                "rules_at": now, "curated_at": now}}, upsert=True))
            frames += 1
            pending += len(waiting)
            if len(ops) >= BULK_BATCH:
                self.curation.bulk_write(ops, ordered=False)
                ops = []
        if ops:
            self.curation.bulk_write(ops, ordered=False)
        return {"frames": frames, "pending_items": pending}

    # -- the judge ------------------------------------------------------------------

    def pending_judge(self, stamps: dict[str, str], *, force: bool = False,
                      limit: int = 0) -> list[str]:
        """Frames with items the judge has not answered under these stamps:
        new pending items, or a judgement under another prompt, schema or
        model. A frame that failed under the same stamps with the same
        pending items is a dead letter and is skipped."""
        dead = {d["_id"]: d for d in self.failures.find({})}
        out: list[str] = []
        query = {"rules.keep": None} if force else {}
        for d in self.curation.find(query, {"pending_qids": 1, "judge": 1,
                                            "rules.keep": 1}):
            judge = d.get("judge") or {}
            has_rule_pending = any(r.get("keep") is None for r in d.get("rules") or [])
            stale = has_rule_pending and any(judge.get(k) != stamps.get(k)
                                             for k in JUDGE_STAMP_KEYS)
            if not (force and has_rule_pending) and not d.get("pending_qids") and not stale:
                continue
            f = dead.get(d["_id"])
            if (f is not None and not force
                    and all(f.get(k) == stamps.get(k) for k in JUDGE_STAMP_KEYS)
                    and f.get("pending_qids") == d.get("pending_qids")):
                continue
            out.append(d["_id"])
            if limit and len(out) >= limit:
                break
        return out

    def judge_units_for(self, frame_ids: Iterable[str], stamps: dict[str, str]) -> list[dict]:
        """What the judge needs per frame: the frame's text, and the items to
        ask about — only the unanswered ones when the stored verdicts are
        under these stamps, every rule-pending item when they are not."""
        ids = list(frame_ids)
        cur = {d["_id"]: d for d in self.curation.find(
            {"_id": {"$in": ids}}, {"rules": 1, "judge": 1, "pending_qids": 1})}
        ents = {e["_id"]: e for e in self.records_for(ids)}
        entries = {e["_id"]: e for e in self.entries.find(
            {"_id": {"$in": ids}}, {"title": 1, "tags": 1, "origin": 1,
                                    "sections.kind": 1, "sections.text": 1})}
        units = []
        for fid in ids:
            c, e = cur.get(fid), ents.get(fid)
            if c is None or e is None or fid not in entries:
                continue
            judge = c.get("judge") or {}
            current = all(judge.get(k) == stamps.get(k) for k in JUDGE_STAMP_KEYS)
            items = kc.judge_items(e, c["rules"], about=kc.about_text(entries[fid]))
            if current:
                wanted = set(c.get("pending_qids") or [])
                items = [it for it in items if it["qid"] in wanted]
            if items:
                units.append({"frame_id": fid, "context": kc.frame_context(entries[fid]),
                              "items": items, "stamps_current": current})
        return units

    def save_judge(self, frame_id: str, result: dict, stamps: dict[str, str],
                   *, merge: bool) -> dict[str, int]:
        """Store the judge's verdicts (merged into the stored ones when they
        were given under the same stamps, replacing them otherwise) and
        re-resolve the frame's decisions."""
        doc = self.curation.find_one({"_id": frame_id}, {"rules": 1, "judge": 1})
        judge = (doc.get("judge") or {}) if merge else {}
        verdicts = {**(judge.get("verdicts") or {}), **result["verdicts"]}
        roles = {**(judge.get("roles") or {}), **(result.get("roles") or {})}
        confirm = result.get("confirm") or {}
        confirm_roles = {**(judge.get("confirm_roles") or {}), **(confirm.get("roles") or {})}
        decisions = kc.resolve(doc["rules"], verdicts)
        waiting = sorted({d["qid"] for d in decisions if d["keep"] is None})
        now = now_utc()
        self.curation.update_one({"_id": frame_id}, {"$set": {
            "judge": {"verdicts": verdicts, "roles": roles, "confirm_roles": confirm_roles,
                      "confirm_digest": confirm.get("digest"),
                      **{k: stamps[k] for k in JUDGE_STAMP_KEYS},
                      "digest": result.get("digest"), "host": result.get("host"),
                      "model_served": result.get("model"), "judged_at": now},
            "decisions": decisions, "pending_qids": waiting,
            "in_graph_count": sum(1 for d in decisions if d["keep"]),
            "curated_at": now}})
        self.failures.delete_one({"_id": frame_id})
        return {"kept": sum(1 for v in result["verdicts"].values() if v),
                "dropped": sum(1 for v in result["verdicts"].values() if not v)}

    def save_failure(self, frame_id: str, result: dict, stamps: dict[str, str]) -> None:
        doc = self.curation.find_one({"_id": frame_id}, {"pending_qids": 1}) or {}
        self.failures.replace_one({"_id": frame_id}, {
            **{k: stamps.get(k) for k in JUDGE_STAMP_KEYS},
            "pending_qids": doc.get("pending_qids"), "error": result.get("error"),
            "error_kind": result.get("error_kind"), "failed_at": now_utc()}, upsert=True)

    # -- the KG build's reads -------------------------------------------------------

    def decisions_for(self, entry_ids: Iterable[str],
                      curated_at_lte=None) -> dict[str, dict]:
        """{frame_url: {"mentions_sha", "keep": {mention key: basis}}} — the
        KEPT mentions of each curated frame, frozen at the build's
        snapshot. kg_store checks the sha against the links it reads."""
        ids = list(entry_ids)
        query: dict[str, Any] = {"_id": {"$in": ids}}
        if curated_at_lte is not None:
            query["curated_at"] = {"$lte": curated_at_lte}
        out: dict[str, dict] = {}
        for d in self.curation.find(query, {"frame_url": 1, "mentions_sha": 1,
                                            "decisions": 1}):
            out[d["frame_url"]] = {
                "mentions_sha": d.get("mentions_sha"),
                "keep": {x["key"]: x["basis"] for x in d.get("decisions") or []
                         if x.get("keep")}}
        return out

    def curation_stamps(self, curated_at_lte=None) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if curated_at_lte is not None:
            query["curated_at"] = {"$lte": curated_at_lte}
        agg = list(self.curation.aggregate([
            {"$match": query},
            {"$group": {"_id": None, "frames": {"$sum": 1},
                        "in_graph": {"$sum": "$in_graph_count"},
                        "pending": {"$sum": {"$size": {"$ifNull": ["$pending_qids", []]}}},
                        "max_curated_at": {"$max": "$curated_at"},
                        "versions": {"$addToSet": "$curation_lists_version"},
                        "models": {"$addToSet": "$judge.judge_model"}}}]))
        row = agg[0] if agg else {}
        latest = as_utc(row.get("max_curated_at"))
        return {"entities_curation_frames": row.get("frames", 0),
                "entities_in_graph": row.get("in_graph", 0),
                "entities_curation_pending": row.get("pending", 0),
                "entities_curation_versions": sorted(v for v in row.get("versions") or [] if v),
                "entities_judge_models": sorted(v for v in row.get("models") or [] if v),
                "entities_max_curated_at": latest.isoformat() if latest else None}

    def stats(self) -> dict[str, Any]:
        by_basis = {f"{r['_id']['basis']}:{r['_id']['keep']}": r["n"]
                    for r in self.curation.aggregate([
                        {"$unwind": "$decisions"},
                        {"$group": {"_id": {"basis": "$decisions.basis",
                                            "keep": "$decisions.keep"},
                                    "n": {"$sum": 1}}}])}
        return {"frames_curated": self.curation.count_documents({}),
                "frames_with_pending": self.curation.count_documents(
                    {"pending_qids.0": {"$exists": True}}),
                "frames_judged": self.curation.count_documents({"judge": {"$exists": True}}),
                "judge_failures": self.failures.count_documents({}),
                "decisions_by_basis": by_basis,
                **self.curation_stamps()}


def get_store(uri: str | None = None, db_name: str | None = None) -> EntityCurationStore:
    return EntityCurationStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facades — the only calls the DAG (and kg_store) make
# ---------------------------------------------------------------------------

def pending_rules(stamps: dict[str, str], *, force: bool = False, limit: int = 0) -> list[str]:
    with get_store() as store:
        return store.pending_rules(stamps, force=force, limit=limit)


def records_for(frame_ids: Iterable[str]) -> list[dict]:
    with get_store() as store:
        return store.records_for(frame_ids)


def save_rules(results, stamps: dict[str, str]) -> dict[str, int]:
    with get_store() as store:
        return store.save_rules(results, stamps)


def pending_judge(stamps: dict[str, str], *, force: bool = False, limit: int = 0) -> list[str]:
    with get_store() as store:
        return store.pending_judge(stamps, force=force, limit=limit)


def judge_units_for(frame_ids: Iterable[str], stamps: dict[str, str]) -> list[dict]:
    with get_store() as store:
        return store.judge_units_for(frame_ids, stamps)


def save_judge(frame_id: str, result: dict, stamps: dict[str, str], *, merge: bool):
    with get_store() as store:
        return store.save_judge(frame_id, result, stamps, merge=merge)


def save_failure(frame_id: str, result: dict, stamps: dict[str, str]) -> None:
    with get_store() as store:
        store.save_failure(frame_id, result, stamps)


def decisions_for(entry_ids: Iterable[str], curated_at_lte=None) -> dict[str, dict]:
    with get_store() as store:
        return store.decisions_for(entry_ids, curated_at_lte)


def curation_stamps(curated_at_lte=None) -> dict[str, Any]:
    with get_store() as store:
        return store.curation_stamps(curated_at_lte)


def curation_stats() -> dict[str, Any]:
    with get_store() as store:
        return store.stats()
