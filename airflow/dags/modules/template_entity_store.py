"""
template_entity_store.py — MongoDB persistence for template image entities
===========================================================================
The ONLY place kym_template_entities touches the database. Knows nothing
about the vision model or the lexicon — kg/template_entities.py decides
what a record says; this module keeps it.

Collections
-----------
``frame_templates``, ``imgflip_templates``   (template_store's, read-only here)
    which templates are kept, by which frames, and their image files.
``entries``, ``entities``                    (read-only)
    the selecting frames' title and About (the model's context) and the
    Wikidata items they already link (the linker's preferred senses).

``template_entities``  ONE DOC PER KEPT TEMPLATE (``_id`` = template id)
    detection     the vision model's reading: regions (name, kind, named,
                  box 0..1, text, confidence), what grounding dropped, the
                  blind re-read for the audit sample, model, digest, host
    DETECTION_STAMP_KEYS at the top level: any of them moving re-reads the
                  template (a new prompt, schema, model, or image)
    links         each region linked to Wikidata (kg/entities.link_label),
                  with in_graph; nil names; LINK_STAMP_KEYS at the top level:
                  a new lexicon or linker re-LINKS without re-reading
    Two levels on purpose: the model call costs ~30 s of shared GPU, the
    link a few milliseconds.

``template_entity_failures``  dead letters, one per template
    A model call can fail for reasons outside the pipeline (a host down, a
    reply the grammar cannot hold). A failed template is not retried until
    something about it changes — its stamps or its image — so a blind retry
    never burns GPU on the same failure; a later success clears it.

Environment: MONGODB_URI, MONGODB_DB, and the collection overrides
MONGODB_TEMPLATE_ENTITIES_COLLECTION (template_entities),
MONGODB_TEMPLATE_ENTITY_FAILURES_COLLECTION (template_entity_failures).
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from modules.mongo_base import MongoStoreBase, as_utc, now_utc

log = logging.getLogger("template_entity_store")

__all__ = [
    "TemplateEntityStore", "get_store", "pending_detection", "units_for",
    "save_detection", "save_failure", "pending_linking", "link_units_for",
    "save_links", "entity_stats", "read_image", "graph_mentions_for", "graph_stamps",
    "DETECTION_STAMP_KEYS", "LINK_STAMP_KEYS",
]

DETECTION_STAMP_KEYS = ("extractor_version", "prompt_version", "schema_sha",
                        "requested_model", "image_sha256")
LINK_STAMP_KEYS = ("linker_version", "lexicon_version", "nlp_model", "senses_version",
                   "template_link_version", "detection_sha",
                   "link_context_sha")
LOOKUP_BATCH = 2000
BULK_BATCH = 200


class TemplateEntityStore(MongoStoreBase):
    """Owner of template_entities and template_entity_failures."""

    def _configure(self) -> None:
        self.frames = self.collection("MONGODB_FRAME_TEMPLATES_COLLECTION", "frame_templates")
        self.templates = self.collection("MONGODB_IMGFLIP_TEMPLATES_COLLECTION",
                                         "imgflip_templates")
        self.entries = self.collection("MONGODB_ENTRIES_COLLECTION", "entries")
        self.entities = self.collection("MONGODB_ENTITIES_COLLECTION", "entities")
        self.detections = self.collection("MONGODB_TEMPLATE_ENTITIES_COLLECTION",
                                          "template_entities")
        self.failures = self.collection("MONGODB_TEMPLATE_ENTITY_FAILURES_COLLECTION",
                                        "template_entity_failures")
        self.detections.create_index("detected_at")
        self.detections.create_index("linked_at")
        self.detections.create_index("links.mentions.qid")

    # -- units ----------------------------------------------------------------

    def kept(self) -> dict[int, list[dict]]:
        """{template id: [{frame_id, frame_url, title, R, priority}]} over
        every frame that selects it."""
        out: dict[int, list[dict]] = {}
        for f in self.frames.find({"status": "selected"},
                                  {"selected.template_id": 1, "selected.R": 1,
                                   "frame_url": 1, "title": 1, "priority": 1}):
            for s in f.get("selected") or []:
                out.setdefault(int(s["template_id"]), []).append(
                    {"frame_id": f["_id"], "frame_url": f.get("frame_url"),
                     "title": f.get("title"), "R": s.get("R", 0.0),
                     "priority": f.get("priority", 9)})
        return out

    def _abouts(self, frame_ids: Iterable[str]) -> dict[str, str]:
        ids = list(dict.fromkeys(frame_ids))
        out: dict[str, str] = {}
        for i in range(0, len(ids), LOOKUP_BATCH):
            for e in self.entries.find({"_id": {"$in": ids[i:i + LOOKUP_BATCH]}},
                                       {"sections.kind": 1, "sections.text": 1}):
                about = next((s for s in e.get("sections") or [] if s.get("kind") == "about"),
                             None)
                out[e["_id"]] = " ".join((about or {}).get("text") or [])
        return out

    def units_for(self, template_ids: Iterable[int],
                  kept: dict[int, list[dict]] | None = None) -> list[dict]:
        """Detection units: the template, its image on disk, and the frames
        that select it (with their About text), in the order given."""
        from modules.kg import template_entities as te

        kept = self.kept() if kept is None else kept
        ids = [int(t) for t in dict.fromkeys(template_ids)]
        docs = {int(d["_id"]): d for d in self.templates.find(
            {"_id": {"$in": ids}}, {"name": 1, "alt_names": 1, "blank_path": 1,
                                     "blank_sha256": 1, "blank_url": 1, "animated": 1})}
        frames = {t: kept.get(t, []) for t in ids}
        abouts = self._abouts(f["frame_id"] for fs in frames.values() for f in fs)
        units = []
        for t in ids:
            doc = docs.get(t)
            if not doc or not doc.get("blank_path") or not doc.get("blank_sha256"):
                continue
            fs = [dict(f, about=abouts.get(f["frame_id"], "")) for f in frames[t]]
            units.append({
                "template_id": t, "image_path": doc["blank_path"],
                "image_sha256": doc["blank_sha256"],
                "image_source": "still" if doc.get("animated") else "blank",
                "context": te.template_context(doc, fs),
                "frames": [{k: f[k] for k in ("frame_id", "frame_url", "title", "R")}
                           for f in fs],
                "priority": min((f["priority"] for f in fs), default=9),
                "best_R": max((f["R"] for f in fs), default=0.0),
            })
        return units

    def select_pending(self, units: Iterable[dict], stamps: dict[str, str]) -> list[dict]:
        """Units whose detection is missing or stale, minus those that
        failed under exactly the same stamps and image (dead letters)."""
        units = list(units)
        if not units:
            return []
        ids = [u["template_id"] for u in units]
        fresh: set[int] = set()
        dead: set[int] = set()
        proj = {k: 1 for k in DETECTION_STAMP_KEYS}
        for i in range(0, len(ids), LOOKUP_BATCH):
            chunk = ids[i:i + LOOKUP_BATCH]
            by_id = {u["template_id"]: u for u in units}
            for d in self.detections.find({"_id": {"$in": chunk}}, proj):
                u = by_id[int(d["_id"])]
                if all(d.get(k) == {**stamps, "image_sha256": u["image_sha256"]}[k]
                       for k in DETECTION_STAMP_KEYS):
                    fresh.add(int(d["_id"]))
            for d in self.failures.find({"_id": {"$in": chunk}}, proj):
                u = by_id[int(d["_id"])]
                if all(d.get(k) == {**stamps, "image_sha256": u["image_sha256"]}[k]
                       for k in DETECTION_STAMP_KEYS):
                    dead.add(int(d["_id"]))
        return [u for u in units if u["template_id"] not in fresh | dead]

    def not_detected_since(self, units: Iterable[dict], since) -> list[dict]:
        units = list(units)
        since = _as_datetime(since)
        if not units or since is None:
            return units
        done = {int(d["_id"]) for d in self.detections.find(
            {"_id": {"$in": [u["template_id"] for u in units]},
             "detected_at": {"$gte": since}}, {"_id": 1})}
        return [u for u in units if u["template_id"] not in done]

    # -- writes ---------------------------------------------------------------

    def save_detection(self, record: dict) -> None:
        """A successful reading REPLACES the template's detection (never a
        merge) and clears its links' freshness via detection_sha; a later
        success clears the dead letter."""
        tid = int(record["template_id"])
        stamps = {k: record.get(k) for k in DETECTION_STAMP_KEYS}
        now = now_utc()
        self.detections.update_one({"_id": tid}, {
            "$set": {"detection": record, **stamps, "detected_at": now,
                     "region_count": len(record.get("regions") or [])},
            "$setOnInsert": {"first_detected_at": now}}, upsert=True)
        self.failures.delete_one({"_id": tid})

    def save_failure(self, record: dict) -> None:
        tid = int(record["template_id"])
        self.failures.replace_one({"_id": tid}, {
            **{k: record.get(k) for k in DETECTION_STAMP_KEYS},
            "error": record.get("error"), "error_kind": record.get("error_kind"),
            "attempts": record.get("attempts"), "failed_at": now_utc()}, upsert=True)

    # -- linking ----------------------------------------------------------------

    def link_context(self, template: dict, frames: list[dict]) -> tuple[str, list[int], str]:
        """(context text, preferred QIDs as ints, its sha) for one template:
        its names, and every selecting frame's title, About and Wikidata links."""
        frame_ids = [f["frame_id"] for f in frames]
        abouts = self._abouts(frame_ids)
        text = " ".join([template.get("name") or "", " ".join(template.get("alt_names") or []),
                         *[f.get("title") or "" for f in frames],
                         *[abouts.get(i, "") for i in frame_ids]])
        prefer: set[int] = set()
        for e in self.entities.find({"_id": {"$in": frame_ids}},
                                    {"mentions.qid": 1, "self_qid": 1}):
            for q in [e.get("self_qid"), *[m.get("qid") for m in e.get("mentions") or []]]:
                if isinstance(q, str) and q.startswith("Q") and q[1:].isdigit():
                    prefer.add(int(q[1:]))
        sha = hashlib.sha256((text + "|" + ",".join(map(str, sorted(prefer)))).encode()
                             ).hexdigest()[:16]
        return text, sorted(prefer), sha

    def link_units_for(self, template_ids: Iterable[int]) -> list[dict]:
        kept = self.kept()
        ids = [int(t) for t in dict.fromkeys(template_ids)]
        names = {int(d["_id"]): d for d in self.templates.find(
            {"_id": {"$in": ids}}, {"name": 1, "alt_names": 1})}
        out = []
        for d in self.detections.find({"_id": {"$in": ids}, "detection.ok": True},
                                      {"detection": 1, "links.detection_sha": 1,
                                       "links.link_context_sha": 1}):
            tid = int(d["_id"])
            text, prefer, sha = self.link_context(names.get(tid, {}), kept.get(tid, []))
            out.append({"template_id": tid, "detection": d["detection"],
                        "context_text": text, "prefer": prefer, "link_context_sha": sha})
        return out

    def pending_linking(self, linker_stamps: dict[str, str], limit: int = 0) -> list[int]:
        """Templates with a detection whose links are missing or stale: a
        new lexicon/linker/model, a new detection, or new frame context."""
        from modules.kg import template_entities as te

        kept = self.kept()
        names = {int(d["_id"]): d for d in self.templates.find(
            {"_id": {"$in": list(kept)}}, {"name": 1, "alt_names": 1})}
        todo: list[int] = []
        for d in self.detections.find({"detection.ok": True},
                                      {"detection": 1, **{f"links.{k}": 1
                                                          for k in LINK_STAMP_KEYS}}):
            tid = int(d["_id"])
            if tid not in kept:
                continue            # no longer selected by any frame
            links = d.get("links") or {}
            _text, _prefer, ctx = self.link_context(names.get(tid, {}), kept[tid])
            want = {**linker_stamps, "detection_sha": te.detection_sha(d["detection"]),
                    "link_context_sha": ctx}
            if any(links.get(k) != want[k] for k in LINK_STAMP_KEYS):
                todo.append(tid)
            if limit and len(todo) >= limit:
                break
        return todo

    def save_links(self, records: Iterable[dict]) -> int:
        from pymongo import UpdateOne

        ops = []
        now = now_utc()
        for r in records:
            ops.append(UpdateOne({"_id": int(r["template_id"])}, {"$set": {
                "links": r, "mention_count": r["mention_count"],
                "in_graph_count": r["in_graph_count"], "linked_at": now}}))
        for i in range(0, len(ops), BULK_BATCH):
            self.detections.bulk_write(ops[i:i + BULK_BATCH], ordered=False)
        return len(ops)

    # -- the KG build's reads ----------------------------------------------------

    def graph_mentions_for(self, template_ids: Iterable[int],
                           linked_at_lte=None) -> dict[int, list[dict]]:
        """{template id: [mention]} — only the mentions marked in_graph
        (named, printed text, the largest generic ones), each with the model
        that read the image, frozen at the build's snapshot."""
        ids = [int(t) for t in dict.fromkeys(template_ids)]
        if not ids:
            return {}
        query: dict[str, Any] = {"_id": {"$in": ids}, "in_graph_count": {"$gt": 0}}
        if linked_at_lte is not None:
            query["linked_at"] = {"$lte": linked_at_lte}
        out: dict[int, list[dict]] = {}
        for d in self.detections.find(query, {"links.mentions": 1, "detection.model": 1}):
            model = (d.get("detection") or {}).get("model")
            out[int(d["_id"])] = [
                {k: m.get(k) for k in ("qid", "label", "description", "text", "score",
                                       "method", "region")} | {"model": model}
                for m in (d.get("links") or {}).get("mentions") or [] if m.get("in_graph")]
        return out

    def graph_stamps(self, linked_at_lte=None) -> dict[str, Any]:
        query: dict[str, Any] = {"links": {"$exists": True}}
        if linked_at_lte is not None:
            query["linked_at"] = {"$lte": linked_at_lte}
        agg = list(self.detections.aggregate([
            {"$match": query},
            {"$group": {"_id": None, "templates": {"$sum": 1},
                        "in_graph": {"$sum": "$in_graph_count"},
                        "max_linked_at": {"$max": "$linked_at"},
                        "lexicons": {"$addToSet": "$links.lexicon_version"},
                        "prompts": {"$addToSet": "$prompt_version"},
                        "models": {"$addToSet": "$detection.model"}}}]))
        row = agg[0] if agg else {}
        latest = as_utc(row.get("max_linked_at"))
        return {"template_entities_templates": row.get("templates", 0),
                "template_entities_in_graph": row.get("in_graph", 0),
                "template_entities_lexicons": sorted(x for x in row.get("lexicons") or [] if x),
                "template_entities_prompts": sorted(x for x in row.get("prompts") or [] if x),
                "template_entities_models": sorted(x for x in row.get("models") or [] if x),
                "template_entities_max_linked_at": latest.isoformat() if latest else None}

    # -- stats ----------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        def unwound(field: str) -> dict[str, int]:
            return {str(r["_id"]): r["n"] for r in self.detections.aggregate([
                {"$unwind": "$links.mentions"},
                {"$group": {"_id": f"$links.mentions.{field}", "n": {"$sum": 1}}}])}

        blind = {"named": 0, "confirmed": 0, "context_only": 0, "templates": 0}
        from modules.kg import template_entities as te
        for d in self.detections.find({"detection.blind.regions": {"$exists": True}},
                                      {"detection.regions": 1, "detection.blind": 1}):
            agree = te.blind_agreement(d["detection"].get("regions") or [],
                                       d["detection"]["blind"]["regions"])
            blind["templates"] += 1
            for k in ("named", "confirmed", "context_only"):
                blind[k] += agree[k]
        return {
            "templates_kept": len(self.kept()),
            "templates_detected": self.detections.count_documents({"detection.ok": True}),
            "templates_failed": self.failures.count_documents({}),
            "templates_linked": self.detections.count_documents({"links": {"$exists": True}}),
            "regions": sum(d.get("region_count", 0) for d in self.detections.find(
                {}, {"region_count": 1})),
            "mentions_by_source": unwound("source"),
            "mentions_in_graph": sum(d.get("in_graph_count", 0) for d in self.detections.find(
                {}, {"in_graph_count": 1})),
            "blind_audit": {**blind, "context_only_rate": round(
                blind["context_only"] / blind["named"], 3) if blind["named"] else None},
        }


def _as_datetime(value) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return as_utc(value)
    try:
        return as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def read_image(unit: dict) -> bytes | None:
    path = Path(unit["image_path"])
    return path.read_bytes() if path.exists() else None


def get_store(uri: str | None = None, db_name: str | None = None) -> TemplateEntityStore:
    return TemplateEntityStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facades — the only calls the DAG makes
# ---------------------------------------------------------------------------

def pending_detection(*, stamps: dict[str, str], force: bool = False,
                      limit: int = 0) -> list[dict]:
    """Kept templates whose reading is missing or stale, the frames' queue
    order first (known imgflip links, then template-type memes...), then
    the best-scoring."""
    with get_store() as store:
        kept = store.kept()
        units = store.units_for(sorted(kept), kept)
        todo = units if force else store.select_pending(units, stamps)
        todo.sort(key=lambda u: (u["priority"], -u["best_R"], u["template_id"]))
        return todo[:limit] if limit else todo


def units_for(template_ids: Iterable[int]) -> list[dict]:
    with get_store() as store:
        return store.units_for(template_ids)


def save_detection(record: dict) -> None:
    with get_store() as store:
        store.save_detection(record)


def save_failure(record: dict) -> None:
    with get_store() as store:
        store.save_failure(record)


def pending_linking(linker_stamps: dict[str, str], limit: int = 0) -> list[int]:
    with get_store() as store:
        return store.pending_linking(linker_stamps, limit)


def link_units_for(template_ids: Iterable[int]) -> list[dict]:
    with get_store() as store:
        return store.link_units_for(template_ids)


def save_links(records: Iterable[dict]) -> int:
    with get_store() as store:
        return store.save_links(records)


def entity_stats() -> dict[str, Any]:
    with get_store() as store:
        return store.stats()


def graph_mentions_for(template_ids: Iterable[int], linked_at_lte=None) -> dict[int, list[dict]]:
    with get_store() as store:
        return store.graph_mentions_for(template_ids, linked_at_lte)


def graph_stamps(linked_at_lte=None) -> dict[str, Any]:
    with get_store() as store:
        return store.graph_stamps(linked_at_lte)
