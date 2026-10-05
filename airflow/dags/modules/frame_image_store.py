"""
frame_image_store.py — MongoDB persistence for what frames' own images show
============================================================================
The ONLY place kym_frame_image_entities touches the database, and the
frame counterpart of template_entity_store.py. Knows nothing about the
vision model or the lexicon — kg/frame_images.py decides what a record says.

Collections
-----------
``entries``, ``entities``   (read-only)
    the frame's image URL (og:image), title, category and About (the
    model's context), and the Wikidata items its text already links (the
    linker's preferred senses).

``frame_image_entities``  ONE DOC PER FRAME (``_id`` = the frame's URL)
    detection     the vision model's reading, as for templates
    DETECTION_STAMP_KEYS at the top level: any of them moving re-reads the
                  frame — a new prompt, schema, model, or a new image URL
                  (KYM's image paths change when the picture does)
    links         each region linked to Wikidata, with in_graph;
                  LINK_STAMP_KEYS at the top level re-link without re-reading

``frame_image_entity_failures``  dead letters, one per frame, retried only
    when its stamps or image change; a later success clears it.

Images are cached on disk under ``<KG_DATA_DIR>/frame_images/files/``,
named by the sha1 of their URL, so a re-read never downloads again.

Environment: MONGODB_URI, MONGODB_DB, MONGODB_ENTRIES_COLLECTION,
MONGODB_ENTITIES_COLLECTION, MONGODB_FRAME_IMAGE_ENTITIES_COLLECTION
(frame_image_entities), MONGODB_FRAME_IMAGE_ENTITY_FAILURES_COLLECTION
(frame_image_entity_failures), KG_DATA_DIR.
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from modules.mongo_base import MongoStoreBase, as_utc, now_utc

log = logging.getLogger("frame_image_store")

__all__ = [
    "FrameImageStore", "get_store", "pending_detection", "units_for", "image_path",
    "read_image", "write_image", "save_detection", "save_failure", "pending_linking",
    "link_units_for", "save_links", "entity_stats", "graph_mentions_for",
    "graph_stamps", "DETECTION_STAMP_KEYS", "LINK_STAMP_KEYS",
]

DETECTION_STAMP_KEYS = ("reader_version", "prompt_version", "schema_sha",
                        "requested_model", "image_url")
LINK_STAMP_KEYS = ("linker_version", "lexicon_version", "nlp_model", "senses_version",
                   "template_link_version", "detection_sha", "link_context_sha")
LOOKUP_BATCH = 2000
BULK_BATCH = 200
ENTRY_PROJECTION = {"url": 1, "og_image": 1, "title": 1, "category": 1,
                    "sections.kind": 1, "sections.text": 1}


def _images_dir() -> Path:
    return Path(os.getenv("KG_DATA_DIR", "/opt/airflow/data/kg")) / "frame_images" / "files"


def image_path(image_url: str) -> Path:
    """Where a frame image is cached: the sha1 of its URL, with its extension."""
    digest = hashlib.sha1(image_url.encode("utf-8")).hexdigest()
    ext = os.path.splitext(urlparse(image_url).path)[1].lower()[:6] or ".img"
    return _images_dir() / digest[:2] / f"{digest}{ext}"


def read_image(unit: dict) -> bytes | None:
    path = image_path(unit["image_url"])
    return path.read_bytes() if path.exists() else None


def write_image(image_url: str, content: bytes) -> Path:
    path = image_path(image_url)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)
    return path


class FrameImageStore(MongoStoreBase):
    """Owner of frame_image_entities and frame_image_entity_failures."""

    def _configure(self) -> None:
        self.entries = self.collection("MONGODB_ENTRIES_COLLECTION", "entries")
        self.entities = self.collection("MONGODB_ENTITIES_COLLECTION", "entities")
        self.detections = self.collection("MONGODB_FRAME_IMAGE_ENTITIES_COLLECTION",
                                          "frame_image_entities")
        self.failures = self.collection("MONGODB_FRAME_IMAGE_ENTITY_FAILURES_COLLECTION",
                                        "frame_image_entity_failures")
        self.detections.create_index("detected_at")
        self.detections.create_index("linked_at")
        self.detections.create_index("links.mentions.qid")

    # -- units ----------------------------------------------------------------

    def units_for(self, frame_urls: Iterable[str] | None = None) -> list[dict]:
        """Detection units, memes first (IMKG's use cases ask about
        kym:Meme), then by URL: {frame_url, image_url, category, context}.
        A frame without an image is not a unit."""
        from modules.kg import frame_images as fi

        query: dict[str, Any] = {"og_image": {"$nin": [None, ""]}}
        if frame_urls is not None:
            query["url"] = {"$in": list(dict.fromkeys(frame_urls))}
        units = []
        for e in self.entries.find(query, ENTRY_PROJECTION):
            if not e.get("url"):
                continue
            units.append({"frame_url": e["url"], "image_url": e["og_image"],
                          "category": e.get("category") or "",
                          "context": fi.frame_context(e)})
        units.sort(key=lambda u: (u["category"] != "meme", u["frame_url"]))
        return units

    def _matching(self, coll, urls: list[str], want: dict[str, dict]) -> set[str]:
        """The URLs whose doc in ``coll`` carries exactly ``want[url]``."""
        out: set[str] = set()
        proj = {k: 1 for k in DETECTION_STAMP_KEYS}
        for i in range(0, len(urls), LOOKUP_BATCH):
            for d in coll.find({"_id": {"$in": urls[i:i + LOOKUP_BATCH]}}, proj):
                if all(d.get(k) == want[d["_id"]][k] for k in DETECTION_STAMP_KEYS):
                    out.add(d["_id"])
        return out

    def select_pending(self, units: Iterable[dict], stamps: dict[str, str]) -> list[dict]:
        """Units whose detection is missing or stale, minus those that failed
        under exactly the same stamps and image URL (dead letters)."""
        units = list(units)
        if not units:
            return []
        want = {u["frame_url"]: {**stamps, "image_url": u["image_url"]} for u in units}
        urls = list(want)
        done = self._matching(self.detections, urls, want) | self._matching(
            self.failures, urls, want)
        return [u for u in units if u["frame_url"] not in done]

    def not_detected_since(self, units: Iterable[dict], since) -> list[dict]:
        units = list(units)
        since = _as_datetime(since)
        if not units or since is None:
            return units
        done = {d["_id"] for d in self.detections.find(
            {"_id": {"$in": [u["frame_url"] for u in units]},
             "detected_at": {"$gte": since}}, {"_id": 1})}
        return [u for u in units if u["frame_url"] not in done]

    # -- writes ---------------------------------------------------------------

    def save_detection(self, record: dict) -> None:
        """A successful reading REPLACES the frame's detection; a later
        success clears the dead letter."""
        url = record["frame_url"]
        now = now_utc()
        self.detections.update_one({"_id": url}, {
            "$set": {"detection": record, **{k: record.get(k) for k in DETECTION_STAMP_KEYS},
                     "detected_at": now, "region_count": len(record.get("regions") or [])},
            "$setOnInsert": {"first_detected_at": now}}, upsert=True)
        self.failures.delete_one({"_id": url})

    def save_failure(self, record: dict) -> None:
        self.failures.replace_one({"_id": record["frame_url"]}, {
            **{k: record.get(k) for k in DETECTION_STAMP_KEYS},
            "error": record.get("error"), "error_kind": record.get("error_kind"),
            "attempts": record.get("attempts"), "failed_at": now_utc()}, upsert=True)

    # -- linking --------------------------------------------------------------

    def link_contexts(self, urls: list[str]) -> dict[str, tuple[str, list[int], str]]:
        """{url: (context text, preferred QIDs as ints, its sha)}: the frame's
        title and About, and the items its own text already links."""
        out: dict[str, tuple[str, list[int], str]] = {}
        units = {u["frame_url"]: u for u in self.units_for(urls)}
        prefer: dict[str, set[int]] = {u: set() for u in urls}
        # entities is keyed by the entry's hash id; its frame_url is the join.
        for i in range(0, len(urls), LOOKUP_BATCH):
            for e in self.entities.find({"frame_url": {"$in": urls[i:i + LOOKUP_BATCH]}},
                                        {"frame_url": 1, "mentions.qid": 1, "self_qid": 1}):
                for q in [e.get("self_qid"), *[m.get("qid") for m in e.get("mentions") or []]]:
                    if isinstance(q, str) and q.startswith("Q") and q[1:].isdigit():
                        prefer.setdefault(e["frame_url"], set()).add(int(q[1:]))
        for url in urls:
            ctx = (units.get(url) or {}).get("context") or {}
            text = " ".join([ctx.get("title", ""), ctx.get("about", "")])
            pref = sorted(prefer.get(url, ()))
            sha = hashlib.sha256((text + "|" + ",".join(map(str, pref))).encode()
                                 ).hexdigest()[:16]
            out[url] = (text, pref, sha)
        return out

    def link_units_for(self, urls: Iterable[str]) -> list[dict]:
        urls = list(dict.fromkeys(urls))
        contexts = self.link_contexts(urls)
        return [{"frame_url": d["_id"], "detection": d["detection"],
                 "context_text": contexts[d["_id"]][0], "prefer": contexts[d["_id"]][1],
                 "link_context_sha": contexts[d["_id"]][2]}
                for d in self.detections.find({"_id": {"$in": urls}, "detection.ok": True},
                                              {"detection": 1})]

    def pending_linking(self, linker_stamps: dict[str, str], limit: int = 0) -> list[str]:
        """Frames with a detection whose links are missing or stale."""
        from modules.kg import frame_images as fi

        docs = list(self.detections.find(
            {"detection.ok": True},
            {"detection": 1, **{f"links.{k}": 1 for k in LINK_STAMP_KEYS}}))
        contexts = self.link_contexts([d["_id"] for d in docs])
        todo: list[str] = []
        for d in docs:
            links = d.get("links") or {}
            want = {**linker_stamps, "detection_sha": fi.detection_sha(d["detection"]),
                    "link_context_sha": contexts[d["_id"]][2]}
            if any(links.get(k) != want[k] for k in LINK_STAMP_KEYS):
                todo.append(d["_id"])
            if limit and len(todo) >= limit:
                break
        return todo

    def save_links(self, records: Iterable[dict]) -> int:
        from pymongo import UpdateOne

        now = now_utc()
        ops = [UpdateOne({"_id": r["frame_url"]}, {"$set": {
            "links": r, "mention_count": r["mention_count"],
            "in_graph_count": r["in_graph_count"], "linked_at": now}}) for r in records]
        for i in range(0, len(ops), BULK_BATCH):
            self.detections.bulk_write(ops[i:i + BULK_BATCH], ordered=False)
        return len(ops)

    # -- the KG build's reads -------------------------------------------------

    def graph_mentions_for(self, frame_urls: Iterable[str],
                           linked_at_lte=None) -> dict[str, list[dict]]:
        """{frame url: [mention]} — only the mentions marked in_graph, each
        with the model that read the image, frozen at the build's snapshot."""
        urls = list(dict.fromkeys(frame_urls))
        if not urls:
            return {}
        query: dict[str, Any] = {"_id": {"$in": urls}, "in_graph_count": {"$gt": 0}}
        if linked_at_lte is not None:
            query["linked_at"] = {"$lte": linked_at_lte}
        out: dict[str, list[dict]] = {}
        for d in self.detections.find(query, {"links.mentions": 1, "detection.model": 1}):
            model = (d.get("detection") or {}).get("model")
            out[d["_id"]] = [
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
            {"$group": {"_id": None, "frames": {"$sum": 1},
                        "in_graph": {"$sum": "$in_graph_count"},
                        "max_linked_at": {"$max": "$linked_at"},
                        "lexicons": {"$addToSet": "$links.lexicon_version"},
                        "prompts": {"$addToSet": "$prompt_version"},
                        "models": {"$addToSet": "$detection.model"}}}]))
        row = agg[0] if agg else {}
        latest = as_utc(row.get("max_linked_at"))
        return {"frame_images_frames": row.get("frames", 0),
                "frame_images_in_graph": row.get("in_graph", 0),
                "frame_images_lexicons": sorted(x for x in row.get("lexicons") or [] if x),
                "frame_images_prompts": sorted(x for x in row.get("prompts") or [] if x),
                "frame_images_models": sorted(x for x in row.get("models") or [] if x),
                "frame_images_max_linked_at": latest.isoformat() if latest else None}

    # -- stats ----------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        from modules.kg import frame_images as fi

        blind = {"named": 0, "confirmed": 0, "context_only": 0, "frames": 0}
        for d in self.detections.find({"detection.blind.regions": {"$exists": True}},
                                      {"detection.regions": 1, "detection.blind": 1}):
            agree = fi.blind_agreement(d["detection"].get("regions") or [],
                                       d["detection"]["blind"]["regions"])
            blind["frames"] += 1
            for k in ("named", "confirmed", "context_only"):
                blind[k] += agree[k]
        return {
            "frames_with_image": self.entries.count_documents({"og_image": {"$nin": [None, ""]}}),
            "frames_detected": self.detections.count_documents({"detection.ok": True}),
            "frames_failed": self.failures.count_documents({}),
            "frames_linked": self.detections.count_documents({"links": {"$exists": True}}),
            "mentions_in_graph": sum(d.get("in_graph_count", 0) for d in self.detections.find(
                {}, {"in_graph_count": 1})),
            "blind_audit": {**blind, "context_only_rate": round(
                blind["context_only"] / blind["named"], 3) if blind["named"] else None},
        }


def _as_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return as_utc(value)
    try:
        return as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def get_store(uri: str | None = None, db_name: str | None = None) -> FrameImageStore:
    return FrameImageStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facades — the only calls the DAG and the KG build make
# ---------------------------------------------------------------------------

def pending_detection(*, stamps: dict[str, str], force: bool = False,
                      limit: int = 0) -> list[dict]:
    with get_store() as store:
        units = store.units_for()
        todo = units if force else store.select_pending(units, stamps)
    return todo[:limit] if limit else todo


def units_for(frame_urls: Iterable[str]) -> list[dict]:
    with get_store() as store:
        return store.units_for(frame_urls)


def save_detection(record: dict) -> None:
    with get_store() as store:
        store.save_detection(record)


def save_failure(record: dict) -> None:
    with get_store() as store:
        store.save_failure(record)


def pending_linking(linker_stamps: dict[str, str], limit: int = 0) -> list[str]:
    with get_store() as store:
        return store.pending_linking(linker_stamps, limit)


def link_units_for(frame_urls: Iterable[str]) -> list[dict]:
    with get_store() as store:
        return store.link_units_for(frame_urls)


def save_links(records: Iterable[dict]) -> int:
    with get_store() as store:
        return store.save_links(records)


def entity_stats() -> dict[str, Any]:
    with get_store() as store:
        return store.stats()


def graph_mentions_for(frame_urls: Iterable[str], linked_at_lte=None) -> dict[str, list[dict]]:
    with get_store() as store:
        return store.graph_mentions_for(frame_urls, linked_at_lte)


def graph_stamps(linked_at_lte=None) -> dict[str, Any]:
    with get_store() as store:
        return store.graph_stamps(linked_at_lte)
