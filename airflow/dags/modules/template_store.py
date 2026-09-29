"""
template_store.py — MongoDB persistence for the imgflip template layer
=======================================================================
The ONLY place the templates DAG touches the database (dom_store rule,
applied to this stage). Knows nothing about HTTP or hashing — those are
imgflip_client.py and kg/visual.py; kg/templates.py decides what a record
says, this module only keeps it.

Collections
-----------
``entries``          (owned by the parse stage, read-only here)

``imgflip_pages``    the raw archive — ONE DOC PER FETCHED URL
    _id              sha1(url)
    url / final_url  as asked / after redirects (a template id -> its slug)
    kind             search | template
    query / page     for search pages
    html             zlib-compressed BSON Binary (dom_store's convention)
    content_sha256   of the uncompressed HTML
    ok, status_code, error, error_kind, transport, attempts, fetched_at
    A failed refetch never overwrites a good page: a parser fix must always
    have the page to re-read (``reparse_only``), and a blip must not erase
    what imgflip said yesterday.

``imgflip_templates``  ONE DOC PER IMGFLIP TEMPLATE ID, seen by any search
    _id              the template id (int)
    key, name, url, featured, animated, thumb_url      from the search page
    hashes           kg/visual.Hashes of the thumbnail; visual_version
    thumb_path       the thumbnail on disk (TEMPLATE_DATA_DIR/thumbs)
    alt_names, description, file_type, width, height, page_url
                     from its /memetemplate page (fetched for kept ones only)
    blank_url, blank_path, blank_sha256                the full-size image
    leader           the id this template's picture is represented by —
                     itself for a kept template, another id for a dropped
                     near-duplicate (Gabi: dropped duplicates live here, and
                     never in the graph); dedup_version
    Updates MERGE, and details are never downgraded: a search that sees a
    template again does not blank the alternate names its page gave.

``frame_templates``  ONE DOC PER FRAME
    _id              sha1(frame url) — the entry's own _id
    search           queries, gold links and ids, candidates
                     ([{t: id, q: query index, r: rank, pre: prefiltered}]),
                     the frame's own image hashes, errors, search_status,
                     and the stamps SEARCH_STAMP_KEYS
    selection        selected ([{template_id, R, method, mmr_rank, members,
                     s_text, s_vis, s_rank}]), suppressed, status
                     (selected | no_results | below_threshold | failed),
                     best_rejected, the SELECTION_STAMP_KEYS, selected_at,
                     assign_run
    "Selected nothing" is a stored outcome with a reason — the frame is not
    searched again until its stamps move.

``template_assignments``  ONE DOC PER SELECTION RUN
    The KG build reads selections as of the last COMPLETED run, so a build
    that overlaps a run never sees half of it.

Environment (docker-compose), same names as the other stores:
    MONGODB_URI, MONGODB_DB, MONGODB_ENTRIES_COLLECTION,
    MONGODB_IMGFLIP_PAGES_COLLECTION        (default: imgflip_pages)
    MONGODB_IMGFLIP_TEMPLATES_COLLECTION    (default: imgflip_templates)
    MONGODB_FRAME_TEMPLATES_COLLECTION      (default: frame_templates)
    MONGODB_TEMPLATE_ASSIGNMENTS_COLLECTION (default: template_assignments)
"""

from __future__ import annotations

import hashlib
import json
import logging
import zlib
from datetime import datetime, timedelta
from typing import Any, Iterable, Iterator

from modules.kg import templates as kg_templates
from modules.mongo_base import MongoStoreBase, as_utc, now_utc, url_doc_id

log = logging.getLogger("template_store")

__all__ = [
    "TemplateStore", "get_store", "pending_units", "units_for", "get_page",
    "save_page", "templates_by_id", "upsert_templates", "save_search",
    "iter_search_records", "save_selections", "set_leaders", "start_assignment",
    "complete_assignment", "templates_needing_details", "save_details",
    "template_stats", "selections_for", "selection_stamps",
    "SEARCH_STAMP_KEYS", "SELECTION_STAMP_KEYS",
]

UNIT_PROJECTION = {"_id": 1, "url": 1, "title": 1, "category": 1, "entry_type": 1,
                   "additional_references": 1, "og_image": 1, "sections.kind": 1,
                   "sections.images.src": 1}

SEARCH_STAMP_KEYS = ("query_version", "parser_version", "visual_version")
SELECTION_STAMP_KEYS = ("relevance_version", "dedup_version", "selection_version",
                        "visual_version")

# What scoring and dedup need from a template — not the page archive.
TEMPLATE_SCORING_FIELDS = ("name", "alt_names", "featured", "animated", "hashes",
                           "key", "url", "thumb_url", "file_type")

LOOKUP_BATCH = 2000
BULK_BATCH = 500


class TemplateStore(MongoStoreBase):
    """Owner of imgflip_pages, imgflip_templates, frame_templates and
    template_assignments; reads entries."""

    def _configure(self) -> None:
        self.entries = self.collection("MONGODB_ENTRIES_COLLECTION", "entries")
        self.pages = self.collection("MONGODB_IMGFLIP_PAGES_COLLECTION", "imgflip_pages")
        self.templates = self.collection("MONGODB_IMGFLIP_TEMPLATES_COLLECTION",
                                         "imgflip_templates")
        self.frames = self.collection("MONGODB_FRAME_TEMPLATES_COLLECTION",
                                      "frame_templates")
        self.assignments = self.collection("MONGODB_TEMPLATE_ASSIGNMENTS_COLLECTION",
                                           "template_assignments")
        self.pages.create_index("fetched_at")
        self.templates.create_index("leader")
        self.templates.create_index("detail_fetched_at")
        self.frames.create_index("search_status")
        self.frames.create_index("status")
        self.frames.create_index("selected_at")
        self.frames.create_index("selected.template_id")      # "who selected t"
        self.assignments.create_index("completed_at")

    # -- selection of frames to search --------------------------------------

    def iter_units(self, limit: int = 0) -> Iterator[dict]:
        """Every ELIGIBLE frame (kg/templates.frame_unit), streamed."""
        yielded = 0
        for entry in self.entries.find({}, UNIT_PROJECTION):
            unit = kg_templates.frame_unit(entry)
            if unit is None:
                continue
            yield unit
            yielded += 1
            if limit and yielded >= limit:
                return

    def select_pending(self, units: Iterable[dict], *, stamps: dict[str, str],
                       research_after_days: int = 0, force: bool = False) -> list[dict]:
        """The units whose search is missing, failed, stale by a stamp or
        by the source, or older than ``research_after_days`` (0 = never)."""
        units = list(units)
        if force or not units:
            return units
        by_id = {u["unit_id"]: u for u in units}
        fresh: set[str] = set()
        cutoff = (now_utc() - timedelta(days=research_after_days)
                  if research_after_days else None)
        projection = {"_id": 1, "source_sha256": 1, "search_status": 1,
                      "searched_at": 1, **{k: 1 for k in SEARCH_STAMP_KEYS}}
        ids = list(by_id)
        for i in range(0, len(ids), LOOKUP_BATCH):
            for doc in self.frames.find({"_id": {"$in": ids[i:i + LOOKUP_BATCH]}},
                                        projection):
                unit = by_id[doc["_id"]]
                if doc.get("search_status") in (None, "failed"):
                    continue
                if doc.get("source_sha256") != unit["source_sha256"]:
                    continue
                if any(doc.get(k) != stamps.get(k) for k in SEARCH_STAMP_KEYS):
                    continue
                searched = as_utc(doc.get("searched_at"))
                if cutoff is not None and (searched is None or searched < cutoff):
                    continue
                fresh.add(doc["_id"])
        return [u for u in units if u["unit_id"] not in fresh]

    def not_searched_since(self, units: Iterable[dict], since) -> list[dict]:
        """The retry-safe filter for a FORCED run (entity_store.not_linked_since)."""
        units = list(units)
        since = _as_datetime(since)
        if not units or since is None:
            return units
        ids = [u["unit_id"] for u in units]
        done: set[str] = set()
        for i in range(0, len(ids), LOOKUP_BATCH):
            done.update(d["_id"] for d in self.frames.find(
                {"_id": {"$in": ids[i:i + LOOKUP_BATCH]}, "searched_at": {"$gte": since}},
                {"_id": 1}))
        return [u for u in units if u["unit_id"] not in done]

    def units_for(self, unit_ids: Iterable[str]) -> list[dict]:
        wanted = list(dict.fromkeys(unit_ids))
        out: list[dict] = []
        for i in range(0, len(wanted), LOOKUP_BATCH):
            for entry in self.entries.find({"_id": {"$in": wanted[i:i + LOOKUP_BATCH]}},
                                           UNIT_PROJECTION):
                unit = kg_templates.frame_unit(entry)
                if unit:
                    out.append(unit)
        order = {uid: n for n, uid in enumerate(wanted)}
        out.sort(key=lambda u: order.get(u["unit_id"], len(order)))
        return out

    # -- the page archive ---------------------------------------------------

    def get_page(self, url: str, max_age_days: float | None = None) -> dict | None:
        """The archived page for ``url`` (html decompressed), or None when
        there is none, it failed, or it is older than ``max_age_days``."""
        doc = self.pages.find_one({"_id": url_doc_id(url)})
        if not doc or not doc.get("ok"):
            return None
        fetched = as_utc(doc.get("fetched_at"))
        if max_age_days is not None and (
                fetched is None or fetched < now_utc() - timedelta(days=max_age_days)):
            return None
        doc["html"] = zlib.decompress(bytes(doc["html"])).decode("utf-8")
        return doc

    def save_page(self, result: dict, *, kind: str, query: str | None = None,
                  page: int | None = None) -> str:
        """Archive one fetch (imgflip_client.PageResult.as_doc()). Returns
        ``saved`` / ``kept_ok`` (a failure did not overwrite a good page) /
        ``failed``."""
        from bson.binary import Binary

        _id = url_doc_id(result["url"])
        meta = {"url": result["url"], "kind": kind, "query": query, "page": page,
                "status_code": result.get("status_code"), "error": result.get("error"),
                "error_kind": result.get("error_kind"),
                "transport": result.get("transport"),
                "attempts": result.get("attempts_used"),
                "fetched_at": _as_datetime(result.get("fetched_at")) or now_utc()}
        if result.get("ok"):
            html = result["html"] or ""
            self.pages.replace_one({"_id": _id}, {
                **meta, "ok": True, "final_url": result.get("final_url"),
                "html": Binary(zlib.compress(html.encode("utf-8"), 6)),
                "content_sha256": hashlib.sha256(html.encode("utf-8")).hexdigest()},
                upsert=True)
            return "saved"
        existing = self.pages.find_one({"_id": _id}, {"ok": 1})
        if existing and existing.get("ok"):
            self.pages.update_one({"_id": _id}, {"$set": {
                "last_error": result.get("error"),
                "last_error_kind": result.get("error_kind"),
                "last_failed_at": meta["fetched_at"]}})
            return "kept_ok"
        self.pages.replace_one({"_id": _id}, {**meta, "ok": False}, upsert=True)
        return "failed"

    # -- templates ----------------------------------------------------------

    def templates_by_id(self, ids: Iterable[int],
                        fields: Iterable[str] | None = None) -> dict[int, dict]:
        wanted = list(dict.fromkeys(int(i) for i in ids))
        projection = {f: 1 for f in fields} if fields else None
        out: dict[int, dict] = {}
        for i in range(0, len(wanted), LOOKUP_BATCH):
            for doc in self.templates.find({"_id": {"$in": wanted[i:i + LOOKUP_BATCH]}},
                                           projection):
                out[int(doc["_id"])] = doc
        return out

    def upsert_templates(self, templates: dict[int, dict],
                         hashes: dict[int, dict] | None = None) -> int:
        """Merge search-level fields (and fresh hashes) into imgflip_templates.

        Search fields fill gaps only ($setOnInsert for identity, $set for the
        rest), and nothing here touches the fields a /memetemplate page
        gives, so a later search never downgrades them.
        """
        from pymongo import UpdateOne

        now = now_utc()
        ops = []
        for tid, t in templates.items():
            fields = {k: t[k] for k in ("key", "name", "url", "featured", "animated",
                                        "thumb_url") if t.get(k) is not None}
            update: dict[str, Any] = {"$set": {**fields, "search_seen_at": now},
                                      "$setOnInsert": {"first_seen_at": now}}
            h = (hashes or {}).get(tid)
            if h:
                update["$set"].update({"hashes": _hash_fields(h),
                                       "visual_version": h.get("visual_version"),
                                       "thumb_path": h.get("path")})
            ops.append(UpdateOne({"_id": int(tid)}, update, upsert=True))
        for i in range(0, len(ops), BULK_BATCH):
            self.templates.bulk_write(ops[i:i + BULK_BATCH], ordered=False)
        return len(ops)

    def save_hashes(self, tid: int, hashed: dict) -> None:
        self.templates.update_one({"_id": int(tid)}, {"$set": {
            "hashes": _hash_fields(hashed), "visual_version": hashed.get("visual_version"),
            "thumb_path": hashed.get("path"), "thumb_error": None}}, upsert=True)

    def save_thumb_error(self, tid: int, error: str) -> None:
        self.templates.update_one({"_id": int(tid)}, {"$set": {"thumb_error": error}},
                                  upsert=True)

    # -- search records -----------------------------------------------------

    def save_search(self, record: dict, stamps: dict[str, str]) -> None:
        """One frame's search (kg/templates.search_frame), minus the template
        bodies, which upsert_templates keeps. Leaves the selection alone:
        the next assignment run re-selects from what is stored."""
        doc = {k: v for k, v in record.items()
               if k not in ("unit_id", "templates", "hashes")}
        now = now_utc()
        doc.update({k: stamps[k] for k in SEARCH_STAMP_KEYS}, searched_at=now)
        self.frames.update_one({"_id": record["unit_id"]},
                               {"$set": doc, "$setOnInsert": {"first_searched_at": now}},
                               upsert=True)

    def iter_search_records(self) -> Iterator[dict]:
        """Every searched frame, as scoring needs it."""
        projection = {"frame_url": 1, "title": 1, "queries": 1, "gold_ids": 1,
                      "candidates": 1,
                      "frame_images": 1, "search_status": 1, "selection_sha": 1,
                      "priority": 1, **{k: 1 for k in SELECTION_STAMP_KEYS}}
        yield from self.frames.find({"search_status": {"$exists": True}}, projection)

    def save_selections(self, selections: Iterable[dict], *, stamps: dict[str, str],
                        run_id: str) -> dict[str, int]:
        """Write each frame's selection — only where it CHANGED (its
        selection_sha or its stamps), so an unchanged frame keeps its
        selected_at and a KG build is not made stale by a no-op run."""
        from pymongo import UpdateOne

        now = now_utc()
        ops, written, unchanged = [], 0, 0
        for sel in selections:
            sha = selection_sha(sel)
            if sel.get("previous_sha") == sha and not sel.get("stamps_moved"):
                unchanged += 1
                continue
            body = {k: sel[k] for k in ("selected", "suppressed", "status",
                                        "best_rejected", "accepted_groups")}
            body.update({k: stamps[k] for k in SELECTION_STAMP_KEYS},
                        selection_sha=sha, selected_at=now, assign_run=run_id)
            ops.append(UpdateOne({"_id": sel["unit_id"]}, {"$set": body}))
            written += 1
            if len(ops) >= BULK_BATCH:
                self.frames.bulk_write(ops, ordered=False)
                ops = []
        if ops:
            self.frames.bulk_write(ops, ordered=False)
        return {"written": written, "unchanged": unchanged}

    def set_leaders(self, leader_of: dict[int, int], dedup_version: str) -> int:
        """Record which kept template each clustered one is represented by."""
        from pymongo import UpdateOne

        ops = [UpdateOne({"_id": int(t)}, {"$set": {"leader": int(l),
                                                    "dedup_version": dedup_version}})
               for t, l in leader_of.items()]
        for i in range(0, len(ops), BULK_BATCH):
            self.templates.bulk_write(ops[i:i + BULK_BATCH], ordered=False)
        return len(ops)

    def start_assignment(self, run_id: str, stamps: dict[str, str]) -> None:
        self.assignments.replace_one({"_id": run_id}, {
            "_id": run_id, "started_at": now_utc(), "completed_at": None,
            "stamps": stamps}, upsert=True)

    def complete_assignment(self, run_id: str, counts: dict[str, Any]) -> None:
        self.assignments.update_one({"_id": run_id}, {"$set": {
            "completed_at": now_utc(), "counts": counts}})

    # -- details ------------------------------------------------------------

    def templates_needing_details(self, limit: int = 0) -> list[int]:
        """Kept templates some frame selects, whose /memetemplate page has
        not been read, in id order."""
        selected: set[int] = set()
        for doc in self.frames.find({"status": "selected"}, {"selected.template_id": 1}):
            selected.update(int(s["template_id"]) for s in doc.get("selected") or [])
        if not selected:
            return []
        have = {int(d["_id"]) for d in self.templates.find(
            {"_id": {"$in": sorted(selected)}, "detail_fetched_at": {"$ne": None}},
            {"_id": 1})}
        todo = sorted(selected - have)
        return todo[:limit] if limit else todo

    def save_details(self, tid: int, details: dict) -> None:
        details = {k: v for k, v in details.items() if v is not None}
        self.templates.update_one({"_id": int(tid)}, {"$set": {
            **details, "detail_fetched_at": now_utc()}}, upsert=True)

    def save_detail_error(self, tid: int, error: str, kind: str | None) -> None:
        self.templates.update_one({"_id": int(tid)}, {"$set": {
            "detail_error": error, "detail_error_kind": kind,
            "detail_failed_at": now_utc()}})

    # -- the KG build's reads ---------------------------------------------------

    def selections_for(self, entry_ids: Iterable[str],
                       selected_at_lte=None) -> dict[str, list[dict]]:
        """{frame_url: [template record]} for one KG build chunk, frozen at
        the build's snapshot. A record is the selection (template_id, R,
        method) plus the template's details: name, alt_names, file_type,
        url (its imgflip page), blank_url, width, height, animated,
        featured. Keyed by URL, as events_for and links_for are."""
        ids = list(entry_ids)
        if not ids:
            return {}
        query: dict[str, Any] = {"_id": {"$in": ids}, "status": "selected"}
        if selected_at_lte is not None:
            query["selected_at"] = {"$lte": selected_at_lte}
        frames = list(self.frames.find(query, {"frame_url": 1, "selected": 1}))
        tids = {int(s["template_id"]) for f in frames for s in f.get("selected") or []}
        details = self.templates_by_id(tids, ("name", "alt_names", "file_type", "url",
                                              "blank_url", "width", "height",
                                              "animated", "featured"))
        out: dict[str, list[dict]] = {}
        for f in frames:
            recs = []
            for s in f.get("selected") or []:
                d = details.get(int(s["template_id"]), {})
                recs.append({"template_id": int(s["template_id"]), "R": s.get("R"),
                             "method": s.get("method"),
                             **{k: d.get(k) for k in ("name", "alt_names", "file_type",
                                                      "url", "blank_url", "width",
                                                      "height", "animated", "featured")}})
            if f.get("frame_url") and recs:
                out[f["frame_url"]] = recs
        return out

    def selection_stamps(self, selected_at_lte=None) -> dict[str, Any]:
        """The template layer's contribution to the KG staleness gate. A
        re-selection that changes WHICH templates or their scores changes
        ``templates_selection_shas`` even when the counts stay put."""
        query: dict[str, Any] = {"status": {"$exists": True}}
        if selected_at_lte is not None:
            query["selected_at"] = {"$lte": selected_at_lte}
        digest = hashlib.sha256()
        frames = links = 0
        latest = None
        versions: set[str] = set()
        for d in self.frames.find(query, {"selection_sha": 1, "selected_at": 1,
                                          "relevance_version": 1, "dedup_version": 1,
                                          "selection_version": 1, "selected": 1}
                                  ).sort("_id", 1):
            frames += 1
            links += len(d.get("selected") or [])
            digest.update(f"{d['_id']}:{d.get('selection_sha')};".encode())
            at = as_utc(d.get("selected_at"))
            if at is not None and (latest is None or at > latest):
                latest = at
            versions.add("/".join(str(d.get(k)) for k in (
                "relevance_version", "dedup_version", "selection_version")))
        detailed = self.templates.count_documents({"detail_fetched_at": {"$ne": None}})
        return {"templates_frames": frames, "templates_links": links,
                "templates_selection_digest": digest.hexdigest()[:16],
                "templates_versions": sorted(versions),
                "templates_detailed": detailed,
                "templates_max_selected_at": latest.isoformat() if latest else None}

    # -- stats --------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        def by(field: str) -> dict[str, int]:
            return {str(r["_id"]): r["n"] for r in self.frames.aggregate([
                {"$group": {"_id": f"${field}", "n": {"$sum": 1}}}])}

        kept = self.frames.aggregate([
            {"$unwind": "$selected"},
            {"$group": {"_id": "$selected.template_id"}}, {"$count": "n"}])
        kept_n = next(iter(kept), {}).get("n", 0)
        k_hist = {str(r["_id"]): r["n"] for r in self.frames.aggregate([
            {"$match": {"status": {"$exists": True}}},
            {"$group": {"_id": {"$size": {"$ifNull": ["$selected", []]}},
                        "n": {"$sum": 1}}}])}
        return {
            "frames_searched": self.frames.count_documents({"search_status": {"$exists": True}}),
            "search_status": by("search_status"),
            "selection_status": by("status"),
            "templates_per_frame": dict(sorted(k_hist.items(), key=lambda kv: int(kv[0]))),
            "templates_seen": self.templates.count_documents({}),
            "templates_hashed": self.templates.count_documents({"hashes": {"$ne": None}}),
            "templates_kept": kept_n,
            "templates_dropped_as_duplicates": self.templates.count_documents(
                {"$expr": {"$and": [{"$ne": ["$leader", None]},
                                    {"$ne": ["$leader", "$_id"]}]}}),
            "templates_with_details": self.templates.count_documents(
                {"detail_fetched_at": {"$ne": None}}),
            "pages_archived": self.pages.count_documents({"ok": True}),
        }


def selection_sha(sel: dict) -> str:
    """What a selection SAYS, independent of when it was made."""
    body = {"status": sel["status"],
            "selected": [(s["template_id"], s["R"], s["method"]) for s in sel["selected"]]}
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def _hash_fields(h: dict) -> dict:
    return {k: h[k] for k in ("md5", "phash", "phash_mirror", "dhash", "dhash_mirror",
                              "width", "height") if k in h}


def _as_datetime(value) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return as_utc(value)
    try:
        return as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def get_store(uri: str | None = None, db_name: str | None = None) -> TemplateStore:
    return TemplateStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facade functions — the only calls the templates DAG makes
# ---------------------------------------------------------------------------

def pending_units(*, stamps: dict[str, str], research_after_days: int = 0,
                  force: bool = False, limit: int = 0, sample_seed: int = 0) -> list[dict]:
    """Eligible frames whose search is missing or stale, highest priority
    first (known imgflip link, then template-type memes, then the rest).

    ``sample_seed`` (non-zero, with a ``limit``): instead, a reproducible
    random sample of ``limit`` frames, each priority represented in
    proportion to its share of the pending frames — what a whole run would
    look like, for measuring the pool before committing to it."""
    with get_store() as store:
        pending: list[dict] = []
        batch: list[dict] = []
        for unit in store.iter_units():
            batch.append(unit)
            if len(batch) >= LOOKUP_BATCH:
                pending.extend(store.select_pending(
                    batch, stamps=stamps, research_after_days=research_after_days,
                    force=force))
                batch = []
        if batch:
            pending.extend(store.select_pending(
                batch, stamps=stamps, research_after_days=research_after_days,
                force=force))
        pending.sort(key=lambda u: (u["priority"], u["unit_id"]))
        if sample_seed and limit and limit < len(pending):
            return stratified_sample(pending, limit, sample_seed)
        return pending[:limit] if limit else pending


def stratified_sample(units: list[dict], n: int, seed: int) -> list[dict]:
    """``n`` units, each priority in proportion to its share (largest
    remainders), drawn with a seeded RNG; returned in priority order."""
    import random

    by: dict[int, list[dict]] = {}
    for u in units:
        by.setdefault(u["priority"], []).append(u)
    total = len(units)
    quota = {p: n * len(us) / total for p, us in by.items()}
    take = {p: int(q) for p, q in quota.items()}
    for p in sorted(by, key=lambda p: quota[p] - take[p], reverse=True)[:n - sum(take.values())]:
        take[p] += 1
    rng = random.Random(seed)
    out: list[dict] = []
    for p in sorted(by):
        pool = sorted(by[p], key=lambda u: u["unit_id"])
        out.extend(sorted(rng.sample(pool, min(take[p], len(pool))),
                          key=lambda u: u["unit_id"]))
    return out


def units_for(unit_ids: Iterable[str]) -> list[dict]:
    with get_store() as store:
        return store.units_for(unit_ids)


def get_page(url: str, max_age_days: float | None = None) -> dict | None:
    with get_store() as store:
        return store.get_page(url, max_age_days)


def save_page(result: dict, *, kind: str, query: str | None = None,
              page: int | None = None) -> str:
    with get_store() as store:
        return store.save_page(result, kind=kind, query=query, page=page)


def templates_by_id(ids: Iterable[int], fields: Iterable[str] | None = None) -> dict[int, dict]:
    with get_store() as store:
        return store.templates_by_id(ids, fields)


def upsert_templates(templates: dict[int, dict], hashes: dict[int, dict] | None = None) -> int:
    with get_store() as store:
        return store.upsert_templates(templates, hashes)


def save_search(record: dict, stamps: dict[str, str]) -> None:
    with get_store() as store:
        store.save_search(record, stamps)


def iter_search_records() -> list[dict]:
    with get_store() as store:
        return list(store.iter_search_records())


def save_selections(selections: Iterable[dict], *, stamps: dict[str, str],
                    run_id: str) -> dict[str, int]:
    with get_store() as store:
        return store.save_selections(selections, stamps=stamps, run_id=run_id)


def set_leaders(leader_of: dict[int, int], dedup_version: str) -> int:
    with get_store() as store:
        return store.set_leaders(leader_of, dedup_version)


def start_assignment(run_id: str, stamps: dict[str, str]) -> None:
    with get_store() as store:
        store.start_assignment(run_id, stamps)


def complete_assignment(run_id: str, counts: dict[str, Any]) -> None:
    with get_store() as store:
        store.complete_assignment(run_id, counts)


def templates_needing_details(limit: int = 0) -> list[int]:
    with get_store() as store:
        return store.templates_needing_details(limit)


def save_details(tid: int, details: dict) -> None:
    with get_store() as store:
        store.save_details(tid, details)


def template_stats() -> dict[str, Any]:
    with get_store() as store:
        return store.stats()


def selections_for(entry_ids: Iterable[str], selected_at_lte=None) -> dict[str, list[dict]]:
    with get_store() as store:
        return store.selections_for(entry_ids, selected_at_lte)


def selection_stamps(selected_at_lte=None) -> dict[str, Any]:
    with get_store() as store:
        return store.selection_stamps(selected_at_lte)
