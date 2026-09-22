"""
entity_store.py — MongoDB persistence for Wikidata entity links
===================================================================
The ONLY place the entity DAG touches the database (dom_store rule, applied
to the entity stage). Knows nothing about spaCy or the lexicon —
kg/entities.py knows nothing about Mongo; the DAG glues them via
frame_unit() -> Linker.link() -> save_links().

Collections
-----------
``entries``   (owned by the parse stage, read-only here)
    We read url, title, tags, the About section and parser_version to build
    the linking units. Streamed, never materialised — an entries doc is
    multi-MB, and the projection below still carries every section's text.

``entities``  (owned by this module) — ONE DOC PER FRAME
    _id                 sha1(url) — the entry's own _id (kg/entities.frame_key)
    entry_id            the same, for symmetry with `events`
    frame_url           canonical page URL
    mentions            list of links, each grounded in the page
                        (kg/entities.py): field (title/tag/about), text,
                        start, end, tag_index, qid, label, description,
                        score, method, ner_label, proper, matched,
                        candidates, margin, features
    mention_count / entity_count     denormalised for coverage stats
    nil                 NER spans no Wikidata item was found for — what the
                        lexicon is missing, kept so coverage is measurable
    rejected_count      spans whose best candidate scored under the threshold
    self_qid            the frame's own Wikidata item (P13484), if any
    source_sha256       sha256 of (url, title, tags, About) — the staleness
                        key: an edit to any of them re-links THIS frame
    linker_version / lexicon_version / nlp_model
                        the three stamps that make a link reproducible; any
                        of them moving re-queues the frame
    parser_version / linked_at / elapsed_s

No dead-letter collection, unlike `events`/`event_failures`. That one
exists because a model call fails for reasons outside the pipeline (a host
down, a grammar the sampler cannot follow) and a blind retry costs GPU
minutes. Linking is local and deterministic: the only way a frame fails is
a bug, which must fail the task loudly — kg/entities.link_units raises on
an audit failure rather than writing — not sit quietly in a collection.

Why the unit is a frame
-----------------------
The event layer's reasons, restated: "linked, found nothing" must be
representable (a frame whose About is two sentences of slang may link
nothing, and must not be re-queued forever); a re-link is one atomic
single-document write, so a frame can never hold half of an old linking and
half of a new one (Mongo here is standalone — no transactions); and the
staleness key (the frame's text) and the document key are the same thing.

A re-link REPLACES a frame's mention list; it never merges. Two linkings
under different lexicons are two readings, not one.

Connection settings come from the environment (docker-compose), same
variable names as the other stores:
    MONGODB_URI                   (default: mongodb://localhost:27017)
    MONGODB_DB                    (default: memes)
    MONGODB_ENTRIES_COLLECTION    (default: entries)
    MONGODB_ENTITIES_COLLECTION   (default: entities)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterable, Iterator

from modules.kg import entities as kg_entities
from modules.mongo_base import MongoStoreBase, as_utc, now_utc

log = logging.getLogger("entity_store")

__all__ = [
    "EntityStore", "get_store", "pending_units", "units_for", "save_links",
    "links_for", "linking_stamps", "entity_stats", "STALENESS_KEYS",
]

# Entry fields the unit builder needs. `sections` is the big one: the About
# text is in there with every other section's, which is why reads stream.
UNIT_PROJECTION = {"_id": 1, "url": 1, "title": 1, "tags": 1,
                   "sections.kind": 1, "sections.text": 1, "parser_version": 1}

# The stamps that make a linking reproducible. All must match for a stored
# frame to count as up to date.
STALENESS_KEYS = ("source_sha256", "linker_version", "lexicon_version",
                  "nlp_model")

# What the KG build needs from each mention — the rest (features, margin,
# offsets) is for review and curation, and stays here.
LINK_PROJECTION_FIELDS = ("field", "text", "qid", "label", "description",
                          "score", "method", "ner_label")

LOOKUP_BATCH = 2000
BULK_BATCH = 500


class EntityStore(MongoStoreBase):
    """Owner of ``entities``; reads ``entries``."""

    def _configure(self) -> None:
        self.entries = self.collection("MONGODB_ENTRIES_COLLECTION", "entries")
        self.entities = self.collection("MONGODB_ENTITIES_COLLECTION", "entities")
        self.entities.create_index("frame_url")
        self.entities.create_index("linked_at")          # the snapshot filter
        self.entities.create_index("mentions.qid")       # multikey: "who mentions Q42"
        self.entities.create_index("lexicon_version")    # re-link progress

    # -- selection ----------------------------------------------------------

    def iter_frame_units(self, ready_only: bool = False,
                         limit: int = 0) -> Iterator[dict]:
        """Stream one unit per entry. A GENERATOR on purpose (see the
        module docstring); ``limit`` counts units."""
        query: dict[str, Any] = {}
        if ready_only:
            query["corpus_status"] = "ready"
        yielded = 0
        for entry in self.entries.find(query, UNIT_PROJECTION):
            unit = kg_entities.frame_unit(entry)
            if not unit:
                continue
            yield unit
            yielded += 1
            if limit and yielded >= limit:
                return

    def select_pending(self, units: Iterable[dict], *, stamps: dict[str, str],
                       force: bool = False, limit: int = 0) -> list[dict]:
        """The subset of ``units`` whose stored linking is missing or stale
        against ``stamps`` (linker_version, lexicon_version, nlp_model)."""
        units = list(units)
        if not units:
            return []
        if force:
            return units[:limit] if limit else units
        by_id = {u["unit_id"]: u for u in units}
        ids = list(by_id)
        fresh: set[str] = set()
        projection = {"_id": 1, **{k: 1 for k in STALENESS_KEYS}}
        for i in range(0, len(ids), LOOKUP_BATCH):
            for doc in self.entities.find({"_id": {"$in": ids[i:i + LOOKUP_BATCH]}},
                                          projection):
                unit = by_id.get(doc["_id"])
                if unit and _up_to_date(doc, unit, stamps):
                    fresh.add(doc["_id"])
        pending = [u for u in units if u["unit_id"] not in fresh]
        return pending[:limit] if limit else pending

    def not_linked_since(self, units: Iterable[dict], since) -> list[dict]:
        """The units NOT already (re-)linked at or after ``since`` — the
        retry-safe filter for a FORCED run (event_store.not_extracted_since
        explains why select_pending cannot serve there)."""
        units = list(units)
        since = _as_datetime(since)
        if not units or since is None:
            return units
        ids = [u["unit_id"] for u in units]
        done: set[str] = set()
        for i in range(0, len(ids), LOOKUP_BATCH):
            done.update(d["_id"] for d in self.entities.find(
                {"_id": {"$in": ids[i:i + LOOKUP_BATCH]},
                 "linked_at": {"$gte": since}}, {"_id": 1}))
        return [u for u in units if u["unit_id"] not in done]

    def units_for(self, unit_ids: Iterable[str]) -> list[dict]:
        """Rebuild specific units from `entries` — the DAG passes ids, never
        unit bodies, between tasks."""
        wanted = list(dict.fromkeys(unit_ids))
        if not wanted:
            return []
        out: list[dict] = []
        for i in range(0, len(wanted), LOOKUP_BATCH):
            for entry in self.entries.find({"_id": {"$in": wanted[i:i + LOOKUP_BATCH]}},
                                           UNIT_PROJECTION):
                unit = kg_entities.frame_unit(entry)
                if unit:
                    out.append(unit)
        order = {uid: n for n, uid in enumerate(wanted)}
        out.sort(key=lambda u: order.get(u["unit_id"], len(order)))
        return out

    # -- writes -------------------------------------------------------------

    def save_links(self, records: Iterable[dict]) -> dict[str, int]:
        """Upsert linking records, REPLACING each frame's mention list."""
        from pymongo import UpdateOne

        ops: list[Any] = []
        frames = mentions = 0
        now = now_utc()

        def flush() -> None:
            if ops:
                self.entities.bulk_write(ops, ordered=False)
                ops.clear()

        for record in records:
            doc = dict(record)
            unit_id = doc.pop("unit_id")
            doc["linked_at"] = _as_datetime(doc.get("linked_at")) or now
            doc["mention_count"] = len(doc.get("mentions") or [])
            ops.append(UpdateOne({"_id": unit_id},
                                 {"$set": doc,
                                  "$setOnInsert": {"first_linked_at": now}},
                                 upsert=True))
            frames += 1
            mentions += doc["mention_count"]
            if len(ops) >= BULK_BATCH:
                flush()
        flush()
        return {"frames": frames, "mentions": mentions}

    # -- reads / stats ------------------------------------------------------

    def links_for(self, entry_ids: Iterable[str],
                  linked_at_lte=None) -> dict[str, list[dict]]:
        """{frame_url: [mention, ...]} for one KG build chunk — only the
        fields kg/build.py uses (LINK_PROJECTION_FIELDS).

        Keyed by URL because the KG build's entries carry no _id (see
        event_store.events_for). ``linked_at_lte`` freezes the read at the
        build's snapshot. Order is the stored order (title, tags, About; page
        order within each), so every CSV's row order is reproducible.
        """
        ids = list(entry_ids)
        if not ids:
            return {}
        query: dict[str, Any] = {"_id": {"$in": ids}, "mention_count": {"$gt": 0}}
        if linked_at_lte is not None:
            query["linked_at"] = {"$lte": linked_at_lte}
        projection = {"frame_url": 1,
                      **{f"mentions.{f}": 1 for f in LINK_PROJECTION_FIELDS}}
        out: dict[str, list[dict]] = {}
        for doc in self.entities.find(query, projection):
            if doc.get("frame_url"):
                out[doc["frame_url"]] = list(doc.get("mentions") or [])
        return out

    def linking_stamps(self, linked_at_lte=None) -> dict[str, Any]:
        """The entity layer's contribution to the KG staleness gate.

        ``entities_mentions`` rides along with ``entities_frames`` because a
        re-link (a new lexicon) changes what the links SAY without changing
        how many frames were linked — and that must rebuild the graph.
        """
        query: dict[str, Any] = {}
        if linked_at_lte is not None:
            query["linked_at"] = {"$lte": linked_at_lte}
        agg = list(self.entities.aggregate([
            {"$match": query},
            {"$group": {"_id": None,
                        "frames": {"$sum": 1},
                        "mentions": {"$sum": "$mention_count"},
                        "max_linked_at": {"$max": "$linked_at"},
                        "linker_versions": {"$addToSet": "$linker_version"},
                        "lexicon_versions": {"$addToSet": "$lexicon_version"},
                        "nlp_models": {"$addToSet": "$nlp_model"}}},
        ]))
        row = agg[0] if agg else {}
        latest = as_utc(row.get("max_linked_at"))
        return {
            "entities_frames": row.get("frames", 0),
            "entities_mentions": row.get("mentions", 0),
            "entities_linker_versions": sorted(row.get("linker_versions") or []),
            "entities_lexicon_versions": sorted(row.get("lexicon_versions") or []),
            "entities_nlp_models": sorted(row.get("nlp_models") or []),
            "entities_max_linked_at": latest.isoformat() if latest else None,
        }

    def stats(self) -> dict[str, Any]:
        def unwound(field: str) -> dict[str, int]:
            return {str(r["_id"]): r["n"] for r in self.entities.aggregate([
                {"$unwind": "$mentions"},
                {"$group": {"_id": f"$mentions.{field}", "n": {"$sum": 1}}}])}

        total = list(self.entities.aggregate([
            {"$group": {"_id": None, "mentions": {"$sum": "$mention_count"},
                        "nil": {"$sum": {"$size": {"$ifNull": ["$nil", []]}}},
                        "rejected": {"$sum": "$rejected_count"}}}]))
        row = total[0] if total else {}
        distinct = list(self.entities.aggregate([
            {"$unwind": "$mentions"}, {"$group": {"_id": "$mentions.qid"}},
            {"$count": "n"}]))
        return {
            "frames_linked": self.entities.count_documents({}),
            "frames_total": self.entries.count_documents({}),
            "frames_with_links": self.entities.count_documents({"mention_count": {"$gt": 0}}),
            "frames_with_self_item": self.entities.count_documents(
                {"self_qid": {"$type": "string"}}),
            "frames_current": self.entities.count_documents(
                {"linker_version": kg_entities.LINKER_VERSION}),
            "mentions_total": row.get("mentions", 0),
            "distinct_entities": distinct[0]["n"] if distinct else 0,
            "nil_total": row.get("nil", 0),
            "rejected_total": row.get("rejected", 0),
            "mentions_by_field": unwound("field"),
            "mentions_by_method": unwound("method"),
            # A mixed corpus is allowed mid-relink but must be VISIBLE.
            "lexicon_versions_in_use": {
                str(v): self.entities.count_documents({"lexicon_version": v})
                for v in self.entities.distinct("lexicon_version")},
        }


def _as_datetime(value) -> datetime | None:
    """A BSON date from whatever the record carries — kg/entities.py stamps
    ``linked_at`` as an ISO string, and a string would be invisible to the
    ``$lte`` snapshot filter (event_store._as_datetime explains the trap)."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return as_utc(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return as_utc(parsed)


def _up_to_date(doc: dict, unit: dict, stamps: dict[str, str]) -> bool:
    if doc.get("source_sha256") != unit.get("source_sha256"):
        return False
    return all(doc.get(key) == value for key, value in stamps.items())


def get_store(uri: str | None = None, db_name: str | None = None) -> EntityStore:
    return EntityStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facade functions — the only calls the entity DAG makes (dom_store style)
# ---------------------------------------------------------------------------

def pending_units(*, stamps: dict[str, str], ready_only: bool = False,
                  force: bool = False, limit: int = 0) -> list[dict]:
    """Unit discovery (streamed from `entries`) + staleness filtering."""
    with get_store() as store:
        pending: list[dict] = []
        batch: list[dict] = []
        for unit in store.iter_frame_units(ready_only=ready_only):
            batch.append(unit)
            if len(batch) >= LOOKUP_BATCH:
                pending.extend(store.select_pending(batch, stamps=stamps, force=force))
                batch = []
                if limit and len(pending) >= limit:
                    break
        if batch and not (limit and len(pending) >= limit):
            pending.extend(store.select_pending(batch, stamps=stamps, force=force))
        if limit:
            pending = pending[:limit]
        log.info("Selected %d frames to link (%s, force=%s)", len(pending),
                 stamps, force)
        return pending


def units_for(unit_ids: Iterable[str]) -> list[dict]:
    with get_store() as store:
        return store.units_for(unit_ids)


def save_links(records: Iterable[dict]) -> dict[str, int]:
    with get_store() as store:
        return store.save_links(records)


def links_for(entry_ids: Iterable[str], linked_at_lte=None) -> dict[str, list[dict]]:
    with get_store() as store:
        return store.links_for(entry_ids, linked_at_lte=linked_at_lte)


def linking_stamps(linked_at_lte=None) -> dict[str, Any]:
    with get_store() as store:
        return store.linking_stamps(linked_at_lte=linked_at_lte)


def entity_stats() -> dict[str, Any]:
    with get_store() as store:
        return store.stats()
