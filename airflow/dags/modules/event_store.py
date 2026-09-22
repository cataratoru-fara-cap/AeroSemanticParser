"""
event_store.py — MongoDB persistence for extracted events
============================================================
The ONLY place the event DAG touches the database (dom_store rule, applied
to the event stage). Knows nothing about prompts or models — kg/events.py
knows nothing about Mongo; the DAG glues them via section_unit() ->
extract() -> save_extraction().

Collections
-----------
``entries``  (owned by the parse stage, read-only here)
    We read url, title, category, sections and parser_version to build the
    extraction units. Streamed, never materialised — an entries doc is
    multi-MB and collecting a chunk of them OOM-killed a real run once.

``events``  (owned by this module) — ONE DOC PER (FRAME, SECTION), not per
            event. See "Why the unit is a section" below.
    _id                 "<sha1(url)>:<source_section>"  (kg/events.unit_id)
    entry_id            sha1(url) — joins to entries._id
    frame_url           canonical page URL
    source_section      "origin" | "spread"
    heading             the section's own heading, as the page wrote it
    events              list of event rows, each validated against
                        kg_config/event_extraction_schema.json and checked
                        against the section (kg/events.py, extractive only):
                        event_id, sentences, source_text (verbatim, copied
                        by the pipeline), date, date_precision, date_text,
                        location, location_type, actors, certainty — plus
                        links / images / embeds attached by POSITION. Every
                        textual value is a span of the page: the model's
                        wording is GROUNDED against the section, not
                        checked and discarded (kg/events.py, 2.2.0)
    event_count         len(events) — denormalised so coverage stats do
                        not have to unwind the array
    sentence_count      how many sentences the section was numbered into
    source_sha256       sha256 of what the unit is built from: the section's
                        paragraphs AND its link/photo/embed positions — the
                        staleness key; a re-parse that changes either
                        re-queues THAT unit and no other
    source_chars        length of the section (never truncated)
    prompt_version / extraction_version / schema_sha
                        the three stamps that make an extraction
                        reproducible; any of them moving re-queues the unit
    parser_version      entries.parser_version at extraction time
    model / digest / host       which weights said this
    extracted_at / elapsed_s / attempts

``event_failures``  (owned by this module) — the dead-letter collection,
    holding only currently-unresolved failures (a later success for the
    same unit deletes the record). Mirrors `parse_failures` exactly,
    including why it exists: it carries the SAME staleness stamps as an
    events doc, and select_pending consults BOTH collections, so a section
    the model reliably chokes on is not re-queued every run — which here
    would cost six attempts at tens of seconds of shared GPU, forever.

Why the unit is a section, not an event
---------------------------------------
One document per (frame, section) rather than per event, for four reasons:

  * **"Extracted, found nothing" has to be representable.** Roughly a third
    of Origin sections describe rather than narrate, and legitimately yield
    zero events. With a doc per event, that is indistinguishable from never
    having been attempted, and select_pending would re-queue it forever.
    A sentinel doc would be needed — and this IS that doc.
  * **Re-extraction is then one atomic write.** Mongo here is standalone,
    so there are no multi-document transactions (see kg_store.py, which
    says so at length). Per-event docs would need delete-then-insert, and
    an interrupted re-extraction would leave a frame holding half its old
    events and half its new ones, with nothing able to detect it.
  * **Staleness is per-section**, because the text hash is. The document
    key and the staleness unit should be the same thing.
  * It is ~36,500 documents. ``events_for`` is one find returning at most
    two docs per entry.

The cost is that a single event's date is not indexable. That is accepted:
Neo4j and Fuseki are the query surface for events, and this collection's
job is to be the durable, resumable record of what was extracted.

A re-extraction REPLACES a unit's event list; it never merges. Two
readings of one text are not one reading — merging them would produce a
list no single extraction ever produced, with near-duplicate events and no
way to tell which model said which.

Connection settings come from the environment (docker-compose), same
variable names as the other stores:
    MONGODB_URI                        (default: mongodb://localhost:27017)
    MONGODB_DB                         (default: memes)
    MONGODB_ENTRIES_COLLECTION         (default: entries)
    MONGODB_EVENTS_COLLECTION          (default: events)
    MONGODB_EVENT_FAILURES_COLLECTION  (default: event_failures)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterable, Iterator

from modules.kg import events as kg_events
from modules.mongo_base import MongoStoreBase, as_utc, now_utc

log = logging.getLogger("event_store")

__all__ = [
    "EventStore", "get_store", "pending_units", "units_for",
    "save_extraction", "save_failures", "events_for", "extraction_stamps",
    "event_stats", "load_from_jsonl",
]

# Entry fields the unit builder needs. `sections` is the big one, which is
# why every read of this projection streams.
UNIT_PROJECTION = {"_id": 1, "url": 1, "title": 1, "category": 1,
                   "sections": 1, "parser_version": 1,
                   # what an event's [n] citation markers resolve against
                   "external_references": 1}

# The stamps that make an extraction reproducible. All four must match for
# a stored unit to count as up to date.
STALENESS_KEYS = ("source_sha256", "prompt_version", "extraction_version",
                  "schema_sha")

# Mongo's query document has a size ceiling and a 36k-element $in is rude
# regardless; the staleness lookup is chunked at this width.
LOOKUP_BATCH = 2000
BULK_BATCH = 1000


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class EventStore(MongoStoreBase):
    """Owner of ``events`` and ``event_failures``; reads ``entries``."""

    def _configure(self) -> None:
        self.entries = self.collection("MONGODB_ENTRIES_COLLECTION", "entries")
        self.events = self.collection("MONGODB_EVENTS_COLLECTION", "events")
        self.failures = self.collection(
            "MONGODB_EVENT_FAILURES_COLLECTION", "event_failures")

        self.events.create_index("entry_id")        # the per-chunk KG read
        self.events.create_index("frame_url")
        self.events.create_index("source_section")
        self.events.create_index("extracted_at")    # the staleness stamp
        self.events.create_index("model")           # model-mix visibility
        self.events.create_index("events.certainty")   # multikey, dashboard
        self.failures.create_index("entry_id")
        self.failures.create_index("error_kind")

    # -- selection ----------------------------------------------------------

    def iter_section_units(self, sections: Iterable[str],
                           ready_only: bool = False,
                           limit: int = 0) -> Iterator[dict]:
        """Stream one unit per (entry, section) that has text.

        A GENERATOR on purpose: `entries` docs carry full section text and
        materialising a corpus-sized list of them is the mistake the parse
        stage's comments are emphatic about. ``limit`` counts UNITS, not
        entries, so it means what the DAG's batch_size says it means.
        """
        sections = list(sections)
        query: dict[str, Any] = {}
        if ready_only:
            query["corpus_status"] = "ready"
        yielded = 0
        for entry in self.entries.find(query, UNIT_PROJECTION):
            for section in sections:
                unit = kg_events.section_unit(entry, section)
                if not unit:
                    continue
                yield unit
                yielded += 1
                if limit and yielded >= limit:
                    return

    def select_pending(self, units: Iterable[dict], *, prompt_version: str,
                       extraction_version: str, schema_sha: str,
                       force: bool = False, limit: int = 0) -> list[dict]:
        """The subset of ``units`` that still needs extracting.

        Consults BOTH `events` and `event_failures`, so a unit that failed
        deterministically is skipped until one of its stamps moves — the
        parse_failures lesson, restated for an LLM where a blind retry
        costs GPU minutes rather than milliseconds.
        """
        units = list(units)
        if not units:
            return []
        if force:
            return units[:limit] if limit else units

        stamps = {"prompt_version": prompt_version,
                  "extraction_version": extraction_version,
                  "schema_sha": schema_sha}
        by_id = {u["unit_id"]: u for u in units}
        fresh: set[str] = set()
        projection = {"_id": 1, **{k: 1 for k in STALENESS_KEYS}}
        ids = list(by_id)
        for collection in (self.events, self.failures):
            for i in range(0, len(ids), LOOKUP_BATCH):
                batch = ids[i:i + LOOKUP_BATCH]
                for doc in collection.find({"_id": {"$in": batch}}, projection):
                    unit = by_id.get(doc["_id"])
                    if unit and _up_to_date(doc, unit, stamps):
                        fresh.add(doc["_id"])

        pending = [u for u in units if u["unit_id"] not in fresh]
        return pending[:limit] if limit else pending

    def not_extracted_since(self, units: Iterable[dict], since) -> list[dict]:
        """The units NOT already (re-)extracted at or after ``since``.

        The retry-safe filter for a FORCED run. select_pending cannot be
        used there: every unit being forced is, by definition, already up
        to date by its stamps, so it would filter the whole chunk away and
        a force would re-extract nothing. And simply not filtering would
        make a retried forced chunk re-buy every unit its first attempt
        already paid for. "Written since this run selected its units" is
        the question that is actually true on both counts.
        """
        units = list(units)
        since = _as_datetime(since)
        if not units or since is None:
            return units
        ids = [u["unit_id"] for u in units]
        done: set[str] = set()
        for i in range(0, len(ids), LOOKUP_BATCH):
            done.update(d["_id"] for d in self.events.find(
                {"_id": {"$in": ids[i:i + LOOKUP_BATCH]},
                 "extracted_at": {"$gte": since}}, {"_id": 1}))
        return [u for u in units if u["unit_id"] not in done]

    def units_for(self, unit_ids: Iterable[str],
                  sections: Iterable[str]) -> list[dict]:
        """Rebuild specific units from `entries`.

        The DAG passes unit IDS between tasks, never unit bodies: a chunk
        of 50 units is ~50 KB of section text, and XCom carries ids and
        small dicts (the rule every DAG docstring here states).
        """
        wanted = set(unit_ids)
        if not wanted:
            return []
        entry_ids = {uid.split(":", 1)[0] for uid in wanted}
        out: list[dict] = []
        for entry in self.entries.find({"_id": {"$in": list(entry_ids)}},
                                       UNIT_PROJECTION):
            for section in sections:
                unit = kg_events.section_unit(entry, section)
                if unit and unit["unit_id"] in wanted:
                    out.append(unit)
        return out

    # -- writes -------------------------------------------------------------

    def save_extraction(self, records: Iterable[dict]) -> dict[str, int]:
        """Upsert extraction records, REPLACING each unit's event list.

        Never ``$push``/``$addToSet``: see the module docstring. A success
        also clears the unit's dead-letter record, which is what makes
        `event_failures` hold only currently-unresolved failures.
        """
        from pymongo import UpdateOne

        ops: list[Any] = []
        resolved: list[str] = []
        units = events = 0
        now = now_utc()

        def flush() -> None:
            if ops:
                self.events.bulk_write(ops, ordered=False)
                ops.clear()

        for record in records:
            doc = dict(record)
            unit_id = doc.pop("unit_id")
            doc["extracted_at"] = _as_datetime(doc.get("extracted_at")) or now
            doc["event_count"] = len(doc.get("events") or [])
            ops.append(UpdateOne({"_id": unit_id},
                                 {"$set": doc,
                                  "$setOnInsert": {"first_extracted_at": now}},
                                 upsert=True))
            resolved.append(unit_id)
            units += 1
            events += doc["event_count"]
            if len(ops) >= BULK_BATCH:
                flush()
        flush()

        cleared = 0
        if resolved:
            for i in range(0, len(resolved), LOOKUP_BATCH):
                cleared += self.failures.delete_many(
                    {"_id": {"$in": resolved[i:i + LOOKUP_BATCH]}}).deleted_count
        return {"units": units, "events": events, "failures_cleared": cleared}

    def save_failures(self, failures: Iterable[dict], *, prompt_version: str,
                      extraction_version: str, schema_sha: str) -> int:
        """Persist extraction failures into `event_failures` — kept OUT of
        `events` so that collection holds only successful readings.

        Invariants, the same three parse_failures has:
          * Never downgrades — a failure cannot touch `events`, so a
            previously extracted unit survives a failed re-extraction.
          * No retry loops — the record carries the same staleness stamps
            as an events doc, and select_pending consults both.
          * Self-cleaning — save_extraction deletes it on a later success.
        """
        now = now_utc()
        n = 0
        for failure in failures:
            self.failures.update_one(
                {"_id": failure["unit_id"]},
                {"$set": {
                    "entry_id": failure.get("entry_id"),
                    "frame_url": failure.get("frame_url"),
                    "source_section": failure.get("source_section"),
                    "source_sha256": failure.get("source_sha256"),
                    "error": str(failure.get("error"))[:2000],
                    "error_kind": failure.get("error_kind"),
                    "failed_at": now,
                    "prompt_version": prompt_version,
                    "extraction_version": extraction_version,
                    "schema_sha": schema_sha,
                 },
                 "$inc": {"attempts": 1}},
                upsert=True)
            n += 1
        return n

    # -- reads / stats ------------------------------------------------------

    def events_for(self, entry_ids: Iterable[str],
                   extracted_at_lte=None) -> dict[str, list[dict]]:
        """{frame_url: [event, ...]} for one KG build chunk.

        Keyed by URL, not entry_id, because kg_store.ENTRY_PROJECTION
        excludes ``_id`` — the entries the KG build streams carry ``url``
        and nothing else to join on.

        ``extracted_at_lte`` freezes the read at the build's snapshot, for
        the same reason kg_store.snapshot filters on parsed_at: a build
        must see one generation, not whatever landed while it ran.

        Order is deterministic — origin before spread, and the model's own
        order within each — so the node and edge stream, and therefore
        every CSV's row order, is reproducible across rebuilds.
        """
        ids = list(entry_ids)
        if not ids:
            return {}
        query: dict[str, Any] = {"entry_id": {"$in": ids}}
        if extracted_at_lte is not None:
            query["extracted_at"] = {"$lte": extracted_at_lte}
        out: dict[str, list[dict]] = {}
        rank = {s: i for i, s in enumerate(kg_events.SOURCE_SECTIONS)}
        rows: dict[str, list[tuple[int, list[dict]]]] = {}
        for doc in self.events.find(query, {"frame_url": 1, "events": 1,
                                            "source_section": 1, "model": 1,
                                            "extraction_version": 1}):
            url = doc.get("frame_url")
            if not url:
                continue
            enriched = [dict(e, model=doc.get("model"),
                             extraction_version=doc.get("extraction_version"))
                        for e in (doc.get("events") or [])]
            rows.setdefault(url, []).append(
                (rank.get(doc.get("source_section"), len(rank)), enriched))
        for url, parts in rows.items():
            out[url] = [e for _, group in sorted(parts, key=lambda p: p[0])
                        for e in group]
        return out

    def extraction_stamps(self, extracted_at_lte=None) -> dict[str, Any]:
        """The event layer's contribution to the KG staleness gate.

        ``events_total`` is carried as well as ``events_units`` because a
        re-extraction can change what the events SAY without changing how
        many units exist — and that must rebuild the graph.
        """
        query: dict[str, Any] = {}
        if extracted_at_lte is not None:
            query["extracted_at"] = {"$lte": extracted_at_lte}
        agg = list(self.events.aggregate([
            {"$match": query},
            {"$group": {"_id": None,
                        "units": {"$sum": 1},
                        "events": {"$sum": "$event_count"},
                        "max_extracted_at": {"$max": "$extracted_at"},
                        "prompt_versions": {"$addToSet": "$prompt_version"},
                        "extraction_versions": {"$addToSet": "$extraction_version"},
                        "schema_shas": {"$addToSet": "$schema_sha"}}},
        ]))
        row = agg[0] if agg else {}
        latest = as_utc(row.get("max_extracted_at"))
        return {
            "events_units": row.get("units", 0),
            "events_total": row.get("events", 0),
            "events_prompt_versions": sorted(row.get("prompt_versions") or []),
            "events_extraction_versions": sorted(
                row.get("extraction_versions") or []),
            "events_schema_shas": sorted(row.get("schema_shas") or []),
            "events_max_extracted_at": latest.isoformat() if latest else None,
        }

    def _units_possible(self) -> int:
        """How many (entry, section) units exist to extract — every origin or
        spread section with at least one non-empty paragraph. The coverage
        denominator: 100% means every one of them is extracted."""
        rows = list(self.entries.aggregate([
            {"$unwind": "$sections"},
            {"$match": {"sections.kind": {"$in": list(kg_events.SOURCE_SECTIONS)},
                        "sections.text": {"$elemMatch": {"$regex": r"\S"}}}},
            {"$group": {"_id": {"e": "$_id", "k": "$sections.kind"}}},
            {"$count": "n"}]))
        return rows[0]["n"] if rows else 0

    def stats(self) -> dict[str, Any]:
        def counts(field: str, collection=None) -> dict[str, int]:
            # `is None`, never `or`: a pymongo Collection refuses bool()
            # (it raised NotImplementedError in the first real kym_events
            # run's summarize task). mongomock allowed it, which is why no
            # test saw it — see _patch_mongomock_truthiness in conftest.py.
            collection = self.events if collection is None else collection
            return {str(v): collection.count_documents({field: v})
                    for v in collection.distinct(field)}

        units = self.events.count_documents({})
        entries_total = self.entries.count_documents({})
        by_precision: dict[str, int] = {}
        by_certainty: dict[str, int] = {}
        by_location_type: dict[str, int] = {}
        for field, sink in (("date_precision", by_precision),
                            ("certainty", by_certainty),
                            ("location_type", by_location_type)):
            for row in self.events.aggregate([
                {"$unwind": "$events"},
                {"$group": {"_id": f"$events.{field}", "n": {"$sum": 1}}},
            ]):
                sink[str(row["_id"])] = row["n"]
        total = list(self.events.aggregate([
            {"$group": {"_id": None, "n": {"$sum": "$event_count"}}}]))
        return {
            "units_total": units,
            "units_by_section": counts("source_section"),
            "events_total": total[0]["n"] if total else 0,
            # Coverage against what EXISTS: every non-empty origin/spread
            # section in `entries` (counted server-side), and how many of
            # them are extracted under the CURRENT contract.
            "units_possible": self._units_possible(),
            "units_current": self.events.count_documents({
                "prompt_version": kg_events.PROMPT_VERSION,
                "extraction_version": kg_events.EXTRACTION_VERSION}),
            "zero_event_units": self.events.count_documents({"event_count": 0}),
            "events_by_precision": by_precision,
            "events_by_certainty": by_certainty,
            "events_by_location_type": by_location_type,
            # A heterogeneous corpus is allowed here (unlike kg/semantics.py)
            # but must be VISIBLE — see kg/events.py's docstring.
            "models_in_use": counts("model"),
            "frames_covered": len(self.events.distinct("entry_id")),
            "frames_total": entries_total,
            "failures_total": self.failures.count_documents({}),
            "failure_kind_counts": counts("error_kind", self.failures),
        }


def _as_datetime(value) -> datetime | None:
    """A BSON date from whatever the record carries.

    kg/events.py stamps ``extracted_at`` as an ISO string, because the same
    record is also a JSONL line. Stored as a string, it would be invisible
    to every snapshot filter: Mongo compares values of different BSON types
    by type order, never by value, so ``{"$lte": <datetime>}`` does not
    match a string at all — the KG build would silently see zero events.
    """
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
    """Every staleness key matches, so this unit needs no work."""
    if doc.get("source_sha256") != unit.get("source_sha256"):
        return False
    return all(doc.get(key) == value for key, value in stamps.items())


def get_store(uri: str | None = None, db_name: str | None = None) -> EventStore:
    return EventStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facade functions — the only calls the event DAG makes (dom_store style)
# ---------------------------------------------------------------------------

def pending_units(sections: Iterable[str] = kg_events.SOURCE_SECTIONS,
                  ready_only: bool = False, prompt_version: str = "",
                  extraction_version: str = "", schema_sha: str = "",
                  force: bool = False, limit: int = 0) -> list[dict]:
    """Unit discovery (streamed from `entries`) + staleness filtering."""
    with get_store() as store:
        # Selection needs only the ids and stamps, so the streamed units
        # are filtered in slices rather than collected first.
        pending: list[dict] = []
        batch: list[dict] = []
        for unit in store.iter_section_units(sections, ready_only=ready_only):
            batch.append(unit)
            if len(batch) >= LOOKUP_BATCH:
                pending.extend(store.select_pending(
                    batch, prompt_version=prompt_version,
                    extraction_version=extraction_version,
                    schema_sha=schema_sha, force=force))
                batch = []
                if limit and len(pending) >= limit:
                    break
        if batch and not (limit and len(pending) >= limit):
            pending.extend(store.select_pending(
                batch, prompt_version=prompt_version,
                extraction_version=extraction_version,
                schema_sha=schema_sha, force=force))
        if limit:
            pending = pending[:limit]
        log.info("Selected %d units to extract (prompt=%s extraction=%s "
                 "schema=%s force=%s)", len(pending), prompt_version,
                 extraction_version, schema_sha, force)
        return pending


def units_for(unit_ids: Iterable[str],
              sections: Iterable[str] = kg_events.SOURCE_SECTIONS) -> list[dict]:
    with get_store() as store:
        return store.units_for(unit_ids, sections)


def save_extraction(records: Iterable[dict]) -> dict[str, int]:
    with get_store() as store:
        written = store.save_extraction(records)
        log.info("Saved extraction — %s", written)
        return written


def save_failures(failures: Iterable[dict], prompt_version: str,
                  extraction_version: str, schema_sha: str) -> int:
    with get_store() as store:
        n = store.save_failures(failures, prompt_version=prompt_version,
                                extraction_version=extraction_version,
                                schema_sha=schema_sha)
        if n:
            log.warning("Recorded %d extraction failures", n)
        return n


def events_for(entry_ids: Iterable[str],
               extracted_at_lte=None) -> dict[str, list[dict]]:
    with get_store() as store:
        return store.events_for(entry_ids, extracted_at_lte=extracted_at_lte)


def extraction_stamps(extracted_at_lte=None) -> dict[str, Any]:
    with get_store() as store:
        return store.extraction_stamps(extracted_at_lte=extracted_at_lte)


def event_stats() -> dict[str, Any]:
    with get_store() as store:
        return store.stats()


def load_from_jsonl(path: str) -> dict[str, int]:
    """Replay a JSONL artifact into Mongo without a model.

    The artifact is written per unit as extraction runs, so this exists for
    recovery — a wiped collection, a migration, an extraction run whose
    Mongo writes failed — not for the normal path. Duplicate lines are
    harmless: the last one for a unit wins, exactly as a re-extraction does.
    """
    records = {r["unit_id"]: r for r in kg_events.iter_jsonl(path)
               if r.get("unit_id")}
    written = save_extraction(records.values())
    log.info("Replayed %s — %s", path, written)
    return written
