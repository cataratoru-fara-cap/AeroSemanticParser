"""
review_store.py — MongoDB persistence for the human review of events
=========================================================================
Owns ``event_reviews``, and nothing else. Reads ``events`` to draw the
sample and to snapshot what the reviewer will see; kg/review.py holds the
sampling and scoring logic and knows nothing about Mongo.

``event_reviews`` (owned by this module) — ONE DOC PER SECTION DRAWN.
    _id                 the unit_id of the section (sha1(url):section)
    entry_id, frame_url, source_section, heading, title
    samples             ["representative", "relative", ...] — a section can
                        be in more than one; it is READ ONCE and the
                        verdict counts towards each, filtered per stratum
                        by kg/review.qualifying()
    sentences           [{id, text}] — what the model was shown, frozen
    events              the events as drawn, frozen (see below)
    extraction_version / prompt_version / schema_sha / source_sha256
                        which extraction this verdict judges
    status              "pending" | "done"
    verdicts            {event_id: {event, date, location, actors, certainty}}
    missed_sentences    [int] — sentences that narrate an event and got
                        none. The recall measurement, and the one thing no
                        automated sweep can produce.
    note, reviewer, reviewed_at, elapsed_s

Why the document carries its own copy of the text and events
------------------------------------------------------------
Two reasons, and the second is the important one:

  1. The dashboard image has no access to the pipeline code (it cannot
     number sentences itself — see dashboard/lib/data.py), so the drawn
     section has to arrive ready to read.
  2. **A verdict belongs to the extraction it judged.** Re-parsing a page
     or re-running the extractor must not move the text out from under a
     review already recorded; the stamps say exactly what was judged, and
     a re-draw after a version bump is a new sample, not a silent edit of
     an old one.

No TTL, no pruning: a few hundred small documents that are the only
human-produced data in the whole pipeline.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable, Sequence

from modules.kg import events as kg_events
from modules.kg import review as kg_review
from modules.mongo_base import MongoStoreBase, now_utc

log = logging.getLogger(__name__)

DISPLAY_EVENT_FIELDS = (
    "event_id", "sentences", "source_text", "date", "date_precision",
    "date_basis", "date_anchor", "date_text", "location", "location_type",
    "actors", "certainty",
)


class ReviewStore(MongoStoreBase):
    """Owner of ``event_reviews``; reads ``events``."""

    def _configure(self) -> None:
        self.entries = self.collection("MONGODB_ENTRIES_COLLECTION", "entries")
        self.events = self.collection("MONGODB_EVENTS_COLLECTION", "events")
        self.reviews = self.collection(
            "MONGODB_EVENT_REVIEWS_COLLECTION", "event_reviews")
        self.reviews.create_index("status")
        self.reviews.create_index("samples")

    # -- drawing ------------------------------------------------------------

    def draw_sample(self, *, seed: int, representative: int,
                    relative_events: int, redraw: bool) -> dict[str, Any]:
        """Draw, and write one pending doc per section.

        Only sections extracted by the CURRENT contract are eligible: a
        verdict on output an older extractor produced says nothing about
        the one that will run the backfill.
        """
        stamps = {"extraction_version": kg_events.EXTRACTION_VERSION,
                  "prompt_version": kg_events.PROMPT_VERSION}
        docs = list(self.events.find(
            {"extraction_version": kg_events.EXTRACTION_VERSION},
            {"events": 1, "source_section": 1, "frame_url": 1, "entry_id": 1,
             "heading": 1, "sentence_count": 1, "source_sha256": 1,
             "schema_sha": 1, "prompt_version": 1}))
        for doc in docs:
            doc["unit_id"] = doc["_id"]
        if not docs:
            raise RuntimeError(
                f"no sections extracted by {kg_events.EXTRACTION_VERSION}; "
                "run kym_events before drawing a review sample")

        if redraw:
            removed = self.reviews.delete_many({"status": "pending"}).deleted_count
            log.info("Discarded %d pending review rows", removed)

        chosen = kg_review.draw(docs, seed=seed,
                                representative=representative,
                                relative_events=relative_events)
        samples_for: dict[str, list[str]] = {}
        for sample, unit_ids in chosen.items():
            for uid in unit_ids:
                samples_for.setdefault(uid, []).append(sample)

        done = {d["_id"] for d in self.reviews.find({"status": "done"}, {"_id": 1})}
        by_id = {d["unit_id"]: d for d in docs}
        written = 0
        for uid, samples in sorted(samples_for.items()):
            if uid in done:                       # never re-open a verdict
                continue
            doc = by_id[uid]
            self.reviews.update_one(
                {"_id": uid},
                {"$set": {
                    "entry_id": doc.get("entry_id"),
                    "frame_url": doc["frame_url"],
                    "source_section": doc["source_section"],
                    "heading": doc.get("heading") or "",
                    "samples": sorted(samples),
                    "sentences": self._sentences(doc),
                    "events": [{k: e.get(k) for k in DISPLAY_EVENT_FIELDS}
                               for e in doc["events"]],
                    "source_sha256": doc.get("source_sha256"),
                    "schema_sha": doc.get("schema_sha"),
                    "status": "pending", "drawn_at": now_utc(),
                    "seed": seed, **stamps},
                 "$setOnInsert": {"verdicts": {}, "missed_sentences": [],
                                  "note": "", "reviewer": None}},
                upsert=True)
            written += 1
        return {"seed": seed, "eligible_sections": len(docs),
                "drawn": {k: len(v) for k, v in chosen.items()},
                "sections_to_read": written, "already_done": len(done),
                "events_to_judge": sum(
                    sum(1 for e in by_id[uid]["events"]
                        if any(kg_review.qualifying(s, e) for s in samples))
                    for uid, samples in samples_for.items() if uid not in done),
                **stamps}

    def _sentences(self, doc: dict) -> list[dict]:
        """The section as the MODEL saw it: numbered, markers hidden.

        Rebuilt from the stored events' verbatim spans is NOT enough (they
        skip sentences nobody narrated, which is exactly what the recall
        question is about), so this re-derives from `entries`.
        """
        raw = self.entries.find_one({"_id": doc.get("entry_id")},
                                    {"sections": 1, "url": 1, "title": 1,
                                     "category": 1, "external_references": 1})
        if not raw:
            return []
        unit = kg_events.section_unit(raw, doc["source_section"])
        if not unit:
            return []
        return [{"id": s["id"],
                 "text": kg_events.strip_footnotes(
                     unit["paragraphs"][s["paragraph"]][s["start"]:s["end"]]),
                 "paragraph": s["paragraph"]}
                for s in unit["sentences"]]

    # -- reviewing ----------------------------------------------------------

    def pending(self, sample: str | None = None, limit: int = 0) -> list[dict]:
        query: dict[str, Any] = {"status": "pending"}
        if sample:
            query["samples"] = sample
        cursor = self.reviews.find(query).sort("_id", 1)
        return list(cursor.limit(limit) if limit else cursor)

    def save_verdict(self, unit_id: str, *, verdicts: dict,
                     missed_sentences: Sequence[int], note: str,
                     reviewer: str, elapsed_s: float | None = None) -> None:
        self.reviews.update_one(
            {"_id": unit_id},
            {"$set": {"verdicts": verdicts,
                      "missed_sentences": sorted(set(int(i) for i in missed_sentences)),
                      "note": note or "", "reviewer": reviewer,
                      "status": "done", "reviewed_at": now_utc(),
                      "elapsed_s": elapsed_s}})

    def progress(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for sample in kg_review.SAMPLES:
            total = self.reviews.count_documents({"samples": sample})
            done = self.reviews.count_documents({"samples": sample,
                                                 "status": "done"})
            out[sample] = {"sections": total, "done": done}
        out["sections_total"] = self.reviews.count_documents({})
        out["sections_done"] = self.reviews.count_documents({"status": "done"})
        return out

    def report(self) -> dict[str, Any]:
        result = kg_review.score(self.reviews.find({"status": "done"}))
        result["progress"] = self.progress()
        return result


def get_store(uri: str | None = None, db_name: str | None = None) -> ReviewStore:
    return ReviewStore(uri, db_name)


def draw_sample(*, seed: int, representative: int, relative_events: int,
                redraw: bool) -> dict[str, Any]:
    with get_store() as store:
        return store.draw_sample(seed=seed, representative=representative,
                                 relative_events=relative_events, redraw=redraw)


def report() -> dict[str, Any]:
    with get_store() as store:
        return store.report()
