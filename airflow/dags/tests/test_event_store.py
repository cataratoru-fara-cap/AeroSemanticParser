"""Tests for event_store.py (no network, no real Mongo — mongomock).

Mirrors the fresh_store() pattern in test_parse_store.py: bypass
EventStore.__init__ and wire mongomock collections directly, so no index
DDL is issued.

What these pin, beyond "it works":

  * **A deterministic failure is not re-queued until a stamp moves.** This
    is the parse_failures lesson restated where it costs far more: a
    section the model reliably chokes on would otherwise burn six attempts
    of shared GPU on every single run, forever.
  * **Re-extraction REPLACES a unit's events; it never merges.** Two
    readings of one text are not one reading. A merge would produce a list
    no extraction ever produced, with near-duplicate events and no way to
    tell which model said which — and Mongo here is standalone, so there
    is no transaction to make delete-then-insert safe.
  * **events_for keys by frame_url, not entry_id.** kg_store's
    ENTRY_PROJECTION excludes ``_id``, so the entries the KG build streams
    have no id to join on. Keying by the wrong field would return events
    for nothing and silently build an event-free graph.
  * **Every staleness stamp re-queues on its own.** Four stamps, four
    tests: a hole in any one of them means an edited prompt, an edited
    schema or a re-parsed page silently keeps stale events.
  * **extraction_stamps honours the build snapshot.** A stamp computed
    over docs the build cannot see would record a count that never
    existed.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_event_store.py -v
"""
import unittest
from datetime import datetime, timedelta, timezone

import mongomock

from modules import event_store as es
from modules.kg import events as kg_events

URL = "https://knowyourmeme.com/memes/doge"
OTHER = "https://knowyourmeme.com/memes/cheems"
SECTION_TEXT = ["Posted to Tumblr on February 23rd, 2010 by Atsuko Sato.",
                "It then spread to 4chan."]

STAMPS = {"prompt_version": "1", "extraction_version": "1.0.0",
          "schema_sha": "abc123"}


def fresh_store() -> es.EventStore:
    client = mongomock.MongoClient()
    store = es.EventStore.__new__(es.EventStore)
    store.client = client
    store.db = client["memes"]
    store.entries = store.db["entries"]
    store.events = store.db["events"]
    store.failures = store.db["event_failures"]
    return store


def entry_doc(url: str = URL, **over) -> dict:
    doc = {
        "_id": kg_events.frame_key(url), "url": url, "title": "Doge",
        "category": "meme", "parser_version": "1.5.0",
        "sections": [
            {"kind": "about", "heading": "About", "text": ["Doge is a meme."]},
            {"kind": "origin", "heading": "Origin", "text": SECTION_TEXT},
            {"kind": "spread", "heading": "Spread", "text": ["It spread."]},
        ],
    }
    doc.update(over)
    return doc


def event_row(sentences=(1,), date="2010-02-23") -> dict:
    """One stored event in the extraction 2.0.0 shape: sentences instead of a
    quote the model wrote, no summary, media attached by position."""
    return {
        "event_id": kg_events.event_id(URL, "origin", list(sentences), date,
                                       "day" if date else "none"),
        "sentences": list(sentences), "source_text": SECTION_TEXT[0],
        "date": date, "date_precision": "day" if date else "none",
        "date_text": "February 23rd, 2010" if date else None,
        "location": "Tumblr", "location_type": "platform",
        "certainty": "confirmed", "actors": ["Atsuko Sato"],
        "links": [], "images": [], "embeds": [],
        "frame_url": URL, "source_section": "origin",
    }


def record(url: str = URL, section: str = "origin", *, events=None,
           sha: str = "sha-origin", when=None, **over) -> dict:
    doc = {
        "unit_id": kg_events.unit_id(url, section),
        "entry_id": kg_events.frame_key(url), "frame_url": url,
        "source_section": section, "heading": section.capitalize(),
        "events": [event_row()] if events is None else events,
        "source_sha256": sha, "source_chars": 120, "sentence_count": 2,
        "parser_version": "1.5.0", "model": "mistral-small3.2:24b",
        "digest": "deadbeef", "host": "https://ollama-ccdd.example",
        "extracted_at": (when if when is not None
                         else datetime(2026, 9, 18, 9, 0, tzinfo=timezone.utc)),
        "elapsed_s": 11.4, "attempts": 1, **STAMPS,
    }
    doc.update(over)
    return doc


def stored(store, url=URL, section="origin") -> dict:
    return store.events.find_one({"_id": kg_events.unit_id(url, section)})


class UnitSelectionTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()
        self.store.entries.insert_one(entry_doc())

    def units(self, **over):
        return list(self.store.iter_section_units(
            over.pop("sections", kg_events.SOURCE_SECTIONS), **over))

    def test_one_unit_per_section_with_text(self):
        units = self.units()
        self.assertEqual({u["source_section"] for u in units},
                         {"origin", "spread"})
        self.assertEqual(units[0]["frame_url"], URL)

    def test_a_section_without_text_yields_no_unit(self):
        self.store.entries.delete_many({})
        self.store.entries.insert_one(entry_doc(sections=[
            {"kind": "origin", "heading": "Origin", "text": []}]))
        self.assertEqual(self.units(), [])

    def test_limit_counts_units_not_entries(self):
        self.store.entries.insert_one(entry_doc(OTHER))
        self.assertEqual(len(self.units(limit=3)), 3)

    def test_ready_only_filters_on_corpus_status(self):
        self.store.entries.delete_many({})
        self.store.entries.insert_one(entry_doc(corpus_status="incomplete"))
        self.store.entries.insert_one(entry_doc(OTHER, corpus_status="ready"))
        units = self.units(ready_only=True)
        self.assertEqual({u["frame_url"] for u in units}, {OTHER})


class PendingTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()
        self.store.entries.insert_one(entry_doc())
        self.units = [dict(u, source_sha256=f"sha-{u['source_section']}")
                      for u in self.store.iter_section_units(
                          kg_events.SOURCE_SECTIONS)]

    def pending(self, **over):
        stamps = dict(STAMPS)
        stamps.update(over)
        return self.store.select_pending(self.units, **stamps)

    def test_never_extracted_units_are_pending(self):
        self.assertEqual(len(self.pending()), 2)

    def test_an_up_to_date_unit_is_skipped(self):
        self.store.save_extraction([record()])
        sections = {u["source_section"] for u in self.pending()}
        self.assertEqual(sections, {"spread"})

    def test_a_changed_page_requeues_only_that_unit(self):
        self.store.save_extraction([record(), record(section="spread",
                                                     sha="sha-spread")])
        self.assertEqual(self.pending(), [])
        # The Origin text is re-parsed into something new.
        self.units[0]["source_sha256"] = "sha-origin-v2"
        pending = self.pending()
        self.assertEqual([u["source_section"] for u in pending], ["origin"])

    def test_each_stamp_requeues_on_its_own(self):
        # One subtest per stamp: a hole in any single one means an edited
        # prompt, an edited schema or a bumped module version silently
        # keeps stale events.
        self.store.save_extraction([record(),
                                    record(section="spread", sha="sha-spread")])
        self.assertEqual(self.pending(), [])           # baseline: all fresh
        for key, changed in (("prompt_version", "2"),
                             ("extraction_version", "2.0.0"),
                             ("schema_sha", "different")):
            with self.subTest(stamp=key):
                self.assertEqual(len(self.pending(**{key: changed})), 2)

    def test_force_returns_everything(self):
        self.store.save_extraction([record(), record(section="spread",
                                                     sha="sha-spread")])
        self.assertEqual(len(self.store.select_pending(
            self.units, force=True, **STAMPS)), 2)

    def test_a_deterministic_failure_is_not_requeued(self):
        # The whole point of event_failures: a section the model always
        # chokes on must not cost six GPU attempts on every run.
        self.store.save_failures([{
            "unit_id": kg_events.unit_id(URL, "origin"),
            "entry_id": kg_events.frame_key(URL), "frame_url": URL,
            "source_section": "origin", "source_sha256": "sha-origin",
            "error": "not json", "error_kind": "invalid"}], **STAMPS)
        self.assertEqual([u["source_section"] for u in self.pending()],
                         ["spread"])

    def test_a_failure_is_requeued_once_a_stamp_moves(self):
        self.store.save_failures([{
            "unit_id": kg_events.unit_id(URL, "origin"),
            "source_sha256": "sha-origin", "error": "x",
            "error_kind": "invalid"}], **STAMPS)
        self.assertEqual(len(self.pending(prompt_version="2")), 2)

    def test_a_forced_run_is_not_undone_by_the_chunk_refilter(self):
        """The bug this pins: force_reextract selected every unit, and then
        each chunk re-filtered with select_pending — which drops every
        up-to-date unit, i.e. every unit a force exists to redo. The force
        re-extracted nothing and reported success."""
        self.store.save_extraction([record(),
                                    record(section="spread", sha="sha-spread")])
        self.assertEqual(self.pending(), [])            # the old path: nothing
        run_started = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(
            len(self.store.not_extracted_since(self.units, run_started)), 2)

    def test_a_retried_forced_chunk_skips_what_this_run_already_redid(self):
        run_started = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)
        self.store.save_extraction([
            record(when=run_started.replace(hour=13)),            # redone
            record(section="spread", sha="sha-spread",
                   when=run_started.replace(hour=9))])            # not yet
        todo = self.store.not_extracted_since(self.units, run_started.isoformat())
        self.assertEqual([u["source_section"] for u in todo], ["spread"])

    def test_limit_applies_after_filtering(self):
        self.assertEqual(len(self.store.select_pending(
            self.units, limit=1, **STAMPS)), 1)


class SaveTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()

    def test_a_record_round_trips_with_its_stamps(self):
        written = self.store.save_extraction([record()])
        self.assertEqual((written["units"], written["events"]), (1, 1))
        doc = stored(self.store)
        self.assertEqual(doc["event_count"], 1)
        self.assertEqual(doc["frame_url"], URL)
        self.assertEqual(doc["schema_sha"], STAMPS["schema_sha"])
        self.assertEqual(doc["events"][0]["certainty"], "confirmed")
        self.assertNotIn("unit_id", doc)      # it is the _id

    def test_a_string_timestamp_is_stored_as_a_date(self):
        """The shape kg/events.extract() really produces.

        Every other test here passes a datetime, which is how this slipped
        past them: extract() stamps extracted_at as an ISO string (the
        record is also a JSONL line). Stored as a string it would crash
        as_utc — or, worse, be invisible to the build's snapshot filter,
        since Mongo never matches a string against a date.
        """
        self.store.save_extraction([record(when="2026-09-18T09:00:00Z")])
        stored_at = stored(self.store)["extracted_at"]
        self.assertIsInstance(stored_at, datetime)
        snapshot = datetime(2026, 9, 18, 10, 0, tzinfo=timezone.utc)
        out = self.store.events_for([kg_events.frame_key(URL)],
                                    extracted_at_lte=snapshot)
        self.assertEqual(list(out), [URL])

    def test_zero_events_is_stored_not_skipped(self):
        # "Extracted, found nothing" must be distinguishable from "never
        # attempted", or select_pending re-queues it forever.
        self.store.save_extraction([record(events=[])])
        doc = stored(self.store)
        self.assertEqual(doc["event_count"], 0)
        self.assertEqual(doc["events"], [])

    def test_reextraction_replaces_and_never_merges(self):
        self.store.save_extraction([record()])
        second = event_row(sentences=(1, 2), date=None)
        self.store.save_extraction([record(events=[second])])
        doc = stored(self.store)
        self.assertEqual(doc["event_count"], 1)
        self.assertEqual(doc["events"][0]["sentences"], [1, 2])

    def test_a_success_clears_the_dead_letter_record(self):
        self.store.save_failures([{"unit_id": kg_events.unit_id(URL, "origin"),
                                   "error": "x", "error_kind": "invalid"}],
                                 **STAMPS)
        self.assertEqual(self.store.failures.count_documents({}), 1)
        written = self.store.save_extraction([record()])
        self.assertEqual(written["failures_cleared"], 1)
        self.assertEqual(self.store.failures.count_documents({}), 0)

    def test_repeated_failures_increment_attempts(self):
        failure = {"unit_id": kg_events.unit_id(URL, "origin"),
                   "error": "not json", "error_kind": "invalid"}
        self.store.save_failures([failure], **STAMPS)
        self.store.save_failures([failure], **STAMPS)
        doc = self.store.failures.find_one({})
        self.assertEqual(doc["attempts"], 2)
        self.assertEqual(doc["error_kind"], "invalid")

    def test_a_failure_never_touches_the_events_collection(self):
        self.store.save_extraction([record()])
        self.store.save_failures([{"unit_id": kg_events.unit_id(URL, "origin"),
                                   "error": "x", "error_kind": "permanent"}],
                                 **STAMPS)
        self.assertEqual(stored(self.store)["event_count"], 1)


class EventsForTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()

    def test_keyed_by_frame_url_because_the_build_has_no_id(self):
        self.store.save_extraction([record()])
        out = self.store.events_for([kg_events.frame_key(URL)])
        self.assertEqual(list(out), [URL])
        self.assertEqual(len(out[URL]), 1)

    def test_origin_comes_before_spread_deterministically(self):
        spread = event_row(sentences=(2,), date=None)
        spread["source_section"] = "spread"
        # Written spread-first, to prove the order is imposed on read.
        self.store.save_extraction([
            record(section="spread", events=[spread], sha="sha-spread"),
            record()])
        out = self.store.events_for([kg_events.frame_key(URL)])
        self.assertEqual([e["source_section"] for e in out[URL]],
                         ["origin", "spread"])

    def test_the_model_is_carried_onto_every_event(self):
        # build.py puts it on the node as mk:extractionModel, so a consumer
        # can tell a model's reading from a scraped fact.
        self.store.save_extraction([record()])
        out = self.store.events_for([kg_events.frame_key(URL)])
        self.assertEqual(out[URL][0]["model"], "mistral-small3.2:24b")
        self.assertEqual(out[URL][0]["extraction_version"], "1.0.0")

    def test_events_newer_than_the_snapshot_are_invisible(self):
        old = datetime(2026, 9, 18, 9, 0, tzinfo=timezone.utc)
        self.store.save_extraction([record(when=old)])
        self.store.save_extraction([record(OTHER, when=old + timedelta(hours=2),
                                           sha="sha-other")])
        ids = [kg_events.frame_key(URL), kg_events.frame_key(OTHER)]
        out = self.store.events_for(ids, extracted_at_lte=old)
        self.assertEqual(list(out), [URL])

    def test_an_empty_chunk_is_an_empty_dict(self):
        self.assertEqual(self.store.events_for([]), {})


class StampTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()

    def test_an_empty_collection_has_zero_stamps(self):
        stamps = self.store.extraction_stamps()
        self.assertEqual(stamps["events_units"], 0)
        self.assertEqual(stamps["events_total"], 0)
        self.assertIsNone(stamps["events_max_extracted_at"])

    def test_stamps_count_units_and_events(self):
        two = [event_row(), event_row(sentences=(2,), date=None)]
        self.store.save_extraction([record(events=two)])
        stamps = self.store.extraction_stamps()
        self.assertEqual(stamps["events_units"], 1)
        self.assertEqual(stamps["events_total"], 2)
        self.assertEqual(stamps["events_prompt_versions"], ["1"])
        self.assertEqual(stamps["events_schema_shas"], ["abc123"])

    def test_events_total_moves_when_only_the_content_changed(self):
        # A re-extraction can change what the events SAY without changing
        # how many units exist; the graph must still rebuild.
        self.store.save_extraction([record()])
        before = self.store.extraction_stamps()["events_total"]
        self.store.save_extraction([record(events=[event_row(),
                                                   event_row(sentences=(2,), date=None)])])
        self.assertNotEqual(self.store.extraction_stamps()["events_total"],
                            before)

    def test_stamps_honour_the_build_snapshot(self):
        old = datetime(2026, 9, 18, 9, 0, tzinfo=timezone.utc)
        self.store.save_extraction([record(when=old)])
        self.store.save_extraction([record(OTHER, when=old + timedelta(hours=2),
                                           sha="sha-other")])
        self.assertEqual(
            self.store.extraction_stamps(extracted_at_lte=old)["events_units"], 1)
        self.assertEqual(self.store.extraction_stamps()["events_units"], 2)

    def test_a_mixed_corpus_is_visible_in_the_stamps(self):
        self.store.save_extraction([record(),
                                    record(OTHER, sha="s", prompt_version="2")])
        self.assertEqual(
            self.store.extraction_stamps()["events_prompt_versions"], ["1", "2"])


class StatsTests(unittest.TestCase):
    def test_stats_report_coverage_the_model_mix_and_failures(self):
        store = fresh_store()
        store.entries.insert_one(entry_doc())
        store.entries.insert_one(entry_doc(OTHER))
        store.save_extraction([record()])
        store.save_extraction([record(section="spread", events=[],
                                      sha="sha-spread")])
        store.save_extraction([record(OTHER, sha="s", model="qwen3.8:27b")])
        store.save_failures([{"unit_id": "x:origin", "error": "boom",
                              "error_kind": "invalid"}], **STAMPS)
        stats = store.stats()
        self.assertEqual(stats["units_total"], 3)
        self.assertEqual(stats["frames_covered"], 2)
        self.assertEqual(stats["frames_total"], 2)
        self.assertEqual(stats["zero_event_units"], 1)
        # Coverage denominator: every origin/spread section with text —
        # two entries, each with both sections.
        self.assertEqual(stats["units_possible"], 4)
        self.assertEqual(stats["units_current"], 0)   # fixtures carry stamp "1"
        self.assertEqual(stats["events_by_certainty"], {"confirmed": 2})
        self.assertEqual(stats["events_by_precision"], {"day": 2})
        self.assertEqual(set(stats["models_in_use"]),
                         {"mistral-small3.2:24b", "qwen3.8:27b"})
        self.assertEqual(stats["failures_total"], 1)
        self.assertEqual(stats["failure_kind_counts"], {"invalid": 1})


if __name__ == "__main__":       # pragma: no cover
    unittest.main(verbosity=2)
