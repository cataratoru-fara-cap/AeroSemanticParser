"""event_store.py. Pinned beyond "it works":

* a deterministic failure is not re-queued until a stamp moves (a section the
  model always chokes on would burn six GPU attempts on every run, forever);
* re-extraction REPLACES a unit's events, never merges (two readings are not
  one reading, and standalone Mongo has no transaction to make it safe);
* events_for keys by frame_url: the KG build's entries carry no _id, and the
  wrong key builds an event-free graph silently;
* every staleness stamp re-queues on its own, and extraction_stamps honours
  the build snapshot.
"""
from datetime import datetime, timedelta, timezone

import pytest

from helpers import mock_store
from modules import event_store as es
from modules.kg import events as kg_events

URL = "https://knowyourmeme.com/memes/doge"
OTHER = "https://knowyourmeme.com/memes/cheems"
SECTION_TEXT = ["Posted to Tumblr on February 23rd, 2010 by Atsuko Sato.", "It then spread to 4chan."]
STAMPS = {"prompt_version": "1", "extraction_version": "1.0.0", "schema_sha": "abc123"}
WHEN = datetime(2026, 9, 18, 9, 0, tzinfo=timezone.utc)
KEY = kg_events.frame_key(URL)


@pytest.fixture
def store():
    return mock_store(es.EventStore)


def entry_doc(url=URL, **over):
    return {"_id": kg_events.frame_key(url), "url": url, "title": "Doge", "category": "meme", "parser_version": "1.5.0",
            "sections": [{"kind": "about", "heading": "About", "text": ["Doge is a meme."]},
                         {"kind": "origin", "heading": "Origin", "text": SECTION_TEXT},
                         {"kind": "spread", "heading": "Spread", "text": ["It spread."]}], **over}


def event_row(sentences=(1,), date="2010-02-23"):
    """A stored event, extraction 2.0.0 shape: sentences, no summary, media by position."""
    return {"event_id": kg_events.event_id(URL, "origin", list(sentences), date, "day" if date else "none"),
            "sentences": list(sentences), "source_text": SECTION_TEXT[0], "date": date,
            "date_precision": "day" if date else "none", "date_text": "February 23rd, 2010" if date else None,
            "location": "Tumblr", "location_type": "platform", "certainty": "confirmed", "actors": ["Atsuko Sato"],
            "links": [], "images": [], "embeds": [], "frame_url": URL, "source_section": "origin"}


def record(url=URL, section="origin", *, events=None, sha="sha-origin", when=WHEN, **over):
    return {"unit_id": kg_events.unit_id(url, section), "entry_id": kg_events.frame_key(url), "frame_url": url,
            "source_section": section, "heading": section.capitalize(),
            "events": [event_row()] if events is None else events, "source_sha256": sha, "source_chars": 120,
            "sentence_count": 2, "parser_version": "1.5.0", "model": "mistral-small3.2:24b", "digest": "deadbeef",
            "host": "https://ollama-ccdd.example", "extracted_at": when, "elapsed_s": 11.4, "attempts": 1,
            **STAMPS, **over}


BOTH = [record(), record(section="spread", sha="sha-spread")]


def failure(section="origin", **over):
    return {"unit_id": kg_events.unit_id(URL, section), "error": "not json", "error_kind": "invalid", **over}


def stored(store, url=URL, section="origin"):
    return store.events.find_one({"_id": kg_events.unit_id(url, section)})


# -- units --------------------------------------------------------------------------

def units(store, **kw):
    return list(store.iter_section_units(kw.pop("sections", kg_events.SOURCE_SECTIONS), **kw))


def test_one_unit_per_section_with_text_and_limit_counts_units(store):
    store.entries.insert_one(entry_doc())
    got = units(store)
    assert {u["source_section"] for u in got} == {"origin", "spread"} and got[0]["frame_url"] == URL
    store.entries.insert_one(entry_doc(OTHER))
    assert len(units(store, limit=3)) == 3
    store.entries.delete_many({})
    store.entries.insert_one(entry_doc(sections=[{"kind": "origin", "heading": "Origin", "text": []}]))
    assert units(store) == []


def test_ready_only_filters_on_corpus_status(store):
    store.entries.insert_many([entry_doc(corpus_status="incomplete"), entry_doc(OTHER, corpus_status="ready")])
    assert {u["frame_url"] for u in units(store, ready_only=True)} == {OTHER}


# -- what is pending -------------------------------------------------------------------

@pytest.fixture
def pending(store):
    store.entries.insert_one(entry_doc())
    us = [dict(u, source_sha256=f"sha-{u['source_section']}") for u in units(store)]

    def go(**over):
        return [u["source_section"] for u in store.select_pending(us, **{**STAMPS, **over})]
    go.store, go.units = store, us
    return go


def test_never_extracted_units_are_pending_until_extracted(pending):
    assert sorted(pending()) == ["origin", "spread"]
    assert len(pending.store.select_pending(pending.units, limit=1, **STAMPS)) == 1     # limit after filtering
    pending.store.save_extraction([record()])
    assert pending() == ["spread"]


def test_a_changed_page_requeues_only_that_unit(pending):
    pending.store.save_extraction(BOTH)
    assert pending() == []
    pending.units[0]["source_sha256"] = "sha-origin-v2"
    assert pending() == ["origin"]


@pytest.mark.parametrize("key, changed", [("prompt_version", "2"), ("extraction_version", "2.0.0"),
                                          ("schema_sha", "different")])
def test_each_stamp_requeues_on_its_own(pending, key, changed):
    pending.store.save_extraction(BOTH)
    assert pending() == [] and len(pending(**{key: changed})) == 2


def test_force_returns_everything(pending):
    pending.store.save_extraction(BOTH)
    assert len(pending.store.select_pending(pending.units, force=True, **STAMPS)) == 2


def test_a_deterministic_failure_is_not_requeued_until_a_stamp_moves(pending):
    pending.store.save_failures([failure(entry_id=KEY, frame_url=URL, source_section="origin",
                                         source_sha256="sha-origin")], **STAMPS)
    assert pending() == ["spread"]
    assert len(pending(prompt_version="2")) == 2


def test_a_forced_run_is_not_undone_by_the_chunk_refilter(pending):
    """force selected every unit, and each chunk's select_pending then dropped
    every up-to-date one: the force redid nothing and reported success."""
    pending.store.save_extraction(BOTH)
    started = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)
    assert pending() == [] and len(pending.store.not_extracted_since(pending.units, started)) == 2
    pending.store.save_extraction([record(when=started.replace(hour=13))])        # this run redid origin
    todo = pending.store.not_extracted_since(pending.units, started.isoformat())
    assert [u["source_section"] for u in todo] == ["spread"]


# -- saving -----------------------------------------------------------------------------

def test_a_record_round_trips_with_its_stamps(store):
    written = store.save_extraction([record()])
    assert (written["units"], written["events"]) == (1, 1)
    doc = stored(store)
    assert (doc["event_count"], doc["frame_url"], doc["schema_sha"], doc["events"][0]["certainty"]) == \
        (1, URL, STAMPS["schema_sha"], "confirmed")
    assert "unit_id" not in doc                       # it is the _id


def test_a_string_timestamp_is_stored_as_a_date(store):
    # what extract() really writes (the record is also a JSONL line): as a
    # string it would crash as_utc, or be invisible to the snapshot filter
    store.save_extraction([record(when="2026-09-18T09:00:00Z")])
    assert isinstance(stored(store)["extracted_at"], datetime)
    assert list(store.events_for([KEY], extracted_at_lte=datetime(2026, 9, 18, 10, 0, tzinfo=timezone.utc))) == [URL]


def test_zero_events_is_stored_and_reextraction_replaces(store):
    # "found nothing" must differ from "never attempted"
    store.save_extraction([record(events=[])])
    assert (stored(store)["event_count"], stored(store)["events"]) == (0, [])
    store.save_extraction([record()])
    store.save_extraction([record(events=[event_row(sentences=(1, 2), date=None)])])
    doc = stored(store)
    assert (doc["event_count"], doc["events"][0]["sentences"]) == (1, [1, 2])


def test_a_field_the_new_record_lacks_does_not_survive(store):
    # 2026-09-29: 99 documents re-extracted at 3.2.0 still carried 2.1's `discarded`
    store.save_extraction([record(discarded=[{"x": 1}], truncated=False)])
    first_seen = stored(store)["first_extracted_at"]
    store.save_extraction([record()])
    doc = stored(store)
    assert not {"discarded", "truncated"} & set(doc) and doc["first_extracted_at"] == first_seen


def test_dead_letters(store):
    store.save_failures([failure()], **STAMPS)
    store.save_failures([failure()], **STAMPS)
    doc = store.failures.find_one({})
    assert (doc["attempts"], doc["error_kind"]) == (2, "invalid")
    assert store.save_extraction([record()])["failures_cleared"] == 1       # a success clears it
    assert store.failures.count_documents({}) == 0
    store.save_failures([failure(error="x", error_kind="permanent")], **STAMPS)
    assert stored(store)["event_count"] == 1                                  # never touches the events


# -- events_for ---------------------------------------------------------------------------

def test_events_for_keys_by_frame_url_and_carries_the_model(store):
    store.save_extraction([record()])
    out = store.events_for([KEY])
    assert list(out) == [URL] and len(out[URL]) == 1
    # build.py puts it on the node: a model's reading, told from a scraped fact
    assert (out[URL][0]["model"], out[URL][0]["extraction_version"]) == ("mistral-small3.2:24b", "1.0.0")
    assert store.events_for([]) == {}


def test_origin_comes_before_spread_whatever_the_write_order(store):
    spread = {**event_row(sentences=(2,), date=None), "source_section": "spread"}
    store.save_extraction([record(section="spread", events=[spread], sha="sha-spread"), record()])
    assert [e["source_section"] for e in store.events_for([KEY])[URL]] == ["origin", "spread"]


# -- stamps and the snapshot ----------------------------------------------------------------

def test_stamps(store):
    empty = store.extraction_stamps()
    assert (empty["events_units"], empty["events_total"], empty["events_max_extracted_at"]) == (0, 0, None)
    store.save_extraction([record()])
    one = store.extraction_stamps()["events_total"]
    store.save_extraction([record(events=[event_row(), event_row(sentences=(2,), date=None)])])
    s = store.extraction_stamps()
    assert (s["events_units"], s["events_total"], s["events_prompt_versions"], s["events_schema_shas"]) == \
        (1, 2, ["1"], ["abc123"])
    assert s["events_total"] != one          # what the events SAY changed: the graph rebuilds
    store.save_extraction([record(OTHER, sha="s", prompt_version="2")])
    assert store.extraction_stamps()["events_prompt_versions"] == ["1", "2"]   # a mixed corpus shows


def test_the_snapshot_hides_what_was_extracted_after_it(store):
    store.save_extraction([record(when=WHEN)])
    store.save_extraction([record(OTHER, when=WHEN + timedelta(hours=2), sha="sha-other")])
    assert list(store.events_for([KEY, kg_events.frame_key(OTHER)], extracted_at_lte=WHEN)) == [URL]
    assert store.extraction_stamps(extracted_at_lte=WHEN)["events_units"] == 1
    assert store.extraction_stamps()["events_units"] == 2


def test_stats_report_coverage_the_model_mix_and_failures(store):
    store.entries.insert_many([entry_doc(), entry_doc(OTHER)])
    store.save_extraction([record()])
    store.save_extraction([record(section="spread", events=[], sha="sha-spread")])
    store.save_extraction([record(OTHER, sha="s", model="qwen3.8:27b")])
    store.save_failures([{"unit_id": "x:origin", "error": "boom", "error_kind": "invalid"}], **STAMPS)
    s = store.stats()
    assert {k: s[k] for k in ("units_total", "frames_covered", "frames_total", "zero_event_units", "units_possible",
                              "units_current", "events_by_certainty", "events_by_precision", "failures_total",
                              "failure_kind_counts")} == {
        "units_total": 3, "frames_covered": 2, "frames_total": 2, "zero_event_units": 1,
        "units_possible": 4,      # every origin/spread section with text
        "units_current": 0,       # the fixtures carry stamp "1"
        "events_by_certainty": {"confirmed": 2}, "events_by_precision": {"day": 2},
        "failures_total": 1, "failure_kind_counts": {"invalid": 1}}
    assert set(s["models_in_use"]) == {"mistral-small3.2:24b", "qwen3.8:27b"}
