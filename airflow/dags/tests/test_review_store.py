"""review_store.py (mongomock). Pinned: a recorded verdict is never re-opened (a
redraw after a version bump must neither discard a person's work nor re-attach
it to output it never judged); only the CURRENT extraction is eligible; the
drawn section carries its own sentences and events (the dashboard has no
pipeline code, and a re-parse must not move the text out from under a recorded
review)."""
import pytest

from helpers import mock_store
from modules import review_store as rs
from modules.kg import events as kg_events

URL = "https://knowyourmeme.com/memes/doge"
UID = kg_events.unit_id(URL, "origin")
P0 = "The photo was posted to Tumblr on February 23rd, 2010 by Atsuko Sato. That same day it spread to 4chan."
STATED = {"event_id": "e1", "sentences": [1], "source_text": P0[:70], "date": "2010-02-23", "date_precision": "day",
          "date_basis": "stated", "date_text": "February 23rd, 2010", "location": "Tumblr",
          "location_type": "platform", "actors": ["Atsuko Sato"], "certainty": "confirmed"}
RELATIVE = {"event_id": "r1", "sentences": [2], "source_text": "That same day.", "date": "2010-02-23",
            "date_precision": "day", "date_basis": "relative", "date_text": "That same day", "location": None,
            "location_type": "unknown", "actors": [], "certainty": "unconfirmed"}


def event_doc(version=kg_events.EXTRACTION_VERSION, events=(STATED,)):
    return {"_id": UID, "entry_id": kg_events.frame_key(URL), "frame_url": URL, "source_section": "origin",
            "heading": "Origin", "events": list(events), "extraction_version": version,
            "prompt_version": kg_events.PROMPT_VERSION, "schema_sha": "abc123", "source_sha256": "sha",
            "sentence_count": 2}


@pytest.fixture
def s():
    store = mock_store(rs.ReviewStore)
    store.entries.insert_one({"_id": kg_events.frame_key(URL), "url": URL, "title": "Doge", "category": "meme",
                              "parser_version": "1.6.1",
                              "sections": [{"kind": "origin", "heading": "Origin", "text": [P0]}]})
    return store


def draw(store, **kw):
    return store.draw_sample(**{"seed": 1, "representative": 10, "relative_events": 10, "redraw": False, **kw})


def test_a_drawn_section_carries_its_sentences_and_events(s):
    s.events.insert_one(event_doc())
    draw(s)
    [doc] = s.reviews.find({})
    assert [x["id"] for x in doc["sentences"]] == [1, 2] and "Atsuko Sato" in doc["sentences"][0]["text"]
    assert (doc["events"][0]["event_id"], doc["status"], doc["extraction_version"]) == \
        ("e1", "pending", kg_events.EXTRACTION_VERSION)
    # including sentences no event covers: the recall question is about exactly those
    covered = {i for e in doc["events"] for i in range(e["sentences"][0], e["sentences"][-1] + 1)}
    assert {x["id"] for x in doc["sentences"]} - covered


def test_an_older_extraction_is_not_eligible(s):
    s.events.insert_one(event_doc(version="0.0.1"))
    with pytest.raises(RuntimeError):
        draw(s)


def test_a_section_can_sit_in_two_samples_and_is_read_once(s):
    s.events.insert_one(event_doc(events=[RELATIVE]))
    draw(s)
    [doc] = s.reviews.find({})
    assert doc["samples"] == ["relative", "representative", "unconfirmed"]


@pytest.fixture
def drawn(s):
    s.events.insert_one(event_doc())
    draw(s)
    return s


def test_a_recorded_verdict_survives_a_redraw(drawn):
    drawn.save_verdict(UID, verdicts={"e1": {"date": "wrong"}}, missed_sentences=[2], note="n", reviewer="gabi")
    out = draw(drawn, redraw=True)
    doc = drawn.reviews.find_one({"_id": UID})
    assert (doc["status"], doc["verdicts"], out["already_done"], out["sections_to_read"]) == \
        ("done", {"e1": {"date": "wrong"}}, 1, 0)


def test_saving_marks_it_done_and_records_who_and_progress_counts(drawn):
    assert drawn.progress()["sections_done"] == 0
    drawn.save_verdict(UID, verdicts={"e1": {"date": "ok"}}, missed_sentences=[2, 2], note="fine", reviewer="gabi",
                       elapsed_s=41.0)
    doc = drawn.reviews.find_one({"_id": UID})
    assert (doc["status"], doc["missed_sentences"], doc["reviewer"]) == ("done", [2], "gabi")   # deduped
    assert doc["reviewed_at"] is not None
    after = drawn.progress()
    assert (after["sections_done"], after["representative"]["done"]) == (1, 1)


def test_report_scores_what_has_been_reviewed(drawn):
    drawn.save_verdict(UID, verdicts={"e1": {"event": "ok", "date": "wrong", "location": "ok", "actors": "ok",
                                             "certainty": "ok"}}, missed_sentences=[2], note="", reviewer="gabi")
    out = drawn.report()
    rep = out["representative"]
    assert (rep["events_judged"], rep["by_field"]["date"]["rate"], rep["recall"]["sentences_with_a_missed_event"],
            out["reviewers"]) == (1, 0.0, 1, ["gabi"])
