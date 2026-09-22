"""Tests for review_store.py (no network, no real Mongo — mongomock).

What these pin, beyond "it works":

  * **A recorded verdict is never re-opened.** Re-drawing after a version
    bump must not silently discard a person's work, and must not quietly
    re-attach it to output it never judged.
  * **Only the CURRENT extraction is eligible.** A verdict on what an
    older extractor produced says nothing about the one that will run the
    backfill.
  * **The drawn section carries its own sentences and events.** The
    dashboard has no access to the pipeline code, and — more importantly —
    a verdict belongs to the extraction it judged, so a re-parse cannot
    move the text out from under a review already recorded.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_review_store.py -v
"""
import unittest

import mongomock

from modules import review_store as rs
from modules.kg import events as kg_events

URL = "https://knowyourmeme.com/memes/doge"
P0 = ("The photo was posted to Tumblr on February 23rd, 2010 by Atsuko Sato. "
      "That same day it spread to 4chan.")


def fresh_store() -> rs.ReviewStore:
    client = mongomock.MongoClient()
    store = rs.ReviewStore.__new__(rs.ReviewStore)
    store.client = client
    store.db = client["memes"]
    store.entries = store.db["entries"]
    store.events = store.db["events"]
    store.reviews = store.db["event_reviews"]
    return store


def entry_doc(url=URL):
    return {"_id": kg_events.frame_key(url), "url": url, "title": "Doge",
            "category": "meme", "parser_version": "1.6.1",
            "sections": [{"kind": "origin", "heading": "Origin", "text": [P0]}]}


def event_doc(url=URL, section="origin", *, version=None, events=None):
    return {"_id": kg_events.unit_id(url, section),
            "entry_id": kg_events.frame_key(url), "frame_url": url,
            "source_section": section, "heading": section.capitalize(),
            "events": events if events is not None else [
                {"event_id": "e1", "sentences": [1], "source_text": P0[:70],
                 "date": "2010-02-23", "date_precision": "day",
                 "date_basis": "stated", "date_text": "February 23rd, 2010",
                 "location": "Tumblr", "location_type": "platform",
                 "actors": ["Atsuko Sato"], "certainty": "confirmed"}],
            "extraction_version": version or kg_events.EXTRACTION_VERSION,
            "prompt_version": kg_events.PROMPT_VERSION,
            "schema_sha": "abc123", "source_sha256": "sha",
            "sentence_count": 2}


class DrawTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()
        self.store.entries.insert_one(entry_doc())

    def draw(self, **kw):
        return self.store.draw_sample(
            **{"seed": 1, "representative": 10, "relative_events": 10,
               "redraw": False, **kw})

    def test_a_drawn_section_carries_its_sentences_and_events(self):
        self.store.events.insert_one(event_doc())
        self.draw()
        [doc] = list(self.store.reviews.find({}))
        self.assertEqual([s["id"] for s in doc["sentences"]], [1, 2])
        self.assertIn("Atsuko Sato", doc["sentences"][0]["text"])
        self.assertEqual(doc["events"][0]["event_id"], "e1")
        self.assertEqual(doc["status"], "pending")
        self.assertEqual(doc["extraction_version"], kg_events.EXTRACTION_VERSION)

    def test_sentences_include_ones_no_event_covers(self):
        """The recall question is about exactly those, so rebuilding the
        text from the events' own spans would beg the question."""
        self.store.events.insert_one(event_doc())
        self.draw()
        [doc] = list(self.store.reviews.find({}))
        covered = {i for e in doc["events"]
                   for i in range(e["sentences"][0], e["sentences"][-1] + 1)}
        self.assertTrue({s["id"] for s in doc["sentences"]} - covered)

    def test_an_older_extraction_is_not_eligible(self):
        self.store.events.insert_one(event_doc(version="0.0.1"))
        self.assertRaises(RuntimeError, self.draw)

    def test_a_recorded_verdict_survives_a_redraw(self):
        self.store.events.insert_one(event_doc())
        self.draw()
        uid = kg_events.unit_id(URL, "origin")
        self.store.save_verdict(uid, verdicts={"e1": {"date": "wrong"}},
                                missed_sentences=[2], note="n", reviewer="gabi")
        out = self.draw(redraw=True)
        doc = self.store.reviews.find_one({"_id": uid})
        self.assertEqual(doc["status"], "done")
        self.assertEqual(doc["verdicts"], {"e1": {"date": "wrong"}})
        self.assertEqual(out["already_done"], 1)
        self.assertEqual(out["sections_to_read"], 0)

    def test_a_section_can_sit_in_two_samples_and_is_read_once(self):
        self.store.events.insert_one(event_doc(events=[
            {"event_id": "r1", "sentences": [2], "source_text": "That same day.",
             "date": "2010-02-23", "date_precision": "day",
             "date_basis": "relative", "date_text": "That same day",
             "location": None, "location_type": "unknown", "actors": [],
             "certainty": "unconfirmed"}]))
        self.draw()
        docs = list(self.store.reviews.find({}))
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["samples"],
                         ["relative", "representative", "unconfirmed"])


class VerdictTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()
        self.store.entries.insert_one(entry_doc())
        self.store.events.insert_one(event_doc())
        self.store.draw_sample(seed=1, representative=10, relative_events=10,
                               redraw=False)
        self.uid = kg_events.unit_id(URL, "origin")

    def test_saving_marks_it_done_and_records_who(self):
        self.store.save_verdict(self.uid, verdicts={"e1": {"date": "ok"}},
                                missed_sentences=[2, 2], note="fine",
                                reviewer="gabi", elapsed_s=41.0)
        doc = self.store.reviews.find_one({"_id": self.uid})
        self.assertEqual(doc["status"], "done")
        self.assertEqual(doc["missed_sentences"], [2])      # deduped
        self.assertEqual(doc["reviewer"], "gabi")
        self.assertIsNotNone(doc["reviewed_at"])

    def test_progress_counts_per_sample(self):
        before = self.store.progress()
        self.assertEqual(before["sections_done"], 0)
        self.store.save_verdict(self.uid, verdicts={"e1": {"date": "ok"}},
                                missed_sentences=[], note="", reviewer="g")
        after = self.store.progress()
        self.assertEqual(after["sections_done"], 1)
        self.assertEqual(after["representative"]["done"], 1)

    def test_report_scores_what_has_been_reviewed(self):
        self.store.save_verdict(
            self.uid,
            verdicts={"e1": {"event": "ok", "date": "wrong", "location": "ok",
                             "actors": "ok", "certainty": "ok"}},
            missed_sentences=[2], note="", reviewer="gabi")
        out = self.store.report()
        rep = out["representative"]
        self.assertEqual(rep["events_judged"], 1)
        self.assertEqual(rep["by_field"]["date"]["rate"], 0.0)
        self.assertEqual(rep["recall"]["sentences_with_a_missed_event"], 1)
        self.assertEqual(out["reviewers"], ["gabi"])


if __name__ == "__main__":       # pragma: no cover
    unittest.main()
