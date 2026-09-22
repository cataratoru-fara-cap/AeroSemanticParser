"""Tests for entity_store.py (no network, no real Mongo — mongomock).

Same fresh_store() pattern as test_event_store.py: bypass __init__ and wire
mongomock collections directly, so no index DDL is issued.

What these pin, beyond "it works":

  * **A re-link REPLACES a frame's mentions; it never merges.** Two
    linkings under different lexicons are two readings.
  * **Every staleness stamp re-queues on its own** — the text, the
    linker, the lexicon, the model. A hole in any one means a new dump is
    downloaded and built and nothing is re-linked against it.
  * **links_for keys by frame_url and hands the KG build only what it
    uses** — the features, offsets and margins stay here, for curation.
  * **The snapshot is honoured**, in links_for and in linking_stamps.
  * **The DAG calls only exported facades** (the kg_store lesson: twice a
    live run died on an AttributeError for a method that was not a facade).

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_entity_store.py -v
"""
import re
import unittest
from datetime import datetime, timezone
from pathlib import Path

import mongomock

from modules import entity_store as st
from modules.kg import entities as kg_entities

URL = "https://knowyourmeme.com/memes/doge"
OTHER = "https://knowyourmeme.com/memes/cheems"
STAMPS = {"linker_version": "1.0.0", "lexicon_version": "lex1",
          "nlp_model": "en_core_web_sm@3.8.0"}


def fresh_store() -> st.EntityStore:
    client = mongomock.MongoClient()
    store = st.EntityStore.__new__(st.EntityStore)
    store.client = client
    store.db = client["memes"]
    store.entries = store.db["entries"]
    store.entities = store.db["entities"]
    return store


def entry_doc(url=URL, about="Doge is a Shiba Inu meme.", **over) -> dict:
    doc = {"_id": kg_entities.frame_key(url), "url": url, "title": "Doge",
           "tags": ["doge", "shiba inu"], "parser_version": "1.6.1",
           "corpus_status": "ready",
           "sections": [{"kind": "about", "heading": "About", "text": [about]}]}
    doc.update(over)
    return doc


def mention(text="Shiba Inu", qid="Q39315", field="about", start=10) -> dict:
    return {"field": field, "text": text, "start": start, "end": start + len(text),
            "qid": qid, "label": text, "description": "dog breed", "score": 0.7,
            "method": "ner", "ner_label": "ORG", "proper": True, "matched": text,
            "candidates": 1, "margin": 0.7,
            "features": {"prior": 0.8, "context": 0.3, "exact": 1.0, "type": 0.0,
                         "kym": 0.0, "clarity": 1.0}}


def record(unit, mentions=(), stamps=STAMPS, linked_at=None) -> dict:
    ms = list(mentions)
    return {"unit_id": unit["unit_id"], "entry_id": unit["entry_id"],
            "frame_url": unit["frame_url"], "mentions": ms,
            "mention_count": len(ms), "entity_count": len({m["qid"] for m in ms}),
            "nil": [], "rejected_count": 0, "self_qid": None,
            "source_sha256": unit["source_sha256"], "source_chars": 10,
            "parser_version": "1.6.1", **stamps,
            "linked_at": linked_at or "2026-09-22T10:00:00Z", "elapsed_s": 0.01}


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.s = fresh_store()
        self.s.entries.insert_many([entry_doc(), entry_doc(OTHER, corpus_status="incomplete")])
        self.units = list(self.s.iter_frame_units())

    def test_every_entry_is_a_unit(self):
        self.assertEqual({u["frame_url"] for u in self.units}, {URL, OTHER})

    def test_ready_only_and_limit(self):
        self.assertEqual([u["frame_url"] for u in self.s.iter_frame_units(ready_only=True)],
                         [URL])
        self.assertEqual(len(list(self.s.iter_frame_units(limit=1))), 1)

    def test_never_linked_is_pending(self):
        self.assertEqual(len(self.s.select_pending(self.units, stamps=STAMPS)), 2)

    def test_linked_under_the_same_stamps_is_not(self):
        self.s.save_links([record(u) for u in self.units])
        self.assertEqual(self.s.select_pending(self.units, stamps=STAMPS), [])

    def test_every_stamp_requeues_on_its_own(self):
        self.s.save_links([record(u) for u in self.units])
        for key in STAMPS:
            moved = dict(STAMPS, **{key: "something-else"})
            self.assertEqual(len(self.s.select_pending(self.units, stamps=moved)), 2, key)

    def test_an_edited_about_requeues_that_frame_only(self):
        self.s.save_links([record(u) for u in self.units])
        self.s.entries.update_one({"url": URL}, {"$set": {
            "sections": [{"kind": "about", "text": ["Doge, edited."]}]}})
        pending = self.s.select_pending(list(self.s.iter_frame_units()), stamps=STAMPS)
        self.assertEqual([u["frame_url"] for u in pending], [URL])

    def test_force_returns_everything_selected(self):
        self.s.save_links([record(u) for u in self.units])
        self.assertEqual(len(self.s.select_pending(self.units, stamps=STAMPS, force=True)), 2)

    def test_a_forced_retry_skips_what_it_already_relinked(self):
        since = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)
        self.s.save_links([record(self.units[0], linked_at="2026-09-22T13:00:00Z"),
                           record(self.units[1], linked_at="2026-09-22T11:00:00Z")])
        todo = self.s.not_linked_since(self.units, since.isoformat())
        self.assertEqual([u["unit_id"] for u in todo], [self.units[1]["unit_id"]])

    def test_units_for_rebuilds_in_the_order_asked(self):
        ids = [kg_entities.frame_key(OTHER), kg_entities.frame_key(URL), "missing"]
        self.assertEqual([u["frame_url"] for u in self.s.units_for(ids)], [OTHER, URL])


class WriteTests(unittest.TestCase):
    def setUp(self):
        self.s = fresh_store()
        self.s.entries.insert_one(entry_doc())
        (self.unit,) = self.s.iter_frame_units()

    def test_a_relink_replaces_the_mentions(self):
        self.s.save_links([record(self.unit, [mention(), mention("Doge", "Q15894956", start=0)])])
        self.s.save_links([record(self.unit, [mention("Doge", "Q15894956", start=0)])])
        doc = self.s.entities.find_one({"_id": self.unit["unit_id"]})
        self.assertEqual([m["qid"] for m in doc["mentions"]], ["Q15894956"])
        self.assertEqual(doc["mention_count"], 1)

    def test_linked_at_is_stored_as_a_date(self):
        # As a string it would be invisible to every $lte snapshot filter.
        self.s.save_links([record(self.unit)])
        doc = self.s.entities.find_one({"_id": self.unit["unit_id"]})
        self.assertIsInstance(doc["linked_at"], datetime)
        self.assertIn("first_linked_at", doc)

    def test_the_unit_id_is_the_key_not_a_field(self):
        self.s.save_links([record(self.unit)])
        doc = self.s.entities.find_one({})
        self.assertEqual(doc["_id"], kg_entities.frame_key(URL))
        self.assertNotIn("unit_id", doc)


class ReadTests(unittest.TestCase):
    def setUp(self):
        self.s = fresh_store()
        self.s.entries.insert_many([entry_doc(), entry_doc(OTHER)])
        units = {u["frame_url"]: u for u in self.s.iter_frame_units()}
        self.s.save_links([
            record(units[URL], [mention(), mention("Doge", "Q15894956", field="title",
                                                   start=0)],
                   linked_at="2026-09-22T10:00:00Z"),
            record(units[OTHER], [], linked_at="2026-09-22T12:00:00Z")])
        self.ids = [kg_entities.frame_key(URL), kg_entities.frame_key(OTHER)]

    def test_links_for_keys_by_frame_url_and_skips_frames_with_none(self):
        out = self.s.links_for(self.ids)
        self.assertEqual(list(out), [URL])
        self.assertEqual([m["qid"] for m in out[URL]], ["Q39315", "Q15894956"])

    def test_links_for_hands_the_build_only_what_it_uses(self):
        (m, _) = self.s.links_for(self.ids)[URL]
        self.assertEqual(set(m), set(st.LINK_PROJECTION_FIELDS))
        self.assertNotIn("features", m)

    def test_links_for_honours_the_snapshot(self):
        before = datetime(2026, 9, 22, 9, tzinfo=timezone.utc)
        self.assertEqual(self.s.links_for(self.ids, linked_at_lte=before), {})
        after = datetime(2026, 9, 22, 11, tzinfo=timezone.utc)
        self.assertEqual(list(self.s.links_for(self.ids, linked_at_lte=after)), [URL])

    def test_linking_stamps_summarise_the_frozen_generation(self):
        stamps = self.s.linking_stamps()
        self.assertEqual(stamps["entities_frames"], 2)
        self.assertEqual(stamps["entities_mentions"], 2)
        self.assertEqual(stamps["entities_lexicon_versions"], ["lex1"])
        self.assertEqual(stamps["entities_nlp_models"], ["en_core_web_sm@3.8.0"])
        early = self.s.linking_stamps(
            linked_at_lte=datetime(2026, 9, 22, 11, tzinfo=timezone.utc))
        self.assertEqual(early["entities_frames"], 1)
        self.assertLess(early["entities_max_linked_at"], stamps["entities_max_linked_at"])

    def test_linking_stamps_of_nothing(self):
        stamps = fresh_store().linking_stamps()
        self.assertEqual((stamps["entities_frames"], stamps["entities_max_linked_at"]),
                         (0, None))

    def test_stats(self):
        s = self.s.stats()
        self.assertEqual(s["frames_linked"], 2)
        self.assertEqual(s["frames_with_links"], 1)
        self.assertEqual(s["mentions_total"], 2)
        self.assertEqual(s["distinct_entities"], 2)
        self.assertEqual(s["mentions_by_field"], {"about": 1, "title": 1})
        self.assertEqual(s["lexicon_versions_in_use"], {"lex1": 2})


class FacadeContractTests(unittest.TestCase):
    """The names kym_entities calls on the store module, read out of the
    DAG source — so a method that is not also a facade fails here."""

    def test_every_facade_the_dag_calls_exists_and_is_exported(self):
        src = (Path(__file__).resolve().parents[1] / "kym_entities_dag.py").read_text(
            encoding="utf-8")
        names = set(re.findall(r"\bstore\.([A-Za-z_]\w*)\(", src))
        self.assertGreaterEqual(names, {"pending_units", "units_for", "save_links",
                                        "entity_stats", "get_store"})
        for name in names:
            self.assertTrue(callable(getattr(st, name, None)), name)
            self.assertIn(name, st.__all__, name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
