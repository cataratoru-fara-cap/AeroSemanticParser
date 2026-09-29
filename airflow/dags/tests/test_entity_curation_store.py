"""Tests for entity_curation_store.py and kg_store's curated read
(mongomock; no network, no model).

What these pin:

  * **Two levels of staleness.** A re-link (other mentions) or a rule
    stamp re-runs the rules; the judge is re-asked only for items it has
    not answered under the current prompt, schema and model.
  * **Verdicts are per item and survive a rule re-run.**
  * **A failed frame is a dead letter** under the same stamps and the same
    pending items; a success clears it.
  * **The KG reads only kept mentions, each with its basis**; an uncurated
    or stale frame keeps its title links and own item only; a curation
    after the snapshot is not seen.
  * **The DAG calls only exported facades.**
"""
import re
import unittest
from datetime import datetime, timezone
from pathlib import Path

import mongomock

from modules import entity_curation_store as st, entity_store as es, kg_store
from modules.kg import curation as kc

URL = "https://knowyourmeme.com/memes/doge"
RULES = {"curation_version": kc.CURATION_VERSION, "curation_lists_version": "l1",
         "lexicon_version": "lex"}
JUDGE = {"judge_prompt_version": kc.JUDGE_PROMPT_VERSION, "judge_schema_sha": "s1",
         "judge_model": "m", "judge_confirm_model": "c"}
LISTS = kc.Lists(platform_classes={}, platform_items={100: "Reddit"}, deny_classes={},
                 deny_items={400: "popularity"}, version="l1")


class NoClasses:
    def closure(self, q):
        return frozenset()

    def direct(self, q):
        return frozenset()


def m(field, qid, start, end, *, tag_index=None, method="noun_chunk", text="w"):
    out = {"field": field, "qid": qid, "start": start, "end": end, "method": method,
           "text": text, "label": qid, "description": "d", "score": 0.6}
    if tag_index is not None:
        out["tag_index"] = tag_index
    return out


MENTIONS = [m("title", "Q1", 0, 4, method="title", text="Doge"),
            m("tag", "Q2", 0, 9, tag_index=0, text="shiba inu"),
            m("about", "Q100", 0, 6, text="Reddit"),
            m("about", "Q400", 10, 20, text="popularity"),
            m("about", "Q3", 25, 28, text="dog"),
            m("about", "Q4", 30, 33, text="mug")]


def fresh_store():
    client = mongomock.MongoClient()
    s = st.EntityCurationStore.__new__(st.EntityCurationStore)
    s.client, s.db = client, client["memes"]
    for attr, name in (("entities", "entities"), ("entries", "entries"),
                       ("curation", "entity_curation"),
                       ("failures", "entity_curation_failures")):
        setattr(s, attr, s.db[name])
    return s


class _NoClose:
    def __init__(self, store):
        self.store = store

    def __enter__(self):
        return self.store

    def __exit__(self, *exc):
        return False


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.s = fresh_store()
        self.s.entities.insert_one({"_id": "f1", "frame_url": URL, "mention_count": 6,
                                    "mentions": [dict(x) for x in MENTIONS],
                                    "linked_at": datetime(2026, 9, 1, tzinfo=timezone.utc)})
        self.s.entries.insert_one({"_id": "f1", "title": "Doge", "tags": ["shiba inu"],
                                   "sections": [{"kind": "about", "text": [
                                       "Reddit had popularity, a dog and a mug."]}]})

    def run_rules(self, stamps=RULES):
        ids = self.s.pending_rules(stamps)
        results = [(r, kc.apply_rules(r, LISTS, NoClasses())) for r in self.s.records_for(ids)]
        return self.s.save_rules(results, stamps)

    def doc(self):
        return self.s.curation.find_one({"_id": "f1"})

    def test_rules_then_staleness(self):
        self.assertEqual(self.s.pending_rules(RULES), ["f1"])
        self.assertEqual(self.run_rules(), {"frames": 1, "pending_items": 3})
        d = self.doc()
        self.assertEqual(d["pending_qids"], ["Q2", "Q3", "Q4"])
        self.assertEqual(d["in_graph_count"], 2)                 # title + platform
        self.assertEqual(self.s.pending_rules(RULES), [])
        self.assertEqual(self.s.pending_rules(dict(RULES, curation_lists_version="l2")), ["f1"])
        self.assertEqual(self.s.pending_rules(RULES, force=True), ["f1"])
        # a re-link that changes any mention makes the curation stale
        self.s.entities.update_one({"_id": "f1"}, {"$set": {"mentions.4.end": 29}})
        self.assertEqual(self.s.pending_rules(RULES), ["f1"])

    def test_judge_per_item_and_merge(self):
        self.run_rules()
        self.assertEqual(self.s.pending_judge(JUDGE), ["f1"])
        unit = self.s.judge_units_for(["f1"], JUDGE)[0]
        self.assertFalse(unit["stamps_current"])
        self.assertEqual([it["qid"] for it in unit["items"]], ["Q2", "Q3", "Q4"])
        self.assertIn("[[dog]]", unit["items"][1]["snippet"])
        self.assertEqual(unit["context"]["title"], "Doge")
        # the judge answers two of three (e.g. a batch failed after the first)
        self.s.save_judge("f1", {"verdicts": {"Q2": True, "Q3": True}, "model": "m",
                                 "digest": "d", "host": "h"}, JUDGE, merge=False)
        d = self.doc()
        self.assertEqual(d["pending_qids"], ["Q4"])
        self.assertEqual(d["in_graph_count"], 4)
        unit = self.s.judge_units_for(["f1"], JUDGE)[0]
        self.assertTrue(unit["stamps_current"])
        self.assertEqual([it["qid"] for it in unit["items"]], ["Q4"])
        self.s.save_judge("f1", {"verdicts": {"Q4": False}, "roles": {"Q4": "incidental"}},
                          JUDGE, merge=True)
        d = self.doc()
        self.assertEqual(d["judge"]["verdicts"], {"Q2": True, "Q3": True, "Q4": False})
        self.assertEqual(d["judge"]["roles"], {"Q4": "incidental"})
        self.assertEqual(d["pending_qids"], [])
        self.assertEqual(self.s.pending_judge(JUDGE), [])
        # a rule re-run reuses the verdicts: nothing to ask
        self.run_rules(dict(RULES, curation_lists_version="l2"))
        self.assertEqual(self.doc()["pending_qids"], [])
        self.assertEqual(self.s.pending_judge(JUDGE), [])
        # a new prompt re-asks every rule-pending item, and replaces the verdicts
        new = dict(JUDGE, judge_prompt_version="99")
        self.assertEqual(self.s.pending_judge(new), ["f1"])
        unit = self.s.judge_units_for(["f1"], new)[0]
        self.assertEqual([it["qid"] for it in unit["items"]], ["Q2", "Q3", "Q4"])
        self.s.save_judge("f1", {"verdicts": {"Q2": False, "Q3": False, "Q4": False}},
                          new, merge=unit["stamps_current"])
        self.assertEqual(self.doc()["in_graph_count"], 2)

    def test_dead_letters(self):
        self.run_rules()
        self.s.save_failure("f1", {"error": "timeout", "error_kind": "retryable"}, JUDGE)
        self.assertEqual(self.s.pending_judge(JUDGE), [])
        self.assertEqual(self.s.pending_judge(JUDGE, force=True), ["f1"])
        self.assertEqual(self.s.pending_judge(dict(JUDGE, judge_model="other")), ["f1"])
        self.assertEqual(self.s.pending_judge(dict(JUDGE, judge_confirm_model="")), ["f1"])
        self.s.save_judge("f1", {"verdicts": {"Q2": True, "Q3": True, "Q4": True}},
                          JUDGE, merge=False)
        self.assertEqual(self.s.failures.count_documents({}), 0)

    def test_decisions_and_stamps(self):
        self.run_rules()
        got = self.s.decisions_for(["f1"])[URL]
        self.assertEqual(got["mentions_sha"], kc.mentions_sha(MENTIONS))
        self.assertEqual(sorted(got["keep"].values()), ["platform", "title"])
        stamps = self.s.curation_stamps()
        self.assertEqual((stamps["entities_curation_frames"], stamps["entities_in_graph"],
                          stamps["entities_curation_pending"]), (1, 2, 3))
        self.assertEqual(stamps["entities_curation_versions"], ["l1"])
        early = datetime(2000, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(self.s.decisions_for(["f1"], curated_at_lte=early), {})
        stats = self.s.stats()
        self.assertEqual(stats["decisions_by_basis"]["pending:None"], 3)


class CuratedReadTests(unittest.TestCase):
    """kg_store.entity_links_for over mongomock entity + curation stores."""

    def setUp(self):
        self.s = fresh_store()
        self.e = es.EntityStore.__new__(es.EntityStore)
        self.e.client, self.e.db = self.s.client, self.s.db
        self.e.entries, self.e.entities = self.s.entries, self.s.entities
        self.s.entities.insert_one({"_id": "f1", "frame_url": URL, "mention_count": 7,
                                    "mentions": [dict(x) for x in MENTIONS] + [
                                        m("about", "Q9", 40, 44, method="kym_id")],
                                    "linked_at": datetime(2026, 9, 1, tzinfo=timezone.utc)})
        self._orig = (st.get_store, es.get_store)
        st.get_store = lambda *a, **k: _NoClose(self.s)
        es.get_store = lambda *a, **k: _NoClose(self.e)
        self.snap = datetime(2100, 1, 1, tzinfo=timezone.utc)

    def tearDown(self):
        st.get_store, es.get_store = self._orig

    def read(self):
        return [(x["qid"], x["relevance_basis"])
                for x in kg_store.entity_links_for(["f1"], self.snap)[URL]]

    def test_uncurated_frames_keep_title_and_own_item(self):
        self.assertEqual(self.read(), [("Q1", "title"), ("Q9", "own_item")])

    def test_curated_frames_keep_what_was_kept(self):
        rec = self.s.records_for(["f1"])[0]
        self.s.save_rules([(rec, kc.apply_rules(rec, LISTS, NoClasses()))], RULES)
        self.s.save_judge("f1", {"verdicts": {"Q2": True, "Q3": False, "Q4": False}},
                          JUDGE, merge=False)
        self.assertEqual(self.read(), [("Q1", "title"), ("Q2", "judge"), ("Q100", "platform"),
                                       ("Q9", "own_item")])
        # a re-link since the curation: back to the fallback
        self.s.entities.update_one({"_id": "f1"}, {"$set": {"mentions.4.qid": "Q33"}})
        self.assertEqual(self.read(), [("Q1", "title"), ("Q9", "own_item")])

    def test_stamps_combine_both_stores(self):
        got = kg_store.linking_stamps(self.snap)
        self.assertTrue(set(kg_store.ENTITY_STAMP_KEYS) <= set(got),
                        set(kg_store.ENTITY_STAMP_KEYS) - set(got))


class FacadeContractTests(unittest.TestCase):
    def test_every_facade_the_dag_calls_exists_and_is_exported(self):
        dag = (Path(__file__).resolve().parents[1] / "kym_entity_curation_dag.py").read_text()
        called = set(re.findall(r"\bstore\.([a-z_]+)\(", dag))
        self.assertTrue(called)
        for name in called:
            self.assertTrue(hasattr(st, name), name)
            self.assertIn(name, st.__all__, name)


if __name__ == "__main__":
    unittest.main()
