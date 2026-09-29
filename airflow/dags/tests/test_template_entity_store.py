"""Tests for template_entity_store.py (mongomock; no network, no model).

What these pin:

  * **A unit is a KEPT template with its image**, told about the frame
    that scores it highest.
  * **Two levels of staleness**: a new prompt/schema/model/image re-reads;
    a new detection or frame context only re-links.
  * **A failed template is not retried under the same stamps**, and a
    later success clears the dead letter.
  * **The frame's own Wikidata links become the preferred senses.**
  * **The DAG calls only exported facades.**
"""
import re
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import mongomock

from modules import template_entity_store as st
from modules.kg import template_entities as te

STAMPS = {"extractor_version": te.EXTRACTOR_VERSION, "prompt_version": te.PROMPT_VERSION,
          "schema_sha": "s1", "requested_model": "qwen3-vl:32b"}
LINK_STAMPS = {"linker_version": "1.2.0", "lexicon_version": "lex", "nlp_model": "sm",
               "senses_version": "s1", "template_link_version": te.TEMPLATE_LINK_VERSION}


def fresh_store() -> st.TemplateEntityStore:
    client = mongomock.MongoClient()
    s = st.TemplateEntityStore.__new__(st.TemplateEntityStore)
    s.client, s.db = client, client["memes"]
    for attr, name in (("frames", "frame_templates"), ("templates", "imgflip_templates"),
                       ("entries", "entries"), ("entities", "entities"),
                       ("detections", "template_entities"),
                       ("failures", "template_entity_failures")):
        setattr(s, attr, s.db[name])
    return s


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.img = Path(self.tmp.name) / "10.jpg"
        self.img.write_bytes(b"jpeg")
        self.s = fresh_store()
        self.s.frames.insert_many([
            {"_id": "f1", "frame_url": "https://knowyourmeme.com/memes/a", "title": "Drakeposting",
             "status": "selected", "priority": 2,
             "selected": [{"template_id": 10, "R": 0.9}, {"template_id": 11, "R": 0.7}]},
            {"_id": "f2", "frame_url": "https://knowyourmeme.com/memes/b", "title": "Drake",
             "status": "selected", "priority": 1, "selected": [{"template_id": 10, "R": 0.6}]},
        ])
        self.s.templates.insert_many([
            {"_id": 10, "name": "Drake Hotline Bling", "alt_names": ["drake yes no"],
             "blank_path": str(self.img), "blank_sha256": "img10"},
            {"_id": 11, "name": "No image yet"},
        ])
        self.s.entries.insert_many([
            {"_id": "f1", "sections": [{"kind": "about", "text": ["Drake says no to one thing."]}]},
            {"_id": "f2", "sections": []}])
        self.s.entities.insert_one({"_id": "f1", "self_qid": "Q33240",
                                    "mentions": [{"qid": "Q183"}, {"qid": "bad"}]})

    def tearDown(self):
        self.tmp.cleanup()

    def test_units_are_kept_templates_with_an_image(self):
        units = self.s.units_for([10, 11])
        self.assertEqual([u["template_id"] for u in units], [10])
        u = units[0]
        self.assertEqual((u["priority"], u["best_R"]), (1, 0.9))
        self.assertEqual(u["context"]["frame_title"], "Drakeposting")   # highest R
        self.assertIn("Drake says no", u["context"]["about"])
        self.assertEqual(st.read_image(u), b"jpeg")

    def detection(self, **over):
        rec = {"template_id": 10, "ok": True, "image_sha256": "img10", **STAMPS,
               "regions": te.ground([{"name": "Drake", "kind": "person", "named": True,
                                      "box": [0, 0, 500, 500], "confidence": "high"}])[0],
               "model": "qwen3-vl:32b", "digest": "d"}
        rec.update(over)
        return rec

    def test_detection_staleness(self):
        units = self.s.units_for([10])
        self.assertEqual(len(self.s.select_pending(units, STAMPS)), 1)
        self.s.save_detection(self.detection())
        self.assertEqual(self.s.select_pending(units, STAMPS), [])
        for key in ("extractor_version", "prompt_version", "schema_sha", "requested_model"):
            self.assertEqual(len(self.s.select_pending(units, dict(STAMPS, **{key: "x"}))), 1,
                             key)
        self.s.templates.update_one({"_id": 10}, {"$set": {"blank_sha256": "new-image"}})
        self.assertEqual(len(self.s.select_pending(self.s.units_for([10]), STAMPS)), 1)

    def test_dead_letters(self):
        units = self.s.units_for([10])
        self.s.save_failure({"template_id": 10, "image_sha256": "img10", **STAMPS,
                             "error": "503", "error_kind": "retryable"})
        self.assertEqual(self.s.select_pending(units, STAMPS), [])
        self.assertEqual(len(self.s.select_pending(units, dict(STAMPS, prompt_version="next"))), 1)
        self.s.save_detection(self.detection())
        self.assertEqual(self.s.failures.count_documents({}), 0)

    def test_link_staleness_and_preferred_senses(self):
        self.s.save_detection(self.detection())
        self.assertEqual(self.s.pending_linking(LINK_STAMPS), [10])
        unit = self.s.link_units_for([10])[0]
        self.assertEqual(unit["prefer"], [183, 33240])
        self.assertIn("Drakeposting", unit["context_text"])
        rec = {"template_id": 10, "mentions": [], "mention_count": 0, "in_graph_count": 0,
               "nil": [], **LINK_STAMPS, "detection_sha": te.detection_sha(unit["detection"]),
               "link_context_sha": unit["link_context_sha"]}
        self.s.save_links([rec])
        self.assertEqual(self.s.pending_linking(LINK_STAMPS), [])
        self.assertEqual(self.s.pending_linking(dict(LINK_STAMPS, lexicon_version="new")), [10])
        # a new reading changes what the links were made from
        self.s.save_detection(self.detection(regions=[]))
        self.assertEqual(self.s.pending_linking(LINK_STAMPS), [10])


class FacadeContractTests(unittest.TestCase):
    def test_every_facade_the_dag_calls_exists_and_is_exported(self):
        dag = (Path(__file__).resolve().parents[1] / "kym_template_entities_dag.py").read_text()
        called = set(re.findall(r"\bstore\.([a-z_]+)\(", dag))
        self.assertTrue(called)
        for name in called:
            self.assertTrue(hasattr(st, name), name)
            self.assertIn(name, st.__all__, name)


if __name__ == "__main__":
    unittest.main()
