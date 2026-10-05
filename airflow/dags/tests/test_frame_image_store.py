"""Tests for frame_image_store.py (mongomock; no network, no model).

What these pin: memes are read first and a frame without an image is not a
unit; a reading is fresh only under the same stamps AND the same image URL;
a dead letter is skipped until something about it changes; images are
cached by their URL; the KG build sees only in-graph mentions, frozen at
its snapshot.
"""
import os
import unittest
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory

import mongomock

from modules import frame_image_store as st
from modules.kg import frame_images as fi
from modules.kg import template_entities as te

STAMPS = {"reader_version": fi.READER_VERSION, "prompt_version": fi.PROMPT_VERSION,
          "schema_sha": "s1", "requested_model": "qwen3-vl:32b"}
LINK_STAMPS = {"linker_version": "1.2.0", "lexicon_version": "lex", "nlp_model": "sm",
               "senses_version": "s1", "template_link_version": te.TEMPLATE_LINK_VERSION}
A = "https://knowyourmeme.com/memes/a"
B = "https://knowyourmeme.com/memes/people/b"
C = "https://knowyourmeme.com/memes/c"
IMG = "https://i.kym-cdn.com/entries/icons/original/000/001/a.jpg"


def fresh_store() -> st.FrameImageStore:
    client = mongomock.MongoClient()
    s = st.FrameImageStore.__new__(st.FrameImageStore)
    s.client, s.db = client, client["memes"]
    for attr, name in (("entries", "entries"), ("entities", "entities"),
                       ("detections", "frame_image_entities"),
                       ("failures", "frame_image_entity_failures")):
        setattr(s, attr, s.db[name])
    return s


def detection(url=A, image=IMG, **over):
    return {"frame_url": url, "image_url": image, "ok": True, "regions": [],
            "model": "qwen3-vl:32b", **STAMPS, **over}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.s = fresh_store()
        self.s.entries.insert_many([
            {"_id": "e-b", "url": B, "og_image": IMG + "?b", "title": "B", "category": "person",
             "sections": [{"kind": "about", "text": ["B is a person."]}]},
            {"_id": "e-a", "url": A, "og_image": IMG, "title": "A", "category": "meme",
             "sections": [{"kind": "about", "text": ["A is a meme."]}]},
            {"_id": "e-c", "url": C, "og_image": "", "title": "C", "category": "meme"},
        ])

    def test_units_memes_first_and_only_frames_with_an_image(self):
        units = self.s.units_for()
        self.assertEqual([u["frame_url"] for u in units], [A, B])
        self.assertEqual(units[0]["context"]["about"], "A is a meme.")

    def test_fresh_only_under_the_same_stamps_and_image(self):
        self.s.save_detection(detection())
        self.assertEqual([u["frame_url"] for u in self.s.select_pending(
            self.s.units_for(), STAMPS)], [B])
        # a new picture at the top of the page: read again
        self.s.entries.update_one({"url": A}, {"$set": {"og_image": IMG + "?new"}})
        self.assertIn(A, [u["frame_url"] for u in self.s.select_pending(
            self.s.units_for(), STAMPS)])
        # a new prompt: read again
        self.assertIn(B, [u["frame_url"] for u in self.s.select_pending(
            self.s.units_for(), {**STAMPS, "prompt_version": "99"})])

    def test_a_dead_letter_waits_for_a_change_and_a_success_clears_it(self):
        self.s.save_failure(detection(url=B, image=IMG + "?b", ok=False, error="x"))
        self.assertNotIn(B, [u["frame_url"] for u in self.s.select_pending(
            self.s.units_for(), STAMPS)])
        self.s.save_detection(detection(url=B, image=IMG + "?b"))
        self.assertEqual(self.s.failures.count_documents({}), 0)

    def test_not_detected_since(self):
        since = datetime.now(timezone.utc) - timedelta(minutes=1)
        self.s.save_detection(detection())
        left = self.s.not_detected_since(self.s.units_for(), since.isoformat())
        self.assertEqual([u["frame_url"] for u in left], [B])

    def test_the_build_sees_only_in_graph_mentions_at_its_snapshot(self):
        self.s.save_detection(detection())
        self.s.save_links([{"frame_url": A, "mention_count": 2, "in_graph_count": 1,
                            "mentions": [{"qid": "Q83279", "label": "SpongeBob", "in_graph": True,
                                          "region": {"kind": "character"}},
                                         {"qid": "Q1", "label": "x", "in_graph": False}]}])
        got = self.s.graph_mentions_for([A, B])
        self.assertEqual(list(got), [A])
        self.assertEqual([(m["qid"], m["model"]) for m in got[A]], [("Q83279", "qwen3-vl:32b")])
        before = datetime.now(timezone.utc) - timedelta(days=1)
        self.assertEqual(self.s.graph_mentions_for([A], linked_at_lte=before), {})
        self.assertEqual(self.s.graph_stamps()["frame_images_in_graph"], 1)

    def test_links_go_stale_with_the_linker_or_the_context(self):
        self.s.save_detection(detection())
        self.assertEqual(self.s.pending_linking(LINK_STAMPS), [A])
        (unit,) = self.s.link_units_for([A])
        rec = {"frame_url": A, "mention_count": 0, "in_graph_count": 0, "mentions": [],
               **LINK_STAMPS, "detection_sha": fi.detection_sha(unit["detection"]),
               "link_context_sha": unit["link_context_sha"]}
        self.s.save_links([rec])
        self.assertEqual(self.s.pending_linking(LINK_STAMPS), [])
        self.assertEqual(self.s.pending_linking({**LINK_STAMPS, "lexicon_version": "new"}), [A])
        # keyed by the entry's hash id, joined on frame_url
        self.s.entities.insert_one({"_id": "e-a", "frame_url": A, "self_qid": "Q42",
                                    "mentions": []})
        self.assertEqual(self.s.pending_linking(LINK_STAMPS), [A])   # new context
        (unit,) = self.s.link_units_for([A])
        self.assertEqual(unit["prefer"], [42])


class ImageCacheTests(unittest.TestCase):
    def test_cached_by_url_with_its_extension(self):
        with TemporaryDirectory() as tmp:
            old = os.environ.get("KG_DATA_DIR")
            os.environ["KG_DATA_DIR"] = tmp
            try:
                path = st.image_path(IMG)
                self.assertTrue(str(path).startswith(tmp) and path.suffix == ".jpg")
                self.assertEqual(st.image_path(IMG), path)              # deterministic
                self.assertIsNone(st.read_image({"image_url": IMG}))
                st.write_image(IMG, b"jpeg")
                self.assertEqual(st.read_image({"image_url": IMG}), b"jpeg")
            finally:
                if old is None:
                    os.environ.pop("KG_DATA_DIR")
                else:
                    os.environ["KG_DATA_DIR"] = old


if __name__ == "__main__":
    unittest.main(verbosity=2)
