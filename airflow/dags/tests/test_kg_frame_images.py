"""Tests for kg/frame_images.py — a KYM entry's own image, read like a
template's (a fake vision client; no network).

What these pin:
  * a frame is read with the FRAME prompt and context (title, category,
    About), and its record is keyed by the frame and carries the frame's
    own stamps — the template reader's version constants are untouched, so
    adding frames re-reads no template;
  * the blind audit sample is deterministic;
  * links are made by the template rule, keyed by the frame.
"""
import io
import json
import unittest
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from modules.kg import frame_images as fi
from modules.kg import template_entities as te

SCHEMA_PATH = str(Path(__file__).resolve().parents[1] / "kg_config"
                  / "template_entity_schema.json")
URL = "https://knowyourmeme.com/memes/spongegar"


def jpeg(size=(1200, 800)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, (20, 120, 220)).save(buf, "JPEG")
    return buf.getvalue()


def ent(name="SpongeBob SquarePants", kind="character", named=True, box=(100, 50, 600, 900)):
    return {"name": name, "kind": kind, "named": named, "box": list(box), "text": None,
            "confidence": "high"}


class FakeClient:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    def chat(self, messages, request, *, purpose=None, format=None, options=None,
             think=None, validate=None, retry_temperature=None):
        self.calls.append({"messages": messages, "purpose": purpose})
        content = self.replies.pop(0)
        try:
            parsed = validate(content)
        except ValueError as exc:
            return SimpleNamespace(ok=False, error_kind="invalid", error=str(exc), attempts=3)
        return SimpleNamespace(ok=True, parsed=parsed, model="qwen3-vl:32b",
                               digest="ff2e46876908", host="ollama-ccdd", attempts=1)


def entry(about=("Spongegar is a caveman version of SpongeBob.",), category="meme"):
    return {"url": URL, "title": "Spongegar", "category": category,
            "sections": [{"kind": "origin", "text": ["ignored"]},
                         {"kind": "about", "text": list(about)}]}


class ContextTests(unittest.TestCase):
    def test_title_category_and_the_opening_of_the_about(self):
        self.assertEqual(fi.frame_context(entry()), {
            "title": "Spongegar", "category": "meme",
            "about": "Spongegar is a caveman version of SpongeBob."})

    def test_no_about(self):
        self.assertEqual(fi.frame_context(entry(about=()))["about"], "(not described)")

    def test_the_about_is_cut(self):
        self.assertEqual(len(fi.frame_context(entry(about=("x " * 1000,)))["about"]), 600)


class DetectTests(unittest.TestCase):
    def setUp(self):
        self.schema, self.sha = te.load_schema(SCHEMA_PATH)
        self.request = fi.model_request(env={})

    def unit(self, url=URL):
        return {"frame_url": url, "image_url": "https://i.kym-cdn.com/entries/icons/original/1.jpg",
                "context": fi.frame_context(entry())}

    def test_a_reading_with_the_frame_prompt(self):
        client = FakeClient([json.dumps({"entities": [ent()]})])
        image = jpeg()
        rec = fi.detect(client, self.request, self.unit(), image, schema=self.schema,
                        schema_sha=self.sha)
        self.assertTrue(rec["ok"])
        self.assertEqual(rec["frame_url"], URL)
        self.assertEqual(rec["reader_version"], fi.READER_VERSION)
        self.assertEqual(len(rec["image_sha256"]), 64)
        self.assertEqual(rec["regions"][0]["name"], "SpongeBob SquarePants")
        call = client.calls[0]
        self.assertEqual(call["purpose"], fi.DETECT_PURPOSE)
        self.assertEqual(call["messages"][0]["content"], fi.SYSTEM_PROMPT)
        self.assertIn("Entry: Spongegar", call["messages"][1]["content"])
        self.assertNotIn("template", call["messages"][1]["content"].lower())

    def test_the_audit_sample_is_deterministic_and_read_blind(self):
        url = next(f"{URL}-{i}" for i in range(10_000) if fi.in_blind_sample(f"{URL}-{i}"))
        self.assertTrue(fi.in_blind_sample(url))
        client = FakeClient([json.dumps({"entities": [ent()]}),
                             json.dumps({"entities": [ent(name="man", named=False)]})])
        rec = fi.detect(client, self.request, self.unit(url), jpeg(), schema=self.schema,
                        schema_sha=self.sha)
        self.assertEqual(rec["blind"]["regions"][0]["name"], "man")
        self.assertEqual(client.calls[1]["messages"][1]["content"], fi.BLIND_USER_TMPL)

    def test_an_unreadable_image_is_a_failed_frame_not_a_crash(self):
        client = FakeClient([])
        rec = fi.detect(client, self.request, self.unit(), b"<html>", schema=self.schema,
                        schema_sha=self.sha)
        self.assertEqual((rec["ok"], rec["error_kind"], rec["frame_url"]), (False, "image", URL))
        self.assertEqual(client.calls, [])

    def test_a_failure_is_data(self):
        rec = fi.detect(FakeClient(["nope"]), self.request, self.unit(), jpeg(),
                        schema=self.schema, schema_sha=self.sha)
        self.assertEqual((rec["ok"], rec["error_kind"]), (False, "invalid"))

    def test_the_template_readers_stamps_are_not_moved(self):
        # Their versions re-read every template when they change.
        self.assertEqual((te.EXTRACTOR_VERSION, te.PROMPT_VERSION), ("1.1.0", "2"))
        self.assertNotEqual(fi.SYSTEM_PROMPT, te.SYSTEM_PROMPT)


class FakeLinker:
    stamps = {"linker_version": "1", "lexicon_version": "lex"}

    def words(self, text):
        return set(text.lower().split())

    def link_label(self, name, **kw):
        return ({"qid": "Q83279", "label": name, "text": name, "score": 0.9,
                 "method": kw.get("method")}, "linked")

    def link_spans(self, text, **kw):
        return []


class LinkTests(unittest.TestCase):
    def test_keyed_by_the_frame_by_the_template_rule(self):
        det = {"frame_url": URL, "regions": te.ground([ent()])[0]}
        rec = fi.link_detection(FakeLinker(), det, context_text="Spongegar", prefer=[83279],
                                link_context_sha="c1")
        self.assertEqual(rec["frame_url"], URL)
        self.assertNotIn("template_id", rec)
        self.assertEqual(rec["mentions"][0]["qid"], "Q83279")
        self.assertTrue(rec["mentions"][0]["in_graph"])          # named: in the graph
        self.assertEqual(rec["template_link_version"], te.TEMPLATE_LINK_VERSION)


if __name__ == "__main__":
    unittest.main(verbosity=2)
