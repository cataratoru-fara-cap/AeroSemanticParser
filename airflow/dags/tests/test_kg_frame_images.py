"""kg/frame_images.py: a KYM entry's own image, read like a template's (a fake
vision client). Pinned: a frame is read with the FRAME prompt and context
(title, category, About), its record is keyed by the frame and carries the
frame's own stamps, and the template reader's version constants are untouched
(adding frames re-reads no template); the blind audit sample is deterministic;
links are made by the template rule, keyed by the frame."""
import json

import pytest

from helpers import KG_CONFIG
from modules.kg import frame_images as fi
from modules.kg import template_entities as te
from vision_stub import VisionClient, jpeg

URL = "https://knowyourmeme.com/memes/spongegar"
SCHEMA, SHA = te.load_schema(str(KG_CONFIG / "template_entity_schema.json"))
REQUEST = fi.model_request(env={})


def ent(name="SpongeBob SquarePants", named=True):
    return {"name": name, "kind": "character", "named": named, "box": [100, 50, 600, 900], "text": None,
            "confidence": "high"}


def entry(about=("Spongegar is a caveman version of SpongeBob.",)):
    return {"url": URL, "title": "Spongegar", "category": "meme",
            "sections": [{"kind": "origin", "text": ["ignored"]}, {"kind": "about", "text": list(about)}]}


@pytest.mark.parametrize("about, want", [
    (("Spongegar is a caveman version of SpongeBob.",), "Spongegar is a caveman version of SpongeBob."),
    ((), "(not described)"),
])
def test_the_context_is_title_category_and_the_opening_of_the_about(about, want):
    assert fi.frame_context(entry(about)) == {"title": "Spongegar", "category": "meme", "about": want}


def test_the_about_is_cut():
    assert len(fi.frame_context(entry(("x " * 1000,)))["about"]) == 600


def detect(replies, url=URL, image=None):
    client = VisionClient([json.dumps(r) if isinstance(r, dict) else r for r in replies])
    unit = {"frame_url": url, "image_url": "https://i.kym-cdn.com/entries/icons/original/1.jpg",
            "context": fi.frame_context(entry())}
    return fi.detect(client, REQUEST, unit, jpeg(color=(20, 120, 220)) if image is None else image, schema=SCHEMA,
                     schema_sha=SHA), client


def test_a_reading_with_the_frame_prompt():
    rec, client = detect([{"entities": [ent()]}])
    assert (rec["ok"], rec["frame_url"], rec["reader_version"], len(rec["image_sha256"])) == \
        (True, URL, fi.READER_VERSION, 64)
    assert rec["regions"][0]["name"] == "SpongeBob SquarePants"
    [call] = client.calls
    assert (call["purpose"], call["messages"][0]["content"]) == (fi.DETECT_PURPOSE, fi.SYSTEM_PROMPT)
    assert "Entry: Spongegar" in call["messages"][1]["content"] and "template" not in call["messages"][1]["content"].lower()


def test_the_audit_sample_is_deterministic_and_read_blind():
    url = next(f"{URL}-{i}" for i in range(10_000) if fi.in_blind_sample(f"{URL}-{i}"))
    assert fi.in_blind_sample(url)
    rec, client = detect([{"entities": [ent()]}, {"entities": [ent(name="man", named=False)]}], url)
    assert rec["blind"]["regions"][0]["name"] == "man" and client.calls[1]["messages"][1]["content"] == fi.BLIND_USER_TMPL


@pytest.mark.parametrize("replies, image, kind", [([], b"<html>", "image"), (["nope"], None, "invalid")])
def test_a_failure_is_data_not_a_crash(replies, image, kind):
    rec, client = detect(replies, image=image)
    assert (rec["ok"], rec["error_kind"], rec["frame_url"]) == (False, kind, URL)
    assert image is None or client.calls == []                   # an unreadable image is never sent


def test_the_template_readers_stamps_are_not_moved():
    # their versions re-read every template when they change
    assert (te.EXTRACTOR_VERSION, te.PROMPT_VERSION) == ("1.1.0", "2") and fi.SYSTEM_PROMPT != te.SYSTEM_PROMPT


class FakeLinker:
    stamps = {"linker_version": "1", "lexicon_version": "lex"}

    def words(self, text):
        return set(text.lower().split())

    def link_label(self, name, **kw):
        return {"qid": "Q83279", "label": name, "text": name, "score": 0.9, "method": kw.get("method")}, "linked"

    def link_spans(self, text, **kw):
        return []


def test_links_are_keyed_by_the_frame_by_the_template_rule():
    det = {"frame_url": URL, "regions": te.ground([ent()])[0]}
    rec = fi.link_detection(FakeLinker(), det, context_text="Spongegar", prefer=[83279], link_context_sha="c1")
    assert rec["frame_url"] == URL and "template_id" not in rec
    assert (rec["mentions"][0]["qid"], rec["mentions"][0]["in_graph"], rec["template_link_version"]) == \
        ("Q83279", True, te.TEMPLATE_LINK_VERSION)                # named: in the graph
