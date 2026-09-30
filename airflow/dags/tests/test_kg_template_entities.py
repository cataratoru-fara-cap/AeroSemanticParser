"""Tests for kg/template_entities.py and Linker.link_label (a fake vision
client, the synthetic lexicon of wikidata_fixture.py; no network).

What these pin:

  * **Nothing unchecked is stored**: a box outside the image, a speck, a
    placeholder name ("caption"), a text region without its text — each is
    dropped with its reason; the model's boxes become fractions 0..1.
  * **Output off the schema is an invalid attempt**, not a record.
  * **The blind audit counts context-only names** — Gabi's switch point.
  * **In the graph: named, printed text, and the 3 largest generic.**
  * **link_label never takes the KYM-slug shortcut**, prefers what the
    frame already links (``frame_agree``), and refuses a weak lone alias.
"""
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from modules.kg import entities as E
from modules.kg import template_entities as te
from modules.kg import wikidata as wd
from wikidata_fixture import write_dump

SCHEMA_PATH = str(Path(__file__).resolve().parents[1] / "kg_config"
                  / "template_entity_schema.json")


def ent(name="Drake", kind="person", named=True, box=(100, 50, 600, 900), text=None,
        confidence="high"):
    return {"name": name, "kind": kind, "named": named, "box": list(box), "text": text,
            "confidence": confidence}


def jpeg(size=(1200, 800)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, (200, 100, 50)).save(buf, "JPEG")
    return buf.getvalue()


class GroundTests(unittest.TestCase):
    def test_boxes_become_fractions(self):
        kept, dropped = te.ground([ent()])
        self.assertEqual(kept[0]["box"], [0.1, 0.05, 0.6, 0.9])
        self.assertAlmostEqual(kept[0]["area"], 0.425)
        self.assertEqual(dropped, [])

    def test_what_is_dropped_and_why(self):
        _kept, dropped = te.ground([
            ent(box=(600, 50, 100, 900)),                  # x0 > x1
            ent(box=(0, 0, 1001, 10)),                     # outside the grid
            ent(box=(0, 0, 20, 20)),                       # a speck
            ent(name="Caption"),                           # placeholder
            ent(name="sign", kind="text", text=" "),       # text without text
            ent(kind="spaceship"),                         # not a kind
        ])
        self.assertEqual([d["reason"] for d in dropped], [
            "not a box inside the image", "not a box inside the image", "box too small",
            "placeholder name", "text region without text", "unknown kind 'spaceship'"])

    def test_duplicates_collapse_and_text_is_never_named(self):
        kept, dropped = te.ground([ent(), ent(box=(105, 55, 600, 900)),
                                   ent(name="CHANGE MY MIND", kind="text", named=True,
                                       text="CHANGE  MY MIND")])
        self.assertEqual(len(kept), 2)
        self.assertEqual(dropped[0]["reason"], "duplicate region")
        self.assertEqual((kept[1]["text"], kept[1]["named"]), ("CHANGE MY MIND", False))

    def test_words_in_the_name_and_names_that_are_not_names(self):
        kept, dropped = te.ground([
            ent(name="Panik", kind="text", named=False, text=None, box=(0, 0, 500, 200)),
            ent(name="text", kind="text", text=None, box=(0, 300, 500, 500)),
            ent(name="text", kind="text", text="Text", box=(0, 500, 500, 600)),
            ent(name="door", kind="object", named=True, box=(600, 0, 900, 900)),
            ent(name="Meme Man", kind="person", named=True, box=(0, 600, 500, 1000))])
        self.assertEqual([(r["name"], r.get("text"), r["named"]) for r in kept],
                         [("Panik", "Panik", False), ("door", None, False),
                          ("Meme Man", None, True)])
        self.assertEqual([d["reason"] for d in dropped], ["text region without text"] * 2)

    def test_the_region_cap(self):
        many = [ent(name=f"thing {i}", box=(0, i * 10, 500, i * 10 + 100)) for i in range(20)]
        self.assertEqual(len(te.ground(many)[0]), te.MAX_REGIONS)


class ValidatorTests(unittest.TestCase):
    def setUp(self):
        schema, _sha = te.load_schema(SCHEMA_PATH)
        self.validate = te.make_validator(schema)

    def test_valid_output(self):
        got = self.validate(json.dumps({"entities": [ent()]}))
        self.assertEqual(got["regions"][0]["name"], "Drake")

    def test_off_schema_output_raises(self):
        for bad in ("not json", json.dumps({"things": []}),
                    json.dumps({"entities": [dict(ent(), box=[1, 2, 3])]}),
                    json.dumps({"entities": [dict(ent(), extra=1)]})):
            with self.assertRaises(ValueError, msg=bad):
                self.validate(bad)


class GrammarTests(unittest.TestCase):
    def test_the_grammar_caps_the_regions(self):
        schema, _ = te.load_schema(str(SCHEMA_PATH))
        fmt = te.request_format(schema)
        self.assertEqual(fmt["properties"]["entities"]["maxItems"], te.MAX_REGIONS)
        self.assertNotIn("maxItems", schema["properties"]["entities"])   # a copy
        self.assertNotIn("description", fmt)


class BlindTests(unittest.TestCase):
    def test_context_only_names_are_counted(self):
        regions, _ = te.ground([ent(), ent(name="Kermit the Frog", kind="character",
                                           box=(600, 0, 1000, 500))])
        blind, _ = te.ground([ent(name="man", named=False)])
        got = te.blind_agreement(regions, blind)
        self.assertEqual(got, {"named": 2, "confirmed": 1, "context_only": 1})

    def test_the_same_name_agrees_whatever_its_kind(self):
        regions, _ = te.ground([ent(name="George Washington", kind="artwork")])
        blind, _ = te.ground([ent(name="george washington", kind="person",
                                  box=(700, 700, 900, 900))])
        self.assertEqual(te.blind_agreement(regions, blind)["confirmed"], 1)

    def test_the_sample_is_deterministic(self):
        self.assertTrue(te.in_blind_sample(1200))
        self.assertFalse(te.in_blind_sample(1210))


class TextLinkTests(unittest.TestCase):
    """The pilot's wrong text links, and right ones, as regression cases."""

    def ok(self, text, label, method, printed=None):
        return te.text_link_ok({"text": text, "label": label, "method": method},
                               printed or text)

    def test_names_from_printed_text_are_kept(self):
        self.assertTrue(self.ok("ANCIENT ALIENS", "Ancient Aliens", "title"))
        self.assertTrue(self.ok("NYPD", "New York City Police Department", "title"))
        self.assertTrue(self.ok("Halo 3", "Halo 3", "ner", "I played Halo 3"))

    def test_the_pilots_mistakes_are_not(self):
        self.assertFalse(self.ok("H", "hydrogen", "title"))
        self.assertFalse(self.ok("3", "3", "title"))
        self.assertFalse(self.ok("RIGHT", "right-wing", "title"))            # a common noun
        self.assertFalse(self.ok("NEAT", "Near-Earth Asteroid Tracking", "ner",
                                 "ISN'T IT NEAT?!"))                        # capitals, NER
        self.assertFalse(self.ok("Tue", "Tuesday", "propn", "Mon Tue"))      # too short


class InGraphTests(unittest.TestCase):
    def m(self, qid, source, area):
        return {"qid": qid, "source": source,
                "region": {"index": 0, "area": area, "box": [0, 0, 1, 1]}}

    def test_named_text_and_three_largest_generic(self):
        ms = [self.m("Q1", "named", 0.1), self.m("Q2", "text", 0.01)] + \
             [self.m(f"Q{10 + i}", "generic", 0.05 * (i + 1)) for i in range(5)]
        te.mark_in_graph(ms)
        kept = {m["qid"] for m in ms if m["in_graph"]}
        self.assertEqual(kept, {"Q1", "Q2", "Q14", "Q13", "Q12"})

    def test_an_item_already_in_is_not_counted_twice(self):
        ms = [self.m("Q1", "named", 0.1), self.m("Q1", "generic", 0.9),
              self.m("Q2", "generic", 0.5)]
        te.mark_in_graph(ms)
        self.assertEqual([m["in_graph"] for m in ms], [True, False, True])


class FakeClient:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    def chat(self, messages, request, *, purpose=None, format=None, options=None,
             think=None, validate=None, retry_temperature=None):
        self.calls.append({"messages": messages, "think": think, "format": format,
                           "retry_temperature": retry_temperature})
        content = self.replies.pop(0)
        try:
            parsed = validate(content)
        except ValueError as exc:
            return SimpleNamespace(ok=False, error_kind="invalid", error=str(exc), attempts=3)
        return SimpleNamespace(ok=True, parsed=parsed, model="qwen3-vl:32b",
                               digest="ff2e46876908", host="ollama-ccdd", attempts=1)


class DetectTests(unittest.TestCase):
    def setUp(self):
        self.schema, self.sha = te.load_schema(SCHEMA_PATH)
        self.request = te.model_request(env={})

    def unit(self, tid=121):
        return {"template_id": tid, "image_sha256": "abc", "image_source": "blank",
                "context": {"name": "Drake Hotline Bling", "alt_names": "(none)",
                            "frame_title": "Drakeposting", "about": "Drake says no."}}

    def test_a_reading(self):
        client = FakeClient([json.dumps({"entities": [ent()]})])
        rec = te.detect(client, self.request, self.unit(), jpeg(), schema=self.schema,
                        schema_sha=self.sha)
        self.assertTrue(rec["ok"])
        self.assertEqual((rec["model"], rec["image_size"]), ("qwen3-vl:32b", [768, 512]))
        call = client.calls[0]
        self.assertIs(call["think"], False)
        self.assertIn("Drakeposting", call["messages"][1]["content"])
        self.assertEqual(len(call["messages"][1]["images"]), 1)
        self.assertNotIn("blind", rec)

    def test_the_audit_sample_is_also_read_blind(self):
        client = FakeClient([json.dumps({"entities": [ent()]}),
                             json.dumps({"entities": [ent(name="man", named=False)]})])
        rec = te.detect(client, self.request, self.unit(tid=1200), jpeg(),
                        schema=self.schema, schema_sha=self.sha)
        self.assertEqual(rec["blind"]["regions"][0]["name"], "man")
        self.assertEqual(client.calls[1]["messages"][1]["content"], te.BLIND_USER_TMPL)

    def test_an_unreadable_image_is_a_failed_template_not_a_crash(self):
        client = FakeClient([])
        rec = te.detect(client, self.request, self.unit(), b"<html>not an image</html>",
                        schema=self.schema, schema_sha=self.sha)
        self.assertEqual((rec["ok"], rec["error_kind"]), (False, "image"))
        self.assertEqual((rec["template_id"], rec["schema_sha"], rec["prompt_version"]),
                         (121, self.sha, te.PROMPT_VERSION))
        self.assertEqual(client.calls, [])                       # the model is not asked

    def test_a_failure_is_data(self):
        rec = te.detect(FakeClient(["nope"]), self.request, self.unit(), jpeg(),
                        schema=self.schema, schema_sha=self.sha)
        self.assertEqual((rec["ok"], rec["error_kind"]), (False, "invalid"))

    def test_the_model_request_needs_vision(self):
        self.assertEqual(self.request.model, "qwen3-vl:32b")
        self.assertIn("vision", self.request.capabilities)
        self.assertFalse(self.request.allow_fallback)     # never another vision model

    def test_a_rejected_reading_is_retried_warmer(self):
        client = FakeClient([json.dumps({"entities": [ent()]})])
        te.detect(client, self.request, self.unit(), jpeg(), schema=self.schema,
                  schema_sha=self.sha)
        self.assertEqual(client.calls[0]["retry_temperature"], te.RETRY_TEMPERATURE)


def _model_available() -> bool:
    try:
        E.load_nlp()
        return True
    except Exception:          # pragma: no cover - no spaCy model installed
        return False


@unittest.skipUnless(_model_available(), "spaCy model not installed")
class LinkLabelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        dump = write_dump(os.path.join(cls._tmp.name, "dump.json.gz"))
        path = os.path.join(cls._tmp.name, "lexicon.sqlite")
        wd.build_lexicon(dump, path, workers=0, progress=lambda _l: None)
        cls.lexicon = wd.Lexicon(path)
        cls.linker = E.Linker(cls.lexicon, E.load_nlp())

    @classmethod
    def tearDownClass(cls):
        cls.lexicon.close()
        cls._tmp.cleanup()

    def test_a_name_links_whole(self):
        m, outcome = self.linker.link_label("Shiba Inu", field="image", context=set())
        self.assertEqual((outcome, m["qid"], m["method"]), ("linked", "Q39315", "label"))

    def test_what_the_frame_links_wins(self):
        m, _ = self.linker.link_label("doge", field="image", context=set(),
                                      prefer={15894956})
        self.assertEqual((m["qid"], m["method"]), ("Q15894956", "frame_agree"))
        self.assertGreaterEqual(m["score"], E.FRAME_AGREE_SCORE)

    def test_no_kym_slug_shortcut(self):
        # link() would give the frame's own item at 1.0; a label never does.
        m, _ = self.linker.link_label("Doge", field="image", context=set())
        self.assertLess(m["score"] if m else 0.0, 1.0)
        self.assertNotEqual((m or {}).get("method"), "kym_id")

    def test_nil_and_rejected(self):
        self.assertEqual(self.linker.link_label("zzqx", field="image", context=set())[1], "nil")
        self.assertEqual(self.linker.link_label("nimbus", field="image", context=set())[1],
                         "rejected")

    def test_link_detection_end_to_end(self):
        detection = {"template_id": 7, "regions": te.ground([
            ent(name="Shiba Inu", kind="animal", named=False, box=(0, 0, 900, 900)),
            ent(name="Kabosu", kind="animal", named=True, box=(0, 0, 500, 500)),
            ent(name="zzqx", kind="object", named=False, box=(0, 0, 300, 300))])[0]}
        rec = te.link_detection(self.linker, detection, context_text="Doge Shiba Inu dog",
                                link_context_sha="ctx")
        qids = {m["qid"]: m for m in rec["mentions"]}
        self.assertEqual(set(qids), {"Q39315", "Q9999903"})
        self.assertTrue(all(m["in_graph"] for m in rec["mentions"]))
        self.assertEqual(rec["nil"][0]["name"], "zzqx")
        self.assertEqual(rec["link_context_sha"], "ctx")
        self.assertEqual(qids["Q9999903"]["region"]["index"], 1)


if __name__ == "__main__":
    unittest.main()
