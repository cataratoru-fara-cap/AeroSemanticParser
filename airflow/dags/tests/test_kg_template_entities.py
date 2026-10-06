"""kg/template_entities.py and Linker.link_label: a fake vision client over the
synthetic lexicon of wikidata_fixture.py. Pinned: nothing unchecked is stored
(a box outside the image, a speck, a placeholder name, a text region without
its text: each dropped with its reason; boxes become fractions 0..1); output
off the schema is an invalid attempt; the blind audit counts context-only names
(Gabi's switch point); the graph gets named regions, printed text and the 3
largest generic ones; link_label never takes the KYM-slug shortcut, prefers
what the frame already links and refuses a weak lone alias."""
import json

import pytest

from helpers import KG_CONFIG
from modules.kg import entities as E
from modules.kg import template_entities as te
from vision_stub import VisionClient as FakeClient
from vision_stub import jpeg

SCHEMA, SHA = te.load_schema(str(KG_CONFIG / "template_entity_schema.json"))


def ent(name="Drake", kind="person", named=True, box=(100, 50, 600, 900), text=None, confidence="high"):
    return {"name": name, "kind": kind, "named": named, "box": list(box), "text": text, "confidence": confidence}


# -- grounding ------------------------------------------------------------------------

def test_boxes_become_fractions():
    kept, dropped = te.ground([ent()])
    assert kept[0]["box"] == [0.1, 0.05, 0.6, 0.9] and kept[0]["area"] == pytest.approx(0.425) and dropped == []


def test_what_is_dropped_and_why():
    _, dropped = te.ground([ent(box=(600, 50, 100, 900)),           # x0 > x1
                            ent(box=(0, 0, 1001, 10)),              # outside the grid
                            ent(box=(0, 0, 20, 20)),                # a speck
                            ent(name="Caption"), ent(name="sign", kind="text", text=" "), ent(kind="spaceship")])
    assert [d["reason"] for d in dropped] == ["not a box inside the image", "not a box inside the image",
                                              "box too small", "placeholder name", "text region without text",
                                              "unknown kind 'spaceship'"]


def test_duplicates_collapse_text_is_never_named_and_the_cap_holds():
    kept, dropped = te.ground([ent(), ent(box=(105, 55, 600, 900)),
                               ent(name="CHANGE MY MIND", kind="text", named=True, text="CHANGE  MY MIND")])
    assert len(kept) == 2 and dropped[0]["reason"] == "duplicate region"
    assert (kept[1]["text"], kept[1]["named"]) == ("CHANGE MY MIND", False)
    many = [ent(name=f"thing {i}", box=(0, i * 10, 500, i * 10 + 100)) for i in range(20)]
    assert len(te.ground(many)[0]) == te.MAX_REGIONS


def test_words_in_the_name_and_names_that_are_not_names():
    kept, dropped = te.ground([
        ent(name="Panik", kind="text", named=False, text=None, box=(0, 0, 500, 200)),
        ent(name="text", kind="text", text=None, box=(0, 300, 500, 500)),
        ent(name="text", kind="text", text="Text", box=(0, 500, 500, 600)),
        ent(name="door", kind="object", named=True, box=(600, 0, 900, 900)),
        ent(name="Meme Man", kind="person", named=True, box=(0, 600, 500, 1000))])
    assert [(r["name"], r.get("text"), r["named"]) for r in kept] == \
        [("Panik", "Panik", False), ("door", None, False), ("Meme Man", None, True)]
    assert [d["reason"] for d in dropped] == ["text region without text"] * 2


# -- the schema and the grammar --------------------------------------------------------

@pytest.mark.parametrize("bad", ["not json", json.dumps({"things": []}),
                                 json.dumps({"entities": [dict(ent(), box=[1, 2, 3])]}),
                                 json.dumps({"entities": [dict(ent(), extra=1)]})])
def test_output_off_the_schema_is_invalid(bad):
    with pytest.raises(ValueError):
        te.make_validator(SCHEMA)(bad)


def test_valid_output_and_the_grammar_caps_the_regions():
    assert te.make_validator(SCHEMA)(json.dumps({"entities": [ent()]}))["regions"][0]["name"] == "Drake"
    fmt = te.request_format(SCHEMA)
    assert fmt["properties"]["entities"]["maxItems"] == te.MAX_REGIONS
    assert "maxItems" not in SCHEMA["properties"]["entities"] and "description" not in fmt     # a copy


# -- the blind audit, text links, what reaches the graph -------------------------------------

def test_the_blind_audit():
    regions, _ = te.ground([ent(), ent(name="Kermit the Frog", kind="character", box=(600, 0, 1000, 500))])
    blind, _ = te.ground([ent(name="man", named=False)])
    assert te.blind_agreement(regions, blind) == {"named": 2, "confirmed": 1, "context_only": 1}
    regions, _ = te.ground([ent(name="George Washington", kind="artwork")])        # whatever its kind
    blind, _ = te.ground([ent(name="george washington", kind="person", box=(700, 700, 900, 900))])
    assert te.blind_agreement(regions, blind)["confirmed"] == 1
    assert te.in_blind_sample(1200) and not te.in_blind_sample(1210)               # deterministic


@pytest.mark.parametrize("text, label, method, printed, ok", [
    ("ANCIENT ALIENS", "Ancient Aliens", "title", None, True),
    ("NYPD", "New York City Police Department", "title", None, True),
    ("Halo 3", "Halo 3", "ner", "I played Halo 3", True),
    # the pilot's mistakes
    ("H", "hydrogen", "title", None, False), ("3", "3", "title", None, False),
    ("RIGHT", "right-wing", "title", None, False),                                # a common noun
    ("NEAT", "Near-Earth Asteroid Tracking", "ner", "ISN'T IT NEAT?!", False),    # capitals, NER
    ("Tue", "Tuesday", "propn", "Mon Tue", False),                               # too short
])
def test_text_links(text, label, method, printed, ok):
    assert te.text_link_ok({"text": text, "label": label, "method": method}, printed or text) is ok


def mention(qid, source, area):
    return {"qid": qid, "source": source, "region": {"index": 0, "area": area, "box": [0, 0, 1, 1]}}


def test_named_text_and_the_three_largest_generic_reach_the_graph_once():
    ms = [mention("Q1", "named", 0.1), mention("Q2", "text", 0.01)] + \
         [mention(f"Q{10 + i}", "generic", 0.05 * (i + 1)) for i in range(5)]
    te.mark_in_graph(ms)
    assert {m["qid"] for m in ms if m["in_graph"]} == {"Q1", "Q2", "Q14", "Q13", "Q12"}
    ms = [mention("Q1", "named", 0.1), mention("Q1", "generic", 0.9), mention("Q2", "generic", 0.5)]
    te.mark_in_graph(ms)
    assert [m["in_graph"] for m in ms] == [True, False, True]       # an item already in is not counted twice


# -- detection ---------------------------------------------------------------------------------

REQUEST = te.model_request(env={})


def unit(tid=121):
    return {"template_id": tid, "image_sha256": "abc", "image_source": "blank",
            "context": {"name": "Drake Hotline Bling", "alt_names": "(none)", "frame_title": "Drakeposting",
                        "about": "Drake says no."}}


def detect(client, image=None, tid=121):
    return te.detect(client, REQUEST, unit(tid), jpeg() if image is None else image, schema=SCHEMA, schema_sha=SHA)


def test_a_reading():
    client = FakeClient([json.dumps({"entities": [ent()]})])
    rec = detect(client)
    assert rec["ok"] and (rec["model"], rec["image_size"]) == ("qwen3-vl:32b", [768, 512]) and "blind" not in rec
    call = client.calls[0]
    assert call["think"] is False and "Drakeposting" in call["messages"][1]["content"]
    assert len(call["messages"][1]["images"]) == 1
    assert call["retry_temperature"] == te.RETRY_TEMPERATURE       # a rejected reading is retried warmer


def test_the_audit_sample_is_also_read_blind():
    client = FakeClient([json.dumps({"entities": [ent()]}), json.dumps({"entities": [ent(name="man", named=False)]})])
    assert detect(client, tid=1200)["blind"]["regions"][0]["name"] == "man"
    assert client.calls[1]["messages"][1]["content"] == te.BLIND_USER_TMPL


def test_an_unreadable_image_or_a_bad_reply_is_data_not_a_crash():
    client = FakeClient([])
    rec = detect(client, image=b"<html>not an image</html>")
    assert (rec["ok"], rec["error_kind"], rec["template_id"], rec["schema_sha"], rec["prompt_version"]) == \
        (False, "image", 121, SHA, te.PROMPT_VERSION)
    assert client.calls == []                                       # the model is not asked
    rec = detect(FakeClient(["nope"]))
    assert (rec["ok"], rec["error_kind"]) == (False, "invalid")


def test_the_model_request_needs_vision_and_never_falls_back():
    assert REQUEST.model == "qwen3-vl:32b" and "vision" in REQUEST.capabilities and not REQUEST.allow_fallback


# -- link_label ---------------------------------------------------------------------------------

def _model_available():
    try:
        E.load_nlp()
        return True
    except Exception:          # pragma: no cover - no spaCy model installed
        return False


needs_model = pytest.mark.skipif(not _model_available(), reason="spaCy model not installed")


@pytest.fixture(scope="module")
def linker(fixture_lexicon):
    return E.Linker(fixture_lexicon, E.load_nlp())


@needs_model
def test_link_label(linker):
    m, outcome = linker.link_label("Shiba Inu", field="image", context=set())
    assert (outcome, m["qid"], m["method"]) == ("linked", "Q39315", "label")
    m, _ = linker.link_label("doge", field="image", context=set(), prefer={15894956})   # what the frame links wins
    assert (m["qid"], m["method"]) == ("Q15894956", "frame_agree") and m["score"] >= E.FRAME_AGREE_SCORE
    m, _ = linker.link_label("Doge", field="image", context=set())   # link() would give the own item at 1.0
    assert (m["score"] if m else 0.0) < 1.0 and (m or {}).get("method") != "kym_id"
    assert linker.link_label("zzqx", field="image", context=set())[1] == "nil"
    assert linker.link_label("nimbus", field="image", context=set())[1] == "rejected"


@needs_model
def test_link_detection_end_to_end(linker):
    detection = {"template_id": 7, "regions": te.ground([
        ent(name="Shiba Inu", kind="animal", named=False, box=(0, 0, 900, 900)),
        ent(name="Kabosu", kind="animal", named=True, box=(0, 0, 500, 500)),
        ent(name="zzqx", kind="object", named=False, box=(0, 0, 300, 300))])[0]}
    rec = te.link_detection(linker, detection, context_text="Doge Shiba Inu dog", link_context_sha="ctx")
    qids = {m["qid"]: m for m in rec["mentions"]}
    assert set(qids) == {"Q39315", "Q9999903"} and all(m["in_graph"] for m in rec["mentions"])
    assert (rec["nil"][0]["name"], rec["link_context_sha"], qids["Q9999903"]["region"]["index"]) == ("zzqx", "ctx", 1)
