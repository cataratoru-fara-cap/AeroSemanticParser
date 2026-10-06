"""kg/entities.py: title, tags and About -> Wikidata entities, with the real
spaCy model (en_core_web_sm) over a lexicon built from wikidata_fixture's
synthetic dump; tests needing the model skip, loudly, without it.

Pinned: every mention is the page's own words and audit() catches each way a
record can lie; the item whose KYM slug is this page wins (over the
more-linked Venetian doge); context separates senses ("Mercury" planet vs
metal); a lone exact match is believed, a close race with nothing to break it
is not; an NER label only ever helps (en_core_web_sm calls Reddit a GPE); a
unit's About is exactly kg/build.py's m4s:about.
"""
import pytest

from helpers import KG_CONFIG
from modules.kg import build
from modules.kg import entities as E
from modules.mongo_base import url_doc_id

URL = "https://knowyourmeme.com/memes/doge"
ABOUT = ("Doge is a slang term for dog that is primarily associated with pictures of Shiba Inus, a breed of dog "
         "from Japan. It spread on Reddit and 4chan, where Zorblax Quentin posted a nimbus of hair.")
SENSES = str(KG_CONFIG / "entity_senses.yaml")


def entry(url=URL, title="Doge", tags=("doge", "shiba inu", "dogs", "eddie_now"), about=(ABOUT,), **over):
    return {"url": url, "title": title, "tags": list(tags), "parser_version": "1.6.1",
            "sections": [{"kind": "about", "heading": "About", "text": list(about)},
                         {"kind": "origin", "heading": "Origin", "text": ["Not linked."]}], **over}


def _model_available():
    try:
        E.load_nlp()
        return True
    except Exception:                # spaCy or the model missing
        return False


needs_model = pytest.mark.skipif(not _model_available(), reason="spaCy / en_core_web_sm not installed")
UNIT = E.frame_unit(entry())


# -- the unit (plain data, no model) ----------------------------------------------------

def test_the_unit():
    assert UNIT["unit_id"] == url_doc_id(URL) == E.frame_key(URL)
    assert "Not linked" not in UNIT["about"]                         # only title, tags and About
    doc = entry(about=("First paragraph.", "", "Second one."))
    frame = next(n for n in build.build_nodes_and_edges(doc)[0] if n["kind"] == "frame")
    assert E.frame_unit(doc)["about"] == frame["about"]
    assert E.frame_unit(entry(tags=[" doge ", "shiba", "doge", "", None]))["tags"] == ["doge", "shiba"]
    assert E.frame_unit({"title": "x"}) is None


@pytest.mark.parametrize("over, moves", [
    ({"title": "Doge 2"}, True), ({"tags": ["doge"]}, True),
    ({"sections": [{"kind": "about", "text": ["Other."]}]}, True),
    ({"sections": [{"kind": "about", "heading": "About", "text": [ABOUT]},
                   {"kind": "origin", "heading": "Origin", "text": ["Edited origin."]}]}, False),  # never read
])
def test_any_source_edit_moves_the_staleness_hash(over, moves):
    assert (E.frame_unit(entry(**over))["source_sha256"] != UNIT["source_sha256"]) is moves


# -- the linker ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def lexicon(fixture_lexicon):
    return fixture_lexicon


@pytest.fixture(scope="module")
def linker(lexicon):
    return E.Linker(lexicon, E.load_nlp())


@pytest.fixture(scope="module")
def record(linker):
    return linker.link(UNIT)


def by(record, field):
    return [(m["text"], m["qid"]) for m in record["mentions"] if m["field"] == field]


@needs_model
def test_the_title_is_the_item_whose_kym_slug_is_this_page(record):
    [m] = [m for m in record["mentions"] if m["field"] == "title"]
    assert (m["qid"], m["score"], m["method"], record["self_qid"]) == ("Q15894956", 1.0, "kym_id", "Q15894956")
    # ...and wins over Q219, the Venetian doge, with twice the Wikipedias
    assert ("doge", "Q15894956") in by(record, "tag") and ("Doge", "Q15894956") in by(record, "about")


@needs_model
def test_without_a_self_item_the_title_is_linked_like_any_text(linker):
    r = linker.link(E.frame_unit(entry(url="https://knowyourmeme.com/memes/japan-stuff", title="Japan", tags=[],
                                       about=["Japan."])))
    assert r["self_qid"] is None and by(r, "title") == [("Japan", "Q17")]


@needs_model
def test_named_entities_concepts_and_tags_are_linked(record):
    about = dict(by(record, "about"))
    assert {t: about.get(t) for t in ("Shiba Inus", "Japan", "Reddit", "4chan", "dog", "hair")} == {
        "Shiba Inus": "Q39315", "Japan": "Q17", "Reddit": "Q1136", "4chan": "Q531", "dog": "Q144", "hair": "Q28472"}
    assert dict(by(record, "tag")) == {"doge": "Q15894956", "shiba inu": "Q39315", "dogs": "Q144"}   # not eddie_now
    assert "Zorblax Quentin" in [n["text"] for n in record["nil"]]      # a named entity with no item
    assert "nimbus" not in about and record["rejected_count"] >= 1      # a weak alias match is rejected


@needs_model
def test_mentions_claim_their_characters_and_are_the_pages_words(record):
    spans = [(m["start"], m["end"]) for m in record["mentions"] if m["field"] == "about"]
    for i, (a, b) in enumerate(spans):
        assert all(b <= c or d <= a for c, d in spans[i + 1:])
    assert "Shiba" not in dict(by(record, "about"))
    for m in record["mentions"]:
        assert E.field_text(UNIT, m["field"], m.get("tag_index"))[m["start"]:m["end"]] == m["text"]


@needs_model
def test_context_separates_two_senses_and_a_close_race_links_nothing(linker):
    def link(**kw):
        return linker.link(E.frame_unit(entry(tags=[], **kw)))
    planet = link(url="https://knowyourmeme.com/memes/a", title="A",
                  about=["Mercury is the smallest planet in the Solar System."])
    metal = link(url="https://knowyourmeme.com/memes/b", title="B",
                 about=["The thermometer held mercury, a toxic liquid metal."])
    assert (dict(by(planet, "about"))["Mercury"], dict(by(metal, "about"))["mercury"]) == ("Q308", "Q925")
    race = linker.link(E.frame_unit(entry(url="https://knowyourmeme.com/memes/c", title="C", tags=["doge"],
                                          about=["An unrelated page."])))
    assert by(race, "tag") == []


@needs_model
def test_an_ner_label_only_ever_helps(linker, lexicon):
    def top(text, key, label):
        return linker.score(text, lexicon.candidates(key), ner_label=label, context=set(), self_qid=None)[0]
    assert top("Reddit", "reddit", "GPE")[0] == top("Reddit", "reddit", None)[0]     # disagreeing costs nothing
    agree = top("Japan", "japan", "GPE")
    assert agree[2]["type"] == 1.0 and agree[0] > top("Japan", "japan", None)[0]     # Q6256 -> Q56061


@needs_model
def test_the_record_is_stamped_deterministic_auditable_and_keeps_features(linker, lexicon, record):
    assert (record["linker_version"], record["lexicon_version"], record["nlp_model"]) == \
        (E.LINKER_VERSION, lexicon.version, E.model_stamp())
    again = linker.link(UNIT)
    assert (again["mentions"], again["nil"]) == (record["mentions"], record["nil"])
    for m in record["mentions"]:                                  # kept for curation
        assert set(m["features"]) == {"prior", "context", "exact", "type", "kym", "clarity"}
        assert "candidates" in m and "margin" in m
    assert E.audit(record, UNIT) == []


@needs_model
def test_link_units_batches_counts_and_refuses_a_lying_record(linker, record, monkeypatch):
    units = [UNIT, E.frame_unit(entry(url="https://knowyourmeme.com/memes/e", title="E", tags=[], about=[]))]
    seen = []
    summary = E.link_units(linker, units, on_record=seen.append, batch_size=1)
    assert (len(seen), summary["units"], summary["frames_with_links"], summary["self_links"]) == (2, 2, 1, 1)
    assert summary["mentions"] == record["mention_count"] and seen[0]["mentions"] == record["mentions"]
    monkeypatch.setattr(linker, "link", lambda unit, **_kw: dict(record, mention_count=999))
    seen = []
    with pytest.raises(AssertionError, match="failed its audit"):
        E.link_units(linker, [UNIT], on_record=seen.append)
    assert seen == []


# -- the audit: each way a record can lie (no model) ------------------------------------------

def audit(record_over=None, **mention):
    m = {"field": "about", "text": "Japan", "start": ABOUT.index("Japan"), "end": ABOUT.index("Japan") + 5,
         "qid": "Q17", "score": 0.66, "method": "ner", **mention}
    rec = {"mentions": [m], "mention_count": 1, "entity_count": 1, "source_sha256": UNIT["source_sha256"],
           "linker_version": "1", "lexicon_version": "v", "nlp_model": "m", "senses_version": "none",
           **(record_over or {})}
    return E.audit(rec, UNIT)


@pytest.mark.parametrize("record_over, mention", [
    (None, {"text": "JAPAN"}), (None, {"start": 5000, "end": 5005}),
    (None, {"field": "tag", "text": "doge", "start": 0, "end": 4}),          # a tag mention needs its index
    (None, {"qid": "Japan"}), (None, {"score": 0.1}), (None, {"score": 1.5}),
    ({"mention_count": 2}, {}), ({"source_sha256": "x"}, {}), ({"nlp_model": None}, {}),
    (None, {"field": "spread"}), (None, {"method": "guess"}),
])
def test_a_lying_record_fails_its_audit(record_over, mention):
    assert audit(record_over, **mention)


def test_a_faithful_record_is_clean_and_overlaps_are_caught():
    assert audit() == []
    assert audit(field="tag", text="doge", start=0, end=4, tag_index=0) == []
    m = {"field": "about", "text": "Japan", "start": ABOUT.index("Japan"), "end": ABOUT.index("Japan") + 5,
         "qid": "Q17", "score": 0.66, "method": "ner"}
    problems = E.audit({"mentions": [m, dict(m)], "mention_count": 2, "entity_count": 1,
                        "source_sha256": UNIT["source_sha256"], "linker_version": "1", "lexicon_version": "v",
                        "nlp_model": "m", "senses_version": "none"}, UNIT)
    assert any("overlaps" in p for p in problems)


# -- the sense list ---------------------------------------------------------------------------

def test_the_shipped_sense_file(tmp_path):
    senses = E.load_senses(SENSES)
    assert senses.version != "none"
    assert senses.by_key["series"].instead == 7725310 and 170198 in senses.by_key["series"].never
    assert senses.by_key["games"] is senses.by_key["game"]                 # `also`
    assert not senses.by_key["sound"].link
    assert E.load_senses(None) is E.NO_SENSES and E.load_senses("/nonexistent.yaml") is E.NO_SENSES
    bad = tmp_path / "bad.yaml"
    bad.write_text("senses:\n  game:\n    never: [game]\n")
    with pytest.raises(ValueError):                                         # a malformed QID
        E.load_senses(str(bad))


@pytest.mark.parametrize("sense, text, span, blocked", [
    (E.Sense(unless_next=("of",)), "a series of videos. The series ended.", (2, 8), True),
    (E.Sense(unless_next=("of",)), "a series of videos. The series ended.", (24, 30), False),
    *[(E.Sense(only_if_prev=("on", "twitter /"), only_if_next=("(formerly",)), t, s, b)
      for t, s, b in [("viral on X today", (9, 10), False), ("Twitter / X", (10, 11), False),
                      ("X (formerly Twitter)", (0, 1), False), ("I Hate X", (7, 8), True),
                      ("Thanks for Not Saying X", (22, 23), True)]],
])
def test_sense_guards(sense, text, span, blocked):
    assert E.sense_blocks(sense, text, *span) is blocked


@pytest.fixture(scope="module")
def sensed(lexicon):
    linker = E.Linker(lexicon, E.load_nlp(), senses=E.load_senses(SENSES))

    def link(about="", title="", tags=()):
        unit = E.frame_unit({"url": "https://knowyourmeme.com/memes/zz-sense-test", "title": title,
                             "tags": list(tags), "sections": [{"kind": "about", "text": [about]}]})
        return {(m["field"], m["text"]): m["qid"] for m in linker.link(unit)["mentions"]}
    link.linker = linker
    return link


@needs_model
def test_linking_with_the_sense_list(sensed):
    assert sensed.linker.stamps["senses_version"] == E.load_senses(SENSES).version
    got = sensed("It is a series of videos. The series is popular.")
    assert list(got.values()).count("Q7725310") == 1 and "Q170198" not in got.values()   # "a series of": nothing
    assert sensed("The game was released in the spring.")[("about", "game")] == "Q7889"
    assert sensed(tags=["games"])[("tag", "games")] == "Q7889"
    got = sensed("Graphics in video games improved.")      # a rejected literal key no longer hides the lemma
    assert got.get(("about", "video games")) == "Q7889" and ("about", "games") not in got
    got = sensed("The post went viral on X, formerly Twitter.", title="I Hate X")
    assert got.get(("about", "X")) == "Q918" and ("title", "X") not in got        # X only as the platform


@needs_model
def test_without_senses_the_old_behaviour(lexicon):
    plain = E.Linker(lexicon, E.load_nlp())
    unit = E.frame_unit({"url": "https://knowyourmeme.com/memes/zz-sense-test", "title": "", "tags": [],
                         "sections": [{"kind": "about", "text": ["The series is long."]}]})
    assert "Q170198" in {m["qid"] for m in plain.link(unit)["mentions"]} and plain.stamps["senses_version"] == "none"
