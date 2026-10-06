"""entity_store.py (mongomock). Pinned beyond "it works": a re-link REPLACES a
frame's mentions, never merges (two lexicons are two readings); every staleness
stamp re-queues on its own (a hole in one means a new dump is built and nothing
is re-linked against it); links_for keys by frame_url and hands the KG build
only what it uses (features, offsets and margins stay here, for curation); the
snapshot is honoured in links_for and linking_stamps."""
from datetime import datetime, timezone

import pytest

from helpers import mock_store
from modules import entity_store as st
from modules.kg import entities as kg_entities

URL = "https://knowyourmeme.com/memes/doge"
OTHER = "https://knowyourmeme.com/memes/cheems"
STAMPS = {"linker_version": "1.0.0", "lexicon_version": "lex1", "nlp_model": "en_core_web_sm@3.8.0"}


def utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


def entry_doc(url=URL, about="Doge is a Shiba Inu meme.", **over):
    return {"_id": kg_entities.frame_key(url), "url": url, "title": "Doge", "tags": ["doge", "shiba inu"],
            "parser_version": "1.6.1", "corpus_status": "ready",
            "sections": [{"kind": "about", "heading": "About", "text": [about]}], **over}


def mention(text="Shiba Inu", qid="Q39315", field="about", start=10):
    return {"field": field, "text": text, "start": start, "end": start + len(text), "qid": qid, "label": text,
            "description": "dog breed", "score": 0.7, "method": "ner", "ner_label": "ORG", "proper": True,
            "matched": text, "candidates": 1, "margin": 0.7,
            "features": {"prior": 0.8, "context": 0.3, "exact": 1.0, "type": 0.0, "kym": 0.0, "clarity": 1.0}}


def record(unit, mentions=(), stamps=STAMPS, linked_at="2026-09-22T10:00:00Z"):
    ms = list(mentions)
    return {"unit_id": unit["unit_id"], "entry_id": unit["entry_id"], "frame_url": unit["frame_url"], "mentions": ms,
            "mention_count": len(ms), "entity_count": len({m["qid"] for m in ms}), "nil": [], "rejected_count": 0,
            "self_qid": None, "source_sha256": unit["source_sha256"], "source_chars": 10, "parser_version": "1.6.1",
            **stamps, "linked_at": linked_at, "elapsed_s": 0.01}


# -- selection --------------------------------------------------------------------------------

@pytest.fixture
def s():
    store = mock_store(st.EntityStore)
    store.entries.insert_many([entry_doc(), entry_doc(OTHER, corpus_status="incomplete")])
    return store


def urls(units):
    return [u["frame_url"] for u in units]


def test_units_and_their_filters(s):
    assert set(urls(s.iter_frame_units())) == {URL, OTHER}
    assert urls(s.iter_frame_units(ready_only=True)) == [URL] and len(list(s.iter_frame_units(limit=1))) == 1
    ids = [kg_entities.frame_key(OTHER), kg_entities.frame_key(URL), "missing"]
    assert urls(s.units_for(ids)) == [OTHER, URL]                       # rebuilt in the order asked


def test_staleness(s):
    units = list(s.iter_frame_units())
    assert len(s.select_pending(units, stamps=STAMPS)) == 2           # never linked
    s.save_links([record(u) for u in units])
    assert s.select_pending(units, stamps=STAMPS) == []
    for key in STAMPS:                                                  # each stamp re-queues on its own
        assert len(s.select_pending(units, stamps={**STAMPS, key: "something-else"})) == 2, key
    assert len(s.select_pending(units, stamps=STAMPS, force=True)) == 2
    s.entries.update_one({"url": URL}, {"$set": {"sections": [{"kind": "about", "text": ["Doge, edited."]}]}})
    assert urls(s.select_pending(list(s.iter_frame_units()), stamps=STAMPS)) == [URL]   # that frame only


def test_a_forced_retry_skips_what_it_already_relinked(s):
    units = list(s.iter_frame_units())
    s.save_links([record(units[0], linked_at="2026-09-22T13:00:00Z"), record(units[1], linked_at="2026-09-22T11:00:00Z")])
    assert [u["unit_id"] for u in s.not_linked_since(units, utc(2026, 9, 22, 12).isoformat())] == [units[1]["unit_id"]]


# -- writing ------------------------------------------------------------------------------------

def test_a_relink_replaces_and_the_stored_shape(s):
    unit = next(u for u in s.iter_frame_units() if u["frame_url"] == URL)
    s.save_links([record(unit, [mention(), mention("Doge", "Q15894956", start=0)])])
    s.save_links([record(unit, [mention("Doge", "Q15894956", start=0)])])
    doc = s.entities.find_one({"_id": unit["unit_id"]})
    assert ([m["qid"] for m in doc["mentions"]], doc["mention_count"]) == (["Q15894956"], 1)
    # a string would be invisible to every $lte snapshot filter
    assert isinstance(doc["linked_at"], datetime) and "first_linked_at" in doc
    assert doc["_id"] == kg_entities.frame_key(URL) and "unit_id" not in doc   # the key, not a field


# -- reading ------------------------------------------------------------------------------------

@pytest.fixture
def linked():
    store = mock_store(st.EntityStore)
    store.entries.insert_many([entry_doc(), entry_doc(OTHER)])
    units = {u["frame_url"]: u for u in store.iter_frame_units()}
    store.save_links([record(units[URL], [mention(), mention("Doge", "Q15894956", field="title", start=0)]),
                      record(units[OTHER], [], linked_at="2026-09-22T12:00:00Z")])
    return store


IDS = [kg_entities.frame_key(URL), kg_entities.frame_key(OTHER)]


def test_links_for(linked):
    out = linked.links_for(IDS)
    assert list(out) == [URL] and [m["qid"] for m in out[URL]] == ["Q39315", "Q15894956"]   # frames with none skipped
    # only what the build uses; tag_index exists on tag mentions only, this one is from the About
    assert set(out[URL][0]) == set(st.LINK_PROJECTION_FIELDS) - {"tag_index"} and "features" not in out[URL][0]
    assert linked.links_for(IDS, linked_at_lte=utc(2026, 9, 22, 9)) == {}
    assert list(linked.links_for(IDS, linked_at_lte=utc(2026, 9, 22, 11))) == [URL]


def test_linking_stamps_summarise_the_frozen_generation(linked):
    stamps = linked.linking_stamps()
    assert (stamps["entities_frames"], stamps["entities_mentions"], stamps["entities_lexicon_versions"],
            stamps["entities_nlp_models"]) == (2, 2, ["lex1"], ["en_core_web_sm@3.8.0"])
    early = linked.linking_stamps(linked_at_lte=utc(2026, 9, 22, 11))
    assert early["entities_frames"] == 1 and early["entities_max_linked_at"] < stamps["entities_max_linked_at"]
    empty = mock_store(st.EntityStore).linking_stamps()
    assert (empty["entities_frames"], empty["entities_max_linked_at"]) == (0, None)


def test_stats(linked):
    got = linked.stats()
    assert {k: got[k] for k in ("frames_linked", "frames_with_links", "mentions_total", "distinct_entities")} == \
        {"frames_linked": 2, "frames_with_links": 1, "mentions_total": 2, "distinct_entities": 2}
    assert (got["mentions_by_field"], got["lexicon_versions_in_use"]) == ({"about": 1, "title": 1}, {"lex1": 2})
