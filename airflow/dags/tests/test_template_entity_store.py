"""template_entity_store.py (mongomock; no network, no model). Pinned: a unit is
a KEPT template with its image, told about the frame that scores it highest;
two levels of staleness (a new prompt, schema, model or image re-reads; a new
detection or frame context only re-links); a failed template is not retried
under the same stamps, and a later success clears the dead letter; the frame's
own Wikidata links become the preferred senses."""
import pytest

from helpers import mock_store
from modules import template_entity_store as st
from modules.kg import template_entities as te

STAMPS = {"extractor_version": te.EXTRACTOR_VERSION, "prompt_version": te.PROMPT_VERSION, "schema_sha": "s1",
          "requested_model": "qwen3-vl:32b"}
LINK_STAMPS = {"linker_version": "1.2.0", "lexicon_version": "lex", "nlp_model": "sm", "senses_version": "s1",
               "template_link_version": te.TEMPLATE_LINK_VERSION}


@pytest.fixture
def s(tmp_path):
    img = tmp_path / "10.jpg"
    img.write_bytes(b"jpeg")
    store = mock_store(st.TemplateEntityStore)
    store.frames.insert_many([
        {"_id": "f1", "frame_url": "https://knowyourmeme.com/memes/a", "title": "Drakeposting", "status": "selected",
         "priority": 2, "selected": [{"template_id": 10, "R": 0.9}, {"template_id": 11, "R": 0.7}]},
        {"_id": "f2", "frame_url": "https://knowyourmeme.com/memes/b", "title": "Drake", "status": "selected",
         "priority": 1, "selected": [{"template_id": 10, "R": 0.6}]}])
    store.templates.insert_many([{"_id": 10, "name": "Drake Hotline Bling", "alt_names": ["drake yes no"],
                                  "blank_path": str(img), "blank_sha256": "img10"},
                                 {"_id": 11, "name": "No image yet"}])
    store.entries.insert_many([{"_id": "f1", "sections": [{"kind": "about", "text": ["Drake says no to one thing."]}]},
                               {"_id": "f2", "sections": []}])
    store.entities.insert_one({"_id": "f1", "self_qid": "Q33240", "mentions": [{"qid": "Q183"}, {"qid": "bad"}]})
    return store


def detection(**over):
    return {"template_id": 10, "ok": True, "image_sha256": "img10", **STAMPS, "model": "qwen3-vl:32b", "digest": "d",
            "regions": te.ground([{"name": "Drake", "kind": "person", "named": True, "box": [0, 0, 500, 500],
                                   "confidence": "high"}])[0], **over}


def test_units_are_kept_templates_with_an_image(s):
    [u] = s.units_for([10, 11])
    assert (u["template_id"], u["priority"], u["best_R"]) == (10, 1, 0.9)
    assert u["context"]["frame_title"] == "Drakeposting" and "Drake says no" in u["context"]["about"]   # highest R
    assert st.read_image(u) == b"jpeg"


def test_detection_staleness_and_dead_letters(s):
    units = s.units_for([10])
    assert len(s.select_pending(units, STAMPS)) == 1
    s.save_failure({"template_id": 10, "image_sha256": "img10", **STAMPS, "error": "503", "error_kind": "retryable"})
    assert s.select_pending(units, STAMPS) == []                         # not retried under the same stamps
    assert len(s.select_pending(units, {**STAMPS, "prompt_version": "next"})) == 1
    s.save_detection(detection())
    assert s.failures.count_documents({}) == 0 and s.select_pending(units, STAMPS) == []
    for key in STAMPS:
        assert len(s.select_pending(units, {**STAMPS, key: "x"})) == 1, key
    s.templates.update_one({"_id": 10}, {"$set": {"blank_sha256": "new-image"}})
    assert len(s.select_pending(s.units_for([10]), STAMPS)) == 1


def test_link_staleness_and_preferred_senses(s):
    s.save_detection(detection())
    assert s.pending_linking(LINK_STAMPS) == [10]
    unit = s.link_units_for([10])[0]
    assert unit["prefer"] == [183, 33240] and "Drakeposting" in unit["context_text"]
    s.save_links([{"template_id": 10, "mentions": [], "mention_count": 0, "in_graph_count": 0, "nil": [],
                   **LINK_STAMPS, "detection_sha": te.detection_sha(unit["detection"]),
                   "link_context_sha": unit["link_context_sha"]}])
    assert s.pending_linking(LINK_STAMPS) == []
    assert s.pending_linking({**LINK_STAMPS, "lexicon_version": "new"}) == [10]
    s.save_detection(detection(regions=[]))              # a new reading changes what the links were made from
    assert s.pending_linking(LINK_STAMPS) == [10]
