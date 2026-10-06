"""frame_image_store.py (mongomock; no network, no model). Pinned: memes are
read first and a frame without an image is not a unit; a reading is fresh only
under the same stamps AND the same image URL; a dead letter is skipped until
something about it changes; images are cached by their URL; the KG build sees
only in-graph mentions, frozen at its snapshot."""
from datetime import datetime, timedelta, timezone

import pytest

from helpers import mock_store
from modules import frame_image_store as st
from modules.kg import frame_images as fi
from modules.kg import template_entities as te

STAMPS = {"reader_version": fi.READER_VERSION, "prompt_version": fi.PROMPT_VERSION, "schema_sha": "s1",
          "requested_model": "qwen3-vl:32b"}
LINK_STAMPS = {"linker_version": "1.2.0", "lexicon_version": "lex", "nlp_model": "sm", "senses_version": "s1",
               "template_link_version": te.TEMPLATE_LINK_VERSION}
A, B, C = (f"https://knowyourmeme.com/memes/{p}" for p in ("a", "people/b", "c"))
IMG = "https://i.kym-cdn.com/entries/icons/original/000/001/a.jpg"


def detection(url=A, image=IMG, **over):
    return {"frame_url": url, "image_url": image, "ok": True, "regions": [], "model": "qwen3-vl:32b", **STAMPS, **over}


@pytest.fixture
def s():
    store = mock_store(st.FrameImageStore)
    store.entries.insert_many([
        {"_id": "e-b", "url": B, "og_image": IMG + "?b", "title": "B", "category": "person",
         "sections": [{"kind": "about", "text": ["B is a person."]}]},
        {"_id": "e-a", "url": A, "og_image": IMG, "title": "A", "category": "meme",
         "sections": [{"kind": "about", "text": ["A is a meme."]}]},
        {"_id": "e-c", "url": C, "og_image": "", "title": "C", "category": "meme"}])
    return store


def pending(store, stamps=STAMPS):
    return [u["frame_url"] for u in store.select_pending(store.units_for(), stamps)]


def test_units_memes_first_and_only_frames_with_an_image(s):
    units = s.units_for()
    assert [u["frame_url"] for u in units] == [A, B] and units[0]["context"]["about"] == "A is a meme."


def test_fresh_only_under_the_same_stamps_and_image(s):
    s.save_detection(detection())
    assert pending(s) == [B]
    assert B in pending(s, {**STAMPS, "prompt_version": "99"})               # a new prompt
    s.entries.update_one({"url": A}, {"$set": {"og_image": IMG + "?new"}})    # a new picture at the top of the page
    assert A in pending(s)


def test_a_dead_letter_waits_for_a_change_and_a_success_clears_it(s):
    s.save_failure(detection(url=B, image=IMG + "?b", ok=False, error="x"))
    assert B not in pending(s)
    s.save_detection(detection(url=B, image=IMG + "?b"))
    assert s.failures.count_documents({}) == 0


def test_not_detected_since(s):
    since = datetime.now(timezone.utc) - timedelta(minutes=1)
    s.save_detection(detection())
    assert [u["frame_url"] for u in s.not_detected_since(s.units_for(), since.isoformat())] == [B]


def test_the_build_sees_only_in_graph_mentions_at_its_snapshot(s):
    s.save_detection(detection())
    s.save_links([{"frame_url": A, "mention_count": 2, "in_graph_count": 1,
                   "mentions": [{"qid": "Q83279", "label": "SpongeBob", "in_graph": True,
                                 "region": {"kind": "character"}}, {"qid": "Q1", "label": "x", "in_graph": False}]}])
    got = s.graph_mentions_for([A, B])
    assert list(got) == [A] and [(m["qid"], m["model"]) for m in got[A]] == [("Q83279", "qwen3-vl:32b")]
    assert s.graph_mentions_for([A], linked_at_lte=datetime.now(timezone.utc) - timedelta(days=1)) == {}
    assert s.graph_stamps()["frame_images_in_graph"] == 1


def test_links_go_stale_with_the_linker_or_the_context(s):
    s.save_detection(detection())
    assert s.pending_linking(LINK_STAMPS) == [A]
    (unit,) = s.link_units_for([A])
    s.save_links([{"frame_url": A, "mention_count": 0, "in_graph_count": 0, "mentions": [], **LINK_STAMPS,
                   "detection_sha": fi.detection_sha(unit["detection"]), "link_context_sha": unit["link_context_sha"]}])
    assert s.pending_linking(LINK_STAMPS) == []
    assert s.pending_linking({**LINK_STAMPS, "lexicon_version": "new"}) == [A]
    # the entry's own item: keyed by its hash id, joined on frame_url; a new context
    s.entities.insert_one({"_id": "e-a", "frame_url": A, "self_qid": "Q42", "mentions": []})
    assert s.pending_linking(LINK_STAMPS) == [A]
    assert s.link_units_for([A])[0]["prefer"] == [42]


def test_images_are_cached_by_url_with_their_extension(tmp_path, monkeypatch):
    monkeypatch.setenv("KG_DATA_DIR", str(tmp_path))
    path = st.image_path(IMG)
    assert str(path).startswith(str(tmp_path)) and path.suffix == ".jpg" and st.image_path(IMG) == path
    assert st.read_image({"image_url": IMG}) is None
    st.write_image(IMG, b"jpeg")
    assert st.read_image({"image_url": IMG}) == b"jpeg"
