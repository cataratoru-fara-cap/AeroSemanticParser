"""entity_curation_store.py and kg_store's curated read (mongomock). Pinned:
two levels of staleness (a re-link or a rule stamp re-runs the rules; the judge
is re-asked only for items it has not answered under the current prompt,
schema and model); verdicts are per item and survive a rule re-run; a failed
frame is a dead letter under the same stamps and pending items, a success
clears it; the KG reads only kept mentions with their basis, an uncurated or
stale frame keeps its title links and own item only, and a curation after the
snapshot is not seen."""
from datetime import datetime, timezone

import pytest

from helpers import mock_store, mock_stores, serving
from modules import entity_curation_store as st
from modules import entity_store as es
from modules import kg_store
from modules.kg import curation as kc

URL = "https://knowyourmeme.com/memes/doge"
RULES = {"curation_version": kc.CURATION_VERSION, "curation_lists_version": "l1", "lexicon_version": "lex"}
JUDGE = {"judge_prompt_version": kc.JUDGE_PROMPT_VERSION, "judge_schema_sha": "s1", "judge_model": "m",
         "judge_confirm_model": "c"}
LISTS = kc.Lists(platform_classes={}, platform_items={100: "Reddit"}, deny_classes={}, deny_items={400: "popularity"},
                 version="l1")


class NoClasses:
    def closure(self, q):
        return frozenset()

    def direct(self, q):
        return frozenset()


def m(field, qid, start, end, *, tag_index=None, method="noun_chunk", text="w"):
    out = {"field": field, "qid": qid, "start": start, "end": end, "method": method, "text": text, "label": qid,
           "description": "d", "score": 0.6}
    return out if tag_index is None else {**out, "tag_index": tag_index}


MENTIONS = [m("title", "Q1", 0, 4, method="title", text="Doge"), m("tag", "Q2", 0, 9, tag_index=0, text="shiba inu"),
            m("about", "Q100", 0, 6, text="Reddit"), m("about", "Q400", 10, 20, text="popularity"),
            m("about", "Q3", 25, 28, text="dog"), m("about", "Q4", 30, 33, text="mug")]
LINKED = datetime(2026, 9, 1, tzinfo=timezone.utc)


@pytest.fixture
def s():
    store = mock_store(st.EntityCurationStore)
    store.entities.insert_one({"_id": "f1", "frame_url": URL, "mention_count": 6,
                               "mentions": [dict(x) for x in MENTIONS], "linked_at": LINKED})
    store.entries.insert_one({"_id": "f1", "title": "Doge", "tags": ["shiba inu"],
                              "sections": [{"kind": "about", "text": ["Reddit had popularity, a dog and a mug."]}]})
    return store


def run_rules(store, stamps=RULES):
    results = [(r, kc.apply_rules(r, LISTS, NoClasses())) for r in store.records_for(store.pending_rules(stamps))]
    return store.save_rules(results, stamps)


def doc(store):
    return store.curation.find_one({"_id": "f1"})


def test_rules_then_staleness(s):
    assert s.pending_rules(RULES) == ["f1"]
    assert run_rules(s) == {"frames": 1, "pending_items": 3}
    assert (doc(s)["pending_qids"], doc(s)["in_graph_count"]) == (["Q2", "Q3", "Q4"], 2)    # title + platform
    assert s.pending_rules(RULES) == []
    assert s.pending_rules({**RULES, "curation_lists_version": "l2"}) == ["f1"] == s.pending_rules(RULES, force=True)
    s.entities.update_one({"_id": "f1"}, {"$set": {"mentions.4.end": 29}})    # a re-link changing any mention
    assert s.pending_rules(RULES) == ["f1"]


def items(store, stamps=JUDGE):
    return [it["qid"] for it in store.judge_units_for(["f1"], stamps)[0]["items"]]


def test_the_judge_per_item_and_the_merge(s):
    run_rules(s)
    assert s.pending_judge(JUDGE) == ["f1"]
    unit = s.judge_units_for(["f1"], JUDGE)[0]
    assert not unit["stamps_current"] and items(s) == ["Q2", "Q3", "Q4"]
    assert "[[dog]]" in unit["items"][1]["snippet"] and unit["context"]["title"] == "Doge"
    # it answers two of three (a batch failed after the first)
    s.save_judge("f1", {"verdicts": {"Q2": True, "Q3": True}, "model": "m", "digest": "d", "host": "h"}, JUDGE, merge=False)
    assert (doc(s)["pending_qids"], doc(s)["in_graph_count"]) == (["Q4"], 4)
    assert s.judge_units_for(["f1"], JUDGE)[0]["stamps_current"] and items(s) == ["Q4"]
    s.save_judge("f1", {"verdicts": {"Q4": False}, "roles": {"Q4": "incidental"}}, JUDGE, merge=True)
    d = doc(s)
    assert d["judge"]["verdicts"] == {"Q2": True, "Q3": True, "Q4": False} and d["judge"]["roles"] == {"Q4": "incidental"}
    assert d["pending_qids"] == [] and s.pending_judge(JUDGE) == []
    run_rules(s, {**RULES, "curation_lists_version": "l2"})       # a rule re-run reuses the verdicts
    assert doc(s)["pending_qids"] == [] and s.pending_judge(JUDGE) == []
    new = {**JUDGE, "judge_prompt_version": "99"}                  # a new prompt re-asks and replaces
    assert s.pending_judge(new) == ["f1"] and items(s, new) == ["Q2", "Q3", "Q4"]
    unit = s.judge_units_for(["f1"], new)[0]
    s.save_judge("f1", {"verdicts": {"Q2": False, "Q3": False, "Q4": False}}, new, merge=unit["stamps_current"])
    assert doc(s)["in_graph_count"] == 2


def test_dead_letters(s):
    run_rules(s)
    s.save_failure("f1", {"error": "timeout", "error_kind": "retryable"}, JUDGE)
    assert s.pending_judge(JUDGE) == []
    for stamps, kw in (({**JUDGE}, {"force": True}), ({**JUDGE, "judge_model": "other"}, {}),
                       ({**JUDGE, "judge_confirm_model": ""}, {})):
        assert s.pending_judge(stamps, **kw) == ["f1"]
    s.save_judge("f1", {"verdicts": {"Q2": True, "Q3": True, "Q4": True}}, JUDGE, merge=False)
    assert s.failures.count_documents({}) == 0


def test_decisions_stamps_and_stats(s):
    run_rules(s)
    got = s.decisions_for(["f1"])[URL]
    assert got["mentions_sha"] == kc.mentions_sha(MENTIONS) and sorted(got["keep"].values()) == ["platform", "title"]
    stamps = s.curation_stamps()
    assert (stamps["entities_curation_frames"], stamps["entities_in_graph"], stamps["entities_curation_pending"],
            stamps["entities_curation_versions"]) == (1, 2, 3, ["l1"])
    assert s.decisions_for(["f1"], curated_at_lte=datetime(2000, 1, 1, tzinfo=timezone.utc)) == {}
    assert s.stats()["decisions_by_basis"]["pending:None"] == 3


# -- kg_store.entity_links_for over both stores ----------------------------------------------

@pytest.fixture
def both():
    cur, ent = mock_stores(st.EntityCurationStore, es.EntityStore)
    cur.entities.insert_one({"_id": "f1", "frame_url": URL, "mention_count": 7, "linked_at": LINKED,
                             "mentions": [dict(x) for x in MENTIONS] + [m("about", "Q9", 40, 44, method="kym_id")]})
    with serving(st, cur), serving(es, ent):
        yield cur


SNAP = datetime(2100, 1, 1, tzinfo=timezone.utc)


def read():
    return [(x["qid"], x["relevance_basis"]) for x in kg_store.entity_links_for(["f1"], SNAP)[URL]]


def test_an_uncurated_frame_keeps_its_title_and_own_item(both):
    assert read() == [("Q1", "title"), ("Q9", "own_item")]


def test_a_curated_frame_keeps_what_was_kept_until_a_relink(both):
    rec = both.records_for(["f1"])[0]
    both.save_rules([(rec, kc.apply_rules(rec, LISTS, NoClasses()))], RULES)
    both.save_judge("f1", {"verdicts": {"Q2": True, "Q3": False, "Q4": False}}, JUDGE, merge=False)
    assert read() == [("Q1", "title"), ("Q2", "judge"), ("Q100", "platform"), ("Q9", "own_item")]
    both.entities.update_one({"_id": "f1"}, {"$set": {"mentions.4.qid": "Q33"}})   # stale: back to the fallback
    assert read() == [("Q1", "title"), ("Q9", "own_item")]


def test_the_stamps_combine_both_stores(both):
    got = kg_store.linking_stamps(SNAP)
    assert set(kg_store.ENTITY_STAMP_KEYS) <= set(got), set(kg_store.ENTITY_STAMP_KEYS) - set(got)
