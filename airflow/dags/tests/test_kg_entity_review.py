"""kg/entity_review.py (pure parts). Pinned: strata by how a link was decided
(titles, own items and undecided links are never sampled; an unknown basis is
an error); the draw is reproducible and capped per stratum; scores are
stratum-weighted, so a small stratum read in full does not outweigh a large
one."""
import pytest

from modules.kg import entity_review as R


def dec(basis, keep, field="about", n=0):
    return {"frame_id": f"f{n}", "key": f"about|-1|{n}|{n + 1}|Q{n}", "keep": keep, "basis": basis, "field": field,
            "qid": f"Q{n}"}


@pytest.mark.parametrize("decision, stratum", [
    (dec("tag_named", True), "rule_keep"), (dec("judge", True), "judge_keep"), (dec("judge", False), "judge_drop"),
    (dec("deny_class", False), "rule_drop"),
    (dec("title", True, "title"), None), (dec("own_item", True), None), (dec("pending", None), None),   # never sampled
])
def test_strata(decision, stratum):
    assert R.stratum(decision) == stratum


def test_an_unknown_basis_is_an_error():
    with pytest.raises(ValueError):
        R.stratum(dec("judge?", True))


def test_the_draw_is_reproducible_and_capped():
    rows = [dec("judge", n % 2 == 0, n=n) for n in range(100)] + [dec("platform", True, n=200)]
    caps = {"judge_keep": 10, "judge_drop": 10, "rule_keep": 5}
    a, corpus = R.draw(rows, caps, seed=1)
    assert [r["key"] for r in a] == [r["key"] for r in R.draw(list(reversed(rows)), caps, seed=1)[0]]
    assert corpus == {"rule_keep": 1, "judge_keep": 50, "judge_drop": 50, "rule_drop": 0} and len(a) == 21


@pytest.mark.parametrize("mention, context", [
    ({"field": "about", "start": 10, "end": 19, "qid": "Q1", "text": "Shiba Inu", "label": "Shiba Inu"},
     "Doge is a [[Shiba Inu]]."),
    ({"field": "tag", "tag_index": 1, "start": 0, "end": 9, "qid": "Q1", "text": "shiba inu"},
     "tags: doge, [[shiba inu]]"),
])
def test_render_marks_the_mention(mention, context):
    entry = {"title": "Doge", "tags": ["doge", "shiba inu"],
             "sections": [{"kind": "about", "text": ["Doge is a Shiba Inu."]}]}
    assert R.render({**dec("judge", True), "stratum": "judge_keep"}, mention, entry)["context"] == context


def test_scores_are_weighted_by_corpus_share():
    sample = [{"key": f"{p}{i}", "stratum": s} for p, s in (("k", "rule_keep"), ("j", "judge_keep"),
                                                             ("d", "judge_drop")) for i in range(10)]
    verdicts = ([{"key": f"k{i}", "relevant": "yes", "right_item": "yes"} for i in range(10)]
                + [{"key": f"j{i}", "relevant": "yes" if i < 5 else "no"} for i in range(10)]
                + [{"key": f"d{i}", "relevant": "no" if i else "unsure"} for i in range(10)])
    # rule_keep is 100% relevant but only 10% of the kept corpus
    got = R.score(sample, verdicts, {"rule_keep": 100, "judge_keep": 900, "judge_drop": 50})
    assert got["kept_precision"]["rate"] == pytest.approx(0.1 * 1.0 + 0.9 * 0.5, abs=5e-4)
    assert (got["lost_rate"]["rate"], got["strata"]["judge_drop"]["unsure"], got["meets_bar"],
            got["missing_verdicts"]) == (0.0, 1, False, 0)
    lo, hi = got["kept_precision"]["interval"]
    assert lo < 0.55 < hi
