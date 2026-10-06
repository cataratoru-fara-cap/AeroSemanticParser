"""kg/review.py: drawing and scoring the human review (gap 08). Pinned: the draw
is deterministic and proportional (evidence you cannot reproduce is an
anecdote; the representative sample keeps the corpus's mix of section kind and
event density); the over-sampled strata never touch the headline (relative and
unconfirmed are rare and risky, folding them in would bias the rate and
describe no population); a stratum scores only the events it is ABOUT; Wilson,
not the normal approximation (at 100% clean the textbook interval runs past
1.0; with 20 events the true rate could be 84%)."""
import pytest

from modules.kg import review

OK = {f: "ok" for f in review.FIELDS}


def ev(eid, *, basis="stated", certainty="confirmed"):
    return {"event_id": eid, "sentences": [1], "date": "2013-05-04", "date_basis": basis, "date_precision": "day",
            "certainty": certainty}


def doc(uid, section, n=0, events=None):
    return {"unit_id": uid, "source_section": section,
            "events": events if events is not None else [ev(f"{uid}-{i}") for i in range(n)]}


CORPUS = ([doc(f"s{i:03d}", "spread", 1 + i % 8) for i in range(120)]
          + [doc(f"o{i:03d}", "origin", 1 + i % 4) for i in range(60)]
          + [doc("rel1", "spread", events=[ev("r1", basis="relative"), ev("r2")]),
             doc("rel2", "origin", events=[ev("r3", basis="relative")]),
             doc("unc1", "spread", events=[ev("u1", certainty="unconfirmed"), ev("u2")])])
BY_ID = {d["unit_id"]: d for d in CORPUS}


def test_the_same_seed_draws_the_same_sections():
    a = review.draw(CORPUS, seed=7, representative=30)
    assert a == review.draw(CORPUS, seed=7, representative=30)
    assert a["representative"] != review.draw(CORPUS, seed=8, representative=30)["representative"]


def test_the_representative_draw_keeps_the_corpus_mix():
    drawn = review.draw(CORPUS, seed=1, representative=60)["representative"]
    share = sum(d["source_section"] == "spread" for d in CORPUS) / len(CORPUS)
    assert len(drawn) == 60
    assert sum(BY_ID[u]["source_section"] == "spread" for u in drawn) / 60 == pytest.approx(share, abs=0.08)
    bands = {review.density_band(len(BY_ID[u]["events"])) for u in review.draw(CORPUS, seed=3, representative=60)[
        "representative"]}
    assert {"6+", "1-2"} <= bands                      # every band, the crowded sections (43 of 1528) too


def test_unconfirmed_is_a_census_and_relative_is_capped():
    drawn = review.draw(CORPUS, seed=1, representative=10, relative_events=1)
    assert drawn["unconfirmed"] == ["unc1"] and len(drawn["relative"]) == 1     # one section was enough


@pytest.mark.parametrize("stratum, event, counts", [
    ("relative", ev("x", basis="relative"), True), ("relative", ev("x"), False),
    ("unconfirmed", ev("x", certainty="debunked"), True), ("unconfirmed", ev("x"), False),
    ("representative", ev("x"), True),
])
def test_a_stratum_scores_only_its_own_events(stratum, event, counts):
    assert review.qualifying(stratum, event) is counts


# -- scoring --------------------------------------------------------------------------------------

def reviewed(samples, events, verdicts, missed=()):
    return {"status": "done", "samples": samples, "events": events, "verdicts": verdicts,
            "missed_sentences": list(missed), "reviewer": "gabi"}


def test_a_clean_section_scores_one_with_an_honest_interval():
    rep = review.score([reviewed(["representative"], [ev("a"), ev("b")], {"a": OK, "b": OK})])["representative"]
    assert (rep["events_judged"], rep["rate"], rep["ci95"][1]) == (2, 1.0, 1.0)
    assert rep["ci95"][0] < 1.0                                    # never certainty from n=2


def test_a_field_verdict_lands_on_that_field_only():
    rep = review.score([reviewed(["representative"], [ev("a")], {"a": {**OK, "date": "wrong"}})])["representative"]
    assert (rep["by_field"]["date"]["rate"], rep["by_field"]["location"]["rate"]) == (0.0, 1.0)
    assert rep["rate"] == 0.0 and rep["problems"] == {"date": {"wrong": 1}}     # clean means clean on all


def test_the_relative_rate_ignores_the_ordinary_events_beside_it():
    out = review.score([reviewed(["relative"], [ev("r", basis="relative"), ev("plain")],
                                 {"r": {**OK, "date": "wrong"}, "plain": OK})])
    assert (out["relative"]["events_judged"], out["relative"]["rate"], out["representative"]["events_judged"]) == \
        (1, 0.0, 0)


def test_recall_counts_only_the_representative_sample_and_unreviewed_counts_nothing():
    out = review.score([reviewed(["representative"], [ev("a")], {"a": OK}, missed=[4, 7]),
                        reviewed(["unconfirmed"], [ev("a")], {"a": OK}, missed=[1, 2, 3])])
    assert out["representative"]["recall"]["sentences_with_a_missed_event"] == 2 and "recall" not in out["unconfirmed"]
    assert out["representative"]["recall"]["rate"] == pytest.approx(1 / 3, abs=5e-4)
    pending = {"status": "pending", "samples": ["representative"], "events": [ev("a")], "verdicts": {}}
    assert review.score([pending])["representative"]["events_judged"] == 0


def test_wilson_does_not_promise_certainty():
    lo, hi = review.wilson(20, 20)
    assert hi == 1.0 and 0.8 < lo < 0.9                 # 20/20 is not "at least 90%"
    assert review.wilson(0, 0) == (0.0, 1.0)
