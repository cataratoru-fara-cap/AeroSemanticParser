"""Tests for kg/review.py — drawing and scoring the human review (gap 08).

What these pin, beyond "it works":

  * **The draw is deterministic and proportional.** A review is evidence,
    and evidence you cannot reproduce is an anecdote. Same seed, same
    sections; and the representative sample keeps the corpus's mix of
    section kind and event density rather than whatever a flat shuffle
    happened to give.
  * **The over-sampled strata never touch the headline.** `relative` and
    `unconfirmed` exist precisely because they are rare and risky; folding
    them into the representative rate would bias it downwards and describe
    no population at all.
  * **A stratum scores only the events it is ABOUT.** The reviewer judges
    every event in a drawn section, but the relative-date rate must not be
    diluted by the ordinary events sharing its page.
  * **Wilson, not normal-approximation.** At 100% clean the textbook
    interval runs past 1.0 and reads as certainty; with 20 events the true
    rate could be 84%.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_review.py -v
"""
import unittest

from modules.kg import review


def ev(eid, *, basis="stated", certainty="confirmed"):
    return {"event_id": eid, "sentences": [1], "date": "2013-05-04",
            "date_basis": basis, "date_precision": "day", "certainty": certainty}


def doc(uid, section, n, **over):
    events = [ev(f"{uid}-{i}") for i in range(n)]
    events = over.pop("events", events)
    return {"unit_id": uid, "source_section": section, "events": events, **over}


def corpus():
    out = []
    for i in range(120):
        out.append(doc(f"s{i:03d}", "spread", 1 + i % 8))
    for i in range(60):
        out.append(doc(f"o{i:03d}", "origin", 1 + i % 4))
    out.append(doc("rel1", "spread", 0,
                   events=[ev("r1", basis="relative"), ev("r2")]))
    out.append(doc("rel2", "origin", 0, events=[ev("r3", basis="relative")]))
    out.append(doc("unc1", "spread", 0,
                   events=[ev("u1", certainty="unconfirmed"), ev("u2")]))
    return out


class DrawTests(unittest.TestCase):
    def test_the_same_seed_draws_the_same_sections(self):
        a = review.draw(corpus(), seed=7, representative=30)
        b = review.draw(corpus(), seed=7, representative=30)
        self.assertEqual(a, b)
        self.assertNotEqual(a["representative"],
                            review.draw(corpus(), seed=8,
                                        representative=30)["representative"])

    def test_the_representative_draw_keeps_the_corpus_mix(self):
        docs = corpus()
        drawn = review.draw(docs, seed=1, representative=60)["representative"]
        by_id = {d["unit_id"]: d for d in docs}
        got = sum(1 for u in drawn if by_id[u]["source_section"] == "spread")
        share = sum(1 for d in docs if d["source_section"] == "spread") / len(docs)
        self.assertAlmostEqual(got / len(drawn), share, delta=0.08)
        self.assertEqual(len(drawn), 60)

    def test_every_band_of_event_density_is_represented(self):
        docs = corpus()
        drawn = review.draw(docs, seed=3, representative=60)["representative"]
        by_id = {d["unit_id"]: d for d in docs}
        bands = {review.density_band(len(by_id[u]["events"])) for u in drawn}
        self.assertIn("6+", bands)          # the crowded sections, 43 of 1528
        self.assertIn("1-2", bands)

    def test_unconfirmed_is_a_census_and_relative_is_capped(self):
        docs = corpus()
        drawn = review.draw(docs, seed=1, representative=10, relative_events=1)
        self.assertEqual(drawn["unconfirmed"], ["unc1"])
        self.assertEqual(len(drawn["relative"]), 1)   # one section was enough

    def test_a_stratum_scores_only_its_own_events(self):
        self.assertTrue(review.qualifying("relative", ev("x", basis="relative")))
        self.assertFalse(review.qualifying("relative", ev("x")))
        self.assertTrue(review.qualifying("unconfirmed",
                                          ev("x", certainty="debunked")))
        self.assertFalse(review.qualifying("unconfirmed", ev("x")))
        self.assertTrue(review.qualifying("representative", ev("x")))


class ScoreTests(unittest.TestCase):
    def reviewed(self, samples, events, verdicts, missed=()):
        return {"status": "done", "samples": list(samples), "events": events,
                "verdicts": verdicts, "missed_sentences": list(missed),
                "reviewer": "gabi"}

    def test_a_clean_section_scores_one_with_an_honest_interval(self):
        events = [ev("a"), ev("b")]
        rows = [self.reviewed(["representative"], events,
                              {"a": {f: "ok" for f in review.FIELDS},
                               "b": {f: "ok" for f in review.FIELDS}})]
        rep = review.score(rows)["representative"]
        self.assertEqual((rep["events_judged"], rep["rate"]), (2, 1.0))
        self.assertLess(rep["ci95"][0], 1.0)      # never certainty from n=2
        self.assertEqual(rep["ci95"][1], 1.0)

    def test_a_field_verdict_lands_on_that_field_only(self):
        events = [ev("a")]
        rows = [self.reviewed(["representative"], events,
                              {"a": {**{f: "ok" for f in review.FIELDS},
                                     "date": "wrong"}})]
        rep = review.score(rows)["representative"]
        self.assertEqual(rep["by_field"]["date"]["rate"], 0.0)
        self.assertEqual(rep["by_field"]["location"]["rate"], 1.0)
        self.assertEqual(rep["rate"], 0.0)        # clean means clean on all
        self.assertEqual(rep["problems"], {"date": {"wrong": 1}})

    def test_the_relative_rate_ignores_the_ordinary_events_beside_it(self):
        events = [ev("r", basis="relative"), ev("plain")]
        rows = [self.reviewed(["relative"], events,
                              {"r": {**{f: "ok" for f in review.FIELDS},
                                     "date": "wrong"},
                               "plain": {f: "ok" for f in review.FIELDS}})]
        out = review.score(rows)
        self.assertEqual(out["relative"]["events_judged"], 1)
        self.assertEqual(out["relative"]["rate"], 0.0)
        self.assertEqual(out["representative"]["events_judged"], 0)

    def test_recall_counts_only_the_representative_sample(self):
        events = [ev("a")]
        ok = {"a": {f: "ok" for f in review.FIELDS}}
        out = review.score([
            self.reviewed(["representative"], events, ok, missed=[4, 7]),
            self.reviewed(["unconfirmed"], events, ok, missed=[1, 2, 3])])
        self.assertEqual(out["representative"]["recall"][
            "sentences_with_a_missed_event"], 2)
        self.assertNotIn("recall", out["unconfirmed"])
        self.assertAlmostEqual(out["representative"]["recall"]["rate"], 1 / 3,
                               places=3)

    def test_an_unreviewed_section_contributes_nothing(self):
        out = review.score([{"status": "pending", "samples": ["representative"],
                             "events": [ev("a")], "verdicts": {}}])
        self.assertEqual(out["representative"]["events_judged"], 0)

    def test_wilson_does_not_promise_certainty(self):
        lo, hi = review.wilson(20, 20)
        self.assertEqual(hi, 1.0)
        self.assertLess(lo, 0.9)             # 20/20 is not "at least 90%"
        self.assertGreater(lo, 0.8)
        self.assertEqual(review.wilson(0, 0), (0.0, 1.0))


if __name__ == "__main__":       # pragma: no cover
    unittest.main()
