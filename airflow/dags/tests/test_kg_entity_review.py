"""Tests for kg/entity_review.py (pure parts).

What these pin:
  * **Strata** by how a link was decided; titles, own items and undecided
    links are never sampled; an unknown basis is an error.
  * **The draw is reproducible** and capped per stratum.
  * **Scores are stratum-weighted**: a small stratum read in full does not
    outweigh a large one.
"""
import unittest

from modules.kg import entity_review as R


def dec(basis, keep, field="about", n=0):
    return {"frame_id": f"f{n}", "key": f"about|-1|{n}|{n + 1}|Q{n}", "keep": keep,
            "basis": basis, "field": field, "qid": f"Q{n}"}


class StratumTests(unittest.TestCase):
    def test_strata(self):
        self.assertEqual(R.stratum(dec("tag_named", True)), "rule_keep")
        self.assertEqual(R.stratum(dec("judge", True)), "judge_keep")
        self.assertEqual(R.stratum(dec("judge", False)), "judge_drop")
        self.assertEqual(R.stratum(dec("deny_class", False)), "rule_drop")
        for d in (dec("title", True, "title"), dec("own_item", True), dec("pending", None)):
            self.assertIsNone(R.stratum(d))
        with self.assertRaises(ValueError):
            R.stratum(dec("judge?", True))

    def test_draw_is_reproducible_and_capped(self):
        rows = [dec("judge", n % 2 == 0, n=n) for n in range(100)] + [dec("platform", True, n=200)]
        a, corpus = R.draw(rows, {"judge_keep": 10, "judge_drop": 10, "rule_keep": 5}, seed=1)
        b, _ = R.draw(list(reversed(rows)), {"judge_keep": 10, "judge_drop": 10, "rule_keep": 5},
                      seed=1)
        self.assertEqual([r["key"] for r in a], [r["key"] for r in b])
        self.assertEqual(corpus, {"rule_keep": 1, "judge_keep": 50, "judge_drop": 50,
                                  "rule_drop": 0})
        self.assertEqual(len(a), 21)

    def test_render_marks_the_mention(self):
        row = dict(dec("judge", True), stratum="judge_keep")
        entry = {"title": "Doge", "tags": ["doge", "shiba inu"],
                 "sections": [{"kind": "about", "text": ["Doge is a Shiba Inu."]}]}
        got = R.render(row, {"field": "about", "start": 10, "end": 19, "qid": "Q1",
                             "text": "Shiba Inu", "label": "Shiba Inu"}, entry)
        self.assertEqual(got["context"], "Doge is a [[Shiba Inu]].")
        got = R.render(row, {"field": "tag", "tag_index": 1, "start": 0, "end": 9, "qid": "Q1",
                             "text": "shiba inu"}, entry)
        self.assertEqual(got["context"], "tags: doge, [[shiba inu]]")


class ScoreTests(unittest.TestCase):
    def test_weighted_by_corpus_share(self):
        sample = ([{"key": f"k{i}", "stratum": "rule_keep"} for i in range(10)]
                  + [{"key": f"j{i}", "stratum": "judge_keep"} for i in range(10)]
                  + [{"key": f"d{i}", "stratum": "judge_drop"} for i in range(10)])
        verdicts = ([{"key": f"k{i}", "relevant": "yes", "right_item": "yes"} for i in range(10)]
                    + [{"key": f"j{i}", "relevant": "yes" if i < 5 else "no"} for i in range(10)]
                    + [{"key": f"d{i}", "relevant": "no" if i else "unsure"} for i in range(10)])
        # rule_keep is 100% relevant but only 10% of the kept corpus
        got = R.score(sample, verdicts, {"rule_keep": 100, "judge_keep": 900, "judge_drop": 50})
        self.assertAlmostEqual(got["kept_precision"]["rate"], 0.1 * 1.0 + 0.9 * 0.5, places=3)
        self.assertEqual(got["lost_rate"]["rate"], 0.0)
        self.assertEqual(got["strata"]["judge_drop"]["unsure"], 1)
        self.assertFalse(got["meets_bar"])
        lo, hi = got["kept_precision"]["interval"]
        self.assertLess(lo, 0.55)
        self.assertGreater(hi, 0.55)
        self.assertEqual(got["missing_verdicts"], 0)


if __name__ == "__main__":
    unittest.main()
