"""Tests for kg/templates.py — pure, no network, no Mongo.

What these pin:

  * **Eligibility is Gabi's rule**: category meme, plus any frame with an
    imgflip link or a KYM Template section; the queue order puts known
    links first.
  * **Queries**: a "(Slang)" qualifier is dropped, " / " alternatives are
    searched, non-Latin ones are not.
  * **Paging stops when a page stops being relevant**, and a failed first
    page makes the frame ``failed``, never ``no_results``.
  * **A KYM link is ground truth**: always accepted, always selected first.
  * **Dedup does not chain** (A~B, B~C, A far from C: C is not merged into
    A), and the representative is the most canonical upload.
  * **Selection is 0, or 1 to K_MAX, most varied first**, never keeps two
    templates within SIMILAR_PHASH, and records why a frame got nothing.
"""
import unittest

from modules import imgflip_parse as ip
from modules.kg import templates as kt

FRAME = "https://knowyourmeme.com/memes/distracted-boyfriend"


def entry(**over) -> dict:
    doc = {"url": FRAME, "title": "Distracted Boyfriend", "category": "meme",
           "entry_type": ["exploitable"], "additional_references": [],
           "og_image": "https://i.kym-cdn.com/entries/icons/original/000/023/732/db.jpg",
           "sections": [{"kind": "about", "images": []}]}
    doc.update(over)
    return doc


def hx(value: int) -> str:
    return f"{value:016x}"


def hashes(p: int, d: int = 0, mirror: int | None = None) -> dict:
    return {"phash": hx(p), "phash_mirror": hx(mirror if mirror is not None else ~p & (2**64 - 1)),
            "dhash": hx(d), "dhash_mirror": hx(~d & (2**64 - 1)), "md5": f"md5-{p}-{d}"}


def flip(value: int, *bits: int) -> int:
    for b in bits:
        value ^= 1 << b
    return value


class UnitTests(unittest.TestCase):
    def test_memes_are_eligible_and_other_categories_need_a_hint(self):
        self.assertIsNotNone(kt.frame_unit(entry()))
        self.assertIsNone(kt.frame_unit(entry(category="event")))
        linked = entry(category="event", additional_references=[
            {"name": "Meme Generator", "url": "https://imgflip.com/memegenerator/112126428/x"}])
        self.assertEqual(kt.frame_unit(linked)["priority"], 1)
        templated = entry(category="person", sections=[{"kind": "template", "images": []}])
        self.assertEqual(kt.frame_unit(templated)["priority"], 4)

    def test_priority_puts_template_types_before_other_memes(self):
        self.assertEqual(kt.frame_unit(entry())["priority"], 2)
        self.assertEqual(kt.frame_unit(entry(entry_type=["catchphrase"]))["priority"], 3)

    def test_gold_links_come_from_the_references_sidebar(self):
        e = entry(additional_references=[
            {"name": "Meme Generator", "url": "https://imgflip.com/memegenerator/112126428/x"},
            {"name": "Wikipedia", "url": "https://en.wikipedia.org/wiki/Distracted_boyfriend"},
            {"name": "Meme Generator", "url": "https://imgflip.com/memegenerator"}])
        gold = kt.gold_links(e)
        self.assertEqual([(g["kind"], g["template_id"]) for g in gold],
                         [("memegenerator", 112126428)])

    def test_frame_images_are_og_then_template_section(self):
        e = entry(sections=[{"kind": "template", "images": [{"src": "https://i.kym-cdn.com/a.jpg"},
                                                            {"src": "https://i.kym-cdn.com/b.jpg"}]},
                            {"kind": "spread", "images": [{"src": "https://i.kym-cdn.com/c.jpg"}]}])
        self.assertEqual(kt.frame_images(e)[1:], ["https://i.kym-cdn.com/a.jpg",
                                                  "https://i.kym-cdn.com/b.jpg"])

    def test_source_hash_moves_with_what_the_search_reads(self):
        a = kt.frame_unit(entry())["source_sha256"]
        self.assertEqual(a, kt.frame_unit(entry())["source_sha256"])
        self.assertNotEqual(a, kt.frame_unit(entry(title="Distracted Bf"))["source_sha256"])
        self.assertNotEqual(a, kt.frame_unit(entry(og_image="https://x/y.jpg"))["source_sha256"])


class QueryTests(unittest.TestCase):
    def test_qualifier_quotes_and_alternatives(self):
        self.assertEqual(kt.frame_queries("Bruh (Slang)"), ["bruh"])
        self.assertEqual(kt.frame_queries('"This Is Fine"'), ["this is fine"])
        self.assertEqual(kt.frame_queries("Ralph Wiggum / I'm In Danger"),
                         ["ralph wiggum", "i'm in danger"])

    def test_non_latin_alternatives_are_dropped_and_the_cap_holds(self):
        self.assertEqual(kt.frame_queries("Друг / Friend"), ["friend"])
        self.assertLessEqual(len(kt.frame_queries("A b / C d / E f / G h")), kt.MAX_QUERIES)

    def test_the_gold_templates_name_is_searched_too(self):
        self.assertEqual(kt.frame_queries("Distracted Boyfriend", ["Guy Looking Back"]),
                         ["distracted boyfriend", "guy looking back"])


class TextSimilarityTests(unittest.TestCase):
    def test_exact_and_case(self):
        self.assertEqual(kt.text_similarity(["distracted boyfriend"], ["Distracted Boyfriend"]), 1.0)

    def test_containment_scores_below_equality(self):
        multi = kt.text_similarity(["distracted boyfriend"], ["Distracted Boyfriend Reversed"])
        single = kt.text_similarity(["doge"], ["Tiny Face Doge"])
        self.assertGreaterEqual(multi, kt.DEFAULT_PARAMS.contain_multi)
        self.assertGreaterEqual(single, kt.DEFAULT_PARAMS.contain_single)
        self.assertLess(single, multi)

    def test_containment_needs_the_words_in_order(self):
        self.assertLess(kt.text_similarity(["drunk history"], ["History Drunk"]),
                        kt.DEFAULT_PARAMS.contain_multi)
        self.assertGreaterEqual(
            kt.text_similarity(["we are not the same"], ["tf2 spy we are not the same"]),
            kt.DEFAULT_PARAMS.contain_multi)

    def test_a_short_name_inside_the_query_is_weak_evidence(self):
        self.assertLess(kt.text_similarity(["drunk history"], ["History"]), 0.55)
        self.assertGreater(kt.text_similarity(["distracted boyfriend"], ["Distracted bf"]), 0.5)

    def test_keyword_fallback(self):
        self.assertEqual(kt.keyword_query("Chinese Man Yelling at a Kitten"),
                         "chinese man yelling kitten")
        self.assertEqual(kt.keyword_query("Ladies, Imagine"), "ladies imagine")
        self.assertIsNone(kt.keyword_query("Bruh"))

    def test_unrelated_names_score_low(self):
        self.assertLess(kt.text_similarity(["distracted boyfriend"], ["abc"]), 0.4)


class FakeIO:
    """search pages as parse-ready html built from result dicts."""

    def __init__(self, pages: dict, fail=(), hashes_by_id=None, gold=None):
        self.pages, self.fail = pages, set(fail)
        self.hashes_by_id = hashes_by_id or {}
        self.gold = gold or {}
        self.asked: list[tuple] = []

    def search_page(self, query, page):
        self.asked.append((query, page))
        if (query, page) in self.fail:
            return {"ok": False, "error_kind": "retryable", "error": "503"}
        results = self.pages.get((query, page))
        if results is None:
            return {"ok": False, "error_kind": "end_of_results"}
        return {"ok": True, "html": render(results, has_next=(query, page + 1) in self.pages)}

    def thumb_hashes(self, t):
        return self.hashes_by_id.get(int(t["template_id"]))

    def image_hashes(self, src):
        return hashes(0xFFFF)

    def resolve_gold(self, link):
        return self.gold.get(link["template_id"])

    def io(self):
        return kt.SearchIO(self.search_page, self.thumb_hashes, self.image_hashes,
                           self.resolve_gold)


def render(results: list[tuple[int, str]], has_next: bool) -> str:
    boxes = "".join(
        f'<div class="mt-box"><h3 class="mt-title"><a href="/meme/{tid}/{name.replace(" ", "-")}">'
        f'{name}</a></h3><div class="mt-img-wrap"><img class="shadow" '
        f'src="//i.imgflip.com/4/{ip.key_from_template_id(tid)}.jpg"/></div></div>'
        for tid, name in results)
    pager = '<div class="pager"><a class="pager-next" href="?page=2">next</a></div>' if has_next else ""
    return f'<html><div id="mt-boxes-wrap"><div class="mt-boxes">{boxes}</div></div>{pager}</html>'


class SearchFrameTests(unittest.TestCase):
    def unit(self, **over):
        return kt.frame_unit(entry(**over))

    def test_relevant_full_page_continues_irrelevant_stops(self):
        relevant = [(1000 + i, "Distracted Boyfriend") for i in range(40)]
        noise = [(2000 + i, "zzz") for i in range(40)]
        io = FakeIO({("distracted boyfriend", 1): relevant,
                     ("distracted boyfriend", 2): noise,
                     ("distracted boyfriend", 3): relevant})
        rec = kt.search_frame(self.unit(), io.io())
        self.assertEqual(io.asked, [("distracted boyfriend", 1), ("distracted boyfriend", 2)])
        self.assertEqual(rec["search_status"], "searched")
        ranks = {c["t"]: c["r"] for c in rec["candidates"]}
        self.assertEqual(ranks[2000], 40)                   # absolute rank

    def test_only_prefiltered_candidates_are_downloaded(self):
        results = [(1, "Distracted Boyfriend")] + [(100 + i, "zzz") for i in range(15)]
        io = FakeIO({("distracted boyfriend", 1): results})
        rec = kt.search_frame(self.unit(), io.io())
        pre = {c["t"] for c in rec["candidates"] if c["pre"]}
        self.assertIn(1, pre)
        self.assertNotIn(100 + 14, pre)                     # rank 14, no name match
        self.assertIn(100 + 3, pre)                         # top-10 of the first query

    def test_a_failed_first_page_is_failed_not_no_results(self):
        io = FakeIO({}, fail={("distracted boyfriend", 1)})
        self.assertEqual(kt.search_frame(self.unit(), io.io())["search_status"], "failed")
        empty = FakeIO({("distracted boyfriend", 1): []})
        self.assertEqual(kt.search_frame(self.unit(), empty.io())["search_status"], "no_results")

    def test_gold_is_resolved_and_its_name_searched(self):
        e = dict(additional_references=[
            {"name": "Meme Generator", "url": "https://imgflip.com/memegenerator/77/x"}])
        gold = {77: {"template_id": 77, "key": ip.key_from_template_id(77),
                     "name": "Guy Looking Back", "url": "https://imgflip.com/meme/77/x",
                     "featured": False, "animated": False}}
        io = FakeIO({("distracted boyfriend", 1): [(5, "Distracted Boyfriend")]}, gold=gold,
                    hashes_by_id={77: hashes(7), 5: hashes(5)})
        rec = kt.search_frame(self.unit(**e), io.io())
        self.assertEqual(rec["gold_ids"], [77])
        self.assertIn(("guy looking back", 1), io.asked)
        self.assertIn(77, rec["hashes"])


class ScoringTests(unittest.TestCase):
    def frame(self, candidates, gold=(), images=()):
        return {"queries": ["distracted boyfriend"], "gold_ids": list(gold),
                "candidates": candidates, "frame_images": list(images)}

    def test_exact_featured_top_result_is_accepted(self):
        t = {1: {"name": "Distracted Boyfriend", "featured": True, "hashes": hashes(1)}}
        s = kt.score_candidates(self.frame([{"t": 1, "q": 0, "r": 0, "pre": True}]), t)[0]
        self.assertAlmostEqual(s["R"], 0.90)
        self.assertTrue(s["accepted"])

    def test_a_weak_name_without_a_picture_match_is_not(self):
        t = {1: {"name": "boyfriend stuff", "hashes": hashes(1)}}
        s = kt.score_candidates(self.frame([{"t": 1, "q": 0, "r": 3, "pre": True}]), t)[0]
        self.assertFalse(s["accepted"])

    def test_a_picture_match_rescues_a_renamed_upload(self):
        pic = hashes(0xABCDEF)
        t = {1: {"name": "guy holding hand with girl and looks back", "hashes": pic}}
        s = kt.score_candidates(self.frame([{"t": 1, "q": 0, "r": 2, "pre": True}],
                                           images=[dict(pic, src="x")]), t)[0]
        self.assertEqual(s["s_vis"], 1.0)
        self.assertTrue(s["accepted"])

    def test_gold_is_always_accepted_and_unhashed_never(self):
        t = {1: {"name": "zzz", "hashes": hashes(1)}, 2: {"name": "Distracted Boyfriend"}}
        scored = {s["template_id"]: s for s in kt.score_candidates(
            self.frame([{"t": 2, "q": 0, "r": 0, "pre": True}], gold=[1]), t)}
        self.assertTrue(scored[1]["accepted"])
        self.assertEqual(scored[1]["method"], kt.GOLD)
        self.assertFalse(scored[2]["accepted"])

    COMMON = frozenset({"enabled", "disabled", "broke", "ass", "strong", "independent",
                        "you", "re", "approaching", "me", "oh", "slow", "clap"})

    def test_generic_caption_parts_are_weak(self):
        self.assertEqual(kt.weak_queries("Enabled / Disabled", ["enabled", "disabled"],
                                         self.COMMON), [True, True])
        # a name, however short, is not ordinary English
        self.assertEqual(kt.weak_queries("Hitori Gotō / Bocchi", ["hitori gotō", "bocchi"],
                                         self.COMMON), [False, False])
        # a longer phrase is specific enough, and a whole title is never weak
        self.assertEqual(kt.weak_queries("Oh? You're Approaching Me? / JoJo Approach",
                                         ["oh? you're approaching me?", "jojo approach"],
                                         self.COMMON), [False, False])
        self.assertEqual(kt.weak_queries("Slow Clap", ["slow clap"], self.COMMON), [False])

    def test_a_name_matching_only_a_weak_part_needs_the_picture(self):
        frame = {"title": "Enabled / Disabled", "queries": ["enabled", "disabled"],
                 "gold_ids": [], "frame_images": [],
                 "candidates": [{"t": 1, "q": 1, "r": 0, "pre": True}]}
        t = {1: {"name": "Disabled", "featured": True, "hashes": hashes(1)}}
        s = kt.score_candidates(frame, t, common=self.COMMON)[0]
        self.assertEqual(s["s_text"], kt.WEAK_TEXT_CAP)
        self.assertFalse(s["accepted"])
        frame["frame_images"] = [dict(hashes(1), src="kym")]
        self.assertTrue(kt.score_candidates(frame, t, common=self.COMMON)[0]["accepted"])

    def test_the_relative_floor_drops_padding_under_a_strong_match(self):
        params = kt.RelevanceParams(tau=0.5, relative_floor=0.05)
        t = {1: {"name": "Distracted Boyfriend", "featured": True, "hashes": hashes(1)},
             2: {"name": "Distracted Boyfriend", "hashes": hashes(2)}}
        scored = {s["template_id"]: s for s in kt.score_candidates(self.frame(
            [{"t": 1, "q": 0, "r": 0, "pre": True}, {"t": 2, "q": 0, "r": 30, "pre": True}]),
            t, params)}
        self.assertTrue(scored[1]["accepted"])
        self.assertFalse(scored[2]["accepted"])


class ClusterTests(unittest.TestCase):
    def test_no_chaining(self):
        a = 0
        b = flip(a, *range(6))               # 6 bits from A
        c = flip(b, *range(6, 12))           # 6 from B, 12 from A
        t = {1: {"hashes": hashes(a)}, 2: {"hashes": hashes(b)}, 3: {"hashes": hashes(c)}}
        leader = kt.cluster([1, 2, 3], t)
        self.assertEqual(leader[2], 1)
        self.assertEqual(leader[3], 3)

    def test_representative_order(self):
        same = hashes(12345)
        t = {10: {"hashes": same}, 20: {"hashes": same, "featured": True},
             5: {"hashes": same, "animated": True}}
        self.assertEqual(set(kt.cluster([5, 10, 20], t).values()), {20})
        self.assertEqual(set(kt.cluster([5, 10, 20], t, gold_ids=[10]).values()), {10})
        self.assertEqual(set(kt.cluster([5, 10], {10: t[10], 5: t[5]}).values()), {10})

    def test_a_mirror_upload_joins_its_original(self):
        original = {"phash": hx(1111), "phash_mirror": hx(2222),
                    "dhash": hx(3), "dhash_mirror": hx(4)}
        mirrored = {"phash": hx(2222), "phash_mirror": hx(1111),
                    "dhash": hx(4), "dhash_mirror": hx(3)}
        leader = kt.cluster([1, 2], {1: {"hashes": original}, 2: {"hashes": mirrored}})
        self.assertEqual(leader[2], 1)

    def test_unhashed_templates_stand_alone(self):
        self.assertEqual(kt.cluster([1, 2], {1: {}, 2: {}}), {1: 1, 2: 2})


class SelectTests(unittest.TestCase):
    def scored(self, tid, r, gold=False, accepted=True):
        return {"template_id": tid, "R": r, "gold": gold, "accepted": accepted,
                "s_text": 0.9, "s_vis": 0.0, "s_rank": 1.0, "hashed": True,
                "method": kt.GOLD if gold else kt.SEARCH}

    def far(self, n):
        """n templates with pairwise-distant hashes."""
        import random
        rnd = random.Random(n)
        return {i: {"hashes": hashes(rnd.getrandbits(64), rnd.getrandbits(64))}
                for i in range(1, n + 1)}

    def test_gold_first_then_by_score(self):
        t = self.far(3)
        got = kt.select([self.scored(1, 0.7), self.scored(2, 1.0, gold=True),
                         self.scored(3, 0.9)], {1: 1, 2: 2, 3: 3}, t)
        self.assertEqual([s["template_id"] for s in got["selected"]], [2, 3, 1])
        self.assertEqual(got["selected"][0]["method"], kt.GOLD)

    def test_at_most_k_max(self):
        t = self.far(15)
        got = kt.select([self.scored(i, 0.8) for i in t], {i: i for i in t}, t)
        self.assertEqual(len(got["selected"]), kt.K_MAX)

    def test_members_of_one_picture_become_one_selection(self):
        t = self.far(3)
        got = kt.select([self.scored(1, 0.7), self.scored(2, 0.9), self.scored(3, 0.8)],
                        {1: 1, 2: 1, 3: 3}, t)
        first = got["selected"][0]
        self.assertEqual((first["template_id"], first["R"], first["members"]), (1, 0.9, [1, 2]))

    def test_near_pictures_are_not_both_kept(self):
        near = flip(0, *range(10))                   # pHash 10: not merged, too close
        t = {1: {"hashes": hashes(0)}, 2: {"hashes": hashes(near, 0xFFFF)}}
        got = kt.select([self.scored(1, 0.9), self.scored(2, 0.8)], {1: 1, 2: 2}, t)
        self.assertEqual([s["template_id"] for s in got["selected"]], [1])
        self.assertEqual(got["suppressed"][0]["template_id"], 2)

    def test_outcomes_say_why_nothing_was_selected(self):
        none = {"selected": [], "suppressed": [], "accepted_groups": 0, "unselected": []}
        self.assertEqual(kt.outcome({}, [], none)["status"], "no_results")
        rejected = [self.scored(1, 0.3, accepted=False)]
        out = kt.outcome({}, rejected, none)
        self.assertEqual((out["status"], out["best_rejected"]["template_id"]),
                         ("below_threshold", 1))
        self.assertEqual(kt.outcome({"search_status": "failed"}, [], none)["status"], "failed")


if __name__ == "__main__":
    unittest.main()
