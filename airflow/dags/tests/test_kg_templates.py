"""kg/templates.py, pure. Pinned: eligibility is Gabi's rule (memes, plus any
frame with an imgflip link or a KYM Template section; known links first);
queries drop a "(Slang)" qualifier and non-Latin alternatives; paging stops
when a page stops being relevant and a failed first page is `failed`, never
`no_results`; a KYM link is ground truth; dedup does not chain and keeps the
most canonical upload; selection is 0 or 1..K_MAX, most varied first, never
two templates within SIMILAR_PHASH, and says why a frame got nothing."""
import random

import pytest

from modules import imgflip_parse as ip
from modules.kg import templates as kt

FRAME = "https://knowyourmeme.com/memes/distracted-boyfriend"
GOLD_REF = {"name": "Meme Generator", "url": "https://imgflip.com/memegenerator/112126428/x"}
COMMON = frozenset({"enabled", "disabled", "broke", "ass", "strong", "independent", "you", "re", "approaching",
                    "me", "oh", "slow", "clap"})


def entry(**over):
    return {"url": FRAME, "title": "Distracted Boyfriend", "category": "meme", "entry_type": ["exploitable"],
            "additional_references": [], "og_image": "https://i.kym-cdn.com/entries/icons/original/000/023/732/db.jpg",
            "sections": [{"kind": "about", "images": []}], **over}


def hx(value):
    return f"{value:016x}"


def hashes(p, d=0, mirror=None):
    return {"phash": hx(p), "phash_mirror": hx(mirror if mirror is not None else ~p & (2**64 - 1)),
            "dhash": hx(d), "dhash_mirror": hx(~d & (2**64 - 1)), "md5": f"md5-{p}-{d}"}


def flip(value, *bits):
    for b in bits:
        value ^= 1 << b
    return value


# -- units --------------------------------------------------------------------------

@pytest.mark.parametrize("over, priority", [
    ({}, 2), ({"entry_type": ["catchphrase"]}, 3),          # template types before other memes
    ({"category": "event", "additional_references": [GOLD_REF]}, 1),
    ({"category": "person", "sections": [{"kind": "template", "images": []}]}, 4),
    ({"category": "event"}, None),                           # not a meme and no hint
])
def test_eligibility_and_queue_order(over, priority):
    unit = kt.frame_unit(entry(**over))
    assert (unit and unit["priority"]) == priority


def test_gold_links_frame_images_and_the_source_hash():
    gold = kt.gold_links(entry(additional_references=[
        GOLD_REF, {"name": "Wikipedia", "url": "https://en.wikipedia.org/wiki/Distracted_boyfriend"},
        {"name": "Meme Generator", "url": "https://imgflip.com/memegenerator"}]))
    assert [(g["kind"], g["template_id"]) for g in gold] == [("memegenerator", 112126428)]
    e = entry(sections=[{"kind": "template", "images": [{"src": "https://i.kym-cdn.com/a.jpg"},
                                                        {"src": "https://i.kym-cdn.com/b.jpg"}]},
                        {"kind": "spread", "images": [{"src": "https://i.kym-cdn.com/c.jpg"}]}])
    assert kt.frame_images(e)[1:] == ["https://i.kym-cdn.com/a.jpg", "https://i.kym-cdn.com/b.jpg"]  # og first
    a = kt.frame_unit(entry())["source_sha256"]
    assert a == kt.frame_unit(entry())["source_sha256"]
    assert a not in {kt.frame_unit(entry(title="Distracted Bf"))["source_sha256"],
                     kt.frame_unit(entry(og_image="https://x/y.jpg"))["source_sha256"]}


# -- queries and names ------------------------------------------------------------------

@pytest.mark.parametrize("args, want", [
    (("Bruh (Slang)",), ["bruh"]), (('"This Is Fine"',), ["this is fine"]),
    (("Ralph Wiggum / I'm In Danger",), ["ralph wiggum", "i'm in danger"]),
    (("Друг / Friend",), ["friend"]),                                         # non-Latin dropped
    (("Distracted Boyfriend", ["Guy Looking Back"]), ["distracted boyfriend", "guy looking back"]),  # gold's name
])
def test_frame_queries(args, want):
    assert kt.frame_queries(*args) == want


def test_queries_are_capped_and_keywords_fall_back():
    assert len(kt.frame_queries("A b / C d / E f / G h")) <= kt.MAX_QUERIES
    assert kt.keyword_query("Chinese Man Yelling at a Kitten") == "chinese man yelling kitten"
    assert kt.keyword_query("Ladies, Imagine") == "ladies imagine" and kt.keyword_query("Bruh") is None


def test_text_similarity():
    sim, p = kt.text_similarity, kt.DEFAULT_PARAMS
    assert sim(["distracted boyfriend"], ["Distracted Boyfriend"]) == 1.0
    multi, single = sim(["distracted boyfriend"], ["Distracted Boyfriend Reversed"]), sim(["doge"], ["Tiny Face Doge"])
    assert multi >= p.contain_multi and p.contain_single <= single < multi       # containment below equality
    assert sim(["drunk history"], ["History Drunk"]) < p.contain_multi           # the words in order
    assert sim(["we are not the same"], ["tf2 spy we are not the same"]) >= p.contain_multi
    assert sim(["drunk history"], ["History"]) < 0.55                            # short name: weak evidence
    assert sim(["distracted boyfriend"], ["Distracted bf"]) > 0.5
    assert sim(["distracted boyfriend"], ["abc"]) < 0.4


@pytest.mark.parametrize("title, queries, weak", [
    ("Enabled / Disabled", ["enabled", "disabled"], [True, True]),
    ("Hitori Gotō / Bocchi", ["hitori gotō", "bocchi"], [False, False]),      # a name, however short
    ("Oh? You're Approaching Me? / JoJo Approach", ["oh? you're approaching me?", "jojo approach"], [False, False]),
    ("Slow Clap", ["slow clap"], [False]),                                    # a whole title never
])
def test_generic_caption_parts_are_weak(title, queries, weak):
    assert kt.weak_queries(title, queries, COMMON) == weak


# -- search -------------------------------------------------------------------------------

def render(results, has_next):
    boxes = "".join(f'<div class="mt-box"><h3 class="mt-title"><a href="/meme/{tid}/{name.replace(" ", "-")}">'
                    f'{name}</a></h3><div class="mt-img-wrap"><img class="shadow" '
                    f'src="//i.imgflip.com/4/{ip.key_from_template_id(tid)}.jpg"/></div></div>' for tid, name in results)
    pager = '<div class="pager"><a class="pager-next" href="?page=2">next</a></div>' if has_next else ""
    return f'<html><div id="mt-boxes-wrap"><div class="mt-boxes">{boxes}</div></div>{pager}</html>'


class FakeIO:
    """Search pages as parse-ready html built from result tuples."""

    def __init__(self, pages, fail=(), hashes_by_id=None, gold=None):
        self.pages, self.fail, self.hashes_by_id, self.gold, self.asked = pages, set(fail), hashes_by_id or {}, gold or {}, []

    def search_page(self, query, page):
        self.asked.append((query, page))
        if (query, page) in self.fail:
            return {"ok": False, "error_kind": "retryable", "error": "503"}
        results = self.pages.get((query, page))
        if results is None:
            return {"ok": False, "error_kind": "end_of_results"}
        return {"ok": True, "html": render(results, has_next=(query, page + 1) in self.pages)}

    def io(self):
        return kt.SearchIO(self.search_page, lambda t: self.hashes_by_id.get(int(t["template_id"])),
                           lambda src: hashes(0xFFFF), lambda link: self.gold.get(link["template_id"]))


def search(io, **over):
    return kt.search_frame(kt.frame_unit(entry(**over)), io.io())


def test_paging_stops_at_the_first_irrelevant_page():
    relevant, noise = [(1000 + i, "Distracted Boyfriend") for i in range(40)], [(2000 + i, "zzz") for i in range(40)]
    io = FakeIO({("distracted boyfriend", 1): relevant, ("distracted boyfriend", 2): noise,
                 ("distracted boyfriend", 3): relevant})
    rec = search(io)
    assert io.asked == [("distracted boyfriend", 1), ("distracted boyfriend", 2)]
    assert rec["search_status"] == "searched" and {c["t"]: c["r"] for c in rec["candidates"]}[2000] == 40  # absolute


def test_only_prefiltered_candidates_are_downloaded():
    rec = search(FakeIO({("distracted boyfriend", 1): [(1, "Distracted Boyfriend")] + [(100 + i, "zzz") for i in range(15)]}))
    pre = {c["t"] for c in rec["candidates"] if c["pre"]}
    assert {1, 103} <= pre and 114 not in pre          # a name match; the first query's top 10; not rank 14


def test_a_failed_first_page_is_failed_not_no_results():
    assert search(FakeIO({}, fail={("distracted boyfriend", 1)}))["search_status"] == "failed"
    assert search(FakeIO({("distracted boyfriend", 1): []}))["search_status"] == "no_results"


def test_gold_is_resolved_and_its_name_searched():
    gold = {77: {"template_id": 77, "key": ip.key_from_template_id(77), "name": "Guy Looking Back",
                 "url": "https://imgflip.com/meme/77/x", "featured": False, "animated": False}}
    io = FakeIO({("distracted boyfriend", 1): [(5, "Distracted Boyfriend")]}, gold=gold,
                hashes_by_id={77: hashes(7), 5: hashes(5)})
    rec = search(io, additional_references=[{"name": "Meme Generator", "url": "https://imgflip.com/memegenerator/77/x"}])
    assert rec["gold_ids"] == [77] and ("guy looking back", 1) in io.asked and 77 in rec["hashes"]


# -- scoring --------------------------------------------------------------------------------

def frame(candidates, gold=(), images=(), title=None, queries=("distracted boyfriend",)):
    return {"title": title, "queries": list(queries), "gold_ids": list(gold), "candidates": candidates,
            "frame_images": list(images)}


def cand(t, r=0, q=0):
    return {"t": t, "q": q, "r": r, "pre": True}


def test_scoring():
    s = kt.score_candidates(frame([cand(1)]), {1: {"name": "Distracted Boyfriend", "featured": True,
                                                   "hashes": hashes(1)}})[0]
    assert s["R"] == pytest.approx(0.90) and s["accepted"]                     # exact, featured, top result
    assert not kt.score_candidates(frame([cand(1, r=3)]), {1: {"name": "boyfriend stuff", "hashes": hashes(1)}})[0]["accepted"]
    pic = hashes(0xABCDEF)                                                     # a picture rescues a renamed upload
    s = kt.score_candidates(frame([cand(1, r=2)], images=[dict(pic, src="x")]),
                            {1: {"name": "guy holding hand with girl and looks back", "hashes": pic}})[0]
    assert s["s_vis"] == 1.0 and s["accepted"]


def test_gold_is_always_accepted_and_unhashed_never():
    scored = {s["template_id"]: s for s in kt.score_candidates(
        frame([cand(2)], gold=[1]), {1: {"name": "zzz", "hashes": hashes(1)}, 2: {"name": "Distracted Boyfriend"}})}
    assert scored[1]["accepted"] and scored[1]["method"] == kt.GOLD and not scored[2]["accepted"]


def test_a_name_matching_only_a_weak_part_needs_the_picture():
    f = frame([cand(1, q=1)], title="Enabled / Disabled", queries=("enabled", "disabled"))
    t = {1: {"name": "Disabled", "featured": True, "hashes": hashes(1)}}
    s = kt.score_candidates(f, t, common=COMMON)[0]
    assert s["s_text"] == kt.WEAK_TEXT_CAP and not s["accepted"]
    f["frame_images"] = [dict(hashes(1), src="kym")]
    assert kt.score_candidates(f, t, common=COMMON)[0]["accepted"]


def test_the_relative_floor_drops_padding_under_a_strong_match():
    t = {1: {"name": "Distracted Boyfriend", "featured": True, "hashes": hashes(1)},
         2: {"name": "Distracted Boyfriend", "hashes": hashes(2)}}
    scored = {s["template_id"]: s["accepted"] for s in kt.score_candidates(
        frame([cand(1), cand(2, r=30)]), t, kt.RelevanceParams(tau=0.5, relative_floor=0.05))}
    assert scored == {1: True, 2: False}


# -- clusters ---------------------------------------------------------------------------------

def test_no_chaining():
    b = flip(0, *range(6))                      # 6 bits from A
    c = flip(b, *range(6, 12))                  # 6 from B, 12 from A
    leader = kt.cluster([1, 2, 3], {1: {"hashes": hashes(0)}, 2: {"hashes": hashes(b)}, 3: {"hashes": hashes(c)}})
    assert (leader[2], leader[3]) == (1, 3)


def test_the_representative_mirrors_and_unhashed_templates():
    same = hashes(12345)
    t = {10: {"hashes": same}, 20: {"hashes": same, "featured": True}, 5: {"hashes": same, "animated": True}}
    assert set(kt.cluster([5, 10, 20], t).values()) == {20}                   # featured first
    assert set(kt.cluster([5, 10, 20], t, gold_ids=[10]).values()) == {10}    # gold over featured
    assert set(kt.cluster([5, 10], {10: t[10], 5: t[5]}).values()) == {10}    # still over animated
    original = {"phash": hx(1111), "phash_mirror": hx(2222), "dhash": hx(3), "dhash_mirror": hx(4)}
    mirrored = {"phash": hx(2222), "phash_mirror": hx(1111), "dhash": hx(4), "dhash_mirror": hx(3)}
    assert kt.cluster([1, 2], {1: {"hashes": original}, 2: {"hashes": mirrored}})[2] == 1
    assert kt.cluster([1, 2], {1: {}, 2: {}}) == {1: 1, 2: 2}


# -- selection --------------------------------------------------------------------------------

def scored(tid, r, gold=False, accepted=True):
    return {"template_id": tid, "R": r, "gold": gold, "accepted": accepted, "s_text": 0.9, "s_vis": 0.0,
            "s_rank": 1.0, "hashed": True, "method": kt.GOLD if gold else kt.SEARCH}


def far(n):
    """n templates with pairwise-distant hashes."""
    rnd = random.Random(n)
    return {i: {"hashes": hashes(rnd.getrandbits(64), rnd.getrandbits(64))} for i in range(1, n + 1)}


def test_selection_order_cap_and_members():
    t = far(3)
    got = kt.select([scored(1, 0.7), scored(2, 1.0, gold=True), scored(3, 0.9)], {1: 1, 2: 2, 3: 3}, t)
    assert [s["template_id"] for s in got["selected"]] == [2, 3, 1] and got["selected"][0]["method"] == kt.GOLD
    t = far(15)
    assert len(kt.select([scored(i, 0.8) for i in t], {i: i for i in t}, t)["selected"]) == kt.K_MAX
    first = kt.select([scored(1, 0.7), scored(2, 0.9), scored(3, 0.8)], {1: 1, 2: 1, 3: 3}, far(3))["selected"][0]
    assert (first["template_id"], first["R"], first["members"]) == (1, 0.9, [1, 2])   # one picture, one selection


def test_near_pictures_are_not_both_kept():
    near = flip(0, *range(10))                  # pHash 10 apart: not merged, too close to keep both
    got = kt.select([scored(1, 0.9), scored(2, 0.8)], {1: 1, 2: 2}, {1: {"hashes": hashes(0)},
                                                                     2: {"hashes": hashes(near, 0xFFFF)}})
    assert [s["template_id"] for s in got["selected"]] == [1] and got["suppressed"][0]["template_id"] == 2


def test_outcomes_say_why_nothing_was_selected():
    none = {"selected": [], "suppressed": [], "accepted_groups": 0, "unselected": []}
    assert kt.outcome({}, [], none)["status"] == "no_results"
    out = kt.outcome({}, [scored(1, 0.3, accepted=False)], none)
    assert (out["status"], out["best_rejected"]["template_id"]) == ("below_threshold", 1)
    assert kt.outcome({"search_status": "failed"}, [], none)["status"] == "failed"
