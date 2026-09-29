"""
templates.py — which imgflip templates fit a KYM frame, deduplicated and chosen
================================================================================
Pure: no HTTP, no Mongo, no Airflow. The search step reaches imgflip only
through the functions it is handed (``SearchIO``), which the DAG builds
from modules/imgflip_client.py and modules/template_store.py. Everything
after the search (scoring, dedup, selection) is a function of stored data,
so a new threshold is re-run offline, without a single request.

The pipeline, per frame
-----------------------
1. **Queries** from the KYM title (``frame_queries``): the title without a
   trailing "(Slang)"-style qualifier, each " / " alternative, and the name
   of the frame's known imgflip template, if it has one.
2. **Search**: imgflip's /memesearch, page 1 always, later pages only while
   they stay relevant (``PAGE_RULE``).
3. **Prefilter**: a candidate whose name is close enough, or that imgflip
   features, or that ranks high, gets its thumbnail downloaded and hashed.
   The rest are recorded but never downloaded.
4. **Relevance** (``score_candidates``): name similarity (IMKG's difflib
   ratio and token containment), a visual match against the frame's OWN
   KYM images, search rank, imgflip's featured flag. The frame's KYM
   "Meme Generator" link to imgflip is ground truth and always relevant.
5. **Dedup** (``cluster``), GLOBAL across every frame: the same picture
   (exact copy, resize, re-encode, mirror image) is one template, whose
   representative is the most canonical upload.
6. **Selection** (``select``): 0 templates when nothing is relevant (the
   reason is recorded; nothing is forced), otherwise 1 to ``K_MAX``, most
   varied first.

A template may be selected by several frames (Gabi, 2026-09-28 — IMKG gave
each template exactly one frame). Each frame's edge carries its own score.

Calibration of the duplicate rule (2026-09-28)
----------------------------------------------
399 thumbnails from page 1 of ten queries, every pair within pHash 24
sheeted side by side and read:

    pHash <= 8, dHash <= 12   the same picture in about 9 of 10 non-identical
                              pairs (resizes, re-encodes, borders, a
                              recoloured button). Misses: panel-swapped
                              "Woman Yelling at a Cat", a "This Is Fine" with
                              text added.
    pHash <= 8, dHash > 12    all crops (the square vs the wide "Is This a
                              Pigeon") — kept apart, as the rule asks.
    pHash 9-12                half re-crops of the same photo, half small
                              EDITS (a hat or a face mask drawn on, sunglasses,
                              a swapped face). Neither dHash band separates
                              them (1 in 3 wrong even at dHash <= 6), so they
                              are NOT merged...
    selection                 ...but no frame keeps two templates within
                              pHash ``SIMILAR_PHASH`` of each other: a
                              re-crop adds no variety, and an edit of an
                              already chosen picture adds little.
    pHash >= 13               different templates start appearing (Drake vs
                              a red-eyed Elmo at pHash 16, dHash 8).
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Callable, Iterable, Sequence

from modules import imgflip_parse as ip
from modules.kg import visual

QUERY_VERSION = "1.0.0"
RELEVANCE_VERSION = "1.0.0"
DEDUP_VERSION = "1.0.0"
SELECTION_VERSION = "1.0.0"

# -- queries and paging ------------------------------------------------------
MAX_QUERIES = 3
MAX_PAGES = 3
# Fetch page n+1 only if page n was full and at least this share of it
# passed the prefilter: deep pages are recent low-use uploads, which is
# where the duplicates are.
PAGE_CONTINUE_SHARE = 0.25
# A thumbnail is downloaded when the name is at least this close, or the
# template is featured, or it ranks in the top PREFILTER_RANK of query 1.
PREFILTER_TEXT = 0.40
PREFILTER_RANK = 10
# KYM images hashed per frame: og:image plus the first few Template-section
# images.
MAX_FRAME_IMAGES = 6

# -- dedup (measured; see the module docstring) --------------------------------
MAX_PHASH = 8
MAX_DHASH = 12
SIMILAR_PHASH = 12
# Within one frame's selected templates (pilot, 427 pairs), a re-crop of
# the same scene and a genuine edit are equally common anywhere in pHash
# 13-24, so nothing past SIMILAR_PHASH is suppressed — Gabi's rule keeps
# crops as templates of their own. Distance only ORDERS the picks: two
# unrelated pictures sit about this far apart.
UNRELATED_PHASH = 32

# -- selection ------------------------------------------------------------------
K_MAX = 10
MMR_LAMBDA = 0.7

# Entry types whose memes are made FROM a template: first in the queue.
TEMPLATE_TYPES = frozenset({"exploitable", "image-macro", "reaction", "photoshop",
                            "character", "snowclone"})
TEMPLATE_SECTION_KIND = "template"


@dataclass(frozen=True)
class RelevanceParams:
    """The relevance rule's constants, tuned on the gold dev split. Any
    change is a new RELEVANCE_VERSION."""
    w_text: float = 0.65
    w_rank: float = 0.15
    w_featured: float = 0.10
    # Half the score: a template whose picture IS the frame's own KYM image
    # belongs to the frame whatever an uploader named it.
    w_visual: float = 0.50
    tau: float = 0.60                 # accept at or above
    gate: float = 0.50                # ...and name OR picture at least this
    # ...and within this of the frame's best, when set. Off: in the pilot a
    # frame's KYM link (R = 1.0) put the floor at 0.70 and cut relevant
    # exact-name variants (more Will Smith, Trollge, Squidward templates) —
    # the opposite of "as many and as varied as possible".
    relative_floor: float | None = None
    visual_scale: float = 24.0        # S_vis = 1 - pHash distance / this
    contain_multi: float = 0.85       # title fully contained, >= 2 words
    contain_single: float = 0.70      # title fully contained, one word


DEFAULT_PARAMS = RelevanceParams()

GOLD = "kym_reference"
SEARCH = "search"


# ---------------------------------------------------------------------------
# Text
# ---------------------------------------------------------------------------

_TRAILING_QUALIFIER = re.compile(r"\s*\([^()]*\)\s*$")
_QUOTES = re.compile(r"[\"“”‘’`´]|(?<!\w)'|'(?!\w)")
_STOPWORDS = frozenset({"a", "an", "the", "of", "and", "meme", "memes", "template",
                        "templates", "blank", "hd", "original", "version", "full",
                        "new", "gif", "vs", "v"})


def norm(text: str | None) -> str:
    """Lower case, NFKC, quotes gone, '&' -> 'and', whitespace collapsed."""
    text = unicodedata.normalize("NFKC", text or "")
    text = _QUOTES.sub("", text).replace("&", " and ")
    return re.sub(r"\s+", " ", text).strip().lower()


def word_list(text: str) -> list[str]:
    """Content words in order."""
    return [t for t in re.findall(r"[a-z0-9]+", norm(text)) if t not in _STOPWORDS]


def tokens(text: str) -> set[str]:
    return set(word_list(text))


def _in_order(needle: Sequence[str], hay: Sequence[str]) -> bool:
    """Every word of ``needle`` appears in ``hay`` in the same order (gaps
    allowed): "Doge" in "Tiny Face Doge", "we are not the same" in "tf2 spy
    we are not the same" — but not "Drunk History" in "History Drunk"."""
    it = iter(hay)
    return all(any(w == h for h in it) for w in needle)


def _mostly_latin(text: str) -> bool:
    letters = [c for c in text if c.isalpha()]
    if not letters:
        return False
    latin = sum(1 for c in letters if "LATIN" in unicodedata.name(c, ""))
    return latin / len(letters) >= 0.5


def frame_queries(title: str, gold_names: Iterable[str] = ()) -> list[str]:
    """Up to MAX_QUERIES search strings for a KYM title, most specific
    first. imgflip names are English, so non-Latin alternatives are dropped
    ("Друг / Friend" searches "Friend")."""
    out: list[str] = []

    def add(text: str) -> None:
        q = norm(_TRAILING_QUALIFIER.sub("", text or ""))
        q = q.strip(" .,:;!?-")
        if len(q) >= 2 and _mostly_latin(q) and q not in out:
            out.append(q)

    # A title with " / " names the same meme twice ("Ralph Wiggum / I'm In
    # Danger"); searched whole, it matches fewer templates than either part.
    if " / " in (title or ""):
        for part in title.split(" / "):
            if len(part.strip()) >= 3:
                add(part)
    else:
        add(title)
    for name in gold_names:
        add(name)
    return out[:MAX_QUERIES]


# Words a keyword fallback leaves out: imgflip's search needs EVERY word to
# match a template's name, so "Chinese Man Yelling at a Kitten" finds
# nothing where "chinese man yelling kitten" might (pilot, 2026-09-28: 17
# of 50 frames' searches came back empty, mostly long or punctuated titles).
_FUNCTION_WORDS = frozenset({
    "at", "in", "on", "to", "for", "with", "by", "from", "or", "but", "if", "so",
    "is", "are", "was", "were", "be", "been", "am", "do", "does", "did", "has",
    "have", "had", "just", "ever", "you", "your", "yours", "my", "me", "we", "our",
    "they", "their", "he", "his", "she", "her", "it", "its", "this", "that",
    "when", "what", "who", "why", "how", "where", "not", "no", "yes", "too",
    "very", "all", "some", "lmao", "lol"})
KEYWORD_MAX = 4

# A " / " title sometimes names ONE meme twice ("Hitori Gotō / Bocchi",
# "Ash Pedreiro / Dat Ash") and sometimes pairs two captions of one image
# ("Enabled / Disabled", "Broke Ass / Strong and Independent"). A part of
# the second kind is ordinary English, and imgflip holds hundreds of
# unrelated uploads named "disabled": in the pilot and holdout those two
# frames kept 2 right templates out of 17. So a part with at most
# WEAK_PART_WORDS content words, every one a common English word, is WEAK
# evidence: a name matching only such parts scores at most WEAK_TEXT_CAP
# — under the gate — and needs the picture to agree. A whole title is
# never weak ("Slow Clap" is its meme's name), nor a longer part.
WEAK_PART_WORDS = 2
WEAK_TEXT_CAP = 0.45


def _common_words() -> frozenset[str]:
    """Lower-case words spaCy's English model knows (its vocabulary strings:
    ~85k, common words and very famous names, not meme or anime names — no
    "bocchi", "jojo", "trollge"). Empty when spaCy is not installed, which
    makes nothing weak."""
    global _COMMON
    if _COMMON is None:
        try:
            from modules.kg import entities as kg_entities
            nlp = kg_entities.load_nlp()
            _COMMON = frozenset(s for s in nlp.vocab.strings if s.isalpha() and s.islower())
        except Exception:                      # pragma: no cover - no model
            _COMMON = frozenset()
    return _COMMON


_COMMON: frozenset[str] | None = None


def weak_queries(title: str, queries: Sequence[str],
                 common: frozenset[str] | None = None) -> list[bool]:
    """For each query, whether it is a weak " / " part (see WEAK_PART_WORDS)."""
    if " / " not in (title or ""):
        return [False] * len(queries)
    common = _common_words() if common is None else common
    parts = {norm(_TRAILING_QUALIFIER.sub("", p)).strip(" .,:;!?-")
             for p in title.split(" / ")}
    out = []
    for q in queries:
        words = word_list(q)
        out.append(q in parts and 0 < len(words) <= WEAK_PART_WORDS
                   and all(w in common for w in words))
    return out


def keyword_query(title: str) -> str | None:
    """The fallback search when every query came back EMPTY: the title's
    first few content words. It only finds candidates — relevance is still
    scored against the full title — so a looser search cannot make a weak
    match look strong."""
    text = norm(_TRAILING_QUALIFIER.sub("", title or ""))
    words: list[str] = []
    for w in re.findall(r"[a-z0-9]+", text):
        if len(w) >= 3 and w not in _STOPWORDS and w not in _FUNCTION_WORDS \
                and w not in words:
            words.append(w)
    if len(words) < 2:
        return None
    return " ".join(words[:KEYWORD_MAX])


def text_similarity(queries: Sequence[str], names: Sequence[str],
                    params: RelevanceParams = DEFAULT_PARAMS) -> float:
    """How well any of a template's names matches any query.

    The max of IMKG's difflib ratio (whole-string) and ordered containment:
    every content word of the query appears in the name, in order ("Doge"
    in "Tiny Face Doge"), scored a little lower for a one-word query, which
    is the easier one to contain by accident. Order matters: the pilot kept
    a random photo named "History Drunk" for the frame "Drunk History".
    """
    best = 0.0
    for q in queries:
        nq, wq = norm(q), word_list(q)
        for n in names:
            nn = norm(n)
            if not nq or not nn:
                continue
            # difflib rewards a SHORT name inside a long query ("History"
            # scores 0.70 against "drunk history" — an uploader's alternate
            # name, in the pilot); scale it by how much of the query's length
            # the name covers. A longer name is already penalised by difflib.
            cover = min(1.0, len(nn) / len(nq)) ** 0.5
            best = max(best, SequenceMatcher(None, nq, nn).ratio() * cover)
            if wq and _in_order(wq, word_list(n)):
                best = max(best, params.contain_multi if len(set(wq)) >= 2
                           else params.contain_single)
            if best >= 1.0:
                return 1.0
    return best


# ---------------------------------------------------------------------------
# Frames
# ---------------------------------------------------------------------------

def frame_key(url: str) -> str:
    """The entry's own _id (mongo_base.url_doc_id), so the collections join."""
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def gold_links(entry: dict) -> list[dict]:
    """The frame's own imgflip links, from KYM's references sidebar (the
    "Meme Generator" row): the ground truth the search is measured against."""
    out: list[dict] = []
    seen: set[str] = set()
    for ref in entry.get("additional_references") or []:
        url = str((ref or {}).get("url") or "")
        link = ip.classify_imgflip_url(url)
        if link["kind"] in ("not_imgflip", "other") or url in seen:
            continue
        seen.add(url)
        out.append({"url": url, "name": (ref or {}).get("name"), **link})
    return out


def frame_images(entry: dict) -> list[str]:
    """The frame's own pictures to match templates against: og:image, then
    the images of its Template section(s)."""
    srcs: list[str] = []
    if entry.get("og_image"):
        srcs.append(str(entry["og_image"]))
    for section in entry.get("sections") or []:
        if section.get("kind") != TEMPLATE_SECTION_KIND:
            continue
        for img in section.get("images") or []:
            src = str((img or {}).get("src") or "")
            if src and src not in srcs:
                srcs.append(src)
    return srcs[:MAX_FRAME_IMAGES]


def frame_unit(entry: dict) -> dict | None:
    """One entries doc -> the unit the search works on, or None when the
    frame is not eligible.

    Eligible (Gabi, 2026-09-28): category ``meme``, plus any frame that
    already links to imgflip or has a KYM Template section.
    """
    url = entry.get("url")
    title = entry.get("title")
    if not url or not title:
        return None
    url = str(url)
    gold = gold_links(entry)
    has_template_section = any((s or {}).get("kind") == TEMPLATE_SECTION_KIND
                               for s in entry.get("sections") or [])
    category = str(entry.get("category") or "")
    if category != "meme" and not gold and not has_template_section:
        return None
    entry_types = sorted(entry.get("entry_type") or [])
    if gold:
        priority = 1
    elif category == "meme" and (has_template_section
                                 or TEMPLATE_TYPES.intersection(entry_types)):
        priority = 2
    elif category == "meme":
        priority = 3
    else:
        priority = 4
    images = frame_images(entry)
    source = {"url": url, "title": title, "category": category,
              "entry_type": entry_types, "gold": [g["url"] for g in gold],
              "images": images}
    return {
        "unit_id": frame_key(url), "frame_url": url, "title": str(title),
        "category": category, "entry_type": entry_types, "priority": priority,
        "gold": gold, "images": images,
        "source_sha256": hashlib.sha256(
            json.dumps(source, sort_keys=True).encode("utf-8")).hexdigest(),
    }


# ---------------------------------------------------------------------------
# Search (network through SearchIO only)
# ---------------------------------------------------------------------------

@dataclass
class SearchIO:
    """What search_frame may do, handed in by the DAG.

    search_page(query, page) -> {"ok", "html", "error_kind", "error"}
        cached by the store, so frames sharing a query share the request.
    thumb_hashes(result) -> hashes dict | None
        a search result's thumbnail, downloaded once per template id.
    image_hashes(url) -> hashes dict | {"error": ...}
        one of the frame's KYM images.
    resolve_gold(link) -> template dict | None
        a gold link (classify_imgflip_url + url) -> {template_id, key, name,
        url, featured, animated, thumb_url, alt_names, ...} via its page.
    """
    search_page: Callable[[str, int], dict]
    thumb_hashes: Callable[[dict], dict | None]
    image_hashes: Callable[[str], dict]
    resolve_gold: Callable[[dict], dict | None]


def _prefiltered(result: dict, rank: int, queries: Sequence[str], query_idx: int,
                 params: RelevanceParams) -> bool:
    """``rank`` is the ABSOLUTE rank over the query's pages, not the rank on
    this page — or every page's top ten would pass and paging never stop."""
    if result.get("featured"):
        return True
    if query_idx == 0 and rank < PREFILTER_RANK:
        return True
    return text_similarity(queries, [result["name"]], params) >= PREFILTER_TEXT


def search_frame(unit: dict, io: SearchIO,
                 params: RelevanceParams = DEFAULT_PARAMS) -> dict:
    """Search imgflip for one frame. Returns the frame's search record and
    every template it saw (``templates``, keyed by id) for the store.

    Failures are data: a query whose page 1 could not be fetched makes the
    record ``failed`` — distinct from ``no_results`` — so it is retried
    rather than stored as "imgflip has nothing".
    """
    templates: dict[int, dict] = {}
    gold_ids: list[int] = []
    gold_errors: list[dict] = []
    for link in unit["gold"]:
        resolved = io.resolve_gold(link)
        if resolved is None:
            gold_errors.append({"url": link["url"], "kind": link["kind"]})
            continue
        tid = int(resolved["template_id"])
        templates[tid] = dict(templates.get(tid, {}), **resolved)
        if tid not in gold_ids:
            gold_ids.append(tid)

    # ``queries`` are what relevance is scored against; ``searched`` adds
    # the keyword fallback, which only ever FINDS candidates.
    queries = frame_queries(unit["title"],
                            [templates[t]["name"] for t in gold_ids if templates[t].get("name")])
    searched = list(queries)
    by_id: dict[int, dict] = {}         # insertion order = first seen
    pages_fetched = 0
    errors: list[dict] = []

    def run(query: str, qi: int) -> None:
        nonlocal pages_fetched
        for page in range(1, MAX_PAGES + 1):
            got = io.search_page(query, page)
            pages_fetched += 1
            if not got.get("ok"):
                if got.get("error_kind") != "end_of_results":
                    errors.append({"query": query, "page": page,
                                   "error_kind": got.get("error_kind"),
                                   "error": got.get("error")})
                return
            parsed = ip.parse_search(got["html"])
            passed = 0
            for res in parsed["results"]:
                tid = int(res["template_id"])
                rank = (page - 1) * ip.PAGE_SIZE + res["rank"]
                keep = _prefiltered(res, rank, queries, qi, params) or tid in gold_ids
                passed += keep
                known = templates.setdefault(tid, {})
                for k in ("template_id", "key", "name", "url", "featured",
                          "animated", "thumb_url"):
                    known.setdefault(k, res[k])
                c = by_id.get(tid)
                if c is None:
                    by_id[tid] = {"t": tid, "q": qi, "r": rank, "pre": keep}
                else:
                    c["pre"] = c["pre"] or keep
                    if rank < c["r"]:
                        c.update(q=qi, r=rank)
            results = parsed["results"]
            if not (parsed["has_next"] and len(results) >= ip.PAGE_SIZE
                    and passed >= PAGE_CONTINUE_SHARE * len(results)):
                return

    for qi, query in enumerate(queries):
        run(query, qi)
    if not by_id and not errors:
        fallback = keyword_query(unit["title"])
        if fallback and fallback not in queries:
            searched.append(fallback)
            run(fallback, len(queries))

    candidates = list(by_id.values())
    hashed: dict[int, dict] = {}
    thumbs_failed: list[int] = []
    for c in candidates:
        if not c["pre"]:
            continue
        h = io.thumb_hashes(templates[c["t"]])
        if h:
            hashed[c["t"]] = h
        else:
            thumbs_failed.append(c["t"])
    for tid in gold_ids:
        if tid not in hashed and tid in templates:
            h = io.thumb_hashes(templates[tid])
            if h:
                hashed[tid] = h
            else:
                thumbs_failed.append(tid)

    images = []
    for src in unit["images"]:
        got = io.image_hashes(src)
        images.append({"src": src, **(got or {"error": "no result"})})

    page1_failed = [e for e in errors if e["page"] == 1]
    if queries and len(page1_failed) == len(queries):
        status = "failed"
    elif not candidates and not gold_ids:
        status = "no_results"
    else:
        status = "searched"
    return {
        "unit_id": unit["unit_id"], "frame_url": unit["frame_url"],
        "title": unit["title"], "category": unit["category"],
        "priority": unit["priority"], "source_sha256": unit["source_sha256"],
        "queries": queries, "searched_queries": searched,
        "gold": [dict(g) for g in unit["gold"]],
        "gold_ids": gold_ids, "gold_errors": gold_errors,
        "candidates": candidates, "frame_images": images,
        "pages_fetched": pages_fetched, "errors": errors,
        "thumbs_failed": thumbs_failed, "search_status": status,
        "templates": templates, "hashes": hashed,
    }


# ---------------------------------------------------------------------------
# Relevance
# ---------------------------------------------------------------------------

def rank_score(rank: int | None) -> float:
    return 0.0 if rank is None else 1.0 / (1.0 + rank / 5.0)


def visual_similarity(hashes: dict | None, frame_imgs: Sequence[dict],
                      params: RelevanceParams = DEFAULT_PARAMS) -> float:
    """How closely a template's picture matches any of the frame's own KYM
    images. Evidence FOR only: KYM's og:image is often a captioned instance
    or an unrelated photo, so a mismatch says nothing."""
    if not hashes:
        return 0.0
    best = 0.0
    for img in frame_imgs:
        if "phash" not in img:
            continue
        p, _d = visual.distance(hashes, img)
        best = max(best, max(0.0, 1.0 - p / params.visual_scale))
    return best


def score_candidates(frame: dict, templates: dict[int, dict],
                     params: RelevanceParams = DEFAULT_PARAMS, *,
                     common: frozenset[str] | None = None) -> list[dict]:
    """Every candidate of one frame, scored; ``accepted`` marks the relevant.

    ``templates`` maps id -> the stored template doc (name, alt_names,
    featured, hashes). A candidate whose template has no hashes cannot be
    deduplicated, so it can be scored but never accepted.
    """
    gold_ids = set(frame.get("gold_ids") or [])
    # Only prefiltered candidates were hashed; the rest can never be accepted.
    by_id: dict[int, dict] = {c["t"]: c for c in frame.get("candidates") or []
                              if c.get("pre")}
    for tid in gold_ids:
        by_id.setdefault(tid, {"t": tid, "q": None, "r": None, "pre": True})
    queries = frame.get("queries") or []
    weak = weak_queries(frame.get("title") or "", queries, common)
    strong_q = [q for q, w in zip(queries, weak) if not w]
    weak_q = [q for q, w in zip(queries, weak) if w]
    scored: list[dict] = []
    for tid, c in by_id.items():
        t = templates.get(tid) or {}
        names = [n for n in [t.get("name"), *(t.get("alt_names") or [])] if n]
        hashes = t.get("hashes")
        s_text = 0.0
        if names:
            s_text = max(text_similarity(strong_q, names, params) if strong_q else 0.0,
                         min(WEAK_TEXT_CAP, text_similarity(weak_q, names, params))
                         if weak_q else 0.0)
        s_vis = visual_similarity(hashes, frame.get("frame_images") or [], params)
        s_rank = rank_score(c.get("r"))
        featured = 1.0 if t.get("featured") else 0.0
        gold = tid in gold_ids
        if gold:
            r = 1.0
        else:
            r = min(1.0, params.w_text * s_text + params.w_rank * s_rank
                    + params.w_featured * featured + params.w_visual * s_vis)
        scored.append({"template_id": tid, "R": round(r, 4),
                       "s_text": round(s_text, 4), "s_vis": round(s_vis, 4),
                       "s_rank": round(s_rank, 4), "featured": bool(featured),
                       "gold": gold, "hashed": bool(hashes),
                       "method": GOLD if gold else SEARCH})
    best = max((s["R"] for s in scored), default=0.0)
    floor = best - params.relative_floor if params.relative_floor is not None else 0.0
    for s in scored:
        s["accepted"] = bool(s["hashed"] and (s["gold"] or (
            s["R"] >= params.tau
            and (s["s_text"] >= params.gate or s["s_vis"] >= params.gate)
            and s["R"] >= floor)))
    scored.sort(key=lambda s: (-s["R"], s["template_id"]))
    return scored


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

def representative_order(tid: int, t: dict, gold: bool) -> tuple:
    """Which upload stands for its picture: the frame's KYM link, then
    imgflip's featured one, then a still over an animation, then the oldest
    (lowest id: imgflip ids grow over time, and the original is usually
    first)."""
    return (not gold, not t.get("featured"), bool(t.get("animated")), tid)


def cluster(template_ids: Iterable[int], templates: dict[int, dict],
            gold_ids: Iterable[int] = (), *, max_phash: int = MAX_PHASH,
            max_dhash: int = MAX_DHASH) -> dict[int, int]:
    """{template id: its representative's id}, over every id given.

    Leader clustering, not union-find: union-find chains drifting variants
    (A~B and B~C merges A with C, however far apart they are). Here each
    template joins the FIRST representative, in representative order, that
    it is the same picture as — or becomes one. Deterministic for a given
    set; a template without hashes stands alone.
    """
    gold = set(gold_ids)
    ids = sorted(set(template_ids),
                 key=lambda t: representative_order(t, templates.get(t) or {}, t in gold))
    index = visual.BandIndex(max_phash)
    leader_of: dict[int, int] = {}
    for tid in ids:
        h = (templates.get(tid) or {}).get("hashes")
        if not h:
            leader_of[tid] = tid
            continue
        found = None
        for leader, lh in index.candidates(h):
            if visual.same_picture(h, lh, max_phash=max_phash, max_dhash=max_dhash):
                found = leader
                break
        if found is None:
            index.add(tid, h)
            leader_of[tid] = tid
        else:
            leader_of[tid] = found
    return leader_of


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

def _phash_distance(a: dict | None, b: dict | None) -> int:
    if not a or not b:
        return visual.HASH_BITS
    return visual.distance(a, b)[0]


def select(scored: Sequence[dict], leader_of: dict[int, int],
           templates: dict[int, dict], *, k_max: int = K_MAX,
           mmr_lambda: float = MMR_LAMBDA, similar_phash: int = SIMILAR_PHASH) -> dict:
    """One frame's accepted candidates -> its chosen templates.

    Candidates are grouped by representative (a frame never gets two
    uploads of one picture); a group scores its best member. The frame's
    KYM-linked templates come first and are always kept. The rest are
    picked by maximal marginal relevance, so each next template is the one
    least like those already chosen, and one within ``similar_phash`` of a
    chosen template is skipped (see the module docstring).
    """
    groups: dict[int, dict] = {}
    for s in scored:
        if not s["accepted"]:
            continue
        leader = leader_of.get(s["template_id"], s["template_id"])
        g = groups.setdefault(leader, {"template_id": leader, "R": 0.0, "gold": False,
                                       "members": [], "best": None})
        g["members"].append(s["template_id"])
        g["gold"] = g["gold"] or s["gold"]
        if g["best"] is None or s["R"] > g["best"]["R"]:
            g["best"] = s
            g["R"] = s["R"]

    def hashes(tid: int) -> dict | None:
        return (templates.get(tid) or {}).get("hashes")

    pool = sorted(groups.values(), key=lambda g: (not g["gold"], -g["R"], g["template_id"]))
    chosen: list[dict] = []
    suppressed: list[dict] = []
    for g in [g for g in pool if g["gold"]]:
        chosen.append(g)
    pool = [g for g in pool if not g["gold"]]
    while pool and len(chosen) < k_max:
        def mmr(g: dict) -> tuple:
            nearest = min((_phash_distance(hashes(g["template_id"]), hashes(c["template_id"]))
                           for c in chosen), default=visual.HASH_BITS)
            # Unrelated pictures sit ~32 bits apart, so similarity is
            # measured on 0-32: a re-crop at 16 is half-similar, anything
            # past 32 not at all, and a crop is picked after a new picture.
            sim = max(0.0, 1.0 - nearest / UNRELATED_PHASH)
            return (mmr_lambda * g["R"] - (1.0 - mmr_lambda) * (sim if chosen else 0.0),
                    -g["template_id"])
        best = max(pool, key=mmr)
        pool.remove(best)
        near = min((_phash_distance(hashes(best["template_id"]), hashes(c["template_id"]))
                    for c in chosen), default=visual.HASH_BITS)
        if near <= similar_phash:
            suppressed.append({"template_id": best["template_id"], "R": best["R"],
                               "near": near})
            continue
        chosen.append(best)

    selected = []
    for rank, g in enumerate(chosen):
        b = g["best"]
        selected.append({"template_id": g["template_id"], "R": g["R"],
                         "method": GOLD if g["gold"] else SEARCH, "mmr_rank": rank,
                         "members": sorted(g["members"]), "s_text": b["s_text"],
                         "s_vis": b["s_vis"], "s_rank": b["s_rank"]})
    return {"selected": selected, "suppressed": suppressed,
            "accepted_groups": len(groups),
            "unselected": [g["template_id"] for g in pool]}


def outcome(frame: dict, scored: Sequence[dict], chosen: dict) -> dict:
    """The frame's selection status, and the best miss when it has none.
    "Selected nothing" is a stored result with a reason, never a gap."""
    if frame.get("search_status") == "failed":
        status = "failed"
    elif chosen["selected"]:
        status = "selected"
    elif not scored:
        status = "no_results"
    else:
        status = "below_threshold"
    best_rejected = next(({k: s[k] for k in ("template_id", "R", "s_text", "s_vis",
                                              "s_rank", "hashed")}
                          for s in scored if not s["accepted"]), None)
    return {"status": status, "best_rejected": best_rejected}


def stamps() -> dict[str, str]:
    """The versions a selection was made under; any of them moving
    re-selects every frame (offline — no fetch)."""
    return {"query_version": QUERY_VERSION, "parser_version": ip.PARSER_VERSION,
            "visual_version": visual.VISUAL_VERSION,
            "relevance_version": RELEVANCE_VERSION, "dedup_version": DEDUP_VERSION,
            "selection_version": SELECTION_VERSION}
