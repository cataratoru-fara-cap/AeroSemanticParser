#!/usr/bin/env python3
"""
kym_parse.py — KYM entry HTML -> KYMEntryScrape (library + thin CLI)
=====================================================================
Pure parsing: html in, validated model out. No Airflow imports, no Mongo
access (the sample CLI goes through dom_store's facade, per the store rule).

Selector map (verified against a live 2026 confirmed-meme page, Doge):

    canonical url    link[rel=canonical]
    title            h1.entry-title  (fallback h1.content-title, og:title)
    details sidebar  aside dl > dt/dd pairs: Status / Type: / Year / Origin
                     / Region / Also Known As / Additional References
    entry types      dd a[href*="/types/"]     (model slugifies)
    tags             dl#entry_tags a
    body             section.bodycopy: h2[id] = level-2 section anchors with
                     STABLE ids (about, origin, spread, search-interest, ...);
                     h4 = level-3 subsections; p = paragraphs; images are
                     lazy-loaded (img.kym-image: real URL in data-src, caption
                     in title, src is a blank gif)
    external refs    h2#external-references + div.references
                     ([n] -> #fnrN anchor, then the real link)
    series_parent    h5.parent a  ("Part of a series on X")
    timestamps       div.entry-timestamps abbr.timeago[title] (ISO), labelled
                     by the preceding "Updated" / "Added" text

Series relation: `series_parent` only, from the "Part of a series on X"
link — KYM's own framing for this relationship. An earlier version also
scraped `related_entries`/`related_sub_entries` from the inline "Related
(Sub-)entries" galleries; reverted as unreliable (fragile dual desktop/
mobile containers) and redundant (those galleries are truncated summaries
of the same series `series_parent` already points to — no information they
added wasn't reachable via series_parent). The legacy dump's `children`/
`siblings` fields have no equivalent here; use series_parent as the join
key back to the series instead.

NSFW/content-warning detection uses the sidebar's own 'Badges:' row (see
_badges()) rather than a URL-path inference — pages under /sensitive/
carry a 'Sensitive' badge there, which is authoritative.

CLI:
    python -m modules.kym_parse --file page.html [--url https://...]
    python -m modules.kym_parse --sample 100          # coverage report
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

from modules.kym_models import (
    Category,
    CorpusPolicy,
    KYMEntryScrape,
    SectionKind,
    corpus_ready,
)

log = logging.getLogger("kym_parse")

BASE_URL = "https://knowyourmeme.com"

# Fallback ONLY for namespaces_for() when a urls doc predates discovery's
# namespace field or has it null. The authoritative value always comes
# from urls.namespace (kym_discover's Taxonomy) — this is deliberately a
# small, dependency-free mirror of that pattern table, not an import of
# kym_discover, to keep this module's boundary (pure HTML parsing) intact.
_NAMESPACE_FALLBACK_PATTERNS: tuple[tuple[str, str], ...] = (
    ("/sensitive/memes/", "sensitive/memes"),
    ("/sensitive/", "sensitive"),
    ("/memes/subcultures/", "memes/subcultures"),
    ("/memes/events/", "memes/events"),
    ("/memes/people/", "memes/people"),
    ("/memes/sites/", "memes/sites"),
    ("/memes/", "memes"),
    ("/editorials/guides/", "editorials/guides"),
    ("/editorials/interviews/", "editorials/interviews"),
    ("/editorials/", "editorials"),
    ("/cultures/", "cultures"),
    ("/subcultures/", "subcultures"),
    ("/people/", "people"),
    ("/events/", "events"),
    ("/videos/", "videos"),
    ("/photos/", "photos"),
    ("/forums/", "forums"),
    ("/users/", "users"),
    ("/news/", "news"),
)


def infer_namespace_from_url(url: str) -> str:
    """Longest-prefix namespace guess from the URL path alone. Only used as
    a fallback when the urls collection doc has no namespace recorded —
    the authoritative source is always discovery's own Taxonomy."""
    path = urlparse(url).path
    for prefix, ns in _NAMESPACE_FALLBACK_PATTERNS:
        if path.startswith(prefix):
            return ns
    return "unknown"

# Bump whenever a selector or classifier change could alter parse output for
# ALREADY-scraped pages (e.g. the tags/additional_references fix, the
# Template SectionKind addition, the nsfw->badges schema change, tags moving
# from required to gated, the year lower-bound loosening, the malformed-URL
# repair layer). parse_store compares this against a previously-stored
# entries doc to decide whether a re-parse is warranted even when the
# underlying DOM hasn't changed.
# 1.6.0: positions (Link.paragraph/offset, Image.after_paragraph) and
# embedded posts (Section.embeds), for the event layer; see kym_models.
# 1.6.1: anchors on the same words as the previous anchor get a position too
# (see _locate).
PARSER_VERSION = "1.6.1"

# h2 id -> kind. Live pages give sections STABLE anchor ids, so this is the
# primary classifier; the text alias table below is the fallback for older
# markup where ids are missing.
_ID_KIND: dict[str, str] = {
    "about": "about",
    "origin": "origin",
    "origins": "origin",
    "spread": "spread",
    "search-interest": "search_interest",
    "notable-examples": "notable_examples",
    "various-examples": "various_examples",
    "related-memes": "related_memes",
    "external-references": "external_references",
}

# Normalised heading text -> kind. This is where the old dump's ~3000-key
# mess (misspellings, punctuation variants, embed junk) collapses into the
# canonical buckets. Extend as the coverage report surfaces new variants.
_TEXT_KIND: dict[str, str] = {
    "about": "about",
    "origin": "origin", "origins": "origin", "orgin": "origin",
    "origin and spread": "origin", "online origins": "origin",
    "spread": "spread", "spread and popularity": "spread",
    "spread & popularity": "spread", "spread and usage": "spread",
    "search interest": "search_interest", "search interests": "search_interest",
    "google insights": "search_interest", "google trends": "search_interest",
    "google insights for search": "search_interest",
    "interest over time": "search_interest",
    "notable examples": "notable_examples",
    "various examples": "various_examples", "examples": "various_examples",
    "example images": "various_examples",
    "related memes": "related_memes", "related entries": "related_memes",
    "external references": "external_references",
    "external reference": "external_references",
    "references": "external_references", "external links": "external_references",
    "external refrences": "external_references",  # yes, really in the data
}

_BLANK_IMG_RE = re.compile(r"/assets/blank-")
_FOOTNOTE_RE = re.compile(r"\[(\d+)\]")
# Scheme typos observed in KYM editors' wiki content: 'https;//' (semicolon
# for colon) and 'https//' (missing colon). Both start with 'http' so a
# startswith filter passes them straight into HttpUrl, which raises.
_SCHEME_TYPO_RE = re.compile(r"^(https?)\s*[;,]?\s*//", re.IGNORECASE)
_EMBEDDED_URL_RE = re.compile(r"https?://", re.IGNORECASE)
_PLAUSIBLE_URL_RE = re.compile(r"^https?://[^\s<>\"']+$", re.IGNORECASE)


def _clean_url(raw: str | None) -> str | None:
    """Repair the malformed-URL classes KYM editors actually produce, or
    return None if the string is unrecoverable.

    Observed in production failures (14 confirmed memes, 2026-07):
      * 'https;//knowyourmeme.com/...'  — semicolon-for-colon typo
      * 'https//knowyourmeme.com/...'   — missing colon
      * '%7Bwidth:425px%7Dhttps://i.kym-cdn.com/...' — wiki image-sizing
        directive ({width:425px}, URL-encoded) fused onto the src

    Rationale: these live in body-section links/images — bulk enrichment
    data, not identity fields. One editor typo must not fail the whole
    page; repair what's mechanically certain, drop the single item
    otherwise (caller logs it).
    """
    if not raw:
        return None
    s = raw.strip()
    # Fix scheme typos at the start.
    s = _SCHEME_TYPO_RE.sub(lambda m: m.group(1).lower() + "://", s)
    # Styling-prefix garbage: real URL begins at the first http(s)://.
    if not s.lower().startswith(("http://", "https://")):
        m = _EMBEDDED_URL_RE.search(s)
        if m:
            s = s[m.start():]
    return s if _PLAUSIBLE_URL_RE.match(s) else None

# URL namespace -> category (mirrors kym_discover's taxonomy; longest first).
_CATEGORY_BY_PREFIX: tuple[tuple[str, Category], ...] = (
    ("/memes/cultures/", Category.culture),
    ("/memes/subcultures/", Category.subculture),
    ("/memes/people/", Category.person),
    ("/memes/sites/", Category.site),
    ("/memes/events/", Category.event),
    ("/memes/", Category.meme),
    ("/cultures/", Category.culture),
    ("/subcultures/", Category.subculture),
    ("/people/", Category.person),
    ("/sites/", Category.site),
    ("/events/", Category.event),
)


def _category_from_url(url: str) -> Category:
    path = urlparse(url).path
    if path.startswith("/sensitive"):
        path = path[len("/sensitive"):]
    for prefix, cat in _CATEGORY_BY_PREFIX:
        if path.startswith(prefix):
            return cat
    return Category.unknown


def _classify(heading_id: str | None, heading_text: str) -> str:
    if heading_id and heading_id in _ID_KIND:
        return _ID_KIND[heading_id]
    key = re.sub(r"[:.\s]+$", "", heading_text.strip().lower())
    if key.startswith("template"):
        # Catches the whole family: "Template", "Templates", "Template / GIF",
        # "Template / Gut Genug Chorus Only", etc. — prefix, not exact match,
        # since the suffix after "/" is often a one-off track/format name.
        return "template"
    return _TEXT_KIND.get(key, "other")


def _abs(href: str | None) -> str | None:
    if not href:
        return None
    return urljoin(BASE_URL, href.split("#")[0]) if href.startswith("/") else href


def _img_dict(img) -> dict | None:
    src = img.get("data-src") or img.get("src")
    if not src or _BLANK_IMG_RE.search(src):
        return None
    cleaned = _clean_url(_abs(src))
    if cleaned is None:
        log.debug("Dropped unrecoverable image src %r", src[:120])
        return None
    return {"src": cleaned,
            "alt": img.get("alt") or None,
            "caption": img.get("title") or None}


# Embedded posts, by the markup KYM actually uses — surveyed on a random
# 400-page sample (2026-09-18): tiktok-embed (382), twitter-tweet(-lazy)
# (122), lazy-iframe with data-src (instagram/youtube/vine/rumble ...),
# instagram-media(-lazy), <video>, imgur-embed-pub. A plain <blockquote>
# with no class is a text quotation, not an embed, and is not captured.
_TWEET_URL_RE = re.compile(r"https?://(?:www\.|mobile\.)?(?:twitter|x)\.com/[^/]+/status/\d+")
_IFRAME_PLATFORMS: tuple[tuple[str, str], ...] = (
    ("youtube.com", "youtube"), ("youtu.be", "youtube"),
    ("instagram.com", "instagram"), ("vine.co", "vine"),
    ("rumble.com", "rumble"), ("streamable.com", "streamable"),
    ("twitch.tv", "twitch"), ("vimeo.com", "vimeo"),
    ("soundcloud.com", "soundcloud"), ("spotify.com", "spotify"),
    ("tiktok.com", "tiktok"), ("twitter.com", "twitter"), ("x.com", "twitter"),
    ("reddit.com", "reddit"), ("facebook.com", "facebook"),
    ("dailymotion.com", "dailymotion"), ("bilibili.com", "bilibili"),
)


def _platform_of(url: str) -> str:
    host = urlparse(url).netloc.lower()
    for domain, platform in _IFRAME_PLATFORMS:
        if host == domain or host.endswith("." + domain):
            return platform
    return "other"


def _embed_url(el) -> tuple[str | None, str | None]:
    """(url, platform) for one embed element, or (None, None)."""
    classes = set(el.get("class") or [])
    if el.name == "blockquote":
        if "tiktok-embed" in classes:
            return el.get("cite"), "tiktok"
        if classes & {"twitter-tweet", "twitter-tweet-lazy", "twitter-video"}:
            # The permalink is the date link at the END of the quote; the
            # first link is usually a t.co link inside the tweet text.
            for a in reversed(el.find_all("a", href=True)):
                m = _TWEET_URL_RE.match(a["href"])
                if m:
                    return m.group(0), "twitter"
            return None, None
        if classes & {"instagram-media", "instagram-media-lazy"}:
            link = el.get("data-instgrm-permalink")
            if not link:
                a = el.find("a", href=True)
                link = a["href"] if a else None
            return (link.split("?", 1)[0] if link else None), "instagram"
        if "imgur-embed-pub" in classes and el.get("data-id"):
            return f"https://imgur.com/{el['data-id']}", "imgur"
        if classes & {"reddit-card", "reddit-embed-bq"}:
            a = el.find("a", href=True)
            return (a["href"] if a else None), "reddit"
        return None, None       # a plain quotation, not an embed
    if el.name == "iframe":
        if "google-trends-iframe" in classes:
            return None, None   # the Search Interest chart, not a post
        src = el.get("data-src") or el.get("src")
        return src, (_platform_of(src) if src else None)
    if el.name == "video":
        src = el.get("src")
        if not src:
            source = el.find("source", src=True)
            src = source["src"] if source else None
        return src, "video"
    return None, None


def _embeds(child) -> list[dict]:
    """Every embedded post in one bodycopy child, in document order."""
    found = ([child] if getattr(child, "name", None) in ("blockquote", "iframe", "video")
             else [])
    found += child.find_all(["blockquote", "iframe", "video"])
    out: list[dict] = []
    seen: set[str] = set()
    for el in found:
        raw, platform = _embed_url(el)
        url = _clean_url(_abs(raw)) if raw else None
        if url and url not in seen:
            seen.add(url)
            out.append({"url": url, "platform": platform or "other"})
    return out


# ---------------------------------------------------------------------------
# Piece extractors (each takes soup, returns plain data; parse_entry composes)
# ---------------------------------------------------------------------------

def _sidebar(soup) -> dict:
    """dt/dd pairs from the details <dl>; keys lowercased, colon-stripped."""
    out: dict = {}
    status_dt = soup.find("dt", string=re.compile(r"^\s*Status\s*$"))
    if not status_dt:
        return out
    dl = status_dt.find_parent("dl")
    for dt in dl.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if dd is None:
            continue
        key = dt.get_text(strip=True).lower().rstrip(":")
        out[key] = dd
    return out


def _tags(soup) -> list[str]:
    """True tags carry a data-tag attribute. NOTE: dl#entry_tags is a shared
    container — it holds the Tags dt/dd AND the Additional References
    dt/dd side by side, so a bare 'dl#entry_tags a' selector bleeds
    reference links into tags. data-tag is the reliable discriminator;
    dt-text scoping below is a fallback if KYM ever drops the attribute."""
    tagged = [a.get_text(strip=True) for a in soup.select("dl#entry_tags a[data-tag]")]
    if tagged:
        return tagged
    dt = soup.find("dt", string=re.compile(r"^\s*Tags\s*$"))
    dd = dt.find_next_sibling("dd") if dt else None
    return [a.get_text(strip=True) for a in dd.find_all("a")] if dd else []


def _badges(soup) -> list[str]:
    """Sidebar 'Badges:' dt/dd — e.g. 'Sensitive' on /sensitive/ pages.
    This is now the AUTHORITATIVE content-warning signal (replaces the old
    URL-path-based nsfw:bool field, which the sidebar itself makes
    redundant). dd content observed as plain text ('Sensitive'), but handle
    a link-based dd too in case KYM ever wraps badge names in <a>."""
    dt = soup.find("dt", string=re.compile(r"^\s*Badges\s*:?\s*$"))
    if not dt:
        return []
    dd = dt.find_next_sibling("dd")
    if not dd:
        return []
    links = [a.get_text(strip=True) for a in dd.find_all("a")]
    if links:
        return links
    text = dd.get_text(" ", strip=True)
    return [b.strip() for b in text.split(",") if b.strip()]


def _additional_refs(soup) -> list[dict]:
    dt = soup.find("dt", string=re.compile(r"(?i)^\s*additional references\s*:?\s*$"))
    if not dt:
        return []
    dd = dt.find_next_sibling("dd")
    if not dd:
        return []
    out = []
    for a in dd.find_all("a", href=True):
        url = _clean_url(_abs(a.get("href")))
        if url:
            out.append({"name": a.get_text(strip=True), "url": url})
    return out


def _external_refs(soup) -> list[dict]:
    box = soup.select_one("div.references")
    if not box:
        return []
    refs: list[dict] = []
    current_index: int | None = None
    for a in box.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        m = _FOOTNOTE_RE.fullmatch(text)
        if m and href.startswith("#"):
            current_index = int(m.group(1))
            continue
        if href.startswith("http"):
            url = _clean_url(href)
            if url:
                refs.append({"index": current_index, "text": text or None,
                             "url": url})
            else:
                log.debug("Dropped unrecoverable reference href %r", href[:120])
            current_index = None
    return refs


def _locate(para: str, needle: str, cursor: int, outer: int | None) -> int:
    """Where an anchor's text sits in its paragraph, searching in order.

    Normally the next occurrence at or after ``cursor``. But KYM often puts
    two anchors on the SAME words — its auto-link nested inside a hand-made
    link, ``<a …/facebook-meta><strong><em><a …/facebook>Facebook</a>…`` —
    and before 1.6.1 the inner one was searched for past the outer one's
    words: 445 of 237,713 links got no position, or the wrong one. So a
    nested anchor (``outer`` = where its enclosing anchor began) is looked
    for there first; failing everything, the nearest occurrence before the
    cursor.
    """
    found = -1
    if outer is not None:
        found = para.find(needle, outer)
    if found < 0:
        found = para.find(needle, cursor)
    if found < 0:
        found = para.rfind(needle, 0, cursor)
    return found


def _sections(soup) -> list[dict]:
    body = soup.select_one("section.bodycopy")
    if not body:
        return []
    sections: list[dict] = []
    current: dict | None = None

    def flush():
        nonlocal current
        if current is not None:
            sections.append(current)
            current = None

    for child in body.children:
        name = getattr(child, "name", None)
        if name is None or name == "table":  # skip strings + the TOC table
            continue
        if name in ("h2", "h4"):
            flush()
            text = child.get_text(" ", strip=True)
            current = {
                "heading": text,
                "kind": _classify(child.get("id"), text),
                "level": 2 if name == "h2" else 3,
                "text": [], "links": [], "images": [], "embeds": [],
            }
            continue
        if current is None:  # content before the first heading (nav, embeds)
            continue
        if name == "p":
            para = child.get_text(" ", strip=True)
            if para:
                current["text"].append(para)
            index = len(current["text"]) - 1 if para else None
            cursor = 0          # anchors are found left to right, in order
            spans: list[tuple] = []   # (anchor element, where its text began)
            for a in child.find_all("a", href=True):
                url = _clean_url(_abs(a["href"]))
                label = a.get_text(strip=True)
                if url and label:
                    offset = None
                    if para:
                        # An anchor NESTED inside an earlier one covers some
                        # of the same words, so it is searched for from
                        # where that one began.
                        outer = next((start for el, start in reversed(spans)
                                      if el in a.parents), None)
                        # The paragraph was joined with " ", so search for
                        # the anchor the same way; fall back to the plain
                        # label for anchors that are one text node anyway.
                        for needle in (a.get_text(" ", strip=True), label):
                            found = _locate(para, needle, cursor, outer)
                            if found >= 0:
                                offset = found
                                spans.append((a, found))
                                cursor = max(cursor, found + len(needle))
                                break
                    current["links"].append({"text": label, "url": url,
                                             "paragraph": index,
                                             "offset": offset})
                elif label and a["href"].strip():
                    log.debug("Dropped unrecoverable link href %r (%r)",
                              a["href"][:120], label[:40])
        after = len(current["text"]) - 1
        for img in child.find_all("img", class_="kym-image"):
            d = _img_dict(img)
            if d:
                d["after_paragraph"] = after
                current["images"].append(d)
        for embed in _embeds(child):
            embed["after_paragraph"] = after
            current["embeds"].append(embed)
    flush()
    return sections


def _timestamps(soup) -> tuple[int | None, int | None]:
    """(kym_last_updated, kym_added) as unix seconds, from abbr.timeago."""
    updated = added = None
    box = soup.select_one("div.entry-timestamps")
    if not box:
        return None, None
    for abbr in box.find_all("abbr", class_="timeago"):
        iso = abbr.get("title")
        label = (abbr.find_previous(string=True) or "").strip().lower()
        try:
            ts = int(datetime.fromisoformat(iso).timestamp())
        except (TypeError, ValueError):
            continue
        if "updated" in label:
            updated = ts
        elif "added" in label:
            added = ts
    return updated, added


def _meta(soup) -> dict[str, str]:
    out: dict[str, str] = {}
    for tag in soup.find_all("meta"):
        key = tag.get("property") or tag.get("name")
        content = tag.get("content")
        if key and content and (key.startswith(("og:", "twitter:"))
                                or key == "description"):
            out[key] = content
    return out


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def parse_entry(html: str, url: str | None = None,
                fetched_at: datetime | None = None) -> KYMEntryScrape:
    """Parse one KYM entry page. Raises pydantic.ValidationError on a page
    that is not a well-formed confirmed-meme entry (missing origin/tags/...).
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    canonical = soup.find("link", rel="canonical")
    page_url = (canonical.get("href") if canonical else None) or url
    if not page_url:
        raise ValueError("no canonical link and no url provided")

    h1 = soup.select_one("h1.entry-title") or soup.select_one("h1.content-title")
    meta = _meta(soup)
    title = (h1.get_text(strip=True) if h1 else None) \
        or (meta.get("og:title") or "").removesuffix(" | Know Your Meme").strip()

    side = _sidebar(soup)

    def dd_text(key: str) -> str | None:
        dd = side.get(key)
        return dd.get_text(" ", strip=True) if dd is not None else None

    entry_type = [a.get("href") for a in side.get("type", []).find_all("a", href=True)] \
        if side.get("type") is not None else []
    region_raw = dd_text("region")
    region = [r.strip() for r in region_raw.split(",")] if region_raw else []
    aka_raw = dd_text("also known as") or dd_text("aka")
    aliases = [a.strip() for a in aka_raw.split(",")] if aka_raw else []

    tags = _tags(soup)  # [] is valid now — gated by CorpusPolicy, not schema-required

    parent_el = soup.select_one("h5.parent a[href]")
    series_parent = None
    if parent_el and not parent_el.get_text(strip=True).startswith("["):
        series_parent = _clean_url(_abs(parent_el["href"]))

    updated, added = _timestamps(soup)
    og_image = meta.get("og:image")

    return KYMEntryScrape.model_validate({
        "url": page_url,
        "title": title,
        "category": _category_from_url(page_url).value,
        "status": dd_text("status"),
        "entry_type": entry_type,
        "year": dd_text("year"),
        "origin": dd_text("origin"),
        "region": region,
        "aliases": aliases,
        "tags": tags,
        "badges": _badges(soup),
        "template_image_url": og_image,
        "og_image": og_image,
        "series_parent": series_parent,
        "additional_references": _additional_refs(soup),
        "external_references": _external_refs(soup),
        "sections": _sections(soup),
        "meta": meta,
        "kym_last_updated": updated,
        "kym_added": added,
        "scraped_at": fetched_at,
    })


# ---------------------------------------------------------------------------
# Coverage sampling (reads via dom_store's facade — no Mongo access here)
# ---------------------------------------------------------------------------

def run_sample(limit: int = 100,
               policy: CorpusPolicy | None = None) -> dict:
    """Parse ``limit`` stored confirmed-meme DOMs and report field coverage.
    This is the measurement that decides CorpusPolicy (require_region etc.).
    """
    from pydantic import ValidationError
    from modules import dom_store  # lazy: keeps module importable w/o pymongo

    policy = policy or CorpusPolicy()
    field_hits: Counter = Counter()
    kind_hits: Counter = Counter()
    other_headings: Counter = Counter()
    gate_missing: Counter = Counter()
    parsed = failed = gate_pass = 0
    failures: list[tuple[str, str]] = []

    for page_url, html in dom_store.iter_ok_html(limit=limit,
                                                 namespaces=["memes"],
                                                 confirmed_only=True):
        try:
            entry = parse_entry(html, url=page_url)
        except (ValidationError, ValueError) as exc:
            failed += 1
            failures.append((page_url, str(exc).splitlines()[0]))
            continue
        parsed += 1
        for name in ("year", "origin", "region", "entry_type", "aliases",
                     "tags", "series_parent", "additional_references",
                     "external_references"):
            if getattr(entry, name):
                field_hits[name] += 1
        for s in entry.sections:
            kind_hits[s.kind] += 1
            if s.kind == "other":
                other_headings[s.heading] += 1
        ready, missing = corpus_ready(entry, policy)
        gate_pass += ready
        for m in missing:
            gate_missing[m] += 1

    report = {
        "sampled": parsed + failed, "parsed": parsed, "failed": failed,
        "gate_pass": gate_pass,
        "field_coverage_pct": {k: round(100 * v / parsed, 1)
                               for k, v in sorted(field_hits.items())} if parsed else {},
        "section_kinds": dict(kind_hits.most_common()),
        "top_unclassified_headings": dict(other_headings.most_common(15)),
        "gate_missing_counts": dict(gate_missing.most_common()),
        "parse_failures": failures[:10],
    }
    return report


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(message)s",
                        datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--file", help="parse one local HTML file and print JSON")
    ap.add_argument("--url", help="entry URL (fallback if no canonical link)")
    ap.add_argument("--sample", type=int, default=0,
                    help="parse N stored confirmed-meme DOMs, print coverage")
    args = ap.parse_args(argv)

    if args.file:
        with open(args.file, encoding="utf-8") as fh:
            entry = parse_entry(fh.read(), url=args.url)
        print(entry.model_dump_json(indent=2, exclude_none=True))
        ready, missing = corpus_ready(entry)
        print(f"\ncorpus_ready={ready}  missing={missing}", file=sys.stderr)
        return 0

    if args.sample:
        print(json.dumps(run_sample(limit=args.sample), indent=2,
                         ensure_ascii=False))
        return 0

    ap.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())