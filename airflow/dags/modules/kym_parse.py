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
    parent           h5.parent a  ("Part of a series on X")
    timestamps       div.entry-timestamps abbr.timeago[title] (ISO), labelled
                     by the preceding "Updated" / "Added" text

Children/siblings are NOT inline on live pages (they sit behind a
"/children" link). Both `children` and `siblings` were removed from the
schema as redundant with `parent` + the related_memes section — a
dedicated targeted-fetch stage would be needed to populate either
faithfully, and neither was ever populated by this parser.

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
# Template SectionKind addition, the nsfw->badges schema change, the
# children/siblings removal, tags moving from required to gated). parse_store
# compares this against a previously-stored entries doc to decide whether a
# re-parse is warranted even when the underlying DOM hasn't changed.
PARSER_VERSION = "1.2.0"

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
    return {"src": _abs(src),
            "alt": img.get("alt") or None,
            "caption": img.get("title") or None}


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
    return [{"name": a.get_text(strip=True), "url": _abs(a.get("href"))}
            for a in dd.find_all("a", href=True)
            if a.get("href", "").startswith("http")]


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
            refs.append({"index": current_index, "text": text or None, "url": href})
            current_index = None
    return refs


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
                "text": [], "links": [], "images": [],
            }
            continue
        if current is None:  # content before the first heading (nav, embeds)
            continue
        if name == "p":
            para = child.get_text(" ", strip=True)
            if para:
                current["text"].append(para)
            for a in child.find_all("a", href=True):
                url = _abs(a["href"])
                label = a.get_text(strip=True)
                if url and label and url.startswith("http"):
                    current["links"].append({"text": label, "url": url})
        for img in child.find_all("img", class_="kym-image"):
            d = _img_dict(img)
            if d:
                current["images"].append(d)
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
    parent = None
    if parent_el and not parent_el.get_text(strip=True).startswith("["):
        parent = _abs(parent_el["href"])

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
        "parent": parent,
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
                     "tags", "parent", "additional_references",
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