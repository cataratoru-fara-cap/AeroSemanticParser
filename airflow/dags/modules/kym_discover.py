#!/usr/bin/env python3
"""
kym_discover.py  —  Know Your Meme URL Discovery (library + thin CLI)
=====================================================================
Discovers (nearly) every entry URL on knowyourmeme.com.

Design rules (why this looks different from v1)
-----------------------------------------------
1. NO mutable module state. Run-scoped knowledge (namespace patterns,
   listing pages) lives in an explicit, frozen ``Taxonomy`` object that is
   passed to whoever needs it. Tunables live in ``CrawlConfig``. This makes
   every function safe to call from a separate process (Airflow task).
2. Functions RETURN new/changed records; they never mutate a shared index
   in place. The caller decides where results go (JSON, Mongo, XCom, ...).
3. NO persistence here. JSON/Mongo sinks live in ``kym_store``; this module
   only discovers. ``main()`` is a thin CLI shell wiring the two together —
   the exact same functions an Airflow DAG calls.

Phases
------
  Phase 1  — Sitemaps          fetch_all_sitemaps()
  Phase 1b — Taxonomy          infer_taxonomy()
  Phase 2  — Listing crawl     crawl_categories() / crawl_listing()

Usage
-----
    python kym_discover.py                       # full run -> kym_urls.json
    python kym_discover.py --sitemap-only        # sitemaps only
    python kym_discover.py --max-category-pages 5
    python kym_discover.py --mongo               # also upsert into MongoDB
    python kym_discover.py --fresh               # ignore existing file

Requires:  requests, beautifulsoup4, lxml   (pymongo only for --mongo)
"""

from __future__ import annotations

import argparse
import gzip
import logging
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, replace
from math import inf
from typing import Iterable
from urllib.parse import urljoin, urlparse

# ---------------------------------------------------------------------------
# Static site knowledge (true constants — never mutated at runtime)
# ---------------------------------------------------------------------------

BASE_URL = "https://knowyourmeme.com"
ROBOTS_URL = f"{BASE_URL}/robots.txt"
SITEMAP_FALLBACK = f"{BASE_URL}/sitemap.xml"

USER_AGENT = "MemeAtlas-Research-Indexer/1.0"

# Top-level sections whose deep URLs are real entries (not nav/listing pages).
ENTRY_ROOTS = frozenset({
    "memes", "cultures", "subcultures", "people", "sites", "events",
    "editorials", "videos", "photos", "sensitive",
})

# Single-segment paths under a section that are status filters / listing
# roots, never entry slugs (/memes/<slug> is an entry, /memes/all is not).
KNOWN_CATEGORY_SEGMENTS = frozenset({
    "all", "new", "confirmed", "submissions", "submission", "deadpool",
    "researching", "newsworthy", "popular", "people", "events",
    "subcultures", "sites", "editorials", "videos", "photos", "cultures",
    "guides", "interviews", "page",
    # Nav/sidebar pages present on every listing page — not entries.
    "trending", "templates", "collections", "white-papers", "insights",
    "episode-notes", "behind-the-scenes", "meme-review", "in-the-media",
    "poll", "meme-insider", "rules-and-guidelines", "the-style-guide",
})

# Fallback namespace patterns (longest prefix first). A run normally replaces
# these with infer_taxonomy() output, carried in a Taxonomy instance.
DEFAULT_NAMESPACE_PATTERNS: tuple[tuple[str, str], ...] = (
    ("/sensitive/memes/events/", "sensitive/memes/events"),
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
)

# Status/listing pages for Phase 2 as (path, confirmed_default). Sitemap
# entries are Confirmed=True and never downgraded, so /memes/all can safely
# default to False. These listings are exactly what the sitemap omits.
DEFAULT_LISTINGS: tuple[tuple[str, bool], ...] = (
    ("/memes/all", False),
    ("/memes/confirmed", True),
    ("/memes/submissions", False),
    ("/memes/researching", False),
    ("/memes/deadpool", False),
)

MIN_NAMESPACE_URL_COUNT = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kym_discover")


# ---------------------------------------------------------------------------
# Run-scoped objects: Taxonomy (what the site looks like) and CrawlConfig
# (how politely/hard to crawl). Both frozen — nothing can mutate them mid-run.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Taxonomy:
    """Namespace patterns + listing pages for one discovery run.

    Serialisable via to_dict()/from_dict() so it survives JSON transports
    (Airflow XCom turns tuples into lists; from_dict coerces them back).
    """
    patterns: tuple[tuple[str, str], ...] = DEFAULT_NAMESPACE_PATTERNS
    listings: tuple[tuple[str, bool], ...] = DEFAULT_LISTINGS

    def namespace_of(self, url_path: str) -> str | None:
        """Return the KYM namespace label for a URL path, or None."""
        url_path = url_path.rstrip("/")
        for prefix, label in self.patterns:
            if url_path.startswith(prefix):
                return label
        return None

    def to_dict(self) -> dict:
        return {
            "patterns": [list(p) for p in self.patterns],
            "listings": [list(l) for l in self.listings],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Taxonomy":
        return cls(
            patterns=tuple((str(p), str(l)) for p, l in data["patterns"]),
            listings=tuple((str(p), bool(c)) for p, c in data["listings"]),
        )


DEFAULT_TAXONOMY = Taxonomy()


@dataclass(frozen=True)
class CrawlConfig:
    """Politeness / retry tunables. Override per run instead of editing globals."""
    sitemap_delay: float = 0.5        # seconds between sitemap fetches
    crawl_delay: float = 0.8          # seconds between listing-page fetches
    max_retries: int = 5
    request_timeout: int = 30
    # Early stop after N consecutive pages with 0 new entries. None (default)
    # disables it: a listing is crawled until it has no next page (or
    # max_pages_per_listing is hit). Set e.g. 3 for quick incremental runs.
    consecutive_empty_limit: int | None = None
    max_pages_per_listing: float = inf


DEFAULT_CONFIG = CrawlConfig()


# ---------------------------------------------------------------------------
# HTTP helpers (requests imported lazily so the module imports without it)
# ---------------------------------------------------------------------------

def make_session():
    """Create a requests Session with the bot User-Agent pre-configured."""
    import requests
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def fetch(url: str, session, cfg: CrawlConfig = DEFAULT_CONFIG,
          delay: float = 0.0) -> bytes | None:
    """
    Fetch a URL and return raw bytes, retrying transient errors with backoff.
    Returns None if all attempts fail.
    """
    import requests
    time.sleep(delay)
    for attempt in range(cfg.max_retries):
        try:
            response = session.get(url, timeout=cfg.request_timeout)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            if attempt == cfg.max_retries - 1:
                log.error("Gave up on %s after %d attempts: %s",
                          url, cfg.max_retries, exc)
                return None
            wait_seconds = 4 ** attempt
            log.warning("Attempt %d failed for %s: %s — retrying in %ds",
                        attempt + 1, url, exc, wait_seconds)
            time.sleep(wait_seconds)
    return None


def decompress_if_gzip(raw: bytes) -> bytes:
    """Transparently decompress gzip-encoded bytes (KYM serves .xml.gz)."""
    if raw[:2] == b"\x1f\x8b":
        return gzip.decompress(raw)
    return raw


# ---------------------------------------------------------------------------
# URL classification / record construction
# ---------------------------------------------------------------------------

def is_entry_url(url: str) -> bool:
    """
    True if ``url`` points at an actual KYM entry rather than a listing/nav
    page. An entry has a recognised top-level section, depth >= 2, and a
    final segment that is not a known status/listing word. Intentionally
    CSS-agnostic so listing-page redesigns don't break discovery.
    """
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc not in ("knowyourmeme.com", "www.knowyourmeme.com"):
        return False
    segments = [s for s in parsed.path.split("/") if s]
    if len(segments) < 2:
        return False
    if segments[0] not in ENTRY_ROOTS:
        return False
    if segments[-1] in KNOWN_CATEGORY_SEGMENTS or segments[-1].isdigit():
        return False
    return True


def make_record(url: str, lastmod: str | None, existing_record: dict | None,
                taxonomy: Taxonomy = DEFAULT_TAXONOMY,
                confirmed: bool | None = None) -> dict:
    """
    Build a discovery record. ``last_scraped`` from an existing record is
    preserved. ``Confirmed`` defaults to "has a sitemap lastmod" unless an
    explicit value is supplied (e.g. from a status listing).

    Invariant: only confirmed entries carry a ``lastmod``; non-confirmed
    entries always have ``lastmod = None``.
    """
    if confirmed is None:
        confirmed = lastmod is not None
    if not confirmed:
        lastmod = None
    return {
        "url": url,
        "namespace": taxonomy.namespace_of(urlparse(url).path),
        "Confirmed": confirmed,
        "lastmod": lastmod,
        "page_template_type": None,
        "last_scraped": existing_record.get("last_scraped") if existing_record else None,
    }


# ---------------------------------------------------------------------------
# Phase 1 — Sitemap discovery
# ---------------------------------------------------------------------------

def discover_sitemap_roots(session, cfg: CrawlConfig = DEFAULT_CONFIG) -> list[str]:
    """Read robots.txt and return all Sitemap: URLs (fallback: /sitemap.xml)."""
    log.info("Reading robots.txt: %s", ROBOTS_URL)
    raw = fetch(ROBOTS_URL, session, cfg)
    if raw is None:
        log.warning("robots.txt unreachable — using fallback %s", SITEMAP_FALLBACK)
        return [SITEMAP_FALLBACK]

    roots = [
        line.split(":", 1)[1].strip()
        for line in raw.decode("utf-8", errors="replace").splitlines()
        if line.strip().lower().startswith("sitemap:")
    ]
    if not roots:
        log.warning("No Sitemap: lines in robots.txt — using fallback %s", SITEMAP_FALLBACK)
        return [SITEMAP_FALLBACK]

    log.info("Found %d sitemap root(s)", len(roots))
    return roots


def parse_sitemap(raw: bytes) -> tuple[list[str], list[tuple[str, str | None]]]:
    """
    Parse a sitemap XML doc. Returns (child_sitemap_urls, [(url, lastmod), ...]).
    Handles <sitemapindex> and <urlset> with any/no XML namespace.
    """
    child_sitemap_urls: list[str] = []
    url_entries: list[tuple[str, str | None]] = []

    try:
        root = ET.fromstring(decompress_if_gzip(raw))
    except ET.ParseError as exc:
        log.error("XML parse error: %s", exc)
        return child_sitemap_urls, url_entries

    if root.tag.startswith("{"):
        ns = root.tag[1:root.tag.index("}")]
        prefix = f"{{{ns}}}"
    else:
        prefix = ""
    root_tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

    def find_all(parent, tag):
        return parent.findall(f"{prefix}{tag}")

    def find_one(parent, tag):
        return parent.find(f"{prefix}{tag}")

    if root_tag == "sitemapindex":
        for sm in find_all(root, "sitemap"):
            loc = find_one(sm, "loc")
            if loc is not None and loc.text:
                child_sitemap_urls.append(loc.text.strip())
    elif root_tag == "urlset":
        for ue in find_all(root, "url"):
            loc = find_one(ue, "loc")
            lm = find_one(ue, "lastmod")
            if loc is None or not loc.text:
                continue
            lastmod = lm.text.strip() if (lm is not None and lm.text) else None
            url_entries.append((loc.text.strip(), lastmod))
    else:
        log.warning("Unrecognised sitemap root tag: %s", root_tag)

    return child_sitemap_urls, url_entries


def fetch_all_sitemaps(session, existing: dict[str, dict],
                       taxonomy: Taxonomy = DEFAULT_TAXONOMY,
                       cfg: CrawlConfig = DEFAULT_CONFIG,
                       ) -> tuple[dict[str, dict], dict]:
    """
    Walk the full sitemap tree.

    ``existing`` is READ-ONLY here — used to skip unchanged entries and to
    preserve last_scraped / a previously-set Confirmed. Returns
    (changed, stats) where ``changed`` holds only NEW or UPDATED records.
    The caller merges/upserts them wherever it likes.
    """
    changed: dict[str, dict] = {}
    stats = {"seen": 0, "added": 0, "updated": 0, "skipped": 0}
    queue = discover_sitemap_roots(session, cfg)
    visited: set[str] = set()

    while queue:
        sitemap_url = queue.pop(0)
        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)

        log.info("Fetching sitemap: %s", sitemap_url)
        raw = fetch(sitemap_url, session, cfg, delay=cfg.sitemap_delay)
        if raw is None:
            continue

        child_urls, url_entries = parse_sitemap(raw)
        for child in child_urls:
            if child not in visited:
                queue.append(child)

        for url, lastmod in url_entries:
            stats["seen"] += 1
            existing_record = changed.get(url) or existing.get(url)
            if existing_record and existing_record.get("lastmod") == lastmod:
                stats["skipped"] += 1
                continue
            # Preserve a previously-set Confirmed (monotonic) on update.
            confirmed = True if lastmod is not None else (
                existing_record.get("Confirmed", False) if existing_record else False
            )
            changed[url] = make_record(url, lastmod, existing_record,
                                       taxonomy, confirmed=confirmed)
            stats["updated" if url in existing else "added"] += 1

    log.info("Sitemaps done — seen=%d added=%d updated=%d skipped=%d",
             stats["seen"], stats["added"], stats["updated"], stats["skipped"])
    return changed, stats


# ---------------------------------------------------------------------------
# Phase 1b — Taxonomy inference
# ---------------------------------------------------------------------------

def infer_taxonomy(urls: Iterable[str]) -> Taxonomy:
    """
    Derive a Taxonomy (namespace patterns + listing pages) from a URL corpus.
    Pure: reads URLs, returns a new frozen object, mutates nothing.
    """
    urls = list(urls)
    if not urls:
        log.warning("Empty corpus — using default taxonomy")
        return DEFAULT_TAXONOMY

    prefix_counts: dict[str, int] = {}
    for url in urls:
        segs = [s for s in urlparse(url).path.rstrip("/").split("/") if s]
        if len(segs) >= 2:
            prefix = "/" + "/".join(segs[:-1]) + "/"
            prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1

    namespaces = [(p, p.strip("/")) for p, c in prefix_counts.items()
                  if c >= MIN_NAMESPACE_URL_COUNT]
    namespaces.sort(key=lambda pair: len(pair[0]), reverse=True)
    if "/memes/" not in {p for p, _ in namespaces}:
        namespaces.append(("/memes/", "memes"))

    # Listings: defaults always present, plus any 2-segment status pages seen.
    listings: list[tuple[str, bool]] = list(DEFAULT_LISTINGS)
    seen_paths = {p for p, _ in listings}
    for url in urls:
        segs = [s for s in urlparse(url).path.rstrip("/").split("/") if s]
        if len(segs) == 2 and segs[0] in ENTRY_ROOTS and segs[1] in KNOWN_CATEGORY_SEGMENTS:
            path = "/" + "/".join(segs)
            if path not in seen_paths:
                listings.append((path, False))
                seen_paths.add(path)

    log.info("Taxonomy inferred — %d namespace patterns, %d listings",
             len(namespaces), len(listings))
    return Taxonomy(patterns=tuple(namespaces), listings=tuple(listings))


# ---------------------------------------------------------------------------
# Phase 2 — Listing crawl
# ---------------------------------------------------------------------------

def extract_entry_links(html: str) -> list[str]:
    """
    Extract entry URLs from a KYM listing page by URL shape.

    Unlike a CSS-class filter (which breaks whenever KYM restyles its cards),
    this keeps every anchor whose resolved URL passes ``is_entry_url`` — so it
    captures top-level *and* sub-namespace entries and survives redesigns.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    found: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        full_url = urljoin(BASE_URL, anchor["href"]).split("?")[0].split("#")[0]
        if full_url in seen:
            continue
        if is_entry_url(full_url):
            found.append(full_url)
            seen.add(full_url)
    return found


def find_next_page(html: str, current_url: str) -> str | None:
    """
    Resolve the next-page URL from a listing page.

    Tries, in order: <link rel="next">, a.pagination__next / a.next_page,
    any anchor whose visible text is "Next", the icon-only a.page-button
    arrow KYM currently uses, and an explicit on-page link to page N+1 of
    the current listing. Returns an absolute URL or None. Following the
    site's own links avoids guessing KYM's pagination URL format.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    candidates = [
        soup.find("link", rel="next"),
        soup.find("a", rel="next"),
        soup.find("a", class_="pagination__next"),
        soup.find("a", class_="next_page"),
        soup.find("a", class_="page-button", attrs={"rel": "next"}),
    ]
    for tag in candidates:
        if tag and tag.get("href"):
            return urljoin(current_url, tag["href"])

    anchors = soup.find_all("a", href=True)

    for anchor in anchors:
        if anchor.get_text(strip=True).lower() in ("next", "next page", "next ›", "›"):
            return urljoin(current_url, anchor["href"])

    # KYM's next arrow has no rel/text — only a Font Awesome icon:
    # <a class="page-button" href="..."><i class="fa fa-chevron-right"></i></a>
    for anchor in anchors:
        if "page-button" not in (anchor.get("class") or []):
            continue
        icon = anchor.find("i")
        if icon is not None and "fa-chevron-right" in (icon.get("class") or []):
            return urljoin(current_url, anchor["href"])

    # Styling-agnostic fallback: follow the page's own link to page N+1.
    parsed_current = urlparse(current_url)
    path = parsed_current.path.rstrip("/")
    match = re.search(r"^(.*)/page/(\d+)$", path)
    listing_path, page_no = (match.group(1), int(match.group(2))) if match else (path, 1)
    successor = f"{listing_path}/page/{page_no + 1}"
    for anchor in anchors:
        target = urljoin(current_url, anchor["href"])
        parsed_target = urlparse(target)
        if parsed_target.netloc and parsed_target.netloc != parsed_current.netloc:
            continue
        if parsed_target.path.rstrip("/") == successor:
            return target
    return None


def crawl_listing(session, known_urls: set[str], path: str, confirmed: bool,
                  taxonomy: Taxonomy = DEFAULT_TAXONOMY,
                  cfg: CrawlConfig = DEFAULT_CONFIG) -> dict[str, dict]:
    """
    Paginate one listing and return ONLY the new records found.

    ``known_urls`` is read (copied) for dedup; the caller's set is never
    mutated. Stops when there is no next-page link or
    ``cfg.max_pages_per_listing`` is reached. If ``cfg.consecutive_empty_limit``
    is set (default: None = disabled), also stops after that many consecutive
    pages that add nothing new.
    """
    seen = set(known_urls)
    new_records: dict[str, dict] = {}
    consecutive_empty = 0
    page = 1
    page_url: str | None = f"{BASE_URL}{path}"

    while page_url and page <= cfg.max_pages_per_listing:
        raw = fetch(page_url, session, cfg, delay=cfg.crawl_delay)
        if raw is None:
            break
        html = raw.decode("utf-8", errors="replace")

        links = extract_entry_links(html)
        new_here = 0
        for url in links:
            if url not in seen:
                new_records[url] = make_record(url, lastmod=None,
                                               existing_record=None,
                                               taxonomy=taxonomy,
                                               confirmed=confirmed)
                seen.add(url)
                new_here += 1

        log.info("  %s page %d: %d links, %d new", path, page, len(links), new_here)

        consecutive_empty = consecutive_empty + 1 if new_here == 0 else 0
        if (cfg.consecutive_empty_limit is not None
                and consecutive_empty >= cfg.consecutive_empty_limit):
            log.info("  %d consecutive empty pages — stopping %s",
                     consecutive_empty, path)
            break

        next_url = find_next_page(html, page_url)
        if not next_url or next_url == page_url:
            log.info("  No next page — stopping %s", path)
            break
        page_url = next_url
        page += 1

    return new_records


def crawl_categories(session, known_urls: set[str],
                     taxonomy: Taxonomy = DEFAULT_TAXONOMY,
                     cfg: CrawlConfig = DEFAULT_CONFIG,
                     ) -> tuple[dict[str, dict], dict]:
    """
    Crawl every listing in ``taxonomy.listings`` sequentially.
    Returns (new_records, stats). ``known_urls`` is not mutated.
    """
    seen = set(known_urls)
    all_new: dict[str, dict] = {}
    for path, confirmed in taxonomy.listings:
        log.info("Crawling listing: %s (confirmed=%s)", path, confirmed)
        new = crawl_listing(session, seen, path, confirmed, taxonomy, cfg)
        all_new.update(new)
        seen.update(new)
    stats = {"added": len(all_new)}
    log.info("Listing crawl done — added=%d", stats["added"])
    return all_new, stats


# ---------------------------------------------------------------------------
# Reporting helper (shared by CLI and DAG summary task)
# ---------------------------------------------------------------------------

def summarize_index(index: dict[str, dict]) -> dict:
    """Compute the summary counts for a full index."""
    namespace_counts: dict[str, int] = {}
    for record in index.values():
        ns = record.get("namespace") or "unknown"
        namespace_counts[ns] = namespace_counts.get(ns, 0) + 1
    return {
        "total": len(index),
        "confirmed": sum(1 for r in index.values() if r.get("Confirmed")),
        "lastmod_null": sum(1 for r in index.values() if r.get("lastmod") is None),
        "namespaces": namespace_counts,
    }


# ---------------------------------------------------------------------------
# Entry point — thin CLI over the library (same calls the Airflow DAG makes)
# ---------------------------------------------------------------------------

def main() -> None:
    import kym_store as store
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Discover all URLs on knowyourmeme.com")
    parser.add_argument("--output", "-o", default="kym_urls.json", help="Output JSON file")
    parser.add_argument("--max-category-pages", type=int, default=None,
                        help="Limit listing pagination to N pages per listing")
    parser.add_argument("--sitemap-only", action="store_true",
                        help="Only run the sitemap phase (skip the listing crawl)")
    parser.add_argument("--mongo", action="store_true",
                        help="Also upsert the result into MongoDB (urls collection)")
    parser.add_argument("--no-file", action="store_true",
                        help="Skip writing the JSON file (use with --mongo)")
    parser.add_argument("--fresh", action="store_true",
                        help="Ignore existing output file, full rescan")
    parser.add_argument("--resume", action="store_true",
                        help="Continue from an existing output file")
    args = parser.parse_args()

    if args.fresh and args.resume:
        parser.error("--fresh and --resume are mutually exclusive")
    if args.sitemap_only and args.max_category_pages is not None:
        parser.error("--sitemap-only and --max-category-pages are mutually exclusive")
    if args.no_file and not args.mongo:
        parser.error("--no-file requires --mongo")

    cfg = DEFAULT_CONFIG
    if args.max_category_pages is not None:
        cfg = replace(cfg, max_pages_per_listing=args.max_category_pages)

    out_path = Path(args.output)
    session = make_session()
    index = {} if args.fresh else store.load_json(out_path)
    crawl_stats = {"added": 0}

    # Phase 1: sitemaps
    changed, sitemap_stats = fetch_all_sitemaps(session, index, DEFAULT_TAXONOMY, cfg)
    index.update(changed)
    if not args.no_file:
        store.save_json(out_path, index)

    # Phase 1b: taxonomy inference
    taxonomy = infer_taxonomy(index.keys())

    # Phase 2: listing crawl
    if not args.sitemap_only:
        new_records, crawl_stats = crawl_categories(session, set(index), taxonomy, cfg)
        index.update(new_records)
        if not args.no_file:
            store.save_json(out_path, index)

    # Optional MongoDB sink
    if args.mongo:
        store.mongo_upsert(index.values())

    # Summary
    summary = summarize_index(index)
    log.info("=" * 55)
    log.info("DISCOVERY COMPLETE")
    log.info("  Total URLs      : %d", summary["total"])
    log.info("  Confirmed       : %d", summary["confirmed"])
    log.info("  Sitemap — added : %d  updated: %d  skipped: %d",
             sitemap_stats["added"], sitemap_stats["updated"], sitemap_stats["skipped"])
    log.info("  Crawl   — added : %d", crawl_stats["added"])
    log.info("  lastmod=null    : %d", summary["lastmod_null"])
    log.info("  Output          : %s", "MongoDB" if args.no_file else out_path)
    log.info("Breakdown by namespace:")
    for ns in sorted(summary["namespaces"]):
        log.info("  %-35s %d", ns, summary["namespaces"][ns])
    log.info("=" * 55)


if __name__ == "__main__":
    main()