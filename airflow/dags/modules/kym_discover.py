#!/usr/bin/env python3
"""
kym_discover.py  —  KnowYourMeme URL Discovery
===============================================
Discovers all URLs on knowyourmeme.com using two complementary phases:

  Phase 1 — Sitemaps
    Reads robots.txt to find the sitemap index (robust to URL drift),
    then walks every child sitemap. Handles gzip-compressed .xml.gz files.

  Phase 2 — Category crawling  (optional, --sitemap-only to skip)
    Paginates through known category listing pages to catch any URLs
    that KYM omits from their sitemaps.

Incremental runs: URLs already in the output file are skipped unless
their lastmod has changed. Use --fresh to force a full rescan.

Output: JSON file  { "metadata": {...}, "urls": [...] }

Usage:
    python kym_discover.py                      # full run
    python kym_discover.py --sitemap-only       # skip category crawl
    python kym_discover.py --fresh              # ignore existing file
    python kym_discover.py --resume             # continue interrupted run
    python kym_discover.py --output my.json     # custom output path

Requires:
    pip install requests beautifulsoup4 lxml
"""

import argparse
import gzip
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL        = "https://knowyourmeme.com"
ROBOTS_URL      = f"{BASE_URL}/robots.txt"
SITEMAP_FALLBACK = f"{BASE_URL}/sitemap.xml"
OUTPUT_FILE     = "kym_urls.json"

SITEMAP_DELAY   = 1   # seconds between sitemap file fetches
CRAWL_DELAY     = 1.5   # seconds between category page fetches
MAX_RETRIES     = 4
REQUEST_TIMEOUT = 30
MAX_CATEGORY_PAGES = 500  # safety cap

USER_AGENT = (
    "MemeAtlas-Research-Bot/1.0 "
    "(academic meme corpus; https://github.com/example/memeatlas)"
)

# Category paths to paginate through in Phase 2.
# Each entry is (path, namespace_label).
# Longer/more-specific paths must come before shorter ones
# so that link filtering works correctly.
CATEGORY_PAGES = [
    ("/memes/all",      "memes/all"),
    # ("/memes/deadpool",      "memes/deadpool"),
    # ("/memes/subcultures", "memes/subcultures"),
    # ("/memes/sites",       "memes/sites"),
    # ("/memes",             "memes"),
]

# Same ordering rule: longest prefix first.
NAMESPACE_PATTERNS = [
    ("/sensitive/memes/events/",     "sensitive/memes/events"),
    ("/sensitive/memes/",            "sensitive/memes"),
    ("/sensitive/",                  "sensitive"),
    ("/memes/subcultures/",          "memes/subcultures"),
    ("/memes/events/",               "memes/events"),
    ("/memes/people/",               "memes/people"),
    ("/memes/sites/",                "memes/sites"),
    ("/memes/",                      "memes"),
    ("/editorials/guides/",          "editorials/guides"),
    ("/editorials/interviews/",      "editorials/interviews"),
    ("/editorials/",                 "editorials"),
    ("/videos/",                     "videos"),
    ("/photos/",                     "photos"),
]

SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kym_discover")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def fetch(url: str, session: requests.Session, delay: float = 0.0) -> bytes | None:
    """
    Fetch a URL and return raw bytes, or None on permanent failure.
    Retries with exponential backoff on transient errors.
    Always waits `delay` seconds before the first attempt.
    """
    time.sleep(delay)
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.content
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES - 1:
                log.error("Gave up on %s after %d attempts: %s", url, MAX_RETRIES, exc)
                return None
            wait = 5** attempt
            log.warning("Attempt %d failed for %s: %s — retrying in %ds",
                        attempt + 1, url, exc, wait)
            time.sleep(wait)


def decompress(raw: bytes) -> bytes:
    """Transparently decompress gzip content (KYM serves .xml.gz sitemaps)."""
    if raw[:2] == b"\x1f\x8b":
        return gzip.decompress(raw)
    return raw


# ---------------------------------------------------------------------------
# URL metadata
# ---------------------------------------------------------------------------

def namespace_of(path: str) -> str | None:
    """Return the namespace label for a URL path, or None if unrecognised."""
    path = path.rstrip("/")
    for prefix, label in NAMESPACE_PATTERNS:
        if path.startswith(prefix):
            return label
    return None


def make_record(url: str, lastmod: str | None, existing: dict | None) -> dict:
    """Build a URL record, preserving first_seen from any existing record."""
    return {
        "url":          url,
        "namespace":    namespace_of(urlparse(url).path),
        "lastmod":      lastmod,
        "first_seen":   existing["first_seen"] if existing else datetime.now(timezone.utc).isoformat(),
        "last_scraped": existing.get("last_scraped") if existing else None,
    }


# ---------------------------------------------------------------------------
# Phase 1 — Sitemap discovery
# ---------------------------------------------------------------------------

def discover_sitemap_roots(session: requests.Session) -> list[str]:
    """Read robots.txt and extract Sitemap: lines. Falls back to /sitemap.xml."""
    log.info("Reading robots.txt: %s", ROBOTS_URL)
    raw = fetch(ROBOTS_URL, session)
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

    log.info("Found %d sitemap root(s): %s", len(roots), roots)
    return roots


def parse_sitemap(raw: bytes) -> tuple[list[str], list[tuple[str, str | None]]]:
    """
    Parse a sitemap XML (index or urlset).
    Returns (child_sitemap_urls, [(url, lastmod), ...]).
    """
    child_sitemaps: list[str]                  = []
    url_entries:    list[tuple[str, str|None]] = []

    try:
        root = ET.fromstring(decompress(raw))
    except ET.ParseError as exc:
        log.error("XML parse error: %s", exc)
        return child_sitemaps, url_entries

    tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag  # strip namespace

    if tag == "sitemapindex":
        for sm in root.findall(f"{{{SITEMAP_NS}}}sitemap"):
            loc = sm.find(f"{{{SITEMAP_NS}}}loc")
            if loc is not None and loc.text:
                child_sitemaps.append(loc.text.strip())

    elif tag == "urlset":
        for url_el in root.findall(f"{{{SITEMAP_NS}}}url"):
            loc = url_el.find(f"{{{SITEMAP_NS}}}loc")
            if loc is None or not loc.text:
                continue
            lm  = url_el.find(f"{{{SITEMAP_NS}}}lastmod")
            url_entries.append((
                loc.text.strip(),
                lm.text.strip() if lm is not None and lm.text else None,
            ))

    return child_sitemaps, url_entries


def fetch_all_sitemaps(
    session:  requests.Session,
    existing: dict[str, dict],
) -> tuple[dict[str, dict], dict]:
    """
    Walk the full sitemap tree. Returns (updated index, stats).
    Skips URLs whose lastmod hasn't changed from the existing record.
    """
    index  = dict(existing)
    stats  = {"seen": 0, "added": 0, "updated": 0, "skipped": 0}
    queue  = discover_sitemap_roots(session)
    visited_sitemaps: set[str] = set()

    while queue:
        sitemap_url = queue.pop(0)
        if sitemap_url in visited_sitemaps:
            continue
        visited_sitemaps.add(sitemap_url)

        log.info("Fetching sitemap: %s", sitemap_url)
        raw = fetch(sitemap_url, session, delay=SITEMAP_DELAY)
        if raw is None:
            continue

        children, entries = parse_sitemap(raw)
        for child in children:
            if child not in visited_sitemaps:
                queue.append(child)
                log.info("  Queued child sitemap: %s", child)

        for url, lastmod in entries:
            stats["seen"] += 1
            existing_rec = index.get(url)

            # Skip if nothing changed
            if existing_rec and existing_rec.get("lastmod") == lastmod:
                stats["skipped"] += 1
                continue

            index[url] = make_record(url, lastmod, existing_rec)

            if existing_rec:
                stats["updated"] += 1
            else:
                stats["added"] += 1

    log.info(
        "Sitemaps done — seen=%d  added=%d  updated=%d  skipped=%d",
        stats["seen"], stats["added"], stats["updated"], stats["skipped"],
    )
    return index, stats


# ---------------------------------------------------------------------------
# Phase 2 — Category page crawling
# ---------------------------------------------------------------------------

def extract_entry_links(html: str, category_namespace: str) -> list[str]:
    """
    Extract meme entry URLs from a category listing page.
    Only returns links whose namespace exactly matches category_namespace,
    so crawling /memes/people doesn't pollute the /memes index.
    """
    soup  = BeautifulSoup(html, "html.parser")
    found = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            full = f"{BASE_URL}{href}"
        elif href.startswith(BASE_URL):
            full = href
        else:
            continue

        if namespace_of(urlparse(full).path) == category_namespace:
            found.append(full)

    return found


def crawl_categories(
    session:  requests.Session,
    existing: dict[str, dict],
) -> tuple[dict[str, dict], dict]:
    """
    Paginate through category listing pages. Returns (updated index, stats).
    """
    index = dict(existing)
    stats = {"seen": 0, "added": 0}

    for cat_path, cat_ns in CATEGORY_PAGES:
        log.info("Crawling category: %s", cat_path)
        page = 1
        consecutive_empty = 0

        while page <= MAX_CATEGORY_PAGES:
            page_url = f"{BASE_URL}{cat_path}?page={page}"
            raw = fetch(page_url, session, delay=CRAWL_DELAY)
            if raw is None:
                break

            html  = raw.decode("utf-8", errors="replace")
            links = extract_entry_links(html, cat_ns)
            new   = 0

            for url in links:
                stats["seen"] += 1
                if url not in index:
                    index[url] = make_record(url, lastmod=None, existing=None)
                    stats["added"] += 1
                    new += 1

            log.info("  Page %d: %d links, %d new", page, len(links), new)

            if new == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    log.info("  3 empty pages — stopping %s", cat_path)
                    break
            else:
                consecutive_empty = 0

            # Detect next page
            soup      = BeautifulSoup(html, "html.parser")
            next_link = (
                soup.find("a", class_="next_page")
                or soup.find("a", string=re.compile(r"next", re.I))
                or soup.find("link", rel="next")
            )
            if not next_link:
                log.info("  No next page — stopping %s", cat_path)
                break

            page += 1

    log.info("Category crawl done — seen=%d  added=%d", stats["seen"], stats["added"])
    return index, stats


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    try:
        data    = json.loads(path.read_text(encoding="utf-8"))
        records = data["urls"] if isinstance(data, dict) and "urls" in data else data
        loaded  = {r["url"]: r for r in records if "url" in r}
        log.info("Loaded %d existing records from %s", len(loaded), path)
        return loaded
    except (json.JSONDecodeError, KeyError) as exc:
        log.warning("Could not load %s: %s — starting fresh", path, exc)
        return {}


def save(path: Path, index: dict[str, dict]) -> None:
    records = sorted(index.values(), key=lambda r: (r.get("namespace") or "", r["url"]))
    output  = {
        "metadata": {
            "source":       BASE_URL,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_urls":   len(records),
            "namespaces":   sorted({r.get("namespace") or "unknown" for r in records}),
        },
        "urls": records,
    }
    path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Saved %d records → %s", len(records), path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Discover all URLs on knowyourmeme.com")
    parser.add_argument("--output",       "-o", default=OUTPUT_FILE, help="Output JSON file")
    parser.add_argument("--sitemap-only", action="store_true",       help="Skip category crawl")
    parser.add_argument("--fresh",        action="store_true",       help="Ignore existing file")
    parser.add_argument("--resume",       action="store_true",       help="Continue from existing file")
    args = parser.parse_args()

    if args.fresh and args.resume:
        parser.error("--fresh and --resume are mutually exclusive")

    out_path = Path(args.output)
    session  = make_session()

    existing = {} if args.fresh else load(out_path)

    # Phase 1: sitemaps
    index, sitemap_stats = fetch_all_sitemaps(session, existing)
    save(out_path, index)  # intermediate save

    # Phase 2: category crawl
    crawl_stats = {"seen": 0, "added": 0}
    if not args.sitemap_only:
        index, crawl_stats = crawl_categories(session, index)
        save(out_path, index)  # intermediate save

    # Final summary
    ns_counts: dict[str, int] = {}
    for r in index.values():
        ns = r.get("namespace") or "unknown"
        ns_counts[ns] = ns_counts.get(ns, 0) + 1

    log.info("=" * 55)
    log.info("DISCOVERY COMPLETE")
    log.info("  Total URLs      : %d", len(index))
    log.info("  Sitemap — added : %d  updated: %d  skipped: %d",
             sitemap_stats["added"], sitemap_stats["updated"], sitemap_stats["skipped"])
    log.info("  Crawl   — added : %d", crawl_stats["added"])
    log.info("  Output          : %s", out_path)
    log.info("Breakdown by namespace:")
    for ns in sorted(ns_counts):
        log.info("  %-35s %d", ns, ns_counts[ns])
    log.info("=" * 55)


if __name__ == "__main__":
    main()