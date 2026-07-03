#!/usr/bin/env python3
"""
kym_discover.py  —  Know Your Meme URL Discovery
================================================
Discovers (nearly) every entry URL on knowyourmeme.com and emits the master
index the rest of the pipeline ingests from (JSON file and/or MongoDB).

Phases
------
  Phase 1  — Sitemaps
      Reads robots.txt to find the sitemap index, walks every child sitemap
      (gzip aware). Sitemaps contain the *confirmed* entries with ``lastmod``.

  Phase 1b — Taxonomy inference
      Derives namespace prefixes and category/listing pages from the sitemap
      corpus so the crawler adapts to new KYM namespaces automatically.

  Phase 2  — Listing crawl (the part that catches what sitemaps omit)
      Paginates the status listings — /memes/all, /memes/confirmed,
      /memes/submissions, /memes/researching, /memes/deadpool — which hold the
      submissions / deadpool / researching entries that never appear in the
      sitemap. Entry links are detected by URL shape (robust to CSS changes)
      and sub-namespace entries (e.g. /memes/events/...) are kept.

Sinks
-----
  * JSON file (default): { "metadata": {...}, "urls": [...] }
  * MongoDB (--mongo):   upserts into the ``urls`` collection so the annotation
                         pipeline can ingest directly (annotate_memes.py
                         --source mongo). Discovery never clobbers a record's
                         ``last_scraped`` and never downgrades ``Confirmed``.

Usage
-----
    python kym_discover.py                       # full run -> kym_urls.json
    python kym_discover.py --sitemap-only        # sitemaps only
    python kym_discover.py --max-category-pages 5
    python kym_discover.py --mongo               # also upsert into MongoDB
    python kym_discover.py --fresh               # ignore existing file

Requires:  pip install -r requirements-discovery.txt   (requests, beautifulsoup4, lxml)
           pip install pymongo                          (only for --mongo)
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from math import inf
from pathlib import Path
from urllib.parse import urljoin, urlparse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://knowyourmeme.com"
ROBOTS_URL = f"{BASE_URL}/robots.txt"
SITEMAP_FALLBACK = f"{BASE_URL}/sitemap.xml"
OUTPUT_FILE = "kym_urls.json"

SITEMAP_DELAY = 0.5            # seconds between sitemap fetches
CRAWL_DELAY = 0.8             # seconds between listing-page fetches
MAX_RETRIES = 5
REQUEST_TIMEOUT = 30
CONSECUTIVE_EMPTY_LIMIT = 3   # stop a listing after N pages with 0 new entries

USER_AGENT = (
    "MemeAtlas-Research-Indexer/1.0 "
)

# Status/listing pages to paginate in Phase 2. Each is (path, confirmed):
# ``confirmed`` is the default Confirmed flag for entries first seen here.
# Sitemap entries are already Confirmed=True and are never downgraded
# (see store.merge_discovery), so /memes/all can safely use confirmed=False.
# These status listings are exactly what the sitemap omits — crawling them is
# what recovers the missing submissions / deadpool / researching entries.
DEFAULT_LISTINGS: list[tuple[str, bool]] = [
    ("/memes/all", False),
    ("/memes/confirmed", True),
    ("/memes/submissions", False),
    ("/memes/researching", False),
    ("/memes/deadpool", False),
]

# Top-level sections whose deep URLs are real entries (not nav/listing pages).
ENTRY_ROOTS = frozenset({
    "memes", "cultures", "subcultures", "people", "sites", "events",
    "editorials", "videos", "photos", "sensitive",
})

# Single-segment paths under a section that are status filters / listing roots,
# never entry slugs. Used to tell /memes/<slug> (entry) from /memes/all (listing).
KNOWN_CATEGORY_SEGMENTS = frozenset({
    "all", "new", "confirmed", "submissions", "submission", "deadpool",
    "researching", "newsworthy", "popular", "people", "events",
    "subcultures", "sites", "editorials", "videos", "photos", "cultures",
    "guides", "interviews", "page",
})

# Fallback namespace patterns (longest prefix first). Rebuilt by infer_taxonomy.
NAMESPACE_PATTERNS: list[tuple[str, str]] = [
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
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kym_discover")


# ---------------------------------------------------------------------------
# HTTP helpers (requests imported lazily so the module imports without it)
# ---------------------------------------------------------------------------

def make_session():
    """Create a requests Session with the bot User-Agent pre-configured."""
    import requests
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def fetch(url: str, session, delay: float = 0.0) -> bytes | None:
    """
    Fetch a URL and return raw bytes, retrying transient errors with backoff.
    Returns None if all attempts fail.
    """
    import requests
    time.sleep(delay)
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES - 1:
                log.error("Gave up on %s after %d attempts: %s", url, MAX_RETRIES, exc)
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
# URL classification helpers
# ---------------------------------------------------------------------------

def namespace_of(url_path: str) -> str | None:
    """Return the KYM namespace label for a URL path, or None if unrecognised."""
    url_path = url_path.rstrip("/")
    for prefix, label in NAMESPACE_PATTERNS:
        if url_path.startswith(prefix):
            return label
    return None


def is_entry_url(url: str) -> bool:
    """
    True if ``url`` points at an actual KYM entry rather than a listing/nav page.

    An entry has a recognised top-level section, depth >= 2, and a final
    segment that is not a known status/listing word. This is intentionally
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
        "namespace": namespace_of(urlparse(url).path),
        "Confirmed": confirmed,
        "lastmod": lastmod,
        "page_template_type": None,
        "last_scraped": existing_record.get("last_scraped") if existing_record else None,
    }


# ---------------------------------------------------------------------------
# Phase 1 — Sitemap discovery
# ---------------------------------------------------------------------------

def discover_sitemap_roots(session) -> list[str]:
    """Read robots.txt and return all Sitemap: URLs (fallback: /sitemap.xml)."""
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


def fetch_all_sitemaps(session, existing: dict[str, dict]) -> tuple[dict[str, dict], dict]:
    """Walk the full sitemap tree; return (index, stats)."""
    index = dict(existing)
    stats = {"seen": 0, "added": 0, "updated": 0, "skipped": 0}
    queue = discover_sitemap_roots(session)
    visited: set[str] = set()

    while queue:
        sitemap_url = queue.pop(0)
        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)

        log.info("Fetching sitemap: %s", sitemap_url)
        raw = fetch(sitemap_url, session, delay=SITEMAP_DELAY)
        if raw is None:
            continue

        child_urls, url_entries = parse_sitemap(raw)
        for child in child_urls:
            if child not in visited:
                queue.append(child)

        for url, lastmod in url_entries:
            stats["seen"] += 1
            existing_record = index.get(url)
            if existing_record and existing_record.get("lastmod") == lastmod:
                stats["skipped"] += 1
                continue
            # Preserve a previously-set Confirmed (monotonic) on update.
            confirmed = True if lastmod is not None else (
                existing_record.get("Confirmed", False) if existing_record else False
            )
            index[url] = make_record(url, lastmod, existing_record, confirmed=confirmed)
            stats["updated" if existing_record else "added"] += 1

    log.info("Sitemaps done — seen=%d added=%d updated=%d skipped=%d",
             stats["seen"], stats["added"], stats["updated"], stats["skipped"])
    return index, stats


# ---------------------------------------------------------------------------
# Phase 1b — Taxonomy inference
# ---------------------------------------------------------------------------

MIN_NAMESPACE_URL_COUNT = 2


def infer_taxonomy(index: dict[str, dict]) -> tuple[list[tuple[str, str]], list[tuple[str, bool]]]:
    """
    Derive namespace patterns and listing pages from the collected corpus.

    Returns (namespace_patterns, listings) where listings is the merged set of
    DEFAULT_LISTINGS plus any status pages observed in the corpus, as
    (path, confirmed_default) pairs.
    """
    prefix_counts: dict[str, int] = {}
    for url in index:
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
    for url in index:
        segs = [s for s in urlparse(url).path.rstrip("/").split("/") if s]
        if len(segs) == 2 and segs[0] in ENTRY_ROOTS and segs[1] in KNOWN_CATEGORY_SEGMENTS:
            path = "/" + "/".join(segs)
            if path not in seen_paths:
                listings.append((path, False))
                seen_paths.add(path)

    log.info("Taxonomy inferred — %d namespace patterns, %d listings",
             len(namespaces), len(listings))
    return namespaces, listings


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
    and finally any anchor whose visible text is "Next". Returns an absolute
    URL or None. Following the site's own next link avoids guessing KYM's
    pagination URL format.
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

    for anchor in soup.find_all("a", href=True):
        if anchor.get_text(strip=True).lower() in ("next", "next page", "next ›", "›"):
            return urljoin(current_url, anchor["href"])
    return None


def crawl_listing(session, index: dict[str, dict], path: str, confirmed: bool,
                  max_pages: float = inf) -> int:
    """
    Paginate one listing, adding new entry URLs to ``index`` in place.

    Stops when: no next-page link, ``CONSECUTIVE_EMPTY_LIMIT`` consecutive
    pages add nothing new, or ``max_pages`` is reached. Returns # added.
    """
    added = 0
    consecutive_empty = 0
    page = 1
    page_url: str | None = f"{BASE_URL}{path}"

    while page_url and page <= max_pages:
        raw = fetch(page_url, session, delay=CRAWL_DELAY)
        if raw is None:
            break
        html = raw.decode("utf-8", errors="replace")

        links = extract_entry_links(html)
        new_here = 0
        for url in links:
            if url not in index:
                index[url] = make_record(url, lastmod=None, existing_record=None,
                                         confirmed=confirmed)
                added += 1
                new_here += 1

        log.info("  %s page %d: %d links, %d new", path, page, len(links), new_here)

        consecutive_empty = consecutive_empty + 1 if new_here == 0 else 0
        if consecutive_empty >= CONSECUTIVE_EMPTY_LIMIT:
            log.info("  %d consecutive empty pages — stopping %s", consecutive_empty, path)
            break

        next_url = find_next_page(html, page_url)
        if not next_url or next_url == page_url:
            log.info("  No next page — stopping %s", path)
            break
        page_url = next_url
        page += 1

    return added


def crawl_categories(session, existing: dict[str, dict], listings: list[tuple[str, bool]],
                     max_category_pages: float = inf) -> tuple[dict[str, dict], dict]:
    """Crawl every configured listing; return (index, stats)."""
    index = dict(existing)
    stats = {"added": 0}
    for path, confirmed in listings:
        log.info("Crawling listing: %s (confirmed=%s)", path, confirmed)
        stats["added"] += crawl_listing(session, index, path, confirmed, max_category_pages)
    log.info("Listing crawl done — added=%d", stats["added"])
    return index, stats


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load(path: Path) -> dict[str, dict]:
    """Load an existing index from JSON (full envelope or bare list)."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        records = data["urls"] if isinstance(data, dict) and "urls" in data else data
        loaded = {r["url"]: r for r in records if "url" in r}
        log.info("Loaded %d existing records from %s", len(loaded), path)
        return loaded
    except (json.JSONDecodeError, KeyError) as exc:
        log.warning("Could not load %s: %s — starting fresh", path, exc)
        return {}


def save(path: Path, index: dict[str, dict]) -> None:
    """Write the full index to JSON with a metadata envelope (sorted records)."""
    records = sorted(index.values(), key=lambda r: (r.get("namespace") or "", r["url"]))
    output = {
        "metadata": {
            "source": BASE_URL,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_urls": len(records),
            "namespaces": sorted({r.get("namespace") or "unknown" for r in records}),
        },
        "urls": records,
    }
    path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Saved %d records → %s", len(records), path)


def save_to_mongo(index: dict[str, dict]) -> None:
    """Upsert every record into the MongoDB ``urls`` collection."""
    from src.db.mongo import get_store
    store = get_store()
    try:
        stats = store.upsert_urls(index.values())
        log.info("MongoDB upsert — added=%d updated=%d (total in db=%d)",
                 stats["added"], stats["updated"], store.count_urls())
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Discover all URLs on knowyourmeme.com")
    parser.add_argument("--output", "-o", default=OUTPUT_FILE, help="Output JSON file")
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

    out_path = Path(args.output)
    session = make_session()
    existing = {} if args.fresh else load(out_path)

    crawl_stats = {"added": 0}

    # Phase 1: sitemaps
    index, sitemap_stats = fetch_all_sitemaps(session, existing)
    if not args.no_file:
        save(out_path, index)

    # Phase 1b: taxonomy inference (updates module globals used by namespace_of)
    listings = list(DEFAULT_LISTINGS)
    if index:
        inferred_namespaces, listings = infer_taxonomy(index)
        global NAMESPACE_PATTERNS
        NAMESPACE_PATTERNS = inferred_namespaces

    # Phase 2: listing crawl
    if not args.sitemap_only:
        max_pages = args.max_category_pages if args.max_category_pages is not None else inf
        index, crawl_stats = crawl_categories(session, index, listings, max_pages)
        if not args.no_file:
            save(out_path, index)

    # Optional MongoDB sink
    if args.mongo:
        save_to_mongo(index)

    # Summary
    namespace_counts: dict[str, int] = {}
    for record in index.values():
        ns = record.get("namespace") or "unknown"
        namespace_counts[ns] = namespace_counts.get(ns, 0) + 1
    confirmed_count = sum(1 for r in index.values() if r.get("Confirmed"))
    null_lastmod = sum(1 for r in index.values() if r.get("lastmod") is None)

    log.info("=" * 55)
    log.info("DISCOVERY COMPLETE")
    log.info("  Total URLs      : %d", len(index))
    log.info("  Confirmed       : %d", confirmed_count)
    log.info("  Sitemap — added : %d  updated: %d  skipped: %d",
             sitemap_stats["added"], sitemap_stats["updated"], sitemap_stats["skipped"])
    log.info("  Crawl   — added : %d", crawl_stats["added"])
    log.info("  lastmod=null    : %d", null_lastmod)
    log.info("  Output          : %s", "MongoDB" if args.no_file else out_path)
    log.info("Breakdown by namespace:")
    for ns in sorted(namespace_counts):
        log.info("  %-35s %d", ns, namespace_counts[ns])
    log.info("=" * 55)


if __name__ == "__main__":
    main()
