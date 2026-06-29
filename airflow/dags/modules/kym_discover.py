#!/usr/bin/env python3
"""
kym_discover.py  —  KnowYourMeme URL Discovery
===============================================
Discovers all URLs on knowyourmeme.com using two complementary phases:

  Phase 1 — Sitemaps
    Reads robots.txt to find the sitemap index (robust to URL drift),
    then walks every child sitemap. Handles gzip-compressed .xml.gz files.

  Phase 1b — Namespace inference  (runs automatically after Phase 1)
    Infers NAMESPACE_PATTERNS and CATEGORY_PAGES from the sitemap URL
    corpus itself, so the script adapts to new KYM namespaces without
    any code changes. The hardcoded lists serve only as a fallback when
    the sitemap corpus is empty (e.g. very first run with no data yet).

  Phase 2 — Category crawling  (optional, --sitemap-only to skip)
    Paginates through /memes/all to catch any URLs that KYM omits from
    their sitemaps (e.g. submissions, deadpool entries).

  Phase 3 — Lastmod enrichment  (optional, --skip-enrich to skip)
    For URLs discovered via crawling that have no lastmod date, fetches
    each page and extracts article:published_time from the meta tags.

Incremental runs: URLs already in the output file are skipped unless
their lastmod has changed. Use --fresh to force a full rescan.

Output: JSON file  { "metadata": {...}, "urls": [...] }

Usage:
    python kym_discover.py                      # full run
    python kym_discover.py --sitemap-only       # skip category crawl and enrichment
    python kym_discover.py --skip-enrich        # skip lastmod enrichment
    python kym_discover.py --enrich-only        # only enrich existing file
    python kym_discover.py --fresh              # ignore existing file, full rescan
    python kym_discover.py --resume             # continue an interrupted run
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
from math import inf
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL         = "https://knowyourmeme.com"
ROBOTS_URL       = f"{BASE_URL}/robots.txt"
SITEMAP_FALLBACK = f"{BASE_URL}/sitemap.xml"
OUTPUT_FILE      = "kym_urls.json"

SITEMAP_DELAY        = 0.5   # seconds between sitemap file fetches
CRAWL_DELAY          = 0.8   # seconds between category page fetches
MAX_RETRIES          = 5
REQUEST_TIMEOUT      = 30

USER_AGENT = (
    "MemeAtlas-Research-Bot/1.0 "
    "(academic meme corpus; https://github.com/example/memeatlas)"
)

# Fallback category pages used on the very first run before any sitemap
# data has been collected. After Phase 1, infer_taxonomy() derives the real
# values directly from the collected URL corpus.
# Each entry is (path, namespace_label, url_template).
CATEGORY_PAGES = [
    ("/memes/all", "memes", "{base}/page/{n}?page=1"),
]

# Fallback namespace patterns used on the very first run before any sitemap
# data has been collected. After Phase 1, infer_taxonomy() rebuilds this
# list from the observed URL paths.
# Order matters: longer/more-specific prefixes must come first.
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

# CSS classes that mark actual meme entry links on listing pages.
# Plain navigation links (/memes/new, /memes/confirmed, etc.) carry no
# class, so requiring one of these filters them out cleanly.
ENTRY_LINK_CLASSES = {"result", "item", "wide-card", "overlayed-card"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kym_discover")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    """Create a requests Session with the bot User-Agent pre-configured."""
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def fetch(url: str, session: requests.Session, delay: float = 0.0) -> bytes | None:
    """
    Fetch a URL and return the raw response bytes.

    Waits `delay` seconds before the first attempt, then retries up to
    MAX_RETRIES times with exponential backoff on transient errors.
    Returns None if all attempts fail.
    """
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
            log.warning(
                "Attempt %d failed for %s: %s — retrying in %ds",
                attempt + 1, url, exc, wait_seconds,
            )
            time.sleep(wait_seconds)


def decompress_if_gzip(raw: bytes) -> bytes:
    """
    Transparently decompress gzip-encoded bytes.

    KYM serves some sitemaps as .xml.gz files. The magic bytes 0x1F 0x8B
    identify gzip format; anything else is returned unchanged.
    """
    if raw[:2] == b"\x1f\x8b":
        return gzip.decompress(raw)
    return raw


# ---------------------------------------------------------------------------
# URL metadata helpers
# ---------------------------------------------------------------------------

def namespace_of(url_path: str) -> str | None:
    """
    Return the KYM namespace label for a URL path, or None if unrecognised.

    Examples:
        /memes/distracted-boyfriend        → "memes"
        /memes/sites/reddit                → "memes/sites"
        /sensitive/memes/dark-humour       → "sensitive/memes"
        /users/noxforte                    → None
    """
    url_path = url_path.rstrip("/")
    for prefix, label in NAMESPACE_PATTERNS:
        if url_path.startswith(prefix):
            return label
    return None


def make_record(url: str, lastmod: str | None, existing_record: dict | None) -> dict:
    """
    Build a URL record dict for the index.

    If a record for this URL already exists (incremental run), its
    last_scraped value is preserved so scraping progress is not lost.
    """
    return {
        "url":          url,
        "namespace":    namespace_of(urlparse(url).path),
        "Confirmed":    True if lastmod != None else False,
        "lastmod":      lastmod,
        "page_template_type": None,
        "last_scraped": existing_record.get("last_scraped") if existing_record else None,
    }

# ---------------------------------------------------------------------------
# Phase 1 — Sitemap discovery
# ---------------------------------------------------------------------------

def discover_sitemap_roots(session: requests.Session) -> list[str]:
    """
    Read robots.txt and return all URLs listed on Sitemap: lines.

    Using robots.txt as the entry point means the script automatically
    picks up new sitemaps if KYM adds them, without any code changes.
    Falls back to /sitemap.xml if robots.txt is unreachable or contains
    no Sitemap: directives.
    """
    log.info("Reading robots.txt: %s", ROBOTS_URL)
    raw = fetch(ROBOTS_URL, session)
    if raw is None:
        log.warning("robots.txt unreachable — using fallback %s", SITEMAP_FALLBACK)
        return [SITEMAP_FALLBACK]

    sitemap_roots = [
        line.split(":", 1)[1].strip()
        for line in raw.decode("utf-8", errors="replace").splitlines()
        if line.strip().lower().startswith("sitemap:")
    ]

    if not sitemap_roots:
        log.warning("No Sitemap: lines in robots.txt — using fallback %s", SITEMAP_FALLBACK)
        return [SITEMAP_FALLBACK]

    log.info("Found %d sitemap root(s): %s", len(sitemap_roots), sitemap_roots)
    return sitemap_roots


def parse_sitemap(raw: bytes) -> tuple[list[str], list[tuple[str, str | None]]]:
    """
    Parse a sitemap XML document and return its contents.

    Handles both document types:
      - <sitemapindex>: returns the child sitemap URLs it lists, with an
        empty entries list.
      - <urlset>: returns the (url, lastmod) pairs it contains, with an
        empty child sitemaps list.

    The XML namespace URI is extracted dynamically from the root tag so
    this works regardless of whether the document declares http://, https://,
    or no namespace at all. KYM currently uses https://, the W3C spec says
    http://, and some sitemaps omit the namespace entirely.
    """
    child_sitemap_urls: list[str]                  = []
    url_entries:        list[tuple[str, str|None]] = []

    try:
        root = ET.fromstring(decompress_if_gzip(raw))
    except ET.ParseError as exc:
        log.error("XML parse error: %s", exc)
        return child_sitemap_urls, url_entries

    # ElementTree stores namespaced tags as "{uri}localname".
    # Extract the URI from the root tag so we can query child elements
    # using the same namespace, whatever it happens to be.
    if root.tag.startswith("{"):
        namespace_uri    = root.tag[1:root.tag.index("}")]
        namespace_prefix = f"{{{namespace_uri}}}"
    else:
        namespace_prefix = ""

    root_tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

    def find_all(parent, local_tag: str):
        return parent.findall(f"{namespace_prefix}{local_tag}")

    def find_one(parent, local_tag: str):
        return parent.find(f"{namespace_prefix}{local_tag}")

    if root_tag == "sitemapindex":
        for sitemap_element in find_all(root, "sitemap"):
            loc_element = find_one(sitemap_element, "loc")
            if loc_element is not None and loc_element.text:
                child_sitemap_urls.append(loc_element.text.strip())

    elif root_tag == "urlset":
        for url_element in find_all(root, "url"):
            loc_element     = find_one(url_element, "loc")
            lastmod_element = find_one(url_element, "lastmod")

            if loc_element is None or not loc_element.text:
                continue

            lastmod = (
                lastmod_element.text.strip()
                if lastmod_element is not None and lastmod_element.text
                else None
            )
            url_entries.append((loc_element.text.strip(), lastmod))

    else:
        log.warning("Unrecognised sitemap root tag: %s", root_tag)

    return child_sitemap_urls, url_entries


def fetch_all_sitemaps(
    session:  requests.Session,
    existing: dict[str, dict],
) -> tuple[dict[str, dict], dict]:
    """
    Walk the full sitemap tree starting from robots.txt and return an
    updated URL index along with run statistics.

    Processes sitemap indexes recursively by maintaining a queue of
    unvisited sitemap URLs. Skips any URL whose lastmod timestamp matches
    the value already stored in the existing index, so incremental runs
    only update records that have actually changed.

    Returns:
        (index, stats) where stats has keys: seen, added, updated, skipped.
    """
    index             = dict(existing)
    stats             = {"seen": 0, "added": 0, "updated": 0, "skipped": 0}
    queue             = discover_sitemap_roots(session)
    visited_sitemaps  = set()

    while queue:
        sitemap_url = queue.pop(0)
        if sitemap_url in visited_sitemaps:
            continue
        visited_sitemaps.add(sitemap_url)

        log.info("Fetching sitemap: %s", sitemap_url)
        raw = fetch(sitemap_url, session, delay=SITEMAP_DELAY)
        if raw is None:
            continue

        child_sitemap_urls, url_entries = parse_sitemap(raw)

        for child_url in child_sitemap_urls:
            if child_url not in visited_sitemaps:
                queue.append(child_url)
                log.info("  Queued child sitemap: %s", child_url)

        for url, lastmod in url_entries:
            stats["seen"] += 1
            existing_record = index.get(url)

            if existing_record and existing_record.get("lastmod") == lastmod:
                stats["skipped"] += 1
                continue

            index[url] = make_record(url, lastmod, existing_record)

            if existing_record:
                stats["updated"] += 1
            else:
                stats["added"] += 1

    log.info(
        "Sitemaps done — seen=%d  added=%d  updated=%d  skipped=%d",
        stats["seen"], stats["added"], stats["updated"], stats["skipped"],
    )
    return index, stats



# ---------------------------------------------------------------------------
# Namespace and category inference from sitemap corpus
# ---------------------------------------------------------------------------

# URL path segments that are KYM status filters or listing roots, not meme
# slugs. A /memes/X path whose X appears in this set is a category page;
# one whose X does not appear here is a meme entry.
KNOWN_CATEGORY_SEGMENTS = frozenset({
    "all", "new", "confirmed", "submissions", "deadpool",
    "newsworthy", "people", "events", "subcultures",
    "sites", "editorials", "videos", "photos", "cultures",
})

# Minimum number of URLs that must share a prefix for it to be recognised
# as a namespace. Prevents one-off paths from becoming spurious namespaces.
MIN_NAMESPACE_URL_COUNT = 2


def infer_taxonomy(index: dict[str, dict]) -> tuple[
    list[tuple[str, str]],
    list[tuple[str, str, str]],
]:
    """
    Derive NAMESPACE_PATTERNS and CATEGORY_PAGES from the URLs already
    collected in the index, without making any additional HTTP requests.

    Namespace inference
    -------------------
    A namespace is any path prefix of depth >= 2 (e.g. /memes/sites/) that
    appears as the parent of at least MIN_NAMESPACE_URL_COUNT distinct URLs.
    The prefix is the path up to and including the second-to-last segment,
    with a trailing slash appended.

    Example: given 200 URLs starting with /memes/sites/, the prefix
    "/memes/sites/" is inferred as the namespace "memes/sites".

    The returned list is sorted longest-prefix-first so that more specific
    patterns take priority when matched top-to-bottom (required by
    namespace_of()).

    Category page inference
    -----------------------
    A category page is any /memes/X or /sensitive/Y path where X or Y is a
    known status/listing segment (KNOWN_CATEGORY_SEGMENTS) rather than a
    meme slug. These are the pages the crawler should paginate through in
    Phase 2.

    Returns:
        (namespace_patterns, category_pages)
        namespace_patterns: list of (prefix, label) sorted longest-first
        category_pages:     list of (path, namespace_label, url_template)
    """
    # Count how many URLs fall under each two-segment prefix
    prefix_counts: dict[str, int] = {}
    for url in index:
        path     = urlparse(url).path.rstrip("/")
        segments = [s for s in path.split("/") if s]
        # Only consider paths deep enough to have a namespace prefix
        if len(segments) >= 2:
            prefix = "/" + "/".join(segments[:-1]) + "/"
            prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1

    # Build namespace patterns from prefixes that appear often enough
    inferred_namespaces: list[tuple[str, str]] = []
    for prefix, count in prefix_counts.items():
        if count < MIN_NAMESPACE_URL_COUNT:
            continue
        # Convert "/memes/sites/" → "memes/sites"
        label = prefix.strip("/").replace("/", "/")
        inferred_namespaces.append((prefix, label))

    # Sort longest prefix first so specific patterns shadow general ones
    inferred_namespaces.sort(key=lambda pair: len(pair[0]), reverse=True)

    # Ensure the root /memes/ namespace is always present as a catch-all
    existing_prefixes = {prefix for prefix, _ in inferred_namespaces}
    if "/memes/" not in existing_prefixes:
        inferred_namespaces.append(("/memes/", "memes"))

    # Infer category pages from known single-depth paths under /memes/ and
    # /sensitive/ whose segment matches a known category name
    inferred_categories: list[tuple[str, str, str]] = []
    seen_category_paths: set[str] = set()

    for url in index:
        path     = urlparse(url).path.rstrip("/")
        segments = [s for s in path.split("/") if s]

        if len(segments) == 2 and segments[1] in KNOWN_CATEGORY_SEGMENTS:
            cat_path = "/" + "/".join(segments)
            if cat_path not in seen_category_paths:
                # Determine URL template from the path:
                # /memes/all uses path-segment pagination, others use ?page=N
                if segments[1] == "all":
                    url_template = "{base}/page/{n}?page=1"
                else:
                    url_template = "{base}?page={n}"
                namespace_label = segments[0]
                inferred_categories.append((cat_path, namespace_label, url_template))
                seen_category_paths.add(cat_path)

    # Always ensure /memes/all is present — it is the primary crawl fallback
    if "/memes/all" not in seen_category_paths:
        inferred_categories.insert(0, ("/memes/all", "memes", "{base}/page/{n}?page=1"))

    log.info(
        "Taxonomy inferred from corpus — %d namespace patterns, %d category pages",
        len(inferred_namespaces), len(inferred_categories),
    )
    for prefix, label in inferred_namespaces:
        log.debug("  namespace: %r → %r", prefix, label)
    for cat_path, label, template in inferred_categories:
        log.debug("  category:  %r  ns=%r", cat_path, label)

    return inferred_namespaces, inferred_categories


# ---------------------------------------------------------------------------
# Phase 2 — Category page crawling
# ---------------------------------------------------------------------------

def extract_entry_links(html: str, category_namespace: str) -> list[str]:
    """
    Extract meme entry URLs from a KYM category listing page.

    Two filters are applied to avoid returning noise:
      1. The <a> tag must carry one of the known entry card CSS classes
         (ENTRY_LINK_CLASSES). Plain navigation links like /memes/new have
         no class and are excluded by this check.
      2. The resolved URL's namespace must exactly match category_namespace,
         so crawling a broad category doesn't pull in links from unrelated
         namespaces.

    Duplicates within a single page are removed (the same entry can appear
    in multiple card styles on the same page).
    """
    soup       = BeautifulSoup(html, "html.parser")
    found_urls = []
    seen_urls  = set()

    for anchor in soup.find_all("a", href=True):
        anchor_classes = set(anchor.get("class") or [])
        if not anchor_classes & ENTRY_LINK_CLASSES:
            continue

        href = anchor["href"]
        if href.startswith("/"):
            full_url = f"{BASE_URL}{href}"
        elif href.startswith(BASE_URL):
            full_url = href
        else:
            continue

        if full_url in seen_urls:
            continue

        if namespace_of(urlparse(full_url).path) == category_namespace:
            found_urls.append(full_url)
            seen_urls.add(full_url)

    return found_urls


def crawl_categories(
    session:  requests.Session,
    existing: dict[str, dict],
    max_category_pages = float(inf),
) -> tuple[dict[str, dict], dict]:
    """
    Paginate through each category in CATEGORY_PAGES and add any URLs not
    already present in the index.

    Stops pagination for a given category when either:
      - Three consecutive pages yield no new URLs (we've caught up with the
        existing index, or reached the end of the listing).
      - No next-page link is found in the HTML.
      - MAX_CATEGORY_PAGES is reached.

    Returns:
        (index, stats) where stats has keys: seen, added.
    """
    index = dict(existing)
    stats = {"seen": 0, "added": 0}

    for category_path, category_namespace, url_template in CATEGORY_PAGES:
        log.info("Crawling category: %s", category_path)
        page              = 1
        consecutive_empty = 0
        base_url          = f"{BASE_URL}{category_path}"

        while page <= max_category_pages:
            page_url = url_template.format(base=base_url, n=page)
            raw      = fetch(page_url, session, delay=CRAWL_DELAY)
            if raw is None:
                break

            html       = raw.decode("utf-8", errors="replace")
            page_links = extract_entry_links(html, category_namespace)
            new_count  = 0

            for url in page_links:
                stats["seen"] += 1
                if url not in index:
                    index[url] = make_record(url, lastmod=None, existing_record=None)
                    stats["added"] += 1
                    new_count += 1

            log.info("  Page %d: %d links, %d new", page, len(page_links), new_count)

            # KYM's /memes/all uses class="page-button" links with path-segment
            # pagination (/memes/all/page/N). Other category pages use a
            # class="next_page" link or a <link rel="next"> tag.
            soup      = BeautifulSoup(html, "html.parser")
            next_link = (
                soup.find("a", class_="page-button", href=lambda h: h and f"/page/{page + 1}" in h)
                or soup.find("a", class_="next_page")
                or soup.find("link", rel="next")
            )
            if not next_link:
                log.info("  No next page — stopping %s", category_path)
                break

            page += 1

    log.info("Category crawl done — seen=%d  added=%d", stats["seen"], stats["added"])
    return index, stats


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load(path: Path) -> dict[str, dict]:
    """
    Load an existing URL index from a JSON file.

    Accepts both the full output format { "metadata": ..., "urls": [...] }
    and a bare list of records. Returns an empty dict if the file does not
    exist or cannot be parsed.
    """
    if not path.exists():
        return {}
    try:
        data    = json.loads(path.read_text(encoding="utf-8"))
        records = data["urls"] if isinstance(data, dict) and "urls" in data else data
        loaded  = {record["url"]: record for record in records if "url" in record}
        log.info("Loaded %d existing records from %s", len(loaded), path)
        return loaded
    except (json.JSONDecodeError, KeyError) as exc:
        log.warning("Could not load %s: %s — starting fresh", path, exc)
        return {}


def save(path: Path, index: dict[str, dict]) -> None:
    """
    Write the full URL index to a JSON file.

    Records are sorted by namespace then URL for deterministic diffs between
    runs. A metadata envelope is included with a generation timestamp,
    total count, and the list of namespaces present.
    """
    records = sorted(index.values(), key=lambda record: (record.get("namespace") or "", record["url"]))
    output  = {
        "metadata": {
            "source":       BASE_URL,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_urls":   len(records),
            "namespaces":   sorted({record.get("namespace") or "unknown" for record in records}),
        },
        "urls": records,
    }
    path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Saved %d records → %s", len(records), path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Parse CLI arguments and run the configured discovery phases.

    Phases run in order: sitemap → category crawl → enrichment.
    Intermediate saves are written after each phase so a restart after a
    crash only needs to re-run the remaining phases.
    """
    parser = argparse.ArgumentParser(description="Discover all URLs on knowyourmeme.com")
    parser.add_argument("--output",       "-o", default=OUTPUT_FILE, help="Output JSON file")
    parser.add_argument("--max-category-pages"                     , help="Limit page number to set value")
    parser.add_argument("--sitemap-only",  action="store_true"     , help="Only run sitemap phase (skip crawl and enrichment)")
    parser.add_argument("--fresh",         action="store_true"     , help="Ignore existing output file, full rescan")
    parser.add_argument("--resume",        action="store_true"     , help="Continue from an existing output file")
    args = parser.parse_args()


    if args.fresh and args.resume:
        parser.error("--fresh and --resume are mutually exclusive")
            
    if args.sitemap_only and args.max_category_pages:
        parser.error("--sitemap-only and --max-category-pages are mutually exclusive")


    out_path = Path(args.output)
    session  = make_session()
    existing = {} if args.fresh else load(out_path)

    sitemap_stats = {"added": 0, "updated": 0, "skipped": 0}
    crawl_stats   = {"seen": 0, "added": 0}
    index         = dict(existing)

    # Phase 1: walk all sitemaps listed in robots.txt
    index, sitemap_stats = fetch_all_sitemaps(session, existing)
    save(out_path, index)

        # Infer namespace patterns and category pages from the collected corpus.
        # This updates the module-level lists so Phase 2 and namespace_of() use
        # values derived from real data rather than hardcoded fallbacks.
    if index:
        inferred_namespaces, inferred_categories = infer_taxonomy(index)
        global NAMESPACE_PATTERNS, CATEGORY_PAGES
        NAMESPACE_PATTERNS = inferred_namespaces
        CATEGORY_PAGES     = inferred_categories

    # Phase 2: paginate category pages to catch what sitemaps miss
    if not args.sitemap_only and args.max_category_pages:
        index, crawl_stats = crawl_categories(session=session, existing=index, max_category_pages=args.max_category_pages)
        save(out_path, index)
    elif not args.sitemap_only:
        index, crawl_stats = crawl_categories(session=session, existing=index)
        save(out_path, index)

    # Summary
    namespace_counts: dict[str, int] = {}
    for record in index.values():
        namespace = record.get("namespace") or "unknown"
        namespace_counts[namespace] = namespace_counts.get(namespace, 0) + 1

    null_lastmod_count = sum(1 for record in index.values() if record.get("lastmod") is None)

    log.info("=" * 55)
    log.info("DISCOVERY COMPLETE")
    log.info("  Total URLs      : %d", len(index))
    log.info("  Sitemap — added : %d  updated: %d  skipped: %d",
             sitemap_stats["added"], sitemap_stats["updated"], sitemap_stats["skipped"])
    log.info("  Crawl   — added : %d", crawl_stats["added"])
    log.info("  lastmod=null    : %d", null_lastmod_count)
    log.info("  Output          : %s", out_path)
    log.info("Breakdown by namespace:")
    for namespace in sorted(namespace_counts):
        log.info("  %-35s %d", namespace, namespace_counts[namespace])
    log.info("=" * 55)


if __name__ == "__main__":
    main()