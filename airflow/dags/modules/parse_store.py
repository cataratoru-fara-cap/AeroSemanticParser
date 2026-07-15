"""
parse_store.py — MongoDB persistence for parsed KYM entries
=============================================================
The ONLY place the parse DAG touches the database (dom_store rule, applied
to the parse stage). Knows nothing about HTML parsing — kym_parse.py knows
nothing about Mongo; the DAG glues them via parse_entry() -> build_entry_doc()
-> upsert_entries().

Collections
-----------
``urls``   (owned by discovery, read-only here)
    We read: url, namespace, Confirmed — to build the candidate set.

``doms``   (owned by the scrape stage, read-only here, via dom_store)
    Selection reads ONLY content_sha256 (dom_store.content_shas — never the
    html field; decompressing every DOM to read its hash OOM-killed the
    first run). Parsing streams html one page at a time
    (dom_store.iter_html_for) so peak memory stays ~one page.

``entries``  (owned by this module)
    _id                     sha1(url)  (same convention as urls/doms)
    url                     canonical page URL
    ...                     every KYMEntryScrape field, flattened
    corpus_status           "ready" | "incomplete"  (from corpus_ready())
    corpus_missing          list[str] — exactly what corpus_ready() returns
    corpus_policy_version   which CorpusPolicy generation graded this entry
    parser_version          kym_parse.PARSER_VERSION at parse time
    dom_content_sha256      copied from the source DOM at parse time —
                            the staleness key: if this no longer matches
                            the current doms.content_sha256, the page
                            changed and is due for re-parse
    parsed_at               when THIS record was written

Nothing is ever discarded for being "incomplete" — corpus_status/missing are
labels, not a filter. A thin entry stays in `entries`, fully queryable
(``db.entries.find({"corpus_missing": "region"})``), and can be re-graded in
place by re-running corpus_ready() without re-parsing, or re-parsed in place
if PARSER_VERSION or the DOM itself has moved on.

Selection (``pending_urls``) walks urls ⋈ doms ⋈ entries and returns URLs
that are OK-scraped but either never parsed, or stale by one of:
    * the source DOM's content_sha256 has changed since the last parse
    * the stored parser_version differs from the one currently running
    * the stored corpus_policy_version differs from the one currently active
    * force_reparse=True (ignore all of the above)

Connection settings come from the environment (docker-compose), same
variable names as dom_store/mongo_store:
    MONGODB_URI                  (default: mongodb://localhost:27017)
    MONGODB_DB                   (default: memes)
    MONGODB_URLS_COLLECTION      (default: urls)
    MONGODB_ENTRIES_COLLECTION   (default: entries)
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

from modules.kym_models import CorpusPolicy, KYMEntryScrape, corpus_ready

log = logging.getLogger("parse_store")


def _url_doc_id(url: str) -> str:
    """Stable _id from the URL; mirrors dom_store._url_doc_id / mongo_store's
    convention so the three collections join cleanly on _id or url."""
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def clean_namespaces(raw):
    """Reuse dom_store's helper rather than re-implement it — same '' | '"'
    | 'a,b' | ['a'] -> None | ['a','b'] contract."""
    from modules.dom_store import clean_namespaces as _clean
    return _clean(raw)


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class ParseStore:
    """Minimal pymongo store; pymongo imported lazily like dom_store."""

    def __init__(self, uri: str | None = None, db_name: str | None = None):
        try:
            from pymongo import MongoClient
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "pymongo is not installed — it is in airflow/requirements.txt."
            ) from exc

        self.client = MongoClient(
            uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
        self.db = self.client[db_name or os.getenv("MONGODB_DB", "memes")]
        self.urls = self.db[os.getenv("MONGODB_URLS_COLLECTION", "urls")]
        self.entries = self.db[os.getenv("MONGODB_ENTRIES_COLLECTION", "entries")]

        self.entries.create_index("url", unique=True)
        self.entries.create_index("corpus_status")
        self.entries.create_index("category")
        self.entries.create_index("status")

    # -- selection ------------------------------------------------------

    def select_pending(self, candidate_shas: dict[str, str],
                       current_parser_version: str,
                       current_policy_version: str,
                       force_reparse: bool = False,
                       limit: int = 0) -> list[str]:
        """``candidate_shas`` is {url: content_sha256} for OK-scraped DOMs
        already restricted to the desired namespace/confirmed filter (the
        caller gets this from dom_store — parse_store doesn't touch `doms`
        directly, keeping the doms/entries boundary the same shape as
        dom_store's urls/doms boundary).

        Returns the subset needing a (re-)parse, per the staleness rules in
        the module docstring.
        """
        if not candidate_shas:
            return []
        if force_reparse:
            pending = list(candidate_shas)
        else:
            existing = {
                d["url"]: d
                for d in self.entries.find(
                    {"url": {"$in": list(candidate_shas)}},
                    {"_id": 0, "url": 1, "dom_content_sha256": 1,
                     "parser_version": 1, "corpus_policy_version": 1})
            }
            pending = []
            for url, sha in candidate_shas.items():
                prior = existing.get(url)
                if prior is None:
                    pending.append(url)
                    continue
                if prior.get("dom_content_sha256") != sha:
                    pending.append(url)
                elif prior.get("parser_version") != current_parser_version:
                    pending.append(url)
                elif prior.get("corpus_policy_version") != current_policy_version:
                    pending.append(url)
        return pending[:limit] if limit else pending

    # -- writes -----------------------------------------------------------

    def build_entry_doc(self, entry: KYMEntryScrape, dom_content_sha256: str,
                        policy: CorpusPolicy, parser_version: str,
                        policy_version: str) -> dict:
        """Grade + flatten one parsed entry into a Mongo-ready dict. Pure
        (no I/O) — kept on the store so the DAG task stays a one-liner per
        page, mirroring FetchResult.as_doc() -> save_result(**doc)."""
        ready, missing = corpus_ready(entry, policy)
        doc = entry.model_dump(mode="json", exclude_none=True)
        doc["_id"] = _url_doc_id(str(entry.url))
        doc["corpus_status"] = "ready" if ready else "incomplete"
        doc["corpus_missing"] = missing
        doc["corpus_policy_version"] = policy_version
        doc["parser_version"] = parser_version
        doc["dom_content_sha256"] = dom_content_sha256
        doc["parsed_at"] = _now_utc()
        return doc

    def upsert_entries(self, docs: Iterable[dict]) -> dict[str, int]:
        """Upsert already-built docs (see build_entry_doc). Returns tallies
        by corpus_status; nothing here ever discards a record."""
        tallies = {"ready": 0, "incomplete": 0}
        for doc in docs:
            self.entries.update_one(
                {"_id": doc["_id"]}, {"$set": doc}, upsert=True)
            tallies[doc["corpus_status"]] += 1
        return tallies

    # -- reads / stats ------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        total = self.entries.count_documents({})
        ready = self.entries.count_documents({"corpus_status": "ready"})
        by_missing: dict[str, int] = {}
        for field in self.entries.distinct("corpus_missing"):
            by_missing[field] = self.entries.count_documents(
                {"corpus_missing": field})
        return {
            "entries_total": total,
            "entries_ready": ready,
            "entries_incomplete": total - ready,
            "missing_field_counts": by_missing,
        }

    def close(self) -> None:
        self.client.close()


def get_store(uri: str | None = None, db_name: str | None = None) -> ParseStore:
    return ParseStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facade functions — the only calls the parse DAG makes (dom_store style)
# ---------------------------------------------------------------------------

def pending_urls(namespaces: Iterable[str] | None = None,
                 confirmed_only: bool = True,
                 current_parser_version: str = "",
                 current_policy_version: str = "",
                 force_reparse: bool = False,
                 limit: int = 0) -> list[str]:
    """Candidate discovery (via dom_store) + staleness filtering (here)."""
    from modules import dom_store

    ns = clean_namespaces(namespaces)
    candidate_shas: dict[str, str] = {}
    store = get_store()
    try:
        query: dict = {}
        if confirmed_only:
            query["Confirmed"] = True
        if ns:
            query["namespace"] = {"$in": ns}
        candidate_urls = [r["url"] for r in
                         store.urls.find(query, {"_id": 0, "url": 1})
                         if r.get("url")]
        if candidate_urls:
            # sha-only projection — selection must NEVER pull the html field:
            # decompressing every candidate DOM just to read its hash is what
            # OOM-killed the first select_urls run (same failure class as the
            # dom_cluster matrix buffers: materializing what should stream).
            candidate_shas = dom_store.content_shas(candidate_urls)

        return store.select_pending(
            candidate_shas, current_parser_version, current_policy_version,
            force_reparse=force_reparse, limit=limit)
    finally:
        store.close()


def iter_html(urls: list[str]):
    """Stream (url, html, dom_content_sha256) one page at a time — thin
    re-export of dom_store.iter_html_for so DAG tasks only import
    parse_store for this stage's reads. STREAMING is load-bearing here:
    a KYM page is multi-MB decompressed and ~10x that inside BeautifulSoup,
    so materializing a whole chunk of pages at once OOMs the worker. Peak
    memory with this generator is one page + one soup, regardless of
    chunk_size."""
    from modules import dom_store
    yield from dom_store.iter_html_for(urls)


def save_parsed(entries_with_meta: Iterable[tuple[KYMEntryScrape, str]],
                policy: CorpusPolicy, parser_version: str,
                policy_version: str) -> dict[str, int]:
    """``entries_with_meta`` is (KYMEntryScrape, dom_content_sha256) pairs.
    Builds + upserts in one pass so the DAG task body stays a loop + one
    call, mirroring dom_store.save_results()."""
    store = get_store()
    try:
        docs = (store.build_entry_doc(entry, sha, policy, parser_version,
                                      policy_version)
                for entry, sha in entries_with_meta)
        return store.upsert_entries(docs)
    finally:
        store.close()


def parse_stats() -> dict[str, Any]:
    store = get_store()
    try:
        return store.stats()
    finally:
        store.close()