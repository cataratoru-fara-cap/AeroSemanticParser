"""
dom_store.py — MongoDB persistence for scraped KYM DOMs
========================================================
Self-contained like mongo_store.py, and the ONLY place the scrape DAG
touches the database (the kym_store rule, applied to the scrape stage).
Knows nothing about HTTP; scrapingant_client knows nothing about Mongo;
the DAG glues them via FetchResult.as_doc() -> save_result(**doc).

Collections
-----------
``urls``  (owned by discovery, read here + one write-back)
    We read: url, namespace, Confirmed, lastmod  — to decide what to scrape.
    We write: last_scraped  — the field mongo_store's discovery merge
    explicitly preserves for this stage.

``doms``  (owned by this module)
    _id             sha1(url)  (same convention as mongo_store)
    url             canonical page URL
    scrape_status   'ok' | 'failed'
    html            zlib-compressed bytes (or utf-8 text, see encoding)
    encoding        'zlib' | 'none'
    content_sha256  hash of the *uncompressed* HTML (change detection)
    content_length  uncompressed size in bytes
    status_code     upstream status ScrapingAnt relayed
    fetched_at      when the stored html was fetched (ok docs only)
    first_fetched_at / last_attempt_at / attempts
    last_error / last_error_kind   'permanent' failures are never re-queued

A failed refetch NEVER clobbers a previously good DOM: failure updates
only the error bookkeeping and leaves scrape_status='ok' + html intact
(mirroring mongo_store's "never downgrade" merge spirit).

Selection (``select_pending``) walks urls ⋈ doms and queues, in order:
    1. never tried
    2. failed with error_kind != 'permanent' and attempts < cap
    3. ok but stale — sitemap lastmod newer than fetched_at, or
       fetched_at older than an optional refetch window

Connection settings come from the environment (docker-compose):
    MONGODB_URI               (default: mongodb://localhost:27017)
    MONGODB_DB                (default: memes)
    MONGODB_URLS_COLLECTION   (default: urls)
    MONGODB_DOMS_COLLECTION   (default: doms)
    DOM_COMPRESSION           zlib (default) | none
"""

from __future__ import annotations

import hashlib
import logging
import os
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

log = logging.getLogger("dom_store")

_ZLIB_LEVEL = 6


def _url_doc_id(url: str) -> str:
    """Stable _id from the URL; mirrors mongo_store._url_doc_id. The two
    collections join on the ``url`` field, so divergence would be
    cosmetic, but sharing the convention keeps cross-referencing easy."""
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(dt: datetime | None) -> datetime | None:
    """pymongo hands back naive datetimes (which are UTC) — normalise."""
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _parse_lastmod(lastmod: str | None) -> datetime | None:
    """Sitemap lastmod: '2024-01-15' or full ISO with offset. None on junk."""
    if not lastmod:
        return None
    try:
        dt = datetime.fromisoformat(lastmod.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _encode_html(html: str, compression: str) -> tuple[Any, str]:
    if compression == "zlib":
        try:
            from bson.binary import Binary
        except ImportError:  # pragma: no cover
            return zlib.compress(html.encode("utf-8"), _ZLIB_LEVEL), "zlib"
        return Binary(zlib.compress(html.encode("utf-8"), _ZLIB_LEVEL)), "zlib"
    return html, "none"


def _decode_html(payload: Any, encoding: str | None) -> str:
    if encoding == "zlib":
        return zlib.decompress(bytes(payload)).decode("utf-8")
    return payload if isinstance(payload, str) else bytes(payload).decode("utf-8")


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class DomStore:
    """Minimal pymongo store; pymongo imported lazily like mongo_store."""

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
        self.doms = self.db[os.getenv("MONGODB_DOMS_COLLECTION", "doms")]
        self.compression = os.getenv("DOM_COMPRESSION", "zlib").lower()

        self.doms.create_index("url", unique=True)
        self.doms.create_index("scrape_status")
        self.doms.create_index("fetched_at")

    # -- selection ----------------------------------------------------------

    def select_pending(self, limit: int = 0,
                       namespaces: Iterable[str] | None = None,
                       confirmed_only: bool = True,
                       refetch_older_than_days: int = 0,
                       max_failed_attempts: int = 3) -> list[str]:
        """Return URLs to scrape, ordered never-tried -> retry -> stale."""
        dom_meta: dict[str, dict] = {}
        for d in self.doms.find({}, {"_id": 0, "url": 1, "scrape_status": 1,
                                     "fetched_at": 1, "attempts": 1,
                                     "last_error_kind": 1}):
            if d.get("url"):
                dom_meta[d["url"]] = d

        query: dict[str, Any] = {}
        if confirmed_only:
            query["Confirmed"] = True
        ns = [n for n in (namespaces or []) if n]
        if ns:
            query["namespace"] = {"$in": ns}

        fresh_cutoff = None
        if refetch_older_than_days > 0:
            fresh_cutoff = _now_utc() - timedelta(days=refetch_older_than_days)

        never, retry, stale = [], [], []
        for rec in self.urls.find(query, {"_id": 0, "url": 1, "lastmod": 1}):
            url = rec.get("url")
            if not url:
                continue
            meta = dom_meta.get(url)
            if meta is None:
                never.append(url)
                continue
            if meta.get("scrape_status") != "ok":
                if (meta.get("last_error_kind") != "permanent"
                        and meta.get("attempts", 0) < max_failed_attempts):
                    retry.append(url)
                continue
            if self._is_stale(rec.get("lastmod"),
                              _as_utc(meta.get("fetched_at")), fresh_cutoff):
                stale.append(url)

        pending = never + retry + stale
        log.info("select_pending — never=%d retry=%d stale=%d (limit=%s)",
                 len(never), len(retry), len(stale), limit or "none")
        return pending[:limit] if limit and limit > 0 else pending

    @staticmethod
    def _is_stale(lastmod: str | None, fetched_at: datetime | None,
                  fresh_cutoff: datetime | None) -> bool:
        if fetched_at is None:
            return True
        if fresh_cutoff is not None and fetched_at < fresh_cutoff:
            return True
        lm = _parse_lastmod(lastmod)
        return lm is not None and lm > fetched_at

    def filter_unscraped(self, urls: list[str]) -> list[str]:
        """Drop URLs that already have a good DOM — makes Airflow task
        retries idempotent and, since ScrapingAnt bills per request,
        guarantees a retried chunk burns zero credits on finished work."""
        if not urls:
            return []
        done = {d["url"] for d in self.doms.find(
            {"url": {"$in": urls}, "scrape_status": "ok"},
            {"_id": 0, "url": 1})}
        return [u for u in urls if u not in done]

    # -- persistence ----------------------------------------------------------

    def save_result(self, *, url: str, ok: bool, html: str | None = None,
                    status_code: int | None = None, error: str | None = None,
                    error_kind: str | None = None, attempts_used: int = 1,
                    fetched_at: datetime | None = None) -> str:
        """Persist one fetch outcome. Returns 'ok' | 'failed' | 'kept_ok'."""
        now = fetched_at or _now_utc()
        doc_id = _url_doc_id(url)

        if ok and html:
            payload, encoding = _encode_html(html, self.compression)
            self.doms.update_one(
                {"_id": doc_id},
                {"$set": {
                    "url": url,
                    "scrape_status": "ok",
                    "html": payload,
                    "encoding": encoding,
                    "content_sha256": hashlib.sha256(
                        html.encode("utf-8")).hexdigest(),
                    "content_length": len(html.encode("utf-8")),
                    "status_code": status_code,
                    "fetched_at": now,
                    "last_attempt_at": now,
                    "last_error": None,
                    "last_error_kind": None,
                 },
                 "$inc": {"attempts": attempts_used},
                 "$setOnInsert": {"first_fetched_at": now}},
                upsert=True,
            )
            # Write-back discovery's scrape-stage field (ISO string, matching
            # the JSON-clean records the discovery DAG round-trips).
            self.urls.update_one({"url": url},
                                 {"$set": {"last_scraped": now.isoformat()}})
            return "ok"

        existing = self.doms.find_one({"_id": doc_id},
                                      {"scrape_status": 1, "html": 1})
        keeps_good = bool(existing
                          and existing.get("scrape_status") == "ok"
                          and existing.get("html") is not None)
        sets: dict[str, Any] = {
            "url": url,
            "last_error": error,
            "last_error_kind": error_kind,
            "last_error_status": status_code,
            "last_attempt_at": now,
        }
        if not keeps_good:
            sets["scrape_status"] = "failed"
        self.doms.update_one(
            {"_id": doc_id},
            {"$set": sets, "$inc": {"attempts": attempts_used},
             "$setOnInsert": {"first_fetched_at": now}},
            upsert=True,
        )
        return "kept_ok" if keeps_good else "failed"

    # -- reads / stats ------------------------------------------------------

    def load_html(self, url: str) -> str | None:
        """Transparent decompression — downstream never sees the encoding."""
        doc = self.doms.find_one({"url": url, "scrape_status": "ok"},
                                 {"html": 1, "encoding": 1})
        if not doc or doc.get("html") is None:
            return None
        return _decode_html(doc["html"], doc.get("encoding"))

    def stats(self) -> dict[str, int]:
        return {
            "urls_confirmed": self.urls.count_documents({"Confirmed": True}),
            "doms_total": self.doms.count_documents({}),
            "doms_ok": self.doms.count_documents({"scrape_status": "ok"}),
            "doms_failed": self.doms.count_documents(
                {"scrape_status": "failed"}),
            "failed_permanent": self.doms.count_documents(
                {"scrape_status": "failed", "last_error_kind": "permanent"}),
        }

    def close(self) -> None:
        self.client.close()


def get_store(uri: str | None = None, db_name: str | None = None) -> DomStore:
    return DomStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facade functions — the only calls the scrape DAG makes (kym_store style)
# ---------------------------------------------------------------------------

def pending_urls(limit: int = 0, namespaces: Iterable[str] | None = None,
                 confirmed_only: bool = True,
                 refetch_older_than_days: int = 0,
                 max_failed_attempts: int = 3) -> list[str]:
    store = get_store()
    try:
        return store.select_pending(
            limit=limit, namespaces=namespaces, confirmed_only=confirmed_only,
            refetch_older_than_days=refetch_older_than_days,
            max_failed_attempts=max_failed_attempts)
    finally:
        store.close()


def filter_unscraped(urls: list[str]) -> list[str]:
    store = get_store()
    try:
        return store.filter_unscraped(urls)
    finally:
        store.close()


def save_results(results: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Persist an iterable of FetchResult.as_doc() dicts; returns tallies.
    Accepts an iterator so the DAG can stream results straight from
    iter_fetch — each page is durable the moment it lands."""
    store = get_store()
    tallies = {"ok": 0, "failed": 0, "kept_ok": 0}
    try:
        for doc in results:
            tallies[store.save_result(**doc)] += 1
        return tallies
    finally:
        store.close()


def load_dom(url: str) -> str | None:
    store = get_store()
    try:
        return store.load_html(url)
    finally:
        store.close()


def scrape_stats() -> dict[str, int]:
    store = get_store()
    try:
        return store.stats()
    finally:
        store.close()