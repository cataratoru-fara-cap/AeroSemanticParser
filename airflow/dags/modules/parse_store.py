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

``entries``  (owned by this module) — SCHEMA-PURE: every doc is a valid
             KYMEntryScrape plus grading/provenance metadata, nothing else
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

``parse_failures``  (owned by this module) — the dead-letter collection,
    holding only currently-unresolved failures (a later successful parse
    of the same url deletes the record):
    _id / url               same convention as entries
    error / error_type      str(exc) truncated / exception class name
    failed_at / attempts    last failure time / how many times it failed
    dom_content_sha256 / parser_version / corpus_policy_version
                            the SAME staleness stamps as entries docs, so
                            select_pending (which consults both
                            collections) skips deterministic failures
                            until the parser or the page actually changes

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
    MONGODB_URI                        (default: mongodb://localhost:27017)
    MONGODB_DB                         (default: memes)
    MONGODB_URLS_COLLECTION            (default: urls)
    MONGODB_ENTRIES_COLLECTION         (default: entries)
    MONGODB_PARSE_FAILURES_COLLECTION  (default: parse_failures)
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from modules.kym_models import CorpusPolicy, KYMEntryScrape, corpus_ready
from modules.mongo_base import (
    MongoStoreBase,
    clean_namespaces,
    now_utc,
    url_doc_id,
)

log = logging.getLogger("parse_store")

__all__ = [
    "ParseStore", "get_store", "clean_namespaces", "pending_urls",
    "iter_html", "save_parsed", "namespaces_for", "save_failures",
    "parse_stats",
]


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class ParseStore(MongoStoreBase):
    """Owner of ``entries`` and ``parse_failures``; reads ``urls``."""

    def _configure(self) -> None:
        self.urls = self.collection("MONGODB_URLS_COLLECTION", "urls")
        self.entries = self.collection("MONGODB_ENTRIES_COLLECTION", "entries")
        self.failures = self.collection(
            "MONGODB_PARSE_FAILURES_COLLECTION", "parse_failures")

        self.entries.create_index("url", unique=True)
        self.entries.create_index("corpus_status")
        self.entries.create_index("category")
        self.entries.create_index("status")
        self.failures.create_index("url", unique=True)
        self.failures.create_index("error_type")
        self.failures.create_index("namespace")

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
            proj = {"_id": 0, "url": 1, "dom_content_sha256": 1,
                    "parser_version": 1, "corpus_policy_version": 1}
            url_list = list(candidate_shas)
            parsed_ok = {d["url"]: d
                         for d in self.entries.find(
                             {"url": {"$in": url_list}}, proj)}
            # Dead-letter records carry the same stamps; consulting them
            # here is what stops a deterministic failure being re-queued
            # on every run (it has no entries doc, so it would otherwise
            # always look "never parsed").
            failed = {d["url"]: d
                      for d in self.failures.find(
                          {"url": {"$in": url_list}}, proj)}

            def _up_to_date(doc: dict | None, sha: str) -> bool:
                return (doc is not None
                        and doc.get("dom_content_sha256") == sha
                        and doc.get("parser_version") == current_parser_version
                        and doc.get("corpus_policy_version") == current_policy_version)

            pending = [url for url, sha in candidate_shas.items()
                       if not (_up_to_date(parsed_ok.get(url), sha)
                               or _up_to_date(failed.get(url), sha))]
        return pending[:limit] if limit else pending

    def namespaces_for(self, urls: Iterable[str]) -> dict[str, str]:
        """{url: namespace} from the urls collection — the authoritative
        source (discovery's Taxonomy), not a re-inference. Falls back to
        kym_parse.infer_namespace_from_url() only for urls whose doc
        predates the namespace field or has it null."""
        from modules.kym_parse import infer_namespace_from_url

        url_list = list(urls)
        if not url_list:
            return {}
        out: dict[str, str] = {}
        for d in self.urls.find({"url": {"$in": url_list}},
                                {"_id": 0, "url": 1, "namespace": 1}):
            out[d["url"]] = d.get("namespace") or infer_namespace_from_url(d["url"])
        for url in url_list:
            out.setdefault(url, infer_namespace_from_url(url))
        return out

    # -- writes -----------------------------------------------------------

    def build_entry_doc(self, entry: KYMEntryScrape, dom_content_sha256: str,
                        policy: CorpusPolicy, parser_version: str,
                        policy_version: str) -> dict:
        """Grade + flatten one parsed entry into a Mongo-ready dict. Pure
        (no I/O) — kept on the store so the DAG task stays a one-liner per
        page, mirroring FetchResult.as_doc() -> save_result(**doc)."""
        ready, missing = corpus_ready(entry, policy)
        doc = entry.model_dump(mode="json", exclude_none=True)
        doc["_id"] = url_doc_id(str(entry.url))
        doc["corpus_status"] = "ready" if ready else "incomplete"
        doc["corpus_missing"] = missing
        doc["corpus_policy_version"] = policy_version
        doc["parser_version"] = parser_version
        doc["dom_content_sha256"] = dom_content_sha256
        doc["parsed_at"] = now_utc()
        return doc

    def upsert_entries(self, docs: Iterable[dict]) -> dict[str, int]:
        """Upsert already-built docs (see build_entry_doc). Returns tallies
        by corpus_status; nothing here ever discards a record. A successful
        parse RESOLVES any dead-letter record for the same url — the doc is
        deleted from `parse_failures` so that collection only ever holds
        currently-unresolved failures (no zombies after a parser fix)."""
        tallies = {"ready": 0, "incomplete": 0}
        resolved_ids: list[str] = []
        for doc in docs:
            self.entries.update_one(
                {"_id": doc["_id"]}, {"$set": doc}, upsert=True)
            tallies[doc["corpus_status"]] += 1
            resolved_ids.append(doc["_id"])
        if resolved_ids:
            self.failures.delete_many({"_id": {"$in": resolved_ids}})
        return tallies

    def save_failures(self, failures: Iterable[dict],
                      parser_version: str, policy_version: str) -> int:
        """Persist parse failures into the dedicated `parse_failures`
        collection — kept OUT of `entries` so that collection stays
        schema-pure (every entries doc is a valid KYMEntryScrape + grading).
        Each failure dict: {url, dom_content_sha256, error, error_type,
        namespace}. ``namespace`` is the label this method is about — e.g.
        a run of failures clustering under namespace='editorials' means an
        editorial URL slipped through the confirmed-meme filter, not that
        the parser itself is broken; that's a very different fix.

        Invariants:
          * Never downgrades — a failure physically cannot touch `entries`;
            a previously parsed entry survives a failed reparse untouched.
          * No retry loops — the record carries the same staleness stamps
            as an entries doc (dom sha + parser + policy versions), and
            select_pending consults BOTH collections, so a deterministic
            failure is not re-queued until the parser is upgraded or the
            page content actually changes.
          * Self-cleaning — upsert_entries deletes the record when the url
            later parses successfully.
        """
        now = now_utc()
        n = 0
        for f in failures:
            url = f["url"]
            self.failures.update_one(
                {"_id": url_doc_id(url)},
                {"$set": {
                    "url": url,
                    "namespace": f.get("namespace"),
                    "error": str(f.get("error"))[:2000],
                    "error_type": f.get("error_type"),
                    "failed_at": now,
                    "dom_content_sha256": f.get("dom_content_sha256"),
                    "parser_version": parser_version,
                    "corpus_policy_version": policy_version,
                 },
                 "$inc": {"attempts": 1}},
                upsert=True)
            n += 1
        return n

    # -- reads / stats ------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        total = self.entries.count_documents({})
        ready = self.entries.count_documents({"corpus_status": "ready"})
        by_missing: dict[str, int] = {}
        for field in self.entries.distinct("corpus_missing"):
            by_missing[field] = self.entries.count_documents(
                {"corpus_missing": field})
        by_error: dict[str, int] = {}
        for etype in self.failures.distinct("error_type"):
            by_error[etype] = self.failures.count_documents(
                {"error_type": etype})
        by_namespace: dict[str, int] = {}
        for ns in self.failures.distinct("namespace"):
            by_namespace[ns] = self.failures.count_documents(
                {"namespace": ns})
        return {
            "entries_total": total,
            "entries_ready": ready,
            "entries_incomplete": total - ready,
            "parse_failures": self.failures.count_documents({}),
            "failure_type_counts": by_error,
            "failure_namespace_counts": by_namespace,
            "missing_field_counts": by_missing,
        }


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
    with get_store() as store:
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


def iter_html(urls: list[str]):
    """Stream (url, html, dom_content_sha256) one page at a time.

    A deliberate re-export of dom_store.iter_html_for: `doms` is the scrape
    stage's collection, so the parse DAG reaches it through this module
    rather than importing dom_store itself — one store import per stage.
    STREAMING is load-bearing here:
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
    with get_store() as store:
        docs = (store.build_entry_doc(entry, sha, policy, parser_version,
                                      policy_version)
                for entry, sha in entries_with_meta)
        return store.upsert_entries(docs)


def namespaces_for(urls: Iterable[str]) -> dict[str, str]:
    """{url: namespace} — see ParseStore.namespaces_for."""
    url_list = list(urls)
    if not url_list:
        return {}
    with get_store() as store:
        return store.namespaces_for(url_list)


def save_failures(failures: list[dict], parser_version: str,
                  policy_version: str) -> int:
    """Persist parse-failure dead-letter records; see ParseStore.save_failures.
    ``failures``: [{url, dom_content_sha256, error, error_type}, ...]"""
    if not failures:
        return 0
    with get_store() as store:
        return store.save_failures(failures, parser_version, policy_version)


def parse_stats() -> dict[str, Any]:
    with get_store() as store:
        return store.stats()