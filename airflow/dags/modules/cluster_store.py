"""
cluster_store.py — MongoDB persistence for DOM structural clustering
=====================================================================
Self-contained like dom_store.py, and the ONLY place the clustering
stage touches the database. Knows nothing about parsing or ML;
dom_cluster.py knows nothing about Mongo; the CLI (and later the DAG)
glues them.

Collections
-----------
``doms``   (owned by the scrape stage — read-only here)
    We read: url, scrape_status, html, encoding, content_sha256 —
    the raw material for feature extraction.

``urls``   (owned by discovery — one write-back)
    We write: page_template_type — the field mongo_store's discovery
    merge explicitly preserves for a later stage (mirrors dom_store's
    last_scraped write-back).
    We read: namespace — for per-namespace cluster breakdowns.

``dom_features``   (owned by this module)
    _id                 sha1(url)  (same convention as mongo_store/dom_store)
    url                 canonical page URL
    content_sha256      hash of the HTML the tokens came from — features
                        are stale once it no longer matches doms
    extractor_version   dom_cluster.EXTRACTOR_VERSION at extract time;
                        bumping the tokenizer invalidates every cache row
    tokens              zlib-compressed JSON [[token, count], ...]
    n_tokens / n_unique_tokens / n_nodes / max_depth
    extracted_at
    cluster_run_id / cluster_id / cluster_probability / assigned_at
                        latest clustering run that labelled this page;
                        cluster_id -1 = outlier / unknown template

``cluster_runs``   (owned by this module)
    _id (run id), started_at, finished_at, params, metrics, n_docs

``dom_clusters``   (owned by this module)
    _id "<run_id>:<cluster_id>", run_id, cluster_id, size, top_tokens,
    medoid_url, sample_urls, namespaces

Extraction is incremental and idempotent: select_pending_extraction()
returns only ok DOMs whose features are missing, stale (content hash
changed after a re-scrape) or from an older extractor version — so
re-running the extract step re-parses nothing it already has, exactly
like scrape_chunk re-filters against Mongo before buying credits.

Connection settings come from the environment (docker-compose):
    MONGODB_URI                      (default: mongodb://localhost:27017)
    MONGODB_DB                       (default: memes)
    MONGODB_URLS_COLLECTION          (default: urls)
    MONGODB_DOMS_COLLECTION          (default: doms)
    MONGODB_FEATURES_COLLECTION      (default: dom_features)
    MONGODB_CLUSTER_RUNS_COLLECTION  (default: cluster_runs)
    MONGODB_CLUSTERS_COLLECTION      (default: dom_clusters)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import zlib
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

log = logging.getLogger("cluster_store")

_ZLIB_LEVEL = 6
_BATCH = 200      # doms fetched per Mongo round-trip in iter_ok_html
_BULK = 1_000     # ops per bulk_write


def _url_doc_id(url: str) -> str:
    """Stable _id from the URL; same convention as mongo_store/dom_store,
    so the four collections cross-reference on _id as well as url."""
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _decode_html(payload: Any, encoding: str | None) -> str:
    """Mirror of dom_store._decode_html (stores stay self-contained)."""
    if encoding == "zlib":
        return zlib.decompress(bytes(payload)).decode("utf-8")
    return payload if isinstance(payload, str) else bytes(payload).decode("utf-8")


def _encode_tokens(tokens: dict[str, int]) -> Any:
    """{token: count} -> zlib(JSON [[token, count], ...]) Binary.

    Stored as pairs, not a sub-document: Mongo field names choke on
    dots, and every CSS class in a token would need escaping otherwise.
    """
    raw = json.dumps(sorted(tokens.items()), ensure_ascii=False).encode("utf-8")
    blob = zlib.compress(raw, _ZLIB_LEVEL)
    try:
        from bson.binary import Binary
    except ImportError:  # pragma: no cover
        return blob
    return Binary(blob)


def _decode_tokens(payload: Any) -> dict[str, int]:
    return dict(json.loads(zlib.decompress(bytes(payload)).decode("utf-8")))


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class ClusterStore:
    """Minimal pymongo store; pymongo imported lazily like the others."""

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
        self.features = self.db[os.getenv("MONGODB_FEATURES_COLLECTION",
                                          "dom_features")]
        self.runs = self.db[os.getenv("MONGODB_CLUSTER_RUNS_COLLECTION",
                                      "cluster_runs")]
        self.clusters = self.db[os.getenv("MONGODB_CLUSTERS_COLLECTION",
                                          "dom_clusters")]

        self.features.create_index("url", unique=True)
        self.features.create_index("extractor_version")
        self.features.create_index([("cluster_run_id", 1), ("cluster_id", 1)])
        self.clusters.create_index("run_id")

    def close(self) -> None:
        self.client.close()

    # -- selection (extract phase) -------------------------------------------

    def select_pending_extraction(self, extractor_version: int,
                                  limit: int = 0,
                                  force: bool = False) -> list[str]:
        """
        URLs with an ok DOM whose features are missing, stale (the DOM's
        content_sha256 changed since extraction) or from an older
        extractor version. force=True re-queues everything ok.
        """
        fresh: dict[str, tuple[str | None, int | None]] = {}
        if not force:
            for f in self.features.find(
                    {}, {"_id": 0, "url": 1, "content_sha256": 1,
                         "extractor_version": 1}):
                if f.get("url"):
                    fresh[f["url"]] = (f.get("content_sha256"),
                                       f.get("extractor_version"))

        pending: list[str] = []
        cur = self.doms.find({"scrape_status": "ok"},
                             {"_id": 0, "url": 1, "content_sha256": 1})
        for d in cur:
            url = d.get("url")
            if not url:
                continue
            have = fresh.get(url)
            if have and have[0] == d.get("content_sha256") \
                    and have[1] == extractor_version:
                continue  # cached tokens are still valid
            pending.append(url)
            if limit and len(pending) >= limit:
                break
        return pending

    # -- reading DOMs ---------------------------------------------------------

    def iter_ok_html(self, urls: Iterable[str]) -> Iterator[dict[str, Any]]:
        """Yield {'url','content_sha256','html'} for ok DOMs, batched so
        the full corpus never sits decompressed in memory at once."""

        def _flush(batch: list[str]) -> Iterator[dict[str, Any]]:
            if not batch:
                return
            cur = self.doms.find(
                {"url": {"$in": batch}, "scrape_status": "ok"},
                {"_id": 0, "url": 1, "html": 1, "encoding": 1,
                 "content_sha256": 1})
            for d in cur:
                try:
                    yield {"url": d["url"],
                           "content_sha256": d.get("content_sha256"),
                           "html": _decode_html(d["html"], d.get("encoding"))}
                except Exception as exc:  # corrupt blob: skip, don't crash
                    log.warning("Could not decode DOM for %s: %s",
                                d.get("url"), exc)

        batch: list[str] = []
        for url in urls:
            batch.append(url)
            if len(batch) >= _BATCH:
                yield from _flush(batch)
                batch = []
        yield from _flush(batch)

    # -- writing features -----------------------------------------------------

    def save_features(self, docs: Iterable[dict[str, Any]]) -> dict[str, int]:
        """Upsert feature docs as they stream in (durable per page, like
        dom_store.save_results). Returns tallies."""
        tallies = {"saved": 0, "failed": 0}
        for doc in docs:
            try:
                url = doc["url"]
                tokens: dict[str, int] = doc["tokens"]
                record = {
                    "url": url,
                    "content_sha256": doc.get("content_sha256"),
                    "extractor_version": doc["extractor_version"],
                    "tokens": _encode_tokens(tokens),
                    "n_tokens": int(sum(tokens.values())),
                    "n_unique_tokens": len(tokens),
                    "n_nodes": doc.get("n_nodes"),
                    "max_depth": doc.get("max_depth"),
                    "extracted_at": _now_utc(),
                }
                self.features.update_one({"_id": _url_doc_id(url)},
                                         {"$set": record}, upsert=True)
                tallies["saved"] += 1
            except Exception as exc:  # noqa: BLE001 — keep the stream alive
                tallies["failed"] += 1
                log.error("Failed to save features for %s: %s",
                          doc.get("url"), exc)
        return tallies

    # -- reading features (cluster phase) --------------------------------------

    def load_features(self, extractor_version: int
                      ) -> tuple[list[str], list[dict[str, int]]]:
        """All token maps for one extractor version, in a stable order."""
        urls: list[str] = []
        maps: list[dict[str, int]] = []
        cur = self.features.find({"extractor_version": extractor_version},
                                 {"_id": 0, "url": 1, "tokens": 1}
                                 ).sort("url", 1)
        for f in cur:
            try:
                maps.append(_decode_tokens(f["tokens"]))
                urls.append(f["url"])
            except Exception as exc:
                log.warning("Skipping corrupt feature row %s: %s",
                            f.get("url"), exc)
        return urls, maps

    # -- persisting a clustering run --------------------------------------------

    def save_assignments(self, run_id: str, urls: list[str],
                         labels: list[int],
                         probabilities: list[float] | None = None) -> int:
        """Stamp each feature row with its cluster for this run."""
        from pymongo import UpdateOne

        now = _now_utc()
        ops: list[Any] = []
        for i, url in enumerate(urls):
            update: dict[str, Any] = {
                "cluster_run_id": run_id,
                "cluster_id": int(labels[i]),
                "assigned_at": now,
            }
            if probabilities is not None:
                update["cluster_probability"] = float(probabilities[i])
            ops.append(UpdateOne({"_id": _url_doc_id(url)}, {"$set": update}))
            if len(ops) >= _BULK:
                self.features.bulk_write(ops, ordered=False)
                ops = []
        if ops:
            self.features.bulk_write(ops, ordered=False)
        return len(urls)

    def save_clusters(self, run_id: str,
                      cluster_docs: Iterable[dict[str, Any]]) -> int:
        n = 0
        for doc in cluster_docs:
            cid = doc["cluster_id"]
            self.clusters.replace_one(
                {"_id": f"{run_id}:{cid}"},
                {"_id": f"{run_id}:{cid}", "run_id": run_id, **doc},
                upsert=True)
            n += 1
        return n

    def save_run(self, run_id: str, params: dict, metrics: dict,
                 n_docs: int, started_at: datetime | None = None) -> None:
        self.runs.replace_one({"_id": run_id}, {
            "_id": run_id,
            "started_at": started_at or _now_utc(),
            "finished_at": _now_utc(),
            "params": params,
            "metrics": metrics,
            "n_docs": n_docs,
        }, upsert=True)

    # -- urls: one read + one write-back ----------------------------------------

    def namespace_map(self, urls: Iterable[str]) -> dict[str, str | None]:
        """url -> namespace, batched (for per-namespace cluster breakdowns)."""
        out: dict[str, str | None] = {}

        def _flush(batch: list[str]) -> None:
            if not batch:
                return
            for d in self.urls.find({"url": {"$in": batch}},
                                    {"_id": 0, "url": 1, "namespace": 1}):
                out[d["url"]] = d.get("namespace")

        batch: list[str] = []
        for url in urls:
            batch.append(url)
            if len(batch) >= _BULK:
                _flush(batch)
                batch = []
        _flush(batch)
        return out

    def write_template_types(self, mapping: dict[str, str]) -> int:
        """The one write-back to ``urls``: page_template_type — the field
        discovery's merge preserves for this stage. Never upserts, so a
        typo'd URL cannot create a phantom discovery record."""
        from pymongo import UpdateOne

        ops: list[Any] = []
        matched = 0
        for url, template_type in mapping.items():
            ops.append(UpdateOne(
                {"_id": _url_doc_id(url)},
                {"$set": {"page_template_type": template_type}}))
            if len(ops) >= _BULK:
                matched += self.urls.bulk_write(ops, ordered=False).matched_count
                ops = []
        if ops:
            matched += self.urls.bulk_write(ops, ordered=False).matched_count
        return matched

    # -- counters ----------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "doms_ok": self.doms.count_documents({"scrape_status": "ok"}),
            "features_total": self.features.count_documents({}),
            "features_by_extractor_version": {
                str(v): self.features.count_documents({"extractor_version": v})
                for v in self.features.distinct("extractor_version")
            },
            "cluster_runs": self.runs.count_documents({}),
        }
        latest = self.runs.find_one(sort=[("started_at", -1)])
        if latest:
            out["latest_run"] = {
                "run_id": latest["_id"],
                "started_at": latest.get("started_at"),
                "n_docs": latest.get("n_docs"),
                "metrics": latest.get("metrics"),
            }
        return out


# ---------------------------------------------------------------------------
# Module-level convenience (what the future DAG's tasks will call)
# ---------------------------------------------------------------------------

def get_store(uri: str | None = None, db_name: str | None = None) -> ClusterStore:
    return ClusterStore(uri=uri, db_name=db_name)


def pending_extraction(extractor_version: int, limit: int = 0,
                       force: bool = False) -> list[str]:
    store = ClusterStore()
    try:
        return store.select_pending_extraction(extractor_version,
                                               limit=limit, force=force)
    finally:
        store.close()


def feature_stats() -> dict[str, Any]:
    store = ClusterStore()
    try:
        return store.stats()
    finally:
        store.close()