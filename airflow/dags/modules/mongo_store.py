"""
mongo_store.py — self-contained MongoDB store for the KYM pipeline
===================================================================
Lives inside airflow/dags/modules/ so it is importable in the container with
no PYTHONPATH tricks and no submodule. Replaces the abandoned src.db.mongo.

Provides exactly what kym_store.py needs:
    get_store()            -> MongoStore
    store.urls             -> raw pymongo collection (for .find())
    store.upsert_urls(recs) -> {"added": int, "updated": int}
    store.count_urls()     -> int
    store.close()

Upsert preserves last_scraped and never downgrades Confirmed True -> False,
so re-running discovery is safe and idempotent.

Connection settings come from the environment (set in docker-compose):
    MONGODB_URI   (default: mongodb://localhost:27017)
    MONGODB_DB    (default: memes)
    MONGODB_URLS_COLLECTION (default: urls)
"""

from __future__ import annotations

import hashlib
import os
from typing import Any, Iterable


def _url_doc_id(url: str) -> str:
    """Stable _id from the URL so re-discovering the same URL upserts, not dupes."""
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def _merge_discovery(old: dict | None, new: dict) -> dict:
    """
    Merge a freshly-discovered record onto an existing one.

    Rules:
      * Confirmed is monotonic: once True it stays True.
      * last_scraped / page_template_type are owned by the scrape stage —
        never clobber an existing value with the discovery default (None).
      * A confirmed lastmod is kept; otherwise the newer value wins.
    """
    if not old:
        return dict(new)

    merged = dict(old)
    merged.update({k: v for k, v in new.items() if v is not None})

    merged["Confirmed"] = bool(old.get("Confirmed")) or bool(new.get("Confirmed"))

    for owned in ("last_scraped", "page_template_type"):
        if old.get(owned) is not None:
            merged[owned] = old[owned]

    if not merged["Confirmed"]:
        merged["lastmod"] = None
    return merged


class MongoStore:
    """Minimal pymongo store. pymongo imported lazily so import never fails."""

    def __init__(self, uri: str | None = None, db_name: str | None = None):
        try:
            from pymongo import MongoClient, UpdateOne
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "pymongo is not installed — add it to the Airflow image "
                "(it is already in airflow/requirements.txt)."
            ) from exc

        self._UpdateOne = UpdateOne
        self.client = MongoClient(uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
        self.db = self.client[db_name or os.getenv("MONGODB_DB", "memes")]
        self.urls = self.db[os.getenv("MONGODB_URLS_COLLECTION", "urls")]
        self.urls.create_index("url", unique=True)
        self.urls.create_index("namespace")
        self.urls.create_index("Confirmed")
        self.urls.create_index("last_scraped")

    def upsert_urls(self, records: Iterable[dict[str, Any]]) -> dict[str, int]:
        new_by_id: dict[str, dict] = {}
        for rec in records:
            url = rec.get("url")
            if url:
                new_by_id[_url_doc_id(url)] = rec

        if not new_by_id:
            return {"added": 0, "updated": 0}

        existing = {
            d["_id"]: d
            for d in self.urls.find({"_id": {"$in": list(new_by_id)}})
        }

        ops, stats = [], {"added": 0, "updated": 0}
        for _id, rec in new_by_id.items():
            old = existing.get(_id)
            merged = _merge_discovery(old, rec)
            merged["_id"] = _id
            ops.append(self._UpdateOne({"_id": _id}, {"$set": merged}, upsert=True))
            stats["updated" if old else "added"] += 1

        self.urls.bulk_write(ops, ordered=False)
        return stats

    def count_urls(self) -> int:
        return self.urls.count_documents({})

    def close(self) -> None:
        self.client.close()


def get_store(uri: str | None = None, db_name: str | None = None) -> MongoStore:
    return MongoStore(uri=uri, db_name=db_name)