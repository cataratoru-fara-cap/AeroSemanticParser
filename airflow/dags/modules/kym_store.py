"""
kym_store.py — persistence for the KYM discovery index
=======================================================
All storage lives here; kym_discover only discovers. Two backends:

  * JSON file  — the standalone-script sink ({ "metadata": ..., "urls": [...] })
  * MongoDB    — the pipeline sink, delegating to src.db.mongo.get_store()
                 (upsert preserves last_scraped and never downgrades Confirmed)

The three mongo_* functions are the ONLY place the Airflow DAG touches the
database, so adapting to a store API change means editing one file.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

BASE_URL = "https://knowyourmeme.com"

log = logging.getLogger("kym_store")


# ---------------------------------------------------------------------------
# JSON backend
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict[str, dict]:
    """Load an existing index from JSON (full envelope or bare list)."""
    path = Path(path)
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


def save_json(path: Path, index: dict[str, dict]) -> None:
    """Write the full index to JSON with a metadata envelope (sorted records)."""
    path = Path(path)
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


# ---------------------------------------------------------------------------
# MongoDB backend (wraps the project store; pymongo imported lazily)
# ---------------------------------------------------------------------------

def _iter_mongo_records(store, projection: dict):
    """
    Yield raw url records from the store.

    NOTE: adjust this ONE function if your src.db.mongo store exposes its own
    read API (e.g. store.iter_urls()). The {"_id": 0} projection keeps
    records JSON-clean so they can go straight into save_json / XCom.
    """
    return store.urls.find({}, projection)


def mongo_load_index() -> dict[str, dict]:
    """Load the full {url: record} index from MongoDB."""
    from src.db.mongo import get_store
    store = get_store()
    try:
        index = {r["url"]: r for r in _iter_mongo_records(store, {"_id": 0})
                 if "url" in r}
        log.info("Loaded %d records from MongoDB", len(index))
        return index
    finally:
        store.close()


def mongo_known_urls() -> set[str]:
    """Load just the URL set — enough for dedup / taxonomy inference."""
    from src.db.mongo import get_store
    store = get_store()
    try:
        urls = {r["url"] for r in
                _iter_mongo_records(store, {"_id": 0, "url": 1}) if "url" in r}
        log.info("Loaded %d known URLs from MongoDB", len(urls))
        return urls
    finally:
        store.close()


def mongo_upsert(records: Iterable[dict]) -> dict:
    """Upsert records, preserving last_scraped / monotonic Confirmed."""
    from src.db.mongo import get_store
    store = get_store()
    try:
        stats = store.upsert_urls(records)
        log.info("MongoDB upsert — added=%d updated=%d (total in db=%d)",
                 stats["added"], stats["updated"], store.count_urls())
        return stats
    finally:
        store.close()
