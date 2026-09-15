"""
mongo_base.py — shared MongoDB plumbing for the pipeline stores
================================================================
Every stage store (mongo_store, dom_store, parse_store, summary_store)
used to repeat the same four things: the lazy pymongo import, the
URI/DB env-var resolution, the sha1 ``_id`` convention, and UTC datetime
normalisation. They now inherit that from here, so the connection
contract is defined once.

What this module does NOT do is own a collection. Collection ownership
stays with the stage store — ``doms`` belongs to dom_store, ``entries``
to parse_store — and this base class only hands a subclass the plumbing
to bind one.

``clean_namespaces`` lives here rather than in a stage store because two
stores need it (dom_store and parse_store both filter ``urls`` by
namespace) and putting it in either one would make the other import a
sibling's private name — which is exactly what it replaces.

Environment (set in docker-compose):
    MONGODB_URI   (default: mongodb://localhost:27017)
    MONGODB_DB    (default: memes)
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Iterable

DEFAULT_MONGODB_URI = "mongodb://localhost:27017"
DEFAULT_MONGODB_DB = "memes"

UNKNOWN_NAMESPACE = "unknown"

# Junk that trigger forms and CLI quoting leak into a namespaces param.
_NS_JUNK = " \t\r\n\"'/"


# ---------------------------------------------------------------------------
# Shared conventions
# ---------------------------------------------------------------------------

def url_doc_id(url: str) -> str:
    """Stable ``_id`` from a URL. Shared by every collection keyed on a page
    (urls, doms, entries, parse_failures) so they join cleanly on ``_id``."""
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(dt: datetime | None) -> datetime | None:
    """pymongo hands back naive datetimes (which are UTC) — normalise."""
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def clean_namespaces(raw: str | Iterable[str] | None) -> list[str] | None:
    """Normalise a namespaces filter from any trigger source.

    ``''`` / ``'"'`` / ``None``  -> None            (no filter: every namespace)
    ``'memes, events'``          -> ['memes', 'events']
    ``['/memes/']``              -> ['memes']

    Returning None (never []) keeps the "no filter" case unrepresentable as a
    truthy empty list, which is how a stray quote silently emptied the corpus.
    """
    if raw is None:
        return None
    tokens = raw.split(",") if isinstance(raw, str) else list(raw)
    out = [t for t in (str(t).strip(_NS_JUNK) for t in tokens) if t]
    return out or None


# ---------------------------------------------------------------------------
# Base store
# ---------------------------------------------------------------------------

class MongoStoreBase:
    """Connection + collection binding for a stage store.

    Subclasses implement ``_configure()`` to bind their collections (via
    ``self.collection()``) and declare their indexes. pymongo is imported
    lazily so importing a store module never fails at DAG-parse time on a
    machine without the driver.

    Usable as a context manager, which is how the facade functions call it::

        with get_store() as store:
            return store.stats()
    """

    def __init__(self, uri: str | None = None, db_name: str | None = None):
        try:
            from pymongo import MongoClient
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "pymongo is not installed — it is in airflow/requirements.txt."
            ) from exc

        self.client = MongoClient(
            uri or os.getenv("MONGODB_URI", DEFAULT_MONGODB_URI))
        self.db = self.client[
            db_name or os.getenv("MONGODB_DB", DEFAULT_MONGODB_DB)]
        self._configure()

    def collection(self, env_var: str, default: str):
        """Bind one collection, name overridable per deployment."""
        return self.db[os.getenv(env_var, default)]

    def _configure(self) -> None:
        """Bind collections and create indexes. Subclass hook."""
        raise NotImplementedError

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
