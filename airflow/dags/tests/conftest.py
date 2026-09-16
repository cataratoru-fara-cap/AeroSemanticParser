"""
Shared pytest bootstrap for the pipeline tests.

The DAG modules import their libraries as ``modules.x``, which works in the
container because /opt/airflow/dags is on sys.path. Outside it, pytest needs
the same thing — so this puts ``airflow/dags`` on sys.path once, here,
instead of each test file rolling its own (one of which hardcoded an
absolute path from a different machine).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1]
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

# Stores read these at connect time. Tests use mongomock and never reach a
# real server, but the defaults keep them from depending on a developer's
# shell environment.
os.environ.setdefault("MONGODB_DB", "memes_test")
os.environ.setdefault("DOM_COMPRESSION", "zlib")


# ---------------------------------------------------------------------------
# mongomock <-> pymongo bulk_write compatibility
# ---------------------------------------------------------------------------
# pymongo 4.17 passes ``sort=`` into the bulk builder for ReplaceOne and
# UpdateOne; mongomock 4.3.0 — the latest release, there is no newer one —
# does not accept that parameter. The result is that *every* bulk upsert
# raises TypeError against the test double while working perfectly against
# real Mongo.
#
# kg_store batches its writes on purpose: the implementation it replaced did
# a find_one plus a replace_one per node, which is ~700k round trips for a
# full build. So the fix belongs here, in the fake — pinning pymongo down or
# adding a try/except fallback in the store would mean the tested path is no
# longer the production path, which defeats having the tests at all.
#
# Deliberately conditional on the installed signature, so it becomes a no-op
# the moment mongomock catches up rather than permanently shadowing the real
# implementation; and it refuses a genuinely non-None sort instead of
# dropping semantics the fake cannot honour.
def _patch_mongomock_bulk() -> None:
    import inspect

    try:
        from mongomock.collection import BulkOperationBuilder
    except ImportError:      # pragma: no cover - mongomock not installed
        return

    for name in ("add_replace", "add_update"):
        original = getattr(BulkOperationBuilder, name, None)
        if original is None:
            continue
        if "sort" in inspect.signature(original).parameters:
            continue         # mongomock supports it; leave the real method alone

        def make(method, method_name):
            def patched(self, *args, sort=None, **kwargs):
                if sort is not None:
                    raise NotImplementedError(
                        f"mongomock cannot honour sort= in {method_name}; "
                        "this test would not be testing real behaviour")
                return method(self, *args, **kwargs)
            return patched

        setattr(BulkOperationBuilder, name, make(original, name))


_patch_mongomock_bulk()
