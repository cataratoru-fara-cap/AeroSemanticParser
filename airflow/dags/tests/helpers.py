"""Shared test helpers (imported by the test modules; conftest.py patches the
fakes these rely on)."""
from __future__ import annotations

from pathlib import Path
from unittest import mock

import mongomock

TESTS = Path(__file__).resolve().parent
FIXTURES = TESTS / "fixtures"
DAGS = TESTS.parent
KG_CONFIG = DAGS / "kg_config"


def mock_store(cls, **kwargs):
    """The real store on an in-memory Mongo: its __init__ and _configure run
    as in production (collections bound, indexes created), each call empty."""
    with mock.patch("pymongo.MongoClient", mongomock.MongoClient):
        return cls(**kwargs)


def mock_stores(*classes):
    """Stores of several stages over ONE in-memory Mongo, as in production."""
    client = mongomock.MongoClient()
    with mock.patch("pymongo.MongoClient", lambda *a, **k: client):
        return [cls() for cls in classes]


def serving(module, store):
    """Make a store module's facade functions use ``store``."""
    return mock.patch.object(module, "get_store", return_value=store)


def assert_dag_calls_are_exported(dag_file: str, module) -> set[str]:
    """Every ``store.<name>(`` the DAG calls exists on the store module and is
    in its __all__ — read from the DAG source, never a hand-kept list: twice a
    live run died of AttributeError on a facade only the store class had."""
    import re
    called = set(re.findall(r"\bstore\.([A-Za-z_]\w*)\(", (DAGS / dag_file).read_text(encoding="utf-8")))
    assert called, dag_file
    for name in sorted(called):
        assert callable(getattr(module, name, None)), f"{dag_file} calls store.{name}() but there is no such facade"
        assert name in module.__all__, name
    return called
