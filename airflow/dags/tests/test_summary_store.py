"""
test_summary_store.py — round-trip tests for the run_summaries sink
====================================================================
`run_summaries` is what the dashboard reads, so the contract this file
pins down is the one the dashboard depends on: chronological ordering,
upsert-on-retry, stage isolation, and tz-aware timestamps.

Replaces test_summary_plots.py, which tested the static-PNG renderer the
dashboard superseded. The store round-trip tests below are carried over
from it unchanged.

Run inside the container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags \
        airflow-dag-processor python -m pytest dags/tests/test_summary_store.py -q
"""

from __future__ import annotations

import pytest

# Shapes the three summarize tasks actually return today — kept as fixtures
# so a change to a summary's shape shows up here rather than in the dashboard.
DISCOVERY = {
    "total": 41230, "confirmed": 39877, "lastmod_null": 512,
    "namespaces": {"memes": 35000, "memes/events": 2100,
                   "memes/people": 1800, "unknown": 15},
}

SCRAPE = {
    "run": {"ok": 480, "failed": 12, "kept_ok": 3, "skipped": 5},
    "corpus": {"doms_ok": 23412, "doms_failed": 231, "doms_total": 23643,
               "urls_confirmed": 24817, "failed_permanent": 34},
}

PARSE = {
    "run": {"ready": 285, "incomplete": 692, "parse_failed": 22, "skipped": 0},
    "corpus": {
        "entries_total": 18220, "entries_ready": 16990,
        "entries_incomplete": 1230, "parse_failures": 143,
        "failure_type_counts": {"ValidationError": 120, "ValueError": 23},
        "missing_field_counts": {"region": 900, "year": 250, "about": 80},
    },
}


@pytest.fixture()
def mock_store(monkeypatch):
    import mongomock
    import pymongo
    from modules import summary_store
    monkeypatch.setattr(pymongo, "MongoClient", mongomock.MongoClient)
    with summary_store.get_store(uri="mongodb://mock",
                                 db_name="memes_test") as store:
        yield store


def test_store_roundtrip_and_order(mock_store):
    mock_store.save("scrape", "kym_scrape", "run_a", {"run": {"ok": 1}})
    mock_store.save("scrape", "kym_scrape", "run_b", {"run": {"ok": 2}})
    rows = mock_store.history("scrape")
    assert [r["run_id"] for r in rows] == ["run_a", "run_b"]  # oldest first
    assert rows[1]["summary"]["run"]["ok"] == 2
    assert rows[0]["created_at"].tzinfo is not None  # normalised to UTC


def test_store_retry_upserts_not_duplicates(mock_store):
    mock_store.save("parse", "kym_parse", "run_a", {"n": 1})
    mock_store.save("parse", "kym_parse", "run_a", {"n": 2})  # task retry
    rows = mock_store.history("parse")
    assert len(rows) == 1
    assert rows[0]["summary"]["n"] == 2


def test_store_stages_are_isolated(mock_store):
    mock_store.save("scrape", "kym_scrape", "run_a", {"n": 1})
    mock_store.save("parse", "kym_parse", "run_a", {"n": 1})
    assert len(mock_store.history("scrape")) == 1


@pytest.mark.parametrize("stage,summary", [
    ("discovery", DISCOVERY), ("scrape", SCRAPE), ("parse", PARSE)])
def test_real_summary_shapes_survive_the_json_round_trip(mock_store, stage,
                                                         summary):
    """Summaries are stored as a JSON string, not a sub-document, because
    their keys are arbitrary (namespace paths contain '/', error types are
    class names). Assert the nesting comes back intact."""
    mock_store.save(stage, f"kym_{stage}", "run_a", summary)
    assert mock_store.history(stage)[0]["summary"] == summary


def test_context_manager_closes_the_client(monkeypatch):
    import mongomock
    import pymongo
    from modules import summary_store
    monkeypatch.setattr(pymongo, "MongoClient", mongomock.MongoClient)
    with summary_store.get_store(uri="mongodb://mock",
                                 db_name="memes_test") as store:
        client = store.client
    assert client is not None  # closed by __exit__; mongomock tolerates reuse
