"""
test_summary_plots.py — fixture-based tests for the summary plot layer
=======================================================================
Fixtures mirror the real shapes the three summarize tasks return today:
discovery (flat counts + a namespaces map), scrape and parse
({"run": tallies, "corpus": stats with nested count maps}). The store
round-trip runs against mongomock, like the other store tests.

Run inside the container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags \
        airflow-dag-processor python -m pytest tests/test_summary_plots.py -q
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from modules import summary_plots

# --------------------------------------------------------------------------
# Fixtures — one summary per stage, shaped like the live summarize outputs
# --------------------------------------------------------------------------

DISCOVERY = {
    "total": 41230, "confirmed": 39877, "lastmod_null": 512,
    "namespaces": {"memes": 35000, "memes/events": 2100,
                   "memes/people": 1800, "memes/sites": 900,
                   "memes/subcultures": 850, "memes/cultures": 300,
                   "memes/participatory-media": 180, "memes/pop-culture": 60,
                   "memes/slang": 25, "unknown": 15},
}

SCRAPE = {
    "run": {"ok": 480, "failed": 12, "kept_ok": 3, "skipped": 5},
    "corpus": {"ok": 23412, "failed": 231, "pending": 16100},
}

PARSE = {
    "run": {"upserted": 470, "parse_failed": 8, "skipped": 2},
    "corpus": {
        "entries_total": 18220, "entries_ready": 16990,
        "entries_incomplete": 1230, "parse_failures": 143,
        "failure_type_counts": {"ValidationError": 120, "ValueError": 23},
        "missing_field_counts": {"region": 900, "year": 250, "about": 80},
    },
}


def _history(summary: dict, n: int = 3) -> list[dict]:
    """n fake runs of the same shape with slightly drifting values."""
    t0 = datetime(2026, 7, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(n):
        s = summary  # values identical run-to-run is fine for rendering
        rows.append({"run_id": f"run_{i}", "created_at": t0 + timedelta(days=i),
                     "summary": s})
    return rows


# --------------------------------------------------------------------------
# analyze() — shape walking
# --------------------------------------------------------------------------

def test_analyze_discovery_flat_and_map():
    scalars, breakdowns = summary_plots.analyze(DISCOVERY)
    # loose top-level numerics: trended + charted under the root group
    assert scalars["total"] == 41230
    assert "total" in breakdowns[summary_plots.ROOT_GROUP]
    # 10 namespaces > MAX_TREND_SERIES_FROM_MAP: snapshot only, no trend keys
    assert "namespaces" in breakdowns
    assert not any(k.startswith("namespaces.") for k in scalars)


def test_analyze_parse_nested_maps():
    scalars, breakdowns = summary_plots.analyze(PARSE)
    assert scalars["corpus.entries_total"] == 18220
    assert scalars["run.parse_failed"] == 8
    # nested count-maps become their own breakdown charts
    assert "corpus.failure_type_counts" in breakdowns
    assert "corpus.missing_field_counts" in breakdowns
    # ...and small ones (2-3 keys) also contribute trend series
    assert scalars["corpus.missing_field_counts.region"] == 900
    # loose corpus scalars are grouped for one snapshot chart
    assert breakdowns["corpus"]["entries_ready"] == 16990


def test_analyze_ignores_non_numeric():
    scalars, breakdowns = summary_plots.analyze(
        {"note": "hello", "flag": True, "n": 3, "empty": {}})
    assert scalars == {"n": 3.0}
    assert list(breakdowns) == [summary_plots.ROOT_GROUP]


# --------------------------------------------------------------------------
# render_all() — files on disk
# --------------------------------------------------------------------------

@pytest.mark.parametrize("stage,summary", [
    ("discovery", DISCOVERY), ("scrape", SCRAPE), ("parse", PARSE)])
def test_render_snapshots_only(tmp_path, stage, summary):
    written = summary_plots.render_all(stage, summary, history=None,
                                       out_dir=tmp_path)
    assert written and all(p.exists() and p.stat().st_size > 0
                           for p in written)
    assert all(p.name.startswith("snapshot_") for p in written)
    assert all(p.parent == tmp_path / stage for p in written)


def test_render_with_history_adds_trends(tmp_path):
    written = summary_plots.render_all("parse", PARSE,
                                       history=_history(PARSE, 3),
                                       out_dir=tmp_path)
    names = {p.name for p in written}
    assert "trend_run.png" in names
    assert "trend_corpus.png" in names


def test_single_run_history_skips_trends(tmp_path):
    written = summary_plots.render_all("scrape", SCRAPE,
                                       history=_history(SCRAPE, 1),
                                       out_dir=tmp_path)
    assert not any(p.name.startswith("trend_") for p in written)


def test_rerender_overwrites_stable_filenames(tmp_path):
    first = summary_plots.render_all("scrape", SCRAPE, out_dir=tmp_path)
    second = summary_plots.render_all("scrape", SCRAPE, out_dir=tmp_path)
    assert sorted(p.name for p in first) == sorted(p.name for p in second)


# --------------------------------------------------------------------------
# summary_store — round-trip against mongomock
# --------------------------------------------------------------------------

@pytest.fixture()
def mock_store(monkeypatch):
    import mongomock
    import pymongo
    from modules import summary_store
    monkeypatch.setattr(pymongo, "MongoClient", mongomock.MongoClient)
    store = summary_store.get_store(uri="mongodb://mock", db_name="memes_test")
    yield store
    store.close()


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
    assert len(mock_store.history("parse")) == 1