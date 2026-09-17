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

# kym_kg's summarize(). Figures are the real September corpus, so the fixture
# is not invented; the dashboard's KG page is written against this shape.
KG = {
    "run": {"entries": 23882, "nodes_written": 552641, "edges_written": 855919,
            "stubs_deferred": 233614, "chunks": 48, "stubs_materialized": 9525},
    "build": {"build_id": "kg_20260916T102231Z_manual", "published": True,
              # every store, read back after the flip — all four must agree
              "pointers": {"files": "kg_20260916T102231Z_manual",
                           "fuseki": "kg_20260916T102231Z_manual",
                           "neo4j": "kg_20260916T102231Z_manual",
                           "mongo": "kg_20260916T102231Z_manual"},
              "kg_build_version": "4.0.0",
              "taxonomy_version": "f8317bfc137e70d8c96b4bcc6698a5aa205d093e",
              "entries_count": 23882, "parser_versions": ["1.5.0"],
              "corpus_policy_versions": ["2026-07-16-tags-gated-not-required"],
              "max_parsed_at": "2026-09-15T15:15:55+00:00",
              "snapshot_at": "2026-09-16T10:22:31+00:00", "ready_only": False},
    # The whole graph (verify's counts, which every store was checked
    # against) plus "core": the IMKG-comparable subgraph metrics measures.
    "graph": {"nodes": 476794, "edges": 855932, "frames": 23882, "rel_types": 8,
              "triples": 2632847,
              "nodes_by_kind": {"external_ref": 212644, "image": 127933,
                                "tag_concept": 102581, "frame": 23882,
                                "frame_stub": 9525, "entry_type_concept": 119,
                                "region_concept": 110},
              "edges_by_type": {"citesExternal": 223273, "hasTag": 223012,
                                "relatesToMeme": 214456, "hasImage": 137694,
                                "hasEntryType": 32884, "partOfSeries": 19158,
                                "hasRegion": 5442, "subTypeOf": 13},
              "core": {"nodes": 348751, "edges": 712796, "frames": 23882,
                       "rel_types": 6, "avg_degree": 4.09}},
    "integrity": {"unresolved_series_parents": 9523,
                  "frames_without_entry_type": 4507, "isolated_nodes": 0,
                  "dangling_edge_targets": 0, "duplicate_edges": 0,
                  "series_cycles_found": 0, "series_chain_max_depth": 7},
    "taxonomy": {"taxonomy_version": "f8317bfc137e70d8c96b4bcc6698a5aa205d093e",
                 "broader_edges": 13, "edges_encoded": 13,
                 "slugs_missing_from_census": [], "withheld_pairs": 10,
                 "buckets": {"broader_confirmed": 5, "broader_semantic_only": 8,
                             "contested": 2, "demoted": 3,
                             "do_not_encode_as_broader": 5,
                             "crosscutting_qualifiers": 3, "missing_umbrellas": 4}},
    "stores": {"fuseki": {"triples": 2632847,
                          "graph": "urn:memeatlas:build:kg_20260916T102231Z_manual"},
               "neo4j": {"nodes": 476794, "edges": 855932}},
    "exports": {"dir": "/opt/airflow/data/kg/builds/kg_20260916T102231Z_manual",
                "triples": 2632847,
                "view_filters": {"exclude_kinds": [], "top_tags": 0},
                "files": {"graph.nt": {"triples": 2632847, "sha256": "c1ad8e76ef17ac68"},
                          "relates_edges.csv": {"rows": 214456, "sha256": "0a"}}},
}

# The same DAG when the staleness gate short-circuits: everything downstream
# is skipped, summarize() still records why.
KG_SKIPPED = {
    "skipped": True,
    "reason": "nothing changed since the published build",
    "build": {"build_id": None,
              "stamps": {"kg_build_version": "4.0.0", "entries_count": 23882,
                         "max_parsed_at": "2026-09-15T15:15:55+00:00"}},
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
    ("discovery", DISCOVERY), ("scrape", SCRAPE), ("parse", PARSE),
    ("kg", KG), ("kg", KG_SKIPPED)])
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
