"""summary_store.py, the run_summaries sink the dashboard reads (mongomock).
Pinned: chronological order, upsert on retry, stage isolation, tz-aware
timestamps, and the summaries' real shapes surviving the JSON round trip."""
import mongomock
import pymongo
import pytest

from modules import summary_store

# The shapes the summarize tasks return, so a change to one shows up here rather than in the dashboard.
DISCOVERY = {"total": 41230, "confirmed": 39877, "lastmod_null": 512,
             "namespaces": {"memes": 35000, "memes/events": 2100, "memes/people": 1800, "unknown": 15}}
SCRAPE = {"run": {"ok": 480, "failed": 12, "kept_ok": 3, "skipped": 5},
          "corpus": {"doms_ok": 23412, "doms_failed": 231, "doms_total": 23643, "urls_confirmed": 24817,
                     "failed_permanent": 34}}
PARSE = {"run": {"ready": 285, "incomplete": 692, "parse_failed": 22, "skipped": 0},
         "corpus": {"entries_total": 18220, "entries_ready": 16990, "entries_incomplete": 1230, "parse_failures": 143,
                    "failure_type_counts": {"ValidationError": 120, "ValueError": 23},
                    "missing_field_counts": {"region": 900, "year": 250, "about": 80}}}
# kym_kg's summarize() over the real September corpus; the dashboard's KG page is written against this shape
BUILD = "kg_20260916T102231Z_manual"
TAXONOMY = "f8317bfc137e70d8c96b4bcc6698a5aa205d093e"
KG = {
    "run": {"entries": 23882, "nodes_written": 552641, "edges_written": 855919, "stubs_deferred": 233614,
            "chunks": 48, "stubs_materialized": 9525},
    "build": {"build_id": BUILD, "published": True,
              "pointers": {"files": BUILD, "fuseki": BUILD, "neo4j": BUILD, "mongo": BUILD},   # all four must agree
              "kg_build_version": "4.0.0", "taxonomy_version": TAXONOMY, "entries_count": 23882,
              "parser_versions": ["1.5.0"], "corpus_policy_versions": ["2026-07-16-tags-gated-not-required"],
              "max_parsed_at": "2026-09-15T15:15:55+00:00", "snapshot_at": "2026-09-16T10:22:31+00:00",
              "ready_only": False},
    # the whole graph (verify's counts) plus "core", the IMKG-comparable subgraph metrics measures
    "graph": {"nodes": 476794, "edges": 855932, "frames": 23882, "rel_types": 8, "triples": 2632847,
              "nodes_by_kind": {"external_ref": 212644, "image": 127933, "tag_concept": 102581, "frame": 23882,
                                "frame_stub": 9525, "entry_type_concept": 119, "region_concept": 110},
              "edges_by_type": {"citesExternal": 223273, "hasTag": 223012, "relatesToMeme": 214456,
                                "hasImage": 137694, "hasEntryType": 32884, "partOfSeries": 19158, "hasRegion": 5442,
                                "subTypeOf": 13},
              "core": {"nodes": 348751, "edges": 712796, "frames": 23882, "rel_types": 6, "avg_degree": 4.09}},
    "integrity": {"unresolved_series_parents": 9523, "frames_without_entry_type": 4507, "isolated_nodes": 0,
                  "dangling_edge_targets": 0, "duplicate_edges": 0, "series_cycles_found": 0,
                  "series_chain_max_depth": 7},
    "taxonomy": {"taxonomy_version": TAXONOMY, "broader_edges": 13, "edges_encoded": 13,
                 "slugs_missing_from_census": [], "withheld_pairs": 10,
                 "buckets": {"broader_confirmed": 5, "broader_semantic_only": 8, "contested": 2, "demoted": 3,
                             "do_not_encode_as_broader": 5, "crosscutting_qualifiers": 3, "missing_umbrellas": 4}},
    "stores": {"fuseki": {"triples": 2632847, "graph": f"urn:memeatlas:build:{BUILD}"},
               "neo4j": {"nodes": 476794, "edges": 855932}},
    "exports": {"dir": f"/opt/airflow/data/kg/builds/{BUILD}", "triples": 2632847,
                "view_filters": {"exclude_kinds": [], "top_tags": 0},
                "files": {"graph.nt": {"triples": 2632847, "sha256": "c1ad8e76ef17ac68"},
                          "relates_edges.csv": {"rows": 214456, "sha256": "0a"}}},
}
# the same DAG when the staleness gate short-circuits: summarize() still records why
KG_SKIPPED = {"skipped": True, "reason": "nothing changed since the published build",
              "build": {"build_id": None, "stamps": {"kg_build_version": "4.0.0", "entries_count": 23882,
                                                     "max_parsed_at": "2026-09-15T15:15:55+00:00"}}}


@pytest.fixture
def store(monkeypatch):
    monkeypatch.setattr(pymongo, "MongoClient", mongomock.MongoClient)
    with summary_store.get_store(uri="mongodb://mock", db_name="memes_test") as s:   # closed by __exit__
        yield s


def test_order_retries_and_stages(store):
    store.save("scrape", "kym_scrape", "run_a", {"run": {"ok": 1}})
    store.save("scrape", "kym_scrape", "run_b", {"run": {"ok": 2}})
    rows = store.history("scrape")
    assert [r["run_id"] for r in rows] == ["run_a", "run_b"] and rows[1]["summary"]["run"]["ok"] == 2   # oldest first
    assert rows[0]["created_at"].tzinfo is not None                                                    # UTC
    store.save("parse", "kym_parse", "run_a", {"n": 1})
    store.save("parse", "kym_parse", "run_a", {"n": 2})                    # a task retry upserts
    assert [r["summary"] for r in store.history("parse")] == [{"n": 2}]
    assert len(store.history("scrape")) == 2                               # stages are isolated


@pytest.mark.parametrize("stage, summary", [("discovery", DISCOVERY), ("scrape", SCRAPE), ("parse", PARSE),
                                            ("kg", KG), ("kg", KG_SKIPPED)])
def test_real_summary_shapes_survive_the_json_round_trip(store, stage, summary):
    # stored as a JSON string, not a sub-document: the keys are arbitrary ('/' in namespaces, class names)
    store.save(stage, f"kym_{stage}", "run_a", summary)
    assert store.history(stage)[0]["summary"] == summary
