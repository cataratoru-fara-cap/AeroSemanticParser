"""Tests for kg_store.py — generational KG persistence (mongomock, no real Mongo).

Mirrors the fresh_store() pattern in test_parse_store.py: bypass
KGStore.__init__ and wire mongomock collections directly, so no index DDL
runs and no environment is read.

The contract these pin is atomicity without transactions. This deployment's
Mongo is standalone (no replica set), so multi-document transactions are
unavailable. Instead every writer works inside its own ``build_id``
namespace and one single-document write flips the authority. The tests that
matter most are therefore:

  * ``test_builds_are_isolated`` — a running build is invisible to readers
  * ``test_failed_build_leaves_the_pointer_untouched``
  * ``test_stub_never_replaces_a_real_frame`` — the invariant the old
    implementation defended with ~700k find_one round trips per build

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_store.py -v
"""
import unittest
from datetime import timedelta

import mongomock

from modules import kg_store as ks
from modules.mongo_base import now_utc

BUILD = "kg_20260916T120000Z_run_a"
OTHER_BUILD = "kg_20260916T130000Z_run_b"

FRAME = "https://knowyourmeme.com/memes/doge"
PARENT = "https://knowyourmeme.com/memes/shiba-inu"


def fresh_store() -> ks.KGStore:
    client = mongomock.MongoClient()
    store = ks.KGStore.__new__(ks.KGStore)
    store.client = client
    store.db = client["memes"]
    store.entries = store.db["entries"]
    store.nodes = store.db["kg_nodes"]
    store.edges = store.db["kg_edges"]
    store.builds = store.db["kg_builds"]
    return store


def frame_node(nid=FRAME, label="Doge"):
    return {"id": nid, "kind": "frame", "label": label,
            "category": "meme", "status": "confirmed"}


def stub_node(nid=PARENT):
    return {"id": nid, "kind": "frame_stub", "label": None,
            "category": "meme", "status": None}


def edge(src, etype, dst):
    return {"src": src, "type": etype, "dst": dst}


class SaveGraphTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()

    def test_nodes_and_edges_are_written_with_build_scoped_ids(self):
        got = self.store.save_graph(
            BUILD, [frame_node()], [edge(FRAME, "hasTag", "tag:doge")])
        self.assertEqual(got["nodes_written"], 1)
        self.assertEqual(got["edges_written"], 1)
        node = self.store.nodes.find_one({})
        self.assertEqual(node["_id"], f"{BUILD}|{FRAME}")
        self.assertEqual(node["build_id"], BUILD)
        self.assertEqual(node["node_id"], FRAME)
        self.assertEqual(self.store.edges.find_one({})["_id"],
                         f"{BUILD}|{FRAME}|hasTag|tag:doge")

    def test_stubs_are_deferred_not_written(self):
        got = self.store.save_graph(BUILD, [frame_node(), stub_node()], [])
        self.assertEqual(got["stubs_deferred"], 1)
        self.assertEqual(got["nodes_written"], 1)
        self.assertEqual(self.store.nodes.count_documents(
            {"kind": "frame_stub"}), 0)

    def test_repeated_node_properties_are_merged_not_replaced(self):
        # The same image is one page's og:image (with a size) and another
        # section's captioned image; neither write may erase the other.
        img = "image:https://i.kym-cdn.com/x.jpg"
        self.store.save_graph(BUILD, [{"id": img, "kind": "image", "width": 600},
                                      {"id": img, "kind": "image", "alt": "a"}], [])
        self.store.save_graph(BUILD, [{"id": img, "kind": "image", "caption": "c"}], [])
        doc = self.store.nodes.find_one({"node_id": img})
        self.assertEqual((doc["width"], doc["alt"], doc["caption"]), (600, "a", "c"))
        self.assertEqual(self.store.nodes.count_documents({}), 1)

    def test_duplicates_within_a_batch_are_collapsed(self):
        e = edge(FRAME, "hasTag", "tag:doge")
        got = self.store.save_graph(
            BUILD, [frame_node(), frame_node()], [e, dict(e)])
        self.assertEqual(got["nodes_written"], 1)
        self.assertEqual(got["edges_written"], 1)

    def test_rerunning_a_chunk_is_idempotent(self):
        for _ in range(2):
            self.store.save_graph(BUILD, [frame_node()],
                                  [edge(FRAME, "hasTag", "tag:doge")])
        self.assertEqual(self.store.nodes.count_documents({}), 1)
        self.assertEqual(self.store.edges.count_documents({}), 1)

    def test_builds_are_isolated(self):
        self.store.save_graph(BUILD, [frame_node()], [])
        self.store.save_graph(OTHER_BUILD, [frame_node(label="Doge v2")], [])
        self.assertEqual(self.store.nodes.count_documents({}), 2)
        self.assertEqual(
            self.store.nodes.find_one({"build_id": BUILD})["label"], "Doge")
        self.assertEqual(
            self.store.nodes.find_one({"build_id": OTHER_BUILD})["label"],
            "Doge v2")


class MaterializeStubsTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()

    def test_missing_edge_target_gets_a_stub(self):
        self.store.save_graph(BUILD, [frame_node()],
                              [edge(FRAME, "partOfSeries", PARENT)])
        got = self.store.materialize_stubs(BUILD)
        self.assertEqual(got["stubs_materialized"], 1)
        stub = self.store.nodes.find_one({"node_id": PARENT})
        self.assertEqual(stub["kind"], "frame_stub")
        self.assertEqual(stub["category"], "meme")   # guessed from the path

    def test_stub_never_replaces_a_real_frame(self):
        # The parent is itself a scraped frame in this build. Order between
        # mapped tasks must not matter — the stub must never win.
        self.store.save_graph(
            BUILD, [frame_node(), frame_node(PARENT, "Shiba Inu")],
            [edge(FRAME, "partOfSeries", PARENT)])
        self.store.materialize_stubs(BUILD)
        parent = self.store.nodes.find_one({"node_id": PARENT})
        self.assertEqual(parent["kind"], "frame")
        self.assertEqual(parent["label"], "Shiba Inu")

    def test_non_kym_target_becomes_an_external_ref(self):
        self.store.save_graph(
            BUILD, [frame_node()],
            [edge(FRAME, "citesExternal", "https://en.wikipedia.org/wiki/Doge")])
        self.store.materialize_stubs(BUILD)
        node = self.store.nodes.find_one({"node_id": {"$regex": "wikipedia"}})
        self.assertEqual(node["kind"], "external_ref")

    def test_concept_targets_are_not_turned_into_frame_stubs(self):
        self.store.save_graph(BUILD, [frame_node()],
                              [edge(FRAME, "hasTag", "tag:doge")])
        self.store.materialize_stubs(BUILD)
        self.assertNotEqual(
            self.store.nodes.find_one({"node_id": "tag:doge"})["kind"],
            "frame_stub")

    def test_only_this_build_is_touched(self):
        self.store.save_graph(OTHER_BUILD, [frame_node()], [])
        self.store.save_graph(BUILD, [frame_node()],
                              [edge(FRAME, "partOfSeries", PARENT)])
        self.store.materialize_stubs(BUILD)
        self.assertEqual(
            self.store.nodes.count_documents({"build_id": OTHER_BUILD}), 1)


class PointerTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()
        self.store.begin_build(BUILD, {"kg_build_version": "2.0.0"})

    def test_nothing_is_published_before_publish(self):
        self.assertIsNone(self.store.current_build_id())
        self.assertIsNone(self.store.current_build())

    def test_publish_flips_the_pointer(self):
        self.store.publish(BUILD, {"triples": 10})
        self.assertEqual(self.store.current_build_id(), BUILD)
        self.assertEqual(self.store.current_build()["state"], "published")
        self.assertEqual(self.store.current_build()["manifest"]["triples"], 10)

    def test_publish_is_one_document_write(self):
        self.store.publish(BUILD)
        self.assertEqual(
            self.store.builds.count_documents({"_id": ks.CURRENT}), 1)

    def test_publish_supersedes_the_previous_build(self):
        self.store.publish(BUILD)
        self.store.begin_build(OTHER_BUILD, {})
        self.store.publish(OTHER_BUILD)
        self.assertEqual(self.store.current_build_id(), OTHER_BUILD)
        self.assertEqual(
            self.store.builds.find_one({"_id": BUILD})["state"], "superseded")

    def test_publish_is_idempotent(self):
        self.store.publish(BUILD)
        self.store.publish(BUILD)
        self.assertEqual(self.store.current_build_id(), BUILD)
        self.assertEqual(
            self.store.builds.find_one({"_id": BUILD})["state"], "published")

    def test_verified_is_distinct_from_building_and_published(self):
        # publish=False on the DAG: the build is complete and checked but the
        # pointer was deliberately left alone. The first live run showed
        # such a build still reading "building".
        self.store.mark_verified(BUILD)
        doc = self.store.builds.find_one({"_id": BUILD})
        self.assertEqual(doc["state"], "verified")
        self.assertIn("verified_at", doc)
        self.assertIsNone(self.store.current_build_id())

    def test_failed_build_leaves_the_pointer_untouched(self):
        self.store.publish(BUILD)
        self.store.begin_build(OTHER_BUILD, {})
        self.store.save_graph(OTHER_BUILD, [frame_node()], [])
        self.store.fail_build(OTHER_BUILD, "verify gate failed")
        self.assertEqual(self.store.current_build_id(), BUILD)
        self.assertEqual(
            self.store.builds.find_one({"_id": OTHER_BUILD})["state"], "failed")


class StalenessTests(unittest.TestCase):
    STAMPS = {"kg_build_version": "2.0.0", "taxonomy_version": "abc",
              "entries_count": 100, "parser_versions": ["1.5.0"],
              "corpus_policy_versions": ["p1"], "max_parsed_at": "t0"}

    def test_no_published_build_is_stale(self):
        stale, why = ks.KGStore.is_stale(self.STAMPS, None)
        self.assertTrue(stale)
        self.assertIn("no published build", why)

    def test_identical_stamps_are_not_stale(self):
        stale, _ = ks.KGStore.is_stale(self.STAMPS, dict(self.STAMPS))
        self.assertFalse(stale)

    def test_force_overrides(self):
        stale, why = ks.KGStore.is_stale(self.STAMPS, dict(self.STAMPS),
                                         force=True)
        self.assertTrue(stale)
        self.assertEqual(why, "force_rebuild")

    def test_each_stamp_triggers_a_rebuild(self):
        for key in self.STAMPS:
            published = dict(self.STAMPS)
            published[key] = "something-else"
            stale, why = ks.KGStore.is_stale(self.STAMPS, published)
            self.assertTrue(stale, key)
            self.assertIn(key, why)


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()
        now = now_utc()
        self.store.entries.insert_many([
            {"_id": "a", "url": FRAME, "parsed_at": now - timedelta(hours=1),
             "parser_version": "1.5.0", "corpus_policy_version": "p1",
             "corpus_status": "ready", "dom_content_sha256": "f00",
             "sections": [{"kind": "other", "text": ["p"],
                           "images": [{"src": "https://i.kym-cdn.com/x.jpg"}]}]},
            {"_id": "b", "url": PARENT, "parsed_at": now - timedelta(hours=2),
             "parser_version": "1.5.0", "corpus_policy_version": "p1",
             "corpus_status": "incomplete"},
        ])

    def test_snapshot_collects_ids_and_versions(self):
        snap = self.store.snapshot()
        self.assertEqual(snap["entries_count"], 2)
        self.assertEqual(sorted(snap["entry_ids"]), ["a", "b"])
        self.assertEqual(snap["parser_versions"], ["1.5.0"])

    def test_entries_written_after_the_snapshot_are_invisible(self):
        snap = self.store.snapshot()
        self.store.entries.insert_one(
            {"_id": "c", "url": "u", "parsed_at": now_utc() + timedelta(hours=1)})
        streamed = list(self.store.iter_entries(["a", "b", "c"],
                                                snap["snapshot_at"]))
        self.assertEqual(len(streamed), 2)

    def test_ready_only_filters(self):
        self.assertEqual(self.store.snapshot(ready_only=True)["entries_count"], 1)

    def test_incomplete_entries_are_included_by_default(self):
        # "Nothing is discarded for being incomplete" applies to the KG too.
        self.assertEqual(self.store.snapshot()["entries_count"], 2)

    def test_projection_carries_the_whole_parsed_record(self):
        # 3.0.0 models section text, images and references, so the
        # projection only EXCLUDES pipeline bookkeeping.
        self.assertTrue(all(v == 0 for v in ks.ENTRY_PROJECTION.values()))
        self.assertNotIn("sections", ks.ENTRY_PROJECTION)
        self.assertIn("dom_content_sha256", ks.ENTRY_PROJECTION)

    def test_iter_entries_returns_sections_in_full(self):
        snap = self.store.snapshot()
        docs = {d["url"]: d for d in
                self.store.iter_entries(snap["entry_ids"], snap["snapshot_at"])}
        self.assertNotIn("dom_content_sha256", docs[FRAME])
        self.assertEqual(docs[FRAME]["sections"][0]["text"], ["p"])
        self.assertEqual(len(docs[FRAME]["sections"][0]["images"]), 1)


class CountsAndPruneTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()

    def test_counts_are_per_build(self):
        self.store.save_graph(
            BUILD, [frame_node(), {"id": "tag:doge", "kind": "tag_concept"}],
            [edge(FRAME, "hasTag", "tag:doge")])
        self.store.save_graph(OTHER_BUILD, [frame_node()], [])
        counts = self.store.counts(BUILD)
        self.assertEqual(counts["nodes"], 2)
        self.assertEqual(counts["edges"], 1)
        self.assertEqual(counts["frames"], 1)
        self.assertEqual(counts["edges_by_type"], {"hasTag": 1})

    def test_prune_keeps_the_published_build_even_if_old(self):
        self.store.begin_build(BUILD, {})
        self.store.save_graph(BUILD, [frame_node()], [])
        self.store.publish(BUILD)
        for i in range(3):
            bid = f"kg_later_{i}"
            self.store.begin_build(bid, {})
            self.store.save_graph(bid, [frame_node()], [])
        self.store.prune(keep=1)
        self.assertEqual(self.store.current_build_id(), BUILD)
        self.assertEqual(
            self.store.nodes.count_documents({"build_id": BUILD}), 1)

    def test_prune_deletes_old_generations_nodes_and_edges(self):
        for i in range(3):
            bid = f"kg_gen_{i}"
            self.store.begin_build(bid, {})
            self.store.save_graph(bid, [frame_node()],
                                  [edge(FRAME, "hasTag", "tag:doge")])
        got = self.store.prune(keep=1)
        self.assertEqual(got["pruned"], 2)
        self.assertEqual(self.store.nodes.count_documents({}), 1)
        self.assertEqual(self.store.edges.count_documents({}), 1)


class FacadeContractTests(unittest.TestCase):
    """The DAGs call module-level facades, never methods on a store.

    Twice now a live run has failed with AttributeError because a DAG
    called ``kg_store.<name>(...)`` and the module had only the METHOD on
    KGStore: ``iter_nodes`` on the first run, ``current_build_id`` after the
    Phase 3b rewrite — the second one slipping past a hand-maintained list
    of names in this very test. So the list is no longer hand-maintained:
    the names are read out of the DAG sources, and every one of them must
    exist on the module and be exported.
    """

    DAG_FILES = ("kym_kg_dag.py", "kym_kg_validate_dag.py")

    @classmethod
    def facades_used_by_the_dags(cls) -> set[str]:
        import re
        from pathlib import Path
        dags_dir = Path(__file__).resolve().parents[1]
        names: set[str] = set()
        for f in cls.DAG_FILES:
            src = (dags_dir / f).read_text(encoding="utf-8")
            # `store.<name>(` — a call on the module. `store.KGStore.x(` and
            # `store.CURRENT` do not match, and are fine.
            names |= set(re.findall(r"\bstore\.([A-Za-z_]\w*)\(", src))
        return names

    def test_the_dags_actually_use_the_module(self):
        names = self.facades_used_by_the_dags()
        self.assertGreater(len(names), 10, names)
        self.assertIn("publish_build", names)
        self.assertIn("current_build_id", names)      # the one that slipped

    def test_every_facade_the_dags_call_exists_and_is_exported(self):
        for name in sorted(self.facades_used_by_the_dags()):
            self.assertTrue(callable(getattr(ks, name, None)),
                            f"kym_kg calls kg_store.{name}() but the module has "
                            f"no such facade — only KGStore has it?")
            self.assertIn(name, ks.__all__, name)

    def test_iter_facades_are_generators_over_one_build(self):
        import mongomock
        import pymongo
        from unittest import mock
        with mock.patch.object(pymongo, "MongoClient", mongomock.MongoClient):
            with ks.get_store(uri="mongodb://mock", db_name="t") as s:
                s.save_graph(BUILD, [frame_node()],
                             [edge(FRAME, "hasTag", "tag:doge")])
                s.save_graph(OTHER_BUILD, [frame_node()], [])
            # Note: mongomock clients do not share state across instances,
            # so exercise the facade against the same client via the store.
            nodes = list(s.iter_nodes(BUILD))
            edges = list(s.iter_edges(BUILD))
        self.assertEqual(len(nodes), 1)
        self.assertEqual(len(edges), 1)
        self.assertNotIn("build_id", nodes[0])      # projection strips it
        self.assertEqual(nodes[0]["id"], FRAME)

    def test_iter_nodes_and_edges_filter_by_kind_type_and_fields(self):
        s = fresh_store()
        s.save_graph(BUILD, [{**frame_node(), "about": "long text"},
                             {"id": "tag:doge", "kind": "tag_concept", "label": "doge"}],
                     [edge(FRAME, "hasTag", "tag:doge"),
                      edge(FRAME, "hasSection", FRAME + "#s0")])
        frames = list(s.iter_nodes(BUILD, kinds=["frame"], fields=["label"]))
        self.assertEqual(frames, [{"id": FRAME, "kind": "frame", "label": "Doge"}])
        self.assertEqual([e["type"] for e in s.iter_edges(BUILD, types=["hasTag"])],
                         ["hasTag"])


class ValidationRecordTests(unittest.TestCase):
    """The RDF diff gate's verdict: a flag on the build, never a veto."""

    def setUp(self):
        self.store = fresh_store()
        self.store.begin_build(BUILD, {})
        self.store.publish(BUILD)

    def test_verdict_is_attached_without_touching_state_or_pointer(self):
        self.store.record_validation(
            BUILD, {"equal": False, "content_divergence": ["hasTag"]})
        doc = self.store.builds.find_one({"_id": BUILD})
        self.assertFalse(doc["validation"]["equal"])
        self.assertEqual(doc["state"], "published")       # flagged, not yanked
        self.assertEqual(self.store.current_build_id(), BUILD)

    def test_a_later_verdict_replaces_the_earlier(self):
        self.store.record_validation(BUILD, {"equal": False})
        self.store.record_validation(BUILD, {"equal": True})
        self.assertTrue(self.store.builds.find_one({"_id": BUILD})
                        ["validation"]["equal"])


class LegacyCoexistenceTests(unittest.TestCase):
    """The live collections hold ~1M documents from the previous
    implementation, none with a build_id. The store must connect over them
    and never count them."""

    def test_legacy_documents_are_invisible_to_build_scoped_reads(self):
        store = fresh_store()
        store.nodes.insert_many([{"_id": "legacy-1", "kind": "frame"},
                                 {"_id": "legacy-2", "kind": "frame"}])
        store.save_graph(BUILD, [frame_node()], [])
        self.assertEqual(store.counts(BUILD)["nodes"], 1)
        self.assertEqual(len(list(store.iter_nodes(BUILD))), 1)

    def test_snapshot_reports_the_newest_parse_as_a_stamp(self):
        store = fresh_store()
        newest = now_utc() - timedelta(minutes=1)
        store.entries.insert_many([
            {"_id": "a", "url": FRAME, "parsed_at": newest - timedelta(days=1)},
            {"_id": "b", "url": PARENT, "parsed_at": newest},
        ])
        snap = store.snapshot()
        self.assertIsNotNone(snap["max_parsed_at"])
        self.assertIsNotNone(snap["max_parsed_at"].tzinfo)
        self.assertEqual(snap["max_parsed_at"].replace(microsecond=0),
                         newest.replace(microsecond=0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
