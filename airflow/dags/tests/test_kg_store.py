"""kg_store.py: generational KG persistence. The contract is atomicity without
transactions (the Mongo is standalone): every writer works inside its own
build_id and one single-document write flips the authority. Most important:
builds are isolated, a failed build leaves the pointer alone, and a stub never
replaces a real frame (once defended with ~700k find_one round trips a build)."""
import re
from datetime import timedelta

import pytest

from helpers import DAGS, mock_store
from modules import kg_store as ks
from modules.mongo_base import now_utc

BUILD, OTHER = "kg_20260916T120000Z_run_a", "kg_20260916T130000Z_run_b"
FRAME = "https://knowyourmeme.com/memes/doge"
PARENT = "https://knowyourmeme.com/memes/shiba-inu"


@pytest.fixture
def store():
    return mock_store(ks.KGStore)


def frame(nid=FRAME, label="Doge"):
    return {"id": nid, "kind": "frame", "label": label, "category": "meme", "status": "confirmed"}


def edge(src, etype, dst):
    return {"src": src, "type": etype, "dst": dst}


TAG = edge(FRAME, "hasTag", "tag:doge")


# -- writing a generation -------------------------------------------------------

def test_nodes_and_edges_are_written_with_build_scoped_ids(store):
    got = store.save_graph(BUILD, [frame()], [TAG])
    assert (got["nodes_written"], got["edges_written"]) == (1, 1)
    node = store.nodes.find_one({})
    assert (node["_id"], node["build_id"], node["node_id"]) == (f"{BUILD}|{FRAME}", BUILD, FRAME)
    assert store.edges.find_one({})["_id"] == f"{BUILD}|{FRAME}|hasTag|tag:doge"


def test_stubs_are_deferred_not_written(store):
    stub = {"id": PARENT, "kind": "frame_stub", "label": None, "category": "meme", "status": None}
    got = store.save_graph(BUILD, [frame(), stub], [])
    assert (got["stubs_deferred"], got["nodes_written"]) == (1, 1)
    assert store.nodes.count_documents({"kind": "frame_stub"}) == 0


def test_repeated_node_properties_are_merged_not_replaced(store):
    # one file is one page's og:image (sized) and another section's captioned image
    img = "image:https://i.kym-cdn.com/x.jpg"
    store.save_graph(BUILD, [{"id": img, "kind": "image", "width": 600}, {"id": img, "kind": "image", "alt": "a"}], [])
    store.save_graph(BUILD, [{"id": img, "kind": "image", "caption": "c"}], [])
    doc = store.nodes.find_one({"node_id": img})
    assert (doc["width"], doc["alt"], doc["caption"], store.nodes.count_documents({})) == (600, "a", "c", 1)


def test_duplicates_collapse_and_a_rerun_chunk_is_idempotent(store):
    got = store.save_graph(BUILD, [frame(), frame()], [TAG, dict(TAG)])
    assert (got["nodes_written"], got["edges_written"]) == (1, 1)
    store.save_graph(BUILD, [frame()], [TAG])
    assert (store.nodes.count_documents({}), store.edges.count_documents({})) == (1, 1)


def test_builds_are_isolated(store):
    store.save_graph(BUILD, [frame()], [])
    store.save_graph(OTHER, [frame(label="Doge v2")], [])
    assert store.nodes.count_documents({}) == 2
    assert [store.nodes.find_one({"build_id": b})["label"] for b in (BUILD, OTHER)] == ["Doge", "Doge v2"]


# -- stubs ----------------------------------------------------------------------

def test_a_missing_edge_target_gets_a_stub_and_only_in_this_build(store):
    store.save_graph(OTHER, [frame()], [])
    store.save_graph(BUILD, [frame()], [edge(FRAME, "partOfSeries", PARENT)])
    assert store.materialize_stubs(BUILD)["stubs_materialized"] == 1
    stub = store.nodes.find_one({"node_id": PARENT})
    assert (stub["kind"], stub["category"]) == ("frame_stub", "meme")    # guessed from the path
    assert store.nodes.count_documents({"build_id": OTHER}) == 1


def test_a_stub_never_replaces_a_real_frame(store):
    # the parent is a scraped frame of this build: mapped-task order must not matter
    store.save_graph(BUILD, [frame(), frame(PARENT, "Shiba Inu")], [edge(FRAME, "partOfSeries", PARENT)])
    store.materialize_stubs(BUILD)
    parent = store.nodes.find_one({"node_id": PARENT})
    assert (parent["kind"], parent["label"]) == ("frame", "Shiba Inu")


@pytest.mark.parametrize("target", ["https://en.wikipedia.org/wiki/Doge",
                                    "https://knowyourmeme.com/photos/1220637-who-would-win"])  # 7.0.0: no frame
def test_a_target_that_is_no_entry_becomes_an_external_ref(store, target):
    store.save_graph(BUILD, [frame()], [edge(FRAME, "citesExternal", target)])
    store.materialize_stubs(BUILD)
    assert store.nodes.find_one({"node_id": target})["kind"] == "external_ref"


@pytest.mark.parametrize("prefix, kind", [("type:", "entry_type_concept"), ("tag:", "tag_concept"),
                                          ("region:", "region_concept"), ("origin:", "origin_concept"),
                                          ("badge:", "badge_concept")])
def test_a_dangling_concept_gets_its_real_kind(store, prefix, kind):
    store.save_graph(BUILD, [frame()], [edge(FRAME, "hasTag", f"{prefix}dangling")])
    store.materialize_stubs(BUILD)
    node = store.nodes.find_one({"node_id": f"{prefix}dangling"})
    assert (node["kind"], node["label"]) == (kind, "dangling")


def test_a_concept_referenced_only_by_a_concept_edge_is_a_concept(store):
    # 5.0.0: origin's umbrella parents are reached only through subTypeOf; as
    # external_ref they were invisible to the RDF (no skos:Concept, no scheme)
    store.save_graph(BUILD, [frame()], [edge("origin:twitter", "subTypeOf", "origin:social-network")])
    store.materialize_stubs(BUILD)
    parent = store.nodes.find_one({"node_id": "origin:social-network"})
    assert (parent["kind"], parent["label"]) == ("origin_concept", "social-network")
    assert store.nodes.find_one({"node_id": "origin:twitter"})["kind"] == "origin_concept"


# -- the pointer -----------------------------------------------------------------

@pytest.fixture
def begun(store):
    store.begin_build(BUILD, {"kg_build_version": "2.0.0"})
    return store


def test_publish_is_one_write_that_flips_the_pointer(begun):
    assert begun.current_build_id() is None and begun.current_build() is None
    begun.publish(BUILD, {"triples": 10})
    assert begun.current_build_id() == BUILD
    assert (begun.current_build()["state"], begun.current_build()["manifest"]["triples"]) == ("published", 10)
    begun.publish(BUILD)                                    # idempotent
    assert (begun.current_build_id(), begun.builds.find_one({"_id": BUILD})["state"]) == (BUILD, "published")
    assert begun.builds.count_documents({"_id": ks.CURRENT}) == 1


def test_publish_supersedes_and_a_failed_build_leaves_the_pointer(begun):
    begun.publish(BUILD)
    begun.begin_build(OTHER, {})
    begun.save_graph(OTHER, [frame()], [])
    begun.fail_build(OTHER, "verify gate failed")
    assert begun.current_build_id() == BUILD
    assert begun.builds.find_one({"_id": OTHER})["state"] == "failed"
    begun.begin_build("kg_later", {})
    begun.publish("kg_later")
    assert begun.current_build_id() == "kg_later"
    assert begun.builds.find_one({"_id": BUILD})["state"] == "superseded"


def test_verified_is_distinct_from_building_and_published(begun):
    # publish=False: complete and checked, the pointer deliberately left alone
    begun.mark_verified(BUILD)
    doc = begun.builds.find_one({"_id": BUILD})
    assert doc["state"] == "verified" and "verified_at" in doc and begun.current_build_id() is None


def test_a_validation_verdict_flags_the_build_and_never_yanks_it(begun):
    begun.publish(BUILD)
    begun.record_validation(BUILD, {"equal": False, "content_divergence": ["hasTag"]})
    doc = begun.builds.find_one({"_id": BUILD})
    assert (doc["validation"]["equal"], doc["state"], begun.current_build_id()) == (False, "published", BUILD)
    begun.record_validation(BUILD, {"equal": True})        # a later verdict replaces it
    assert begun.builds.find_one({"_id": BUILD})["validation"]["equal"] is True


# -- staleness ------------------------------------------------------------------------

STAMPS = {
    "kg_build_version": "2.0.0", "taxonomy_version": "abc", "entries_count": 100, "parser_versions": ["1.5.0"],
    "corpus_policy_versions": ["p1"], "max_parsed_at": "t0",
    # each layer's: a change in what a layer SAYS, not only its size, rebuilds
    "events_units": 10, "events_total": 31, "events_prompt_versions": ["1"],
    "events_extraction_versions": ["1.0.0"], "events_schema_shas": ["6323fdafc8996ce4"],
    "events_max_extracted_at": "2026-09-18T09:00:00+00:00",
    "entities_frames": 20, "entities_mentions": 140, "entities_linker_versions": ["1.0.0"],
    "entities_lexicon_versions": ["5d2737e5595693eb"], "entities_nlp_models": ["en_core_web_sm@3.8.0"],
    "entities_max_linked_at": "2026-09-22T09:00:00+00:00",
    "entities_senses_versions": ["0f3b2c1d4e5a6978"], "entities_curation_frames": 20, "entities_in_graph": 60,
    "entities_curation_pending": 12, "entities_curation_versions": ["a1b2c3d4e5f60718"],
    "entities_judge_models": ["mistral-small3.2:24b"], "entities_max_curated_at": "2026-09-29T09:00:00+00:00",
    "templates_frames": 15, "templates_links": 18, "templates_selection_digest": "9c1e",
    "templates_versions": ["1.0.0"], "templates_detailed": 12, "templates_max_selected_at": "2026-10-01T09:00:00+00:00",
    "template_entities_templates": 12, "template_entities_in_graph": 30,
    "template_entities_lexicons": ["5d2737e5595693eb"], "template_entities_prompts": ["1"],
    "template_entities_models": ["qwen3-vl:32b"], "template_entities_max_linked_at": "2026-10-01T10:00:00+00:00",
    "frame_images_frames": 20, "frame_images_in_graph": 41, "frame_images_lexicons": ["5d2737e5595693eb"],
    "frame_images_prompts": ["1"], "frame_images_models": ["qwen3-vl:32b"],
    "frame_images_max_linked_at": "2026-10-05T09:00:00+00:00",
    "wikidata_statement_items": 90, "wikidata_statements": 1100,
    "wikidata_statement_dumps": ["wikidata-20260914-all.json.gz"], "wikidata_statement_versions": ["1.0.0"],
    "wikidata_statements_max_extracted_at": "2026-10-05T10:00:00+00:00",
    # gap 14: a mark that moves to another kept address changes only the digest
    "duplicate_addresses": 830, "duplicate_addresses_digest": "4f2a"}


def test_every_layers_stamps_are_in_the_fixture():
    for keys in (ks.EVENT_STAMP_KEYS, ks.ENTITY_STAMP_KEYS, ks.TEMPLATE_STAMP_KEYS, ks.FRAME_IMAGE_STAMP_KEYS,
                 ks.WIKIDATA_STATEMENT_STAMP_KEYS, ks.DUPLICATE_ADDRESS_STAMP_KEYS):
        assert set(keys) <= set(STAMPS)


def test_staleness():
    assert ks.KGStore.is_stale(STAMPS, None)[0] and "no published build" in ks.KGStore.is_stale(STAMPS, None)[1]
    assert ks.KGStore.is_stale(STAMPS, dict(STAMPS)) == (False, ks.KGStore.is_stale(STAMPS, dict(STAMPS))[1])
    assert ks.KGStore.is_stale(STAMPS, dict(STAMPS), force=True) == (True, "force_rebuild")


@pytest.mark.parametrize("key", list(STAMPS))
def test_each_stamp_triggers_a_rebuild(key):
    stale, why = ks.KGStore.is_stale(STAMPS, {**STAMPS, key: "something-else"})
    assert stale and key in why


# -- the snapshot ------------------------------------------------------------------------

@pytest.fixture
def corpus(store):
    now = now_utc()
    store.entries.insert_many([
        {"_id": "a", "url": FRAME, "parsed_at": now - timedelta(hours=1), "parser_version": "1.5.0",
         "corpus_policy_version": "p1", "corpus_status": "ready", "dom_content_sha256": "f00",
         "sections": [{"kind": "other", "text": ["p"], "images": [{"src": "https://i.kym-cdn.com/x.jpg"}]}]},
        {"_id": "b", "url": PARENT, "parsed_at": now - timedelta(hours=2), "parser_version": "1.5.0",
         "corpus_policy_version": "p1", "corpus_status": "incomplete"}])
    return store


def test_the_snapshot_collects_ids_versions_and_the_newest_parse(corpus):
    snap = corpus.snapshot()
    assert (snap["entries_count"], sorted(snap["entry_ids"]), snap["parser_versions"]) == (2, ["a", "b"], ["1.5.0"])
    newest = corpus.entries.find_one({"_id": "a"})["parsed_at"]
    assert snap["max_parsed_at"].tzinfo is not None
    assert snap["max_parsed_at"].replace(microsecond=0, tzinfo=None) == newest.replace(microsecond=0, tzinfo=None)
    # "nothing is discarded for being incomplete" holds for the KG too
    assert corpus.snapshot(ready_only=True)["entries_count"] == 1


def test_the_snapshot_reads_entries_without_version_stamps(store):
    newest = now_utc() - timedelta(minutes=1)
    store.entries.insert_many([{"_id": "a", "url": FRAME, "parsed_at": newest - timedelta(days=1)},
                               {"_id": "b", "url": PARENT, "parsed_at": newest}])
    snap = store.snapshot()
    assert (snap["parser_versions"], snap["corpus_policy_versions"]) == ([], [])
    assert snap["max_parsed_at"].replace(microsecond=0) == newest.replace(microsecond=0)


def test_entries_written_after_the_snapshot_are_invisible(corpus):
    snap = corpus.snapshot()
    corpus.entries.insert_one({"_id": "c", "url": "u", "parsed_at": now_utc() + timedelta(hours=1)})
    assert len(list(corpus.iter_entries(["a", "b", "c"], snap["snapshot_at"]))) == 2


def test_the_projection_carries_the_whole_parsed_record(corpus):
    # 3.0.0 models section text, images and references: it only EXCLUDES bookkeeping
    assert all(v == 0 for v in ks.ENTRY_PROJECTION.values())
    assert "sections" not in ks.ENTRY_PROJECTION and "dom_content_sha256" in ks.ENTRY_PROJECTION
    snap = corpus.snapshot()
    doc = {d["url"]: d for d in corpus.iter_entries(snap["entry_ids"], snap["snapshot_at"])}[FRAME]
    assert "dom_content_sha256" not in doc
    assert (doc["sections"][0]["text"], len(doc["sections"][0]["images"])) == (["p"], 1)


def test_the_snapshot_leaves_duplicate_addresses_out(corpus):
    corpus.urls.insert_one({"url": FRAME, "duplicate_of": FRAME + "-kept"})
    dups = corpus.duplicate_addresses()
    assert dups == {FRAME: FRAME + "-kept"}
    assert corpus.snapshot(exclude_urls=dups)["entry_ids"] == ["b"]
    assert [d.get("url") for d in corpus.iter_field("url", now_utc(), exclude_urls=dups)] == [PARENT]


def test_the_duplicate_stamp_moves_when_a_mark_moves():
    a, b = ks.duplicate_address_stamps({FRAME: PARENT}), ks.duplicate_address_stamps({FRAME: PARENT + "-2"})
    assert a["duplicate_addresses"] == b["duplicate_addresses"] == 1
    assert a["duplicate_addresses_digest"] != b["duplicate_addresses_digest"]
    assert a == ks.duplicate_address_stamps({FRAME: PARENT})


# -- counts and pruning -------------------------------------------------------------------

def test_counts_are_per_build(store):
    store.save_graph(BUILD, [frame(), {"id": "tag:doge", "kind": "tag_concept"}], [TAG])
    store.save_graph(OTHER, [frame()], [])
    c = store.counts(BUILD)
    assert (c["nodes"], c["edges"], c["frames"], c["edges_by_type"]) == (2, 1, 1, {"hasTag": 1})


def test_prune_keeps_the_published_build_even_if_old(store):
    store.begin_build(BUILD, {})
    store.save_graph(BUILD, [frame()], [])
    store.publish(BUILD)
    for i in range(3):
        store.begin_build(f"kg_later_{i}", {})
        store.save_graph(f"kg_later_{i}", [frame()], [])
    store.prune(keep=1)
    assert store.current_build_id() == BUILD and store.nodes.count_documents({"build_id": BUILD}) == 1


def test_prune_deletes_old_generations_nodes_and_edges(store):
    for i in range(3):
        store.begin_build(f"kg_gen_{i}", {})
        store.save_graph(f"kg_gen_{i}", [frame()], [TAG])
    assert store.prune(keep=1)["pruned"] == 2
    assert (store.nodes.count_documents({}), store.edges.count_documents({})) == (1, 1)


def test_legacy_documents_are_invisible_to_build_scoped_reads(store):
    # the live collections still hold ~1M documents of the old implementation, no build_id
    store.nodes.insert_many([{"_id": "legacy-1", "kind": "frame"}, {"_id": "legacy-2", "kind": "frame"}])
    store.save_graph(BUILD, [frame()], [])
    assert store.counts(BUILD)["nodes"] == 1 and len(list(store.iter_nodes(BUILD))) == 1


# -- the facades the DAGs call ----------------------------------------------------------------
# Twice a live run died of AttributeError: a DAG called kg_store.<name>() that
# only KGStore had. The names are read from the DAG sources, not listed by hand.

def facades_used_by_the_dags():
    return {name for f in ("kym_kg_dag.py", "kym_kg_validate_dag.py")
            for name in re.findall(r"\bstore\.([A-Za-z_]\w*)\(", (DAGS / f).read_text(encoding="utf-8"))}


def test_every_facade_the_dags_call_exists_and_is_exported():
    names = facades_used_by_the_dags()
    assert len(names) > 10 and {"publish_build", "current_build_id"} <= names    # the one that slipped
    for name in sorted(names):
        assert callable(getattr(ks, name, None)), f"kym_kg calls kg_store.{name}() but there is no such facade"
        assert name in ks.__all__, name


def test_the_facade_store_is_a_context_manager():
    from unittest import mock
    import mongomock
    with mock.patch("pymongo.MongoClient", mongomock.MongoClient), ks.get_store(uri="mongodb://mock", db_name="t") as s:
        s.save_graph(BUILD, [frame()], [TAG])
    assert [n["id"] for n in s.iter_nodes(BUILD)] == [FRAME]


def test_iter_nodes_and_edges_read_one_build_and_filter(store):
    store.save_graph(BUILD, [{**frame(), "about": "long text"}, {"id": "tag:doge", "kind": "tag_concept", "label": "doge"}],
                     [TAG, edge(FRAME, "hasRegion", "region:Japan")])
    store.save_graph(OTHER, [frame()], [])
    nodes = list(store.iter_nodes(BUILD))
    assert len(nodes) == 2 and not [n for n in nodes if "build_id" in n]      # the projection strips it
    assert list(store.iter_nodes(BUILD, kinds=["frame"], fields=["label"])) == \
        [{"id": FRAME, "kind": "frame", "label": "Doge"}]
    assert [e["type"] for e in store.iter_edges(BUILD, types=["hasTag"])] == ["hasTag"]
    assert len(list(store.iter_edges(BUILD))) == 2


def test_edges_keep_their_occurrences_unless_asked_not_to(store):
    occ = [{"anchor_text": "Cheems", "in_section": "About"}, {"citation_index": 2}]
    store.save_graph(BUILD, [frame()], [{**edge(FRAME, "citesMediaFrame", PARENT), "occurrences": occ}])
    [full] = store.iter_edges(BUILD)
    [lean] = store.iter_edges(BUILD, occurrences=False)
    assert full["occurrences"] == occ and "occurrences" not in lean
