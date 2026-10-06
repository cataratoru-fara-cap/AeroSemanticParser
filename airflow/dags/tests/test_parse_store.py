"""parse_store: grading, upserts, staleness, dead letters, duplicate addresses."""
from unittest import mock

import pytest

from helpers import FIXTURES, mock_store, serving
from modules import dom_store
from modules import parse_store as ps
from modules.kym_models import CORPUS_POLICY_VERSION as POLICY_V
from modules.kym_models import DEFAULT_CORPUS_POLICY as POLICY
from modules.kym_models import CorpusPolicy, KYMEntryScrape
from modules.kym_parse import PARSER_VERSION as PARSER_V
from modules.kym_parse import parse_entry

DOGE = "https://knowyourmeme.com/memes/doge"
SENS = "https://knowyourmeme.com/sensitive/memes/doge"
BROKEN = "https://knowyourmeme.com/memes/broken-page"
OMIT = object()


@pytest.fixture
def store():
    return mock_store(ps.ParseStore)


def entry(url="https://knowyourmeme.com/memes/thin-stub", **fields):
    """Valid but corpus-incomplete: no year, entry type, region or sections."""
    data = {"url": url, "title": "Thin Stub", "category": "meme",
            "status": "confirmed", "origin": "Twitter", "tags": ["stub"], **fields}
    return KYMEntryScrape.model_validate({k: v for k, v in data.items() if v is not OMIT})


def doc(store, e=None, sha="sha1", policy=POLICY, parser=PARSER_V, policy_v=POLICY_V, **kw):
    return store.build_entry_doc(e or entry(), sha, policy, parser, policy_v, **kw)


def failure(url=BROKEN, sha="sha_bad", **kw):
    return {"url": url, "dom_content_sha256": sha, "error_type": "ValidationError",
            "error": "6 validation errors for KYMEntryScrape ...", **kw}


# -- grading ------------------------------------------------------------------

def test_a_complete_entry_is_ready_and_stamped(store):
    d = doc(store, parse_entry((FIXTURES / "doge.html").read_text()), sha="sha_v1")
    assert (d["corpus_status"], d["corpus_missing"]) == ("ready", [])
    assert (d["dom_content_sha256"], d["parser_version"], d["corpus_policy_version"]) \
        == ("sha_v1", PARSER_V, POLICY_V)
    assert "parsed_at" in d


def test_an_incomplete_entry_is_labelled_not_dropped(store):
    d = doc(store)
    assert d["corpus_status"] == "incomplete"
    assert set(d["corpus_missing"]) == {"year", "entry_type", "region", "section:about",
                                        "section:origin", "section:spread"}


def test_an_entry_without_tags_validates_and_is_labelled(store):
    # a real confirmed meme may have no tags: kept, labelled incomplete
    d = doc(store, entry(tags=OMIT))
    assert d["corpus_status"] == "incomplete" and "tags" in d["corpus_missing"]


@pytest.mark.parametrize("policy, field, fields", [
    (CorpusPolicy(require_region=False), "region", {}),
    (CorpusPolicy(require_tags=False), "tags", {"tags": OMIT}),
])
def test_a_lenient_policy_stops_requiring_its_field(store, policy, field, fields):
    assert field not in doc(store, entry(**fields), policy=policy, policy_v="lenient")["corpus_missing"]


# -- upserts ------------------------------------------------------------------

def test_upsert_tallies_persists_and_is_idempotent_on_url(store):
    ready = doc(store, parse_entry((FIXTURES / "doge.html").read_text()))
    assert store.upsert_entries([ready]) == {"ready": 1, "incomplete": 0}
    store.upsert_entries([doc(store)])
    store.upsert_entries([doc(store)])
    assert store.entries.count_documents({}) == 2


def test_a_retired_field_leaves_the_doc_on_reparse(store):
    # template_image_url went in parser 1.7.0; $set alone would keep it forever
    d = doc(store)
    store.entries.insert_one({"_id": d["_id"], "template_image_url": "https://i.kym-cdn.com/x.jpg"})
    store.upsert_entries([d])
    stored = store.entries.find_one({"_id": d["_id"]})
    assert "template_image_url" not in stored and stored["title"] == "Thin Stub"


def test_stats_break_down_entries_and_failures(store):
    store.upsert_entries([doc(store)])
    store.save_failures([failure(namespace="memes"),
                         failure("https://knowyourmeme.com/editorials/oops", "sha2",
                                 namespace="editorials")], PARSER_V, POLICY_V)
    s = store.stats()
    assert (s["entries_total"], s["entries_incomplete"], s["missing_field_counts"]["year"]) == (1, 1, 1)
    assert (s["parse_failures"], s["failure_type_counts"]) == (2, {"ValidationError": 2})
    assert s["failure_namespace_counts"] == {"memes": 1, "editorials": 1}


# -- staleness ----------------------------------------------------------------

NEW = "https://knowyourmeme.com/memes/brand-new"


@pytest.mark.parametrize("shas, parser, policy_v, force, expected", [
    ({DOGE: "sha_v1"}, PARSER_V, POLICY_V, False, []),               # unchanged
    ({DOGE: "sha_v2"}, PARSER_V, POLICY_V, False, [DOGE]),           # the page changed
    ({DOGE: "sha_v1"}, "9.9.9", POLICY_V, False, [DOGE]),            # parser upgraded
    ({DOGE: "sha_v1"}, PARSER_V, "other-policy", False, [DOGE]),     # policy changed
    ({DOGE: "sha_v1", NEW: "s"}, PARSER_V, POLICY_V, False, [NEW]),  # never parsed
    ({DOGE: "sha_v1"}, PARSER_V, POLICY_V, True, [DOGE]),            # forced
    ({}, PARSER_V, POLICY_V, False, []),
])
def test_what_is_due_for_a_parse(store, shas, parser, policy_v, force, expected):
    store.upsert_entries([doc(store, entry(DOGE), sha="sha_v1")])
    assert store.select_pending(shas, parser, policy_v, force_reparse=force) == expected


def test_limit_truncates(store):
    shas = {f"https://knowyourmeme.com/memes/new-{i}": "s" for i in range(5)}
    assert len(store.select_pending(shas, PARSER_V, POLICY_V, limit=2)) == 2


# -- dead letters -------------------------------------------------------------

def test_a_failure_goes_to_the_dead_letters_only(store):
    assert store.save_failures([failure()], PARSER_V, POLICY_V) == 1
    assert store.entries.count_documents({}) == 0
    d = store.failures.find_one({"url": BROKEN})
    assert (d["error_type"], d["attempts"]) == ("ValidationError", 1)
    assert "validation errors" in d["error"] and "failed_at" in d


def test_a_repeated_failure_counts_attempts_in_one_record(store):
    store.save_failures([failure()], PARSER_V, POLICY_V)
    store.save_failures([failure()], "1.0.2", POLICY_V)
    d = store.failures.find_one({"url": BROKEN})
    assert (d["attempts"], d["parser_version"], store.failures.count_documents({})) == (2, "1.0.2", 1)


@pytest.mark.parametrize("sha, parser, expected", [
    ("sha_bad", PARSER_V, []),        # same page, same parser: a deterministic failure
    ("sha_bad", "9.9.9", [BROKEN]),   # parser upgraded
    ("sha_new", PARSER_V, [BROKEN]),  # the page changed
])
def test_a_failure_is_retried_only_when_something_changed(store, sha, parser, expected):
    store.save_failures([failure()], PARSER_V, POLICY_V)
    assert store.select_pending({BROKEN: sha}, parser, POLICY_V) == expected


def test_a_failed_reparse_never_touches_the_good_entry_nor_loops(store):
    store.upsert_entries([doc(store, entry(BROKEN), sha="sha_v1")])
    before = store.entries.find_one({"url": BROKEN})
    store.save_failures([failure(sha="sha_v2")], PARSER_V, POLICY_V)
    assert store.entries.find_one({"url": BROKEN}) == before
    assert store.select_pending({BROKEN: "sha_v2"}, PARSER_V, POLICY_V) == []


def test_a_successful_reparse_deletes_the_dead_letter(store):
    store.save_failures([failure()], PARSER_V, POLICY_V)
    store.upsert_entries([doc(store, entry(BROKEN), sha="sha_fixed")])
    assert (store.failures.count_documents({}), store.entries.count_documents({})) == (0, 1)


def test_entries_stay_schema_pure(store):
    store.upsert_entries([doc(store)])
    stored = store.entries.find_one({})
    assert not {"parse_status", "last_parse_error", "last_parse_error_type"} & set(stored)


@pytest.mark.parametrize("url_doc, expected", [
    ({"url": BROKEN, "namespace": "editorials"}, "editorials"),  # discovery's label wins
    ({"url": BROKEN}, "memes"),                                   # no label: from the path
    (None, "memes"),                                              # unknown to discovery
])
def test_a_namespace_comes_from_discovery_else_the_path(store, url_doc, expected):
    if url_doc:
        store.urls.insert_one(url_doc)
    assert store.namespaces_for([BROKEN])[BROKEN] == expected


def test_a_failure_record_keeps_its_namespace(store):
    store.urls.insert_one({"url": BROKEN, "namespace": "memes"})
    ns = store.namespaces_for([BROKEN])[BROKEN]
    store.save_failures([failure(namespace=ns)], PARSER_V, POLICY_V)
    assert store.failures.find_one({"url": BROKEN})["namespace"] == "memes"


# -- one entry, one address (gap 14) ------------------------------------------

@pytest.fixture
def twins(store):
    for url in (DOGE, SENS):
        store.upsert_entries([doc(store, entry(url), sha="sha")])
        store.urls.insert_one({"url": url, "Confirmed": True})
    store.urls.update_one({"url": DOGE}, {"$set": {"duplicate_of": SENS}})
    store.failures.insert_one({"_id": ps.url_doc_id(DOGE), "url": DOGE})
    return store


def test_retiring_drops_the_entry_and_its_dead_letter(twins):
    out = twins.retire_duplicates()
    assert (out["duplicate_addresses"], out["entries_retired"]) == (1, 1)
    assert out["examples"] == [{"retired": DOGE, "kept": SENS}]
    assert [e["url"] for e in twins.entries.find()] == [SENS]
    assert twins.failures.count_documents({}) == 0
    assert twins.retire_duplicates()["entries_retired"] == 0


def test_an_entry_is_filed_where_its_page_was_collected(store):
    # a moved page names the new address in a canonical link; filed under it,
    # TikTok's two pages overwrote one entry
    d = doc(store, entry(DOGE), address=SENS)
    assert (d["url"], d["_id"]) == (SENS, ps.url_doc_id(SENS))


def test_a_duplicate_address_is_never_selected(twins):
    with serving(ps, twins), mock.patch.object(
            dom_store, "content_shas", side_effect=lambda urls: {u: "new" for u in urls}):
        assert ps.pending_urls(current_parser_version=PARSER_V,
                               current_policy_version=POLICY_V) == [SENS]
