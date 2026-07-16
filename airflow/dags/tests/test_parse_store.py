"""Smoke tests for parse_store.py (no network, no real Mongo — mongomock).

Mirrors the fresh_store() pattern in test_scrape_pipeline.py: bypass
ParseStore.__init__ and wire mongomock collections directly.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_parse_store.py -v
"""
import os
import unittest

os.environ.setdefault("MONGODB_DB", "memes")

import mongomock

from modules import parse_store as ps
from modules.kym_models import (
    CORPUS_POLICY_VERSION,
    DEFAULT_CORPUS_POLICY,
    CorpusPolicy,
    KYMEntryScrape,
)
from modules.kym_parse import PARSER_VERSION, parse_entry

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "doge.html")
DOGE_URL = "https://knowyourmeme.com/memes/doge"


def fresh_store() -> ps.ParseStore:
    client = mongomock.MongoClient()
    store = ps.ParseStore.__new__(ps.ParseStore)
    store.client = client
    store.db = client["memes"]
    store.urls = store.db["urls"]
    store.entries = store.db["entries"]
    store.failures = store.db["parse_failures"]
    return store


def _thin_entry(url: str = "https://knowyourmeme.com/memes/thin-stub"
               ) -> KYMEntryScrape:
    """A validly-scraped but corpus-incomplete entry, for gate testing."""
    return KYMEntryScrape.model_validate({
        "url": url, "title": "Thin Stub", "category": "meme",
        "status": "confirmed", "origin": "Twitter", "tags": ["stub"],
    })


class BuildEntryDocTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(FIXTURE, encoding="utf-8") as fh:
            cls.doge = parse_entry(fh.read())

    def setUp(self):
        self.store = fresh_store()

    def test_ready_entry_graded_correctly(self):
        doc = self.store.build_entry_doc(
            self.doge, "sha_v1", DEFAULT_CORPUS_POLICY, PARSER_VERSION,
            CORPUS_POLICY_VERSION)
        self.assertEqual(doc["corpus_status"], "ready")
        self.assertEqual(doc["corpus_missing"], [])
        self.assertEqual(doc["dom_content_sha256"], "sha_v1")
        self.assertEqual(doc["parser_version"], PARSER_VERSION)
        self.assertEqual(doc["corpus_policy_version"], CORPUS_POLICY_VERSION)
        self.assertIn("parsed_at", doc)

    def test_incomplete_entry_flagged_not_dropped(self):
        thin = _thin_entry()
        doc = self.store.build_entry_doc(
            thin, "sha_thin", DEFAULT_CORPUS_POLICY, PARSER_VERSION,
            CORPUS_POLICY_VERSION)
        self.assertEqual(doc["corpus_status"], "incomplete")
        self.assertEqual(
            set(doc["corpus_missing"]),
            {"year", "entry_type", "region",
             "section:about", "section:origin", "section:spread"})

    def test_region_optional_policy_changes_grading(self):
        lenient = CorpusPolicy(require_region=False)
        doc = self.store.build_entry_doc(
            _thin_entry(), "sha", lenient, PARSER_VERSION, "lenient-v1")
        self.assertNotIn("region", doc["corpus_missing"])


class UpsertTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()

    def test_upsert_tallies_and_persists(self):
        with open(FIXTURE, encoding="utf-8") as fh:
            doge = parse_entry(fh.read())
        doc = self.store.build_entry_doc(
            doge, "sha1", DEFAULT_CORPUS_POLICY, PARSER_VERSION,
            CORPUS_POLICY_VERSION)
        tallies = self.store.upsert_entries([doc])
        self.assertEqual(tallies, {"ready": 1, "incomplete": 0})
        self.assertEqual(self.store.entries.count_documents({}), 1)

    def test_upsert_is_idempotent_on_url(self):
        doc = self.store.build_entry_doc(
            _thin_entry(), "sha1", DEFAULT_CORPUS_POLICY, PARSER_VERSION,
            CORPUS_POLICY_VERSION)
        self.store.upsert_entries([doc])
        self.store.upsert_entries([doc])  # re-run, same url
        self.assertEqual(self.store.entries.count_documents({}), 1)

    def test_stats_breaks_down_missing_fields(self):
        doc = self.store.build_entry_doc(
            _thin_entry(), "sha1", DEFAULT_CORPUS_POLICY, PARSER_VERSION,
            CORPUS_POLICY_VERSION)
        self.store.upsert_entries([doc])
        stats = self.store.stats()
        self.assertEqual(stats["entries_total"], 1)
        self.assertEqual(stats["entries_incomplete"], 1)
        self.assertEqual(stats["missing_field_counts"]["year"], 1)


class SelectPendingTests(unittest.TestCase):
    """Staleness-detection logic — the reason entries/parser/policy
    versions are stamped on every doc in the first place."""

    def setUp(self):
        self.store = fresh_store()
        entry = _thin_entry(DOGE_URL)
        doc = self.store.build_entry_doc(
            entry, "sha_v1", DEFAULT_CORPUS_POLICY, PARSER_VERSION,
            CORPUS_POLICY_VERSION)
        self.store.upsert_entries([doc])

    def test_unchanged_needs_no_reparse(self):
        pending = self.store.select_pending(
            {DOGE_URL: "sha_v1"}, PARSER_VERSION, CORPUS_POLICY_VERSION)
        self.assertEqual(pending, [])

    def test_dom_content_change_triggers_reparse(self):
        pending = self.store.select_pending(
            {DOGE_URL: "sha_v2"}, PARSER_VERSION, CORPUS_POLICY_VERSION)
        self.assertEqual(pending, [DOGE_URL])

    def test_parser_upgrade_triggers_reparse(self):
        pending = self.store.select_pending(
            {DOGE_URL: "sha_v1"}, "9.9.9", CORPUS_POLICY_VERSION)
        self.assertEqual(pending, [DOGE_URL])

    def test_policy_change_triggers_reparse(self):
        pending = self.store.select_pending(
            {DOGE_URL: "sha_v1"}, PARSER_VERSION, "some-other-policy")
        self.assertEqual(pending, [DOGE_URL])

    def test_never_parsed_url_is_pending(self):
        new_url = "https://knowyourmeme.com/memes/brand-new"
        pending = self.store.select_pending(
            {DOGE_URL: "sha_v1", new_url: "sha_new"},
            PARSER_VERSION, CORPUS_POLICY_VERSION)
        self.assertEqual(pending, [new_url])

    def test_force_reparse_ignores_all_staleness_checks(self):
        pending = self.store.select_pending(
            {DOGE_URL: "sha_v1"}, PARSER_VERSION, CORPUS_POLICY_VERSION,
            force_reparse=True)
        self.assertEqual(pending, [DOGE_URL])

    def test_empty_candidates_returns_empty(self):
        self.assertEqual(
            self.store.select_pending({}, PARSER_VERSION, CORPUS_POLICY_VERSION),
            [])

    def test_limit_truncates(self):
        shas = {f"https://knowyourmeme.com/memes/new-{i}": "s" for i in range(5)}
        pending = self.store.select_pending(
            shas, PARSER_VERSION, CORPUS_POLICY_VERSION, limit=2)
        self.assertEqual(len(pending), 2)


class SaveFailuresTests(unittest.TestCase):
    """Dead-letter behavior: failures live in `parse_failures`, never in
    `entries` (which stays schema-pure)."""

    URL = "https://knowyourmeme.com/memes/broken-page"

    def setUp(self):
        self.store = fresh_store()
        self.fail = {"url": self.URL, "dom_content_sha256": "sha_bad",
                     "error": "6 validation errors for KYMEntryScrape ...",
                     "error_type": "ValidationError"}

    def test_failure_goes_to_failures_collection_only(self):
        n = self.store.save_failures([self.fail], PARSER_VERSION,
                                     CORPUS_POLICY_VERSION)
        self.assertEqual(n, 1)
        self.assertEqual(self.store.entries.count_documents({}), 0)
        doc = self.store.failures.find_one({"url": self.URL})
        self.assertEqual(doc["error_type"], "ValidationError")
        self.assertIn("validation errors", doc["error"])
        self.assertEqual(doc["attempts"], 1)
        self.assertIn("failed_at", doc)

    def test_repeat_failure_increments_attempts(self):
        self.store.save_failures([self.fail], PARSER_VERSION,
                                 CORPUS_POLICY_VERSION)
        self.store.save_failures([self.fail], "1.0.2",
                                 CORPUS_POLICY_VERSION)
        doc = self.store.failures.find_one({"url": self.URL})
        self.assertEqual(doc["attempts"], 2)
        self.assertEqual(doc["parser_version"], "1.0.2")
        self.assertEqual(self.store.failures.count_documents({}), 1)

    def test_failure_not_requeued_until_something_changes(self):
        self.store.save_failures([self.fail], PARSER_VERSION,
                                 CORPUS_POLICY_VERSION)
        # same sha + same versions -> deterministic failure, do NOT retry
        pending = self.store.select_pending(
            {self.URL: "sha_bad"}, PARSER_VERSION, CORPUS_POLICY_VERSION)
        self.assertEqual(pending, [])
        # parser upgraded -> retry
        pending = self.store.select_pending(
            {self.URL: "sha_bad"}, "9.9.9", CORPUS_POLICY_VERSION)
        self.assertEqual(pending, [self.URL])
        # DOM content changed -> retry
        pending = self.store.select_pending(
            {self.URL: "sha_new"}, PARSER_VERSION, CORPUS_POLICY_VERSION)
        self.assertEqual(pending, [self.URL])

    def test_failure_never_touches_prior_ok_entry(self):
        good = self.store.build_entry_doc(
            _thin_entry(self.URL), "sha_v1", DEFAULT_CORPUS_POLICY,
            PARSER_VERSION, CORPUS_POLICY_VERSION)
        self.store.upsert_entries([good])
        before = self.store.entries.find_one({"url": self.URL})
        # page changed, new version fails to parse
        self.store.save_failures(
            [{**self.fail, "dom_content_sha256": "sha_v2"}],
            PARSER_VERSION, CORPUS_POLICY_VERSION)
        after = self.store.entries.find_one({"url": self.URL})
        self.assertEqual(before, after)  # entries doc byte-identical
        # and the failure record's stamp prevents a retry loop on the
        # same broken DOM (select_pending consults both collections)
        pending = self.store.select_pending(
            {self.URL: "sha_v2"}, PARSER_VERSION, CORPUS_POLICY_VERSION)
        self.assertEqual(pending, [])

    def test_successful_reparse_deletes_dead_letter(self):
        self.store.save_failures([self.fail], PARSER_VERSION,
                                 CORPUS_POLICY_VERSION)
        good = self.store.build_entry_doc(
            _thin_entry(self.URL), "sha_fixed", DEFAULT_CORPUS_POLICY,
            PARSER_VERSION, CORPUS_POLICY_VERSION)
        self.store.upsert_entries([good])
        self.assertEqual(self.store.failures.count_documents({}), 0)
        self.assertEqual(self.store.entries.count_documents({}), 1)

    def test_entries_stays_schema_pure(self):
        good = self.store.build_entry_doc(
            _thin_entry(), "sha1", DEFAULT_CORPUS_POLICY, PARSER_VERSION,
            CORPUS_POLICY_VERSION)
        self.store.upsert_entries([good])
        doc = self.store.entries.find_one({})
        for legacy in ("parse_status", "last_parse_error",
                       "last_parse_error_type"):
            self.assertNotIn(legacy, doc)

    def test_stats_counts_failures_by_type(self):
        self.store.save_failures([self.fail], PARSER_VERSION,
                                 CORPUS_POLICY_VERSION)
        good = self.store.build_entry_doc(
            _thin_entry(), "sha1", DEFAULT_CORPUS_POLICY, PARSER_VERSION,
            CORPUS_POLICY_VERSION)
        self.store.upsert_entries([good])
        stats = self.store.stats()
        self.assertEqual(stats["parse_failures"], 1)
        self.assertEqual(stats["failure_type_counts"],
                         {"ValidationError": 1})
        self.assertEqual(stats["entries_total"], 1)
        self.assertEqual(stats["entries_incomplete"], 1)

    def test_namespace_from_urls_collection_is_authoritative(self):
        self.store.urls.insert_one({"url": self.URL, "namespace": "editorials"})
        ns = self.store.namespaces_for([self.URL])
        self.assertEqual(ns[self.URL], "editorials")

    def test_namespace_falls_back_when_missing_from_urls_doc(self):
        # urls doc exists but has no namespace field recorded
        self.store.urls.insert_one({"url": self.URL})
        ns = self.store.namespaces_for([self.URL])
        self.assertEqual(ns[self.URL], "memes")  # inferred from /memes/... path

    def test_namespace_falls_back_when_url_not_in_urls_collection(self):
        ns = self.store.namespaces_for([self.URL])
        self.assertEqual(ns[self.URL], "memes")

    def test_failure_record_stores_namespace(self):
        self.store.urls.insert_one({"url": self.URL, "namespace": "memes"})
        ns_by_url = self.store.namespaces_for([self.URL])
        fail_with_ns = {**self.fail, "namespace": ns_by_url[self.URL]}
        self.store.save_failures([fail_with_ns], PARSER_VERSION,
                                 CORPUS_POLICY_VERSION)
        doc = self.store.failures.find_one({"url": self.URL})
        self.assertEqual(doc["namespace"], "memes")

    def test_stats_breaks_down_failures_by_namespace(self):
        editorial_url = "https://knowyourmeme.com/editorials/oops"
        self.store.save_failures(
            [{**self.fail, "namespace": "memes"},
             {"url": editorial_url, "dom_content_sha256": "sha2",
              "error": "e", "error_type": "ValidationError",
              "namespace": "editorials"}],
            PARSER_VERSION, CORPUS_POLICY_VERSION)
        stats = self.store.stats()
        self.assertEqual(stats["failure_namespace_counts"],
                         {"memes": 1, "editorials": 1})


if __name__ == "__main__":
    unittest.main(verbosity=2)