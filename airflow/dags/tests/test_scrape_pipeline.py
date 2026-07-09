"""Smoke tests for scrapingant_client + dom_store (no network, no Mongo)."""
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

sys.path.insert(0, "/home/claude/AeroSemanticParser/airflow/dags")

os.environ["DOM_COMPRESSION"] = "zlib"

import mongomock

from modules import scrapingant_client as sac
from modules import dom_store

BIG_HTML = "<html><body>" + "meme " * 2000 + "</body></html>"


class FakeResponse:
    def __init__(self, status, text="", ctype="text/html; charset=utf-8"):
        self.status_code = status
        self.text = text
        self.headers = {"Content-Type": ctype}
        self.encoding = "utf-8"

    def json(self):
        return {"detail": self.text}


class FakeSession:
    """Yields queued responses; records how many calls were made."""
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0
        self.seen_params = []

    def get(self, endpoint, params=None, headers=None, timeout=None):
        self.calls += 1
        self.seen_params.append(params)
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


FAST = sac.ScrapeConfig(api_key="k", max_attempts=3,
                        backoff_base_s=0.001, backoff_max_s=0.002,
                        request_delay_s=0)


class ClientTests(unittest.TestCase):
    def test_ok_first_try_sends_browser_false(self):
        s = FakeSession([FakeResponse(200, BIG_HTML)])
        r = sac.fetch_html(s, "https://kym/x", FAST)
        self.assertTrue(r.ok)
        self.assertEqual(r.attempts_used, 1)
        self.assertEqual(s.seen_params[0]["browser"], "false")

    def test_404_is_permanent_single_attempt(self):
        s = FakeSession([FakeResponse(404, "gone")])
        r = sac.fetch_html(s, "https://kym/x", FAST)
        self.assertFalse(r.ok)
        self.assertEqual(r.error_kind, "permanent")
        self.assertEqual(s.calls, 1)

    def test_409_then_200_retries(self):
        s = FakeSession([FakeResponse(409, "busy"), FakeResponse(200, BIG_HTML)])
        r = sac.fetch_html(s, "https://kym/x", FAST)
        self.assertTrue(r.ok)
        self.assertEqual(r.attempts_used, 2)

    def test_exhausted_retries_marked_retryable(self):
        s = FakeSession([FakeResponse(423, "blocked")] * 3)
        r = sac.fetch_html(s, "https://kym/x", FAST)
        self.assertFalse(r.ok)
        self.assertEqual(r.error_kind, "retryable")
        self.assertEqual(s.calls, 3)

    def test_403_raises_auth(self):
        s = FakeSession([FakeResponse(403, "bad key")])
        with self.assertRaises(sac.ScrapingAntAuthError):
            sac.fetch_html(s, "https://kym/x", FAST)

    def test_thin_body_retried_then_ok(self):
        s = FakeSession([FakeResponse(200, "<html></html>"),
                         FakeResponse(200, BIG_HTML)])
        r = sac.fetch_html(s, "https://kym/x", FAST)
        self.assertTrue(r.ok)
        self.assertEqual(r.attempts_used, 2)

    def test_transport_error_retried(self):
        import requests as rq
        s = FakeSession([rq.ConnectionError("boom"), FakeResponse(200, BIG_HTML)])
        r = sac.fetch_html(s, "https://kym/x", FAST)
        self.assertTrue(r.ok)


def fresh_store():
    client = mongomock.MongoClient()
    store = dom_store.DomStore.__new__(dom_store.DomStore)
    store.client = client
    store.db = client["memes"]
    store.urls = store.db["urls"]
    store.doms = store.db["doms"]
    store.compression = "zlib"
    return store


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.store = fresh_store()
        self.store.urls.insert_many([
            {"url": "https://kym/a", "Confirmed": True, "namespace": "memes",
             "lastmod": "2026-01-01", "last_scraped": None},
            {"url": "https://kym/b", "Confirmed": True, "namespace": "memes",
             "lastmod": None, "last_scraped": None},
            {"url": "https://kym/c", "Confirmed": False, "namespace": "events",
             "lastmod": None, "last_scraped": None},
        ])

    def test_roundtrip_compression_and_last_scraped(self):
        self.store.save_result(url="https://kym/a", ok=True, html=BIG_HTML,
                               status_code=200)
        self.assertEqual(self.store.load_html("https://kym/a"), BIG_HTML)
        doc = self.store.doms.find_one({"url": "https://kym/a"})
        self.assertEqual(doc["encoding"], "zlib")
        self.assertLess(len(bytes(doc["html"])), len(BIG_HTML))  # did compress
        urec = self.store.urls.find_one({"url": "https://kym/a"})
        self.assertIsNotNone(urec["last_scraped"])

    def test_selection_buckets_and_confirmed_filter(self):
        pending = self.store.select_pending()
        self.assertEqual(set(pending), {"https://kym/a", "https://kym/b"})
        pending_all = self.store.select_pending(confirmed_only=False)
        self.assertIn("https://kym/c", pending_all)

    def test_failed_retryable_requeued_until_cap(self):
        for _ in range(2):
            self.store.save_result(url="https://kym/b", ok=False,
                                   error="503: hiccup", error_kind="retryable")
        self.assertIn("https://kym/b", self.store.select_pending(max_failed_attempts=3))
        self.store.save_result(url="https://kym/b", ok=False,
                               error="503: hiccup", error_kind="retryable")
        self.assertNotIn("https://kym/b", self.store.select_pending(max_failed_attempts=3))

    def test_permanent_failure_never_requeued(self):
        self.store.save_result(url="https://kym/b", ok=False,
                               error="404: gone", error_kind="permanent")
        self.assertNotIn("https://kym/b", self.store.select_pending())

    def test_failed_refetch_keeps_good_dom(self):
        self.store.save_result(url="https://kym/a", ok=True, html=BIG_HTML)
        outcome = self.store.save_result(url="https://kym/a", ok=False,
                                         error="503", error_kind="retryable")
        self.assertEqual(outcome, "kept_ok")
        doc = self.store.doms.find_one({"url": "https://kym/a"})
        self.assertEqual(doc["scrape_status"], "ok")
        self.assertEqual(self.store.load_html("https://kym/a"), BIG_HTML)

    def test_lastmod_staleness_triggers_requeue(self):
        old = datetime(2025, 12, 1, tzinfo=timezone.utc)
        self.store.save_result(url="https://kym/a", ok=True, html=BIG_HTML,
                               fetched_at=old)
        pending = self.store.select_pending()  # lastmod 2026-01-01 > fetched
        self.assertIn("https://kym/a", pending)
        self.store.save_result(url="https://kym/a", ok=True, html=BIG_HTML)
        self.assertNotIn("https://kym/a", self.store.select_pending())

    def test_refetch_window(self):
        old = datetime.now(timezone.utc) - timedelta(days=90)
        self.store.save_result(url="https://kym/b", ok=True, html=BIG_HTML,
                               fetched_at=old)
        self.assertNotIn("https://kym/b",
                         self.store.select_pending(refetch_older_than_days=0))
        self.assertIn("https://kym/b",
                      self.store.select_pending(refetch_older_than_days=30))

    def test_filter_unscraped(self):
        self.store.save_result(url="https://kym/a", ok=True, html=BIG_HTML)
        chunk = ["https://kym/a", "https://kym/b"]
        self.assertEqual(self.store.filter_unscraped(chunk), ["https://kym/b"])

    def test_glue_path_fetchresult_as_doc(self):
        """The exact DAG glue: iter_fetch -> as_doc -> save_result."""
        s = FakeSession([FakeResponse(200, BIG_HTML), FakeResponse(404, "gone")])
        tallies = {"ok": 0, "failed": 0, "kept_ok": 0}
        for r in sac.iter_fetch(s, ["https://kym/a", "https://kym/b"], FAST):
            tallies[self.store.save_result(**r.as_doc())] += 1
        self.assertEqual(tallies, {"ok": 1, "failed": 1, "kept_ok": 0})
        self.assertEqual(self.store.stats()["doms_ok"], 1)
        self.assertEqual(self.store.stats()["failed_permanent"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)