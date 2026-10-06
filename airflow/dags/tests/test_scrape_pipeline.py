"""Smoke tests for scrapingant_client + dom_store (no network, no Mongo).

sys.path and the DOM_COMPRESSION default come from tests/conftest.py.
"""
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

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

    def test_iter_html_for_carries_the_fetch_time(self):
        # Parser 1.7.0: an entry's scraped_at is when its stored page was
        # fetched. Before, nothing passed it and scraped_at was always empty.
        when = datetime(2026, 7, 9, 23, 47, 44, tzinfo=timezone.utc)
        self.store.save_result(url="https://kym/a", ok=True, html=BIG_HTML,
                               fetched_at=when)
        with mock.patch.object(dom_store, "get_store", return_value=self.store):
            (url, html, sha, fetched_at), = list(dom_store.iter_html_for(["https://kym/a"]))
        self.assertEqual((url, html, fetched_at), ("https://kym/a", BIG_HTML, when))
        self.assertEqual(sha, self.store.doms.find_one({"url": url})["content_sha256"])

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



def entry_page(address: str, title: str) -> str:
    return (f"<html><head><meta property='og:url' content='{address}' />"
            f"<meta property='og:title' content='{title} | Know Your Meme' />"
            "</head><body>" + "meme " * 200 + "</body></html>")


class DuplicateTests(unittest.TestCase):
    """Gap 14: one entry, one address — the scrape stage's bookkeeping."""
    PUB = "https://knowyourmeme.com/memes/doge"
    SENS = "https://knowyourmeme.com/sensitive/memes/doge"

    def setUp(self):
        self.store = fresh_store()
        self.store.urls.insert_many([
            {"_id": dom_store.url_doc_id(u), "url": u, "Confirmed": True,
             "namespace": ns, "lastmod": None, "last_scraped": None}
            for u, ns in ((self.PUB, "memes"), (self.SENS, "sensitive/memes"))])

    def test_a_stored_page_records_what_it_says_about_itself(self):
        self.store.save_result(url=self.PUB, ok=True, html=entry_page(self.SENS, "Doge"))
        doc = self.store.doms.find_one({"url": self.PUB})
        self.assertEqual((doc["page_url"], doc["page_title"]), (self.SENS, "Doge"))

    def test_pages_stored_before_are_read_once_and_written_back(self):
        self.store.save_result(url=self.PUB, ok=True, html=entry_page(self.PUB, "Doge"))
        self.store.doms.update_one({"url": self.PUB},
                                   {"$unset": {"page_url": "", "page_title": ""}})
        got = self.store.page_identities()
        self.assertEqual((got[self.PUB]["page_url"], got[self.PUB]["page_title"]),
                         (self.PUB, "Doge"))
        self.assertEqual(self.store.doms.find_one({"url": self.PUB})["page_title"], "Doge")
        self.assertEqual(got[self.PUB]["fetched_at"].tzinfo, timezone.utc)

    def test_marks_are_set_kept_and_lifted(self):
        first = self.store.mark_duplicates({self.PUB: self.SENS})
        self.assertEqual(first, {"marked": 1, "unchanged": 0, "unmarked": 0, "duplicates": 1})
        since = self.store.urls.find_one({"url": self.PUB})["duplicate_since"]
        again = self.store.mark_duplicates({self.PUB: self.SENS})
        self.assertEqual(again["unchanged"], 1)
        self.assertEqual(self.store.urls.find_one({"url": self.PUB})["duplicate_since"], since)
        moved_back = self.store.mark_duplicates({self.SENS: self.PUB})
        self.assertEqual((moved_back["marked"], moved_back["unmarked"]), (1, 1))
        self.assertNotIn("duplicate_of", self.store.urls.find_one({"url": self.PUB}))
        self.assertEqual(self.store.urls.find_one({"url": self.SENS})["duplicate_of"], self.PUB)

    def test_the_whole_step_keeps_the_address_the_newest_page_names(self):
        old = datetime(2026, 7, 10, tzinfo=timezone.utc)
        self.store.save_result(url=self.PUB, ok=True, html=entry_page(self.PUB, "Doge"),
                               fetched_at=old)
        self.store.save_result(url=self.SENS, ok=True, html=entry_page(self.SENS, "Doge"))
        with mock.patch.object(dom_store, "get_store", return_value=self.store):
            out = dom_store.resolve_duplicates()
        self.assertEqual(self.store.urls.find_one({"url": self.PUB})["duplicate_of"], self.SENS)
        self.assertEqual((out["entries_with_duplicates"], out["pages_dropped"],
                          out["kept_sensitive"], out["marks"]["marked"]), (1, 1, 1, 1))

    def test_a_duplicate_is_still_refetched_when_it_goes_stale(self):
        # Its fresh page is the evidence that KYM moved the entry back.
        old = datetime(2025, 12, 1, tzinfo=timezone.utc)
        self.store.save_result(url=self.PUB, ok=True, html=BIG_HTML, fetched_at=old)
        self.store.urls.update_one({"url": self.PUB}, {"$set": {
            "lastmod": "2026-01-01", "duplicate_of": self.SENS}})
        self.assertIn(self.PUB, self.store.select_pending())


if __name__ == "__main__":
    unittest.main(verbosity=2)