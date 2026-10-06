"""scrapingant_client + dom_store: fetching (no network) and the stored pages
(mongomock), including one entry, one address (gap 14)."""
from datetime import datetime, timedelta, timezone

import pytest
import requests

from helpers import mock_store, serving
from modules import dom_store
from modules import scrapingant_client as sac

BIG_HTML = "<html><body>" + "meme " * 2000 + "</body></html>"
FAST = sac.ScrapeConfig(api_key="k", max_attempts=3, backoff_base_s=0.001, backoff_max_s=0.002, request_delay_s=0)
A, B, C = "https://kym/a", "https://kym/b", "https://kym/c"


class FakeResponse:
    def __init__(self, status, text="", ctype="text/html; charset=utf-8"):
        self.status_code, self.text, self.headers, self.encoding = status, text, {"Content-Type": ctype}, "utf-8"

    def json(self):
        return {"detail": self.text}


class FakeSession:
    """Serves queued responses (or raises queued exceptions) and records the calls."""

    def __init__(self, responses):
        self.responses, self.calls, self.seen_params = list(responses), 0, []

    def get(self, endpoint, params=None, headers=None, timeout=None):
        self.calls += 1
        self.seen_params.append(params)
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


# -- the client --------------------------------------------------------------------------

@pytest.mark.parametrize("responses, ok, kind, attempts, calls", [
    ([FakeResponse(200, BIG_HTML)], True, None, 1, 1),
    ([FakeResponse(404, "gone")], False, "permanent", 1, 1),                    # one attempt
    ([FakeResponse(409, "busy"), FakeResponse(200, BIG_HTML)], True, None, 2, 2),
    ([FakeResponse(423, "blocked")] * 3, False, "retryable", 3, 3),              # retries exhausted
    ([FakeResponse(200, "<html></html>"), FakeResponse(200, BIG_HTML)], True, None, 2, 2),   # a thin body
    ([requests.ConnectionError("boom"), FakeResponse(200, BIG_HTML)], True, None, 2, 2),     # transport error
])
def test_fetching(responses, ok, kind, attempts, calls):
    s = FakeSession(responses)
    r = sac.fetch_html(s, "https://kym/x", FAST)
    assert (r.ok, r.error_kind, r.attempts_used, s.calls) == (ok, kind, attempts, calls)
    assert s.seen_params[0]["browser"] == "false"


def test_403_is_an_auth_error():
    with pytest.raises(sac.ScrapingAntAuthError):
        sac.fetch_html(FakeSession([FakeResponse(403, "bad key")]), "https://kym/x", FAST)


# -- the store -----------------------------------------------------------------------------

@pytest.fixture
def store():
    s = mock_store(dom_store.DomStore)
    s.urls.insert_many([
        {"url": A, "Confirmed": True, "namespace": "memes", "lastmod": "2026-01-01", "last_scraped": None},
        {"url": B, "Confirmed": True, "namespace": "memes", "lastmod": None, "last_scraped": None},
        {"url": C, "Confirmed": False, "namespace": "events", "lastmod": None, "last_scraped": None}])
    return s


def test_a_page_round_trips_compressed_and_stamps_last_scraped(store):
    store.save_result(url=A, ok=True, html=BIG_HTML, status_code=200)
    doc = store.doms.find_one({"url": A})
    assert store.load_html(A) == BIG_HTML and doc["encoding"] == "zlib" and len(bytes(doc["html"])) < len(BIG_HTML)
    assert store.urls.find_one({"url": A})["last_scraped"] is not None


def test_selection_buckets_and_the_confirmed_filter(store):
    assert set(store.select_pending()) == {A, B} and C in store.select_pending(confirmed_only=False)


def test_failures_retryable_until_the_cap_permanent_never(store):
    for _ in range(2):
        store.save_result(url=B, ok=False, error="503: hiccup", error_kind="retryable")
    assert B in store.select_pending(max_failed_attempts=3)
    store.save_result(url=B, ok=False, error="503: hiccup", error_kind="retryable")
    assert B not in store.select_pending(max_failed_attempts=3)
    store.save_result(url=A, ok=False, error="404: gone", error_kind="permanent")
    assert A not in store.select_pending()


def test_a_failed_refetch_keeps_the_good_dom(store):
    store.save_result(url=A, ok=True, html=BIG_HTML)
    assert store.save_result(url=A, ok=False, error="503", error_kind="retryable") == "kept_ok"
    assert store.doms.find_one({"url": A})["scrape_status"] == "ok" and store.load_html(A) == BIG_HTML


def test_staleness_by_lastmod_and_by_refetch_window(store):
    store.save_result(url=A, ok=True, html=BIG_HTML, fetched_at=datetime(2025, 12, 1, tzinfo=timezone.utc))
    assert A in store.select_pending()                      # lastmod 2026-01-01 > fetched
    store.save_result(url=A, ok=True, html=BIG_HTML)
    assert A not in store.select_pending()
    store.save_result(url=B, ok=True, html=BIG_HTML, fetched_at=datetime.now(timezone.utc) - timedelta(days=90))
    assert B not in store.select_pending(refetch_older_than_days=0)
    assert B in store.select_pending(refetch_older_than_days=30)


def test_iter_html_for_carries_the_fetch_time(store):
    # parser 1.7.0: an entry's scraped_at is when its stored page was fetched
    when = datetime(2026, 7, 9, 23, 47, 44, tzinfo=timezone.utc)
    store.save_result(url=A, ok=True, html=BIG_HTML, fetched_at=when)
    with serving(dom_store, store):
        [(url, html, sha, fetched_at)] = list(dom_store.iter_html_for([A]))
    assert (url, html, fetched_at, sha) == (A, BIG_HTML, when, store.doms.find_one({"url": A})["content_sha256"])


def test_filter_unscraped_and_the_dag_glue(store):
    store.save_result(url=A, ok=True, html=BIG_HTML)
    assert store.filter_unscraped([A, B]) == [B]
    # iter_fetch -> as_doc -> save_result, as the DAG does
    tallies = {"ok": 0, "failed": 0, "kept_ok": 0}
    for r in sac.iter_fetch(FakeSession([FakeResponse(200, BIG_HTML), FakeResponse(404, "gone")]), [A, B], FAST):
        tallies[store.save_result(**r.as_doc())] += 1
    assert tallies == {"ok": 1, "failed": 1, "kept_ok": 0}
    assert (store.stats()["doms_ok"], store.stats()["failed_permanent"]) == (1, 1)


# -- one entry, one address (gap 14) ----------------------------------------------------------

PUB, SENS = "https://knowyourmeme.com/memes/doge", "https://knowyourmeme.com/sensitive/memes/doge"


def entry_page(address, title):
    return (f"<html><head><meta property='og:url' content='{address}' />"
            f"<meta property='og:title' content='{title} | Know Your Meme' /></head><body>" + "meme " * 200 + "</body></html>")


@pytest.fixture
def twins():
    s = mock_store(dom_store.DomStore)
    s.urls.insert_many([{"_id": dom_store.url_doc_id(u), "url": u, "Confirmed": True, "namespace": ns,
                         "lastmod": None, "last_scraped": None} for u, ns in ((PUB, "memes"), (SENS, "sensitive/memes"))])
    return s


def test_a_stored_page_records_what_it_says_about_itself_and_old_pages_are_read_once(twins):
    twins.save_result(url=PUB, ok=True, html=entry_page(SENS, "Doge"))
    doc = twins.doms.find_one({"url": PUB})
    assert (doc["page_url"], doc["page_title"]) == (SENS, "Doge")
    twins.doms.update_one({"url": PUB}, {"$unset": {"page_url": "", "page_title": ""}})    # stored before
    got = twins.page_identities()[PUB]
    assert (got["page_url"], got["page_title"], got["fetched_at"].tzinfo) == (SENS, "Doge", timezone.utc)
    assert twins.doms.find_one({"url": PUB})["page_title"] == "Doge"                       # written back


def test_marks_are_set_kept_and_lifted(twins):
    assert twins.mark_duplicates({PUB: SENS}) == {"marked": 1, "unchanged": 0, "unmarked": 0, "duplicates": 1}
    since = twins.urls.find_one({"url": PUB})["duplicate_since"]
    assert twins.mark_duplicates({PUB: SENS})["unchanged"] == 1
    assert twins.urls.find_one({"url": PUB})["duplicate_since"] == since
    moved_back = twins.mark_duplicates({SENS: PUB})
    assert (moved_back["marked"], moved_back["unmarked"]) == (1, 1)
    assert "duplicate_of" not in twins.urls.find_one({"url": PUB})
    assert twins.urls.find_one({"url": SENS})["duplicate_of"] == PUB


def test_the_whole_step_keeps_the_address_the_newest_page_names(twins):
    twins.save_result(url=PUB, ok=True, html=entry_page(PUB, "Doge"), fetched_at=datetime(2026, 7, 10, tzinfo=timezone.utc))
    twins.save_result(url=SENS, ok=True, html=entry_page(SENS, "Doge"))
    with serving(dom_store, twins):
        out = dom_store.resolve_duplicates()
    assert twins.urls.find_one({"url": PUB})["duplicate_of"] == SENS
    assert (out["entries_with_duplicates"], out["pages_dropped"], out["kept_sensitive"], out["marks"]["marked"]) == \
        (1, 1, 1, 1)


def test_a_duplicate_is_still_refetched_when_it_goes_stale(twins):
    # its fresh page is the evidence that KYM moved the entry back
    twins.save_result(url=PUB, ok=True, html=BIG_HTML, fetched_at=datetime(2025, 12, 1, tzinfo=timezone.utc))
    twins.urls.update_one({"url": PUB}, {"$set": {"lastmod": "2026-01-01", "duplicate_of": SENS}})
    assert PUB in twins.select_pending()
