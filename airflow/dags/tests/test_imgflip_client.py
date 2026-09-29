"""Tests for imgflip_client.py — a fake session, a fake clock, no network.

What these pin:

  * **Politeness is enforced by the client**, not hoped for: consecutive
    page requests are at least ``html_delay_s`` apart.
  * **A refusal is never "no results"**: 403/429/challenge pages count
    toward ``blocked_after``, then switch to ScrapingAnt (auto) or raise
    (direct) so the task fails loudly.
  * **404 is permanent; a 500 past page 1 is the end of results**; a 200
    without the expected markup is retried, not parsed.
  * **robots.txt is obeyed.**
  * **ScrapingAnt is capped** at its credit budget.
"""
import unittest
from types import SimpleNamespace

from modules import imgflip_client as ic

SEARCH_OK = '<html><div id="mt-boxes-wrap"></div></html>'
ROBOTS = "user-agent: *\ndisallow: /orig/\ndisallow: /browse/\n"


class FakeResponse:
    def __init__(self, status=200, text="", headers=None, content=None, url=None):
        self.status_code = status
        self.text = text
        self.content = content if content is not None else text.encode()
        self.headers = headers or {"Content-Type": "text/html"}
        self.url = url


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.urls: list[str] = []

    def get(self, url, timeout=None):
        if url.endswith("/robots.txt"):
            return FakeResponse(200, ROBOTS)
        self.urls.append(url)
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        if r.url is None:
            r.url = url
        return r


class Clock:
    def __init__(self):
        self.now = 1000.0
        self.slept: list[float] = []

    def time(self):
        return self.now

    def sleep(self, s):
        self.slept.append(s)
        self.now += s


def client(responses, clock=None, fallback=None, **cfg):
    clock = clock or Clock()
    c = ic.ImgflipClient(ic.ImgflipConfig(**cfg), FakeSession(responses),
                         scrapingant_fetch=fallback, sleep=clock.sleep, clock=clock.time)
    return c, clock


URL = ic.search_url("distracted boyfriend")


class PageTests(unittest.TestCase):
    def test_ok_and_user_agent(self):
        c, _ = client([FakeResponse(200, SEARCH_OK)], contact="https://example.org/p")
        res = c.fetch_page(URL, markers=ic.SEARCH_MARKERS)
        self.assertTrue(res.ok)
        self.assertEqual(c.session.headers["User-Agent"],
                         "MemeAtlas-Research-Indexer/1.0 (+https://example.org/p)")

    def test_requests_are_spaced(self):
        c, clock = client([FakeResponse(200, SEARCH_OK)] * 2, html_delay_s=2.0)
        c.fetch_page(URL, markers=ic.SEARCH_MARKERS)
        c.fetch_page(URL + "&page=2", markers=ic.SEARCH_MARKERS)
        self.assertEqual(clock.slept, [2.0])

    def test_404_is_permanent_and_not_retried(self):
        c, _ = client([FakeResponse(404)])
        res = c.fetch_page(URL, markers=ic.SEARCH_MARKERS)
        self.assertEqual((res.ok, res.error_kind, res.attempts_used), (False, "permanent", 1))

    def test_500_past_page_one_is_the_end(self):
        c, _ = client([FakeResponse(500)])
        res = c.fetch_page(URL + "&page=251", markers=ic.SEARCH_MARKERS,
                           end_of_results_on_500=True)
        self.assertEqual(res.error_kind, "end_of_results")

    def test_a_200_without_the_markup_is_retried(self):
        c, _ = client([FakeResponse(200, "<html>ad</html>"), FakeResponse(200, SEARCH_OK)])
        res = c.fetch_page(URL, markers=ic.SEARCH_MARKERS)
        self.assertTrue(res.ok)
        self.assertEqual(res.attempts_used, 2)

    def test_retry_after_is_honoured(self):
        c, clock = client([FakeResponse(429, headers={"Retry-After": "7"}),
                           FakeResponse(200, SEARCH_OK)], html_delay_s=0)
        self.assertTrue(c.fetch_page(URL, markers=ic.SEARCH_MARKERS).ok)
        self.assertIn(7.0, clock.slept)
        self.assertEqual(c.consecutive_blocks, 0)            # reset by the success

    def test_robots_disallow_is_refused_without_a_request(self):
        c, _ = client([])
        res = c.fetch_page("https://imgflip.com/browse/x", markers=ic.SEARCH_MARKERS)
        self.assertEqual((res.ok, res.error_kind), (False, "permanent"))
        self.assertEqual(c.session.urls, [])


class BlockTests(unittest.TestCase):
    def challenge(self):
        return FakeResponse(403, "<title>Just a moment...</title>")

    def test_direct_raises_after_blocked_after_refusals(self):
        c, _ = client([self.challenge()] * 3, transport="direct", blocked_after=3,
                      max_attempts=4)
        with self.assertRaises(ic.ImgflipBlockedError):
            c.fetch_page(URL, markers=ic.SEARCH_MARKERS)

    def test_a_challenge_with_status_200_is_a_block_too(self):
        c, _ = client([FakeResponse(200, "<title>Just a moment...</title>")] * 2,
                      transport="direct", blocked_after=2)
        with self.assertRaises(ic.ImgflipBlockedError):
            c.fetch_page(URL, markers=ic.SEARCH_MARKERS)

    def test_auto_switches_to_scrapingant(self):
        calls = []

        def fallback(url):
            calls.append(url)
            return SimpleNamespace(ok=True, html=SEARCH_OK, error_kind=None, error=None,
                                   status_code=200)

        c, _ = client([self.challenge()] * 2, fallback=fallback, blocked_after=2)
        res = c.fetch_page(URL, markers=ic.SEARCH_MARKERS)
        self.assertTrue(res.ok)
        self.assertEqual((res.transport, c.transport, c.credits_used),
                         ("scrapingant", "scrapingant", 1))
        self.assertEqual(calls, [URL])

    def test_auto_without_a_scrapingant_key_raises(self):
        c, _ = client([self.challenge()] * 2, fallback=None, blocked_after=2)
        with self.assertRaises(ic.ImgflipBlockedError):
            c.fetch_page(URL, markers=ic.SEARCH_MARKERS)

    def test_the_credit_budget_is_a_hard_stop(self):
        ok = SimpleNamespace(ok=True, html=SEARCH_OK, error_kind=None, error=None,
                             status_code=200)
        c, _ = client([], fallback=lambda url: ok, transport="scrapingant",
                      scrapingant_max_credits=1)
        self.assertTrue(c.fetch_page(URL, markers=ic.SEARCH_MARKERS).ok)
        with self.assertRaises(ic.ImgflipBlockedError):
            c.fetch_page(URL + "&page=2", markers=ic.SEARCH_MARKERS)


class ImageTests(unittest.TestCase):
    def test_an_image(self):
        c, _ = client([FakeResponse(200, headers={"Content-Type": "image/jpeg"},
                                    content=b"\xff\xd8jpeg")])
        got = c.fetch_image("https://i.imgflip.com/4/1ur9b0.jpg")
        self.assertEqual((got.ok, got.content), (True, b"\xff\xd8jpeg"))

    def test_a_wrong_extension_is_a_permanent_404(self):
        c, _ = client([FakeResponse(404, "<html>not found</html>")])
        got = c.fetch_image("https://i.imgflip.com/3jpogl.jpg")
        self.assertEqual((got.ok, got.error_kind), (False, "permanent"))

    def test_html_served_as_an_image_is_permanent(self):
        c, _ = client([FakeResponse(200, "<html>", headers={"Content-Type": "text/html"})])
        self.assertEqual(c.fetch_image("https://i.imgflip.com/x.jpg").error_kind, "permanent")


class ConfigTests(unittest.TestCase):
    def test_from_env_rejects_an_unknown_transport(self):
        import os
        old = os.environ.get("IMGFLIP_TRANSPORT")
        os.environ["IMGFLIP_TRANSPORT"] = "carrier-pigeon"
        try:
            with self.assertRaises(ValueError):
                ic.ImgflipConfig.from_env()
        finally:
            if old is None:
                os.environ.pop("IMGFLIP_TRANSPORT")
            else:
                os.environ["IMGFLIP_TRANSPORT"] = old

    def test_search_url(self):
        self.assertEqual(ic.search_url("this is fine"),
                         "https://imgflip.com/memesearch?q=this+is+fine")
        self.assertEqual(ic.search_url("doge", 3), "https://imgflip.com/memesearch?q=doge&page=3")


if __name__ == "__main__":
    unittest.main()
