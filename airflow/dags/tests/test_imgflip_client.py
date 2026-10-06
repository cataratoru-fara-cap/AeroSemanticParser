"""imgflip_client.py: a fake session and a fake clock, no network. Pinned:
politeness is enforced by the client (page requests at least html_delay_s
apart); a refusal is never "no results" (403/429/challenge pages count toward
blocked_after, then switch to ScrapingAnt in auto or raise in direct, so the
task fails loudly); 404 is permanent and a 500 past page 1 the end of results;
a 200 without the expected markup is retried, not parsed; robots.txt is
obeyed; ScrapingAnt is capped at its credit budget."""
from types import SimpleNamespace

import pytest

from modules import imgflip_client as ic

SEARCH_OK = '<html><div id="mt-boxes-wrap"></div></html>'
ROBOTS = "user-agent: *\ndisallow: /orig/\ndisallow: /browse/\n"
URL = ic.search_url("distracted boyfriend")
CHALLENGE = "<title>Just a moment...</title>"


class FakeResponse:
    def __init__(self, status=200, text="", headers=None, content=None):
        self.status_code, self.text, self.url = status, text, None
        self.content = content if content is not None else text.encode()
        self.headers = headers or {"Content-Type": "text/html"}


class FakeSession:
    def __init__(self, responses):
        self.responses, self.headers, self.urls = list(responses), {}, []

    def get(self, url, timeout=None):
        if url.endswith("/robots.txt"):
            return FakeResponse(200, ROBOTS)
        self.urls.append(url)
        r = self.responses.pop(0)
        r.url = r.url or url
        return r


class Clock:
    def __init__(self):
        self.now, self.slept = 1000.0, []

    def time(self):
        return self.now

    def sleep(self, s):
        self.slept.append(s)
        self.now += s


def client(responses, fallback=None, **cfg):
    clock = Clock()
    return ic.ImgflipClient(ic.ImgflipConfig(**cfg), FakeSession(responses), scrapingant_fetch=fallback,
                            sleep=clock.sleep, clock=clock.time), clock


def page(c, url=URL, **kw):
    return c.fetch_page(url, markers=ic.SEARCH_MARKERS, **kw)


def ant_ok(url):
    return SimpleNamespace(ok=True, html=SEARCH_OK, error_kind=None, error=None, status_code=200)


# -- pages ------------------------------------------------------------------------------------

def test_ok_with_our_user_agent_and_spaced_requests():
    c, clock = client([FakeResponse(200, SEARCH_OK)] * 2, contact="https://example.org/p", html_delay_s=2.0)
    assert page(c).ok and page(c, URL + "&page=2").ok
    assert c.session.headers["User-Agent"] == "MemeAtlas-Research-Indexer/1.0 (+https://example.org/p)"
    assert clock.slept == [2.0]


@pytest.mark.parametrize("responses, url, kw, want", [
    ([FakeResponse(404)], URL, {}, (False, "permanent", 1)),                                  # not retried
    ([FakeResponse(500)], URL + "&page=251", {"end_of_results_on_500": True}, (False, "end_of_results", 1)),
    ([FakeResponse(200, "<html>ad</html>"), FakeResponse(200, SEARCH_OK)], URL, {}, (True, None, 2)),  # no markup
])
def test_outcomes(responses, url, kw, want):
    res = page(client(responses)[0], url, **kw)
    assert (res.ok, res.error_kind, res.attempts_used) == want


def test_retry_after_is_honoured_and_a_success_resets_the_block_count():
    c, clock = client([FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, SEARCH_OK)], html_delay_s=0)
    assert page(c).ok and 7.0 in clock.slept and c.consecutive_blocks == 0


def test_robots_disallow_is_refused_without_a_request():
    c, _ = client([])
    res = page(c, "https://imgflip.com/browse/x")
    assert (res.ok, res.error_kind, c.session.urls) == (False, "permanent", [])


# -- blocks --------------------------------------------------------------------------------------

@pytest.mark.parametrize("responses, cfg", [
    ([FakeResponse(403, CHALLENGE)] * 3, {"transport": "direct", "blocked_after": 3, "max_attempts": 4}),
    ([FakeResponse(200, CHALLENGE)] * 2, {"transport": "direct", "blocked_after": 2}),   # a challenge with 200
    ([FakeResponse(403, CHALLENGE)] * 2, {"blocked_after": 2}),                          # auto, no ScrapingAnt key
])
def test_blocked_raises(responses, cfg):
    with pytest.raises(ic.ImgflipBlockedError):
        page(client(responses, **cfg)[0])


def test_auto_switches_to_scrapingant():
    calls = []
    c, _ = client([FakeResponse(403, CHALLENGE)] * 2, fallback=lambda url: calls.append(url) or ant_ok(url),
                  blocked_after=2)
    res = page(c)
    assert res.ok and (res.transport, c.transport, c.credits_used, calls) == ("scrapingant", "scrapingant", 1, [URL])


def test_the_credit_budget_is_a_hard_stop():
    c, _ = client([], fallback=ant_ok, transport="scrapingant", scrapingant_max_credits=1)
    assert page(c).ok
    with pytest.raises(ic.ImgflipBlockedError):
        page(c, URL + "&page=2")


# -- images, config -------------------------------------------------------------------------------

@pytest.mark.parametrize("response, url, want", [
    (FakeResponse(200, headers={"Content-Type": "image/jpeg"}, content=b"\xff\xd8jpeg"),
     "https://i.imgflip.com/4/1ur9b0.jpg", (True, None, b"\xff\xd8jpeg")),
    (FakeResponse(404, "<html>not found</html>"), "https://i.imgflip.com/3jpogl.jpg", (False, "permanent", None)),  # a wrong extension
    (FakeResponse(200, "<html>"), "https://i.imgflip.com/x.jpg", (False, "permanent", None)),   # HTML as an image
])
def test_images(response, url, want):
    got = client([response])[0].fetch_image(url)
    assert (got.ok, got.error_kind, got.content if got.ok else None) == want


def test_config_and_search_url(monkeypatch):
    monkeypatch.setenv("IMGFLIP_TRANSPORT", "carrier-pigeon")
    with pytest.raises(ValueError):
        ic.ImgflipConfig.from_env()
    assert ic.search_url("this is fine") == "https://imgflip.com/memesearch?q=this+is+fine"
    assert ic.search_url("doge", 3) == "https://imgflip.com/memesearch?q=doge&page=3"
