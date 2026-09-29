"""
imgflip_client.py — polite HTTP client for imgflip pages and images
====================================================================
Pure HTTP library + thin CLI, in the scrapingant_client / openwebui_client
mould: no Airflow, no Mongo, no module-level environment reads or mutable
state. The templates DAG glues it to template_store; this file only knows
how to turn a URL into a page or an image, politely.

Why direct, and ScrapingAnt only as a fallback
-----------------------------------------------
imgflip renders its search and template pages on the server, and on
2026-09-28 served ~150 plain requests with no Cloudflare challenge. Its
robots.txt disallows only /orig/ and /browse/ and sets no crawl-delay. So
pages are fetched directly, identified by a research user agent, at about
one request every two seconds per task. ScrapingAnt (1 credit a page) is
used only once imgflip starts refusing us, and only up to a credit budget
per run. Images come from imgflip's CDN and are always fetched directly:
ScrapingAnt returns HTML, not bytes.

imgflip's own search box calls an internal JSON endpoint
(``/ajax_meme_search_new``). This client never does: imgflip's terms ask
users not to access the service "using a method other than the interface
and the instructions that we provide", and Gabi chose the public pages.

Error taxonomy
--------------
    ok              200 and the page looks like what was asked for (an
                    expected marker is in it)
    permanent       404/410 — the page or image is gone. Recorded, never
                    retried. A wrong image extension is also a 404.
    end_of_results  500 on a search page past the first — imgflip answers
                    500 past page 250, for every query. Not retried.
    blocked         403, 429, or a challenge page ("Just a moment",
                    cf-chl). Retry-After is honoured. After
                    ``blocked_after`` of these in a row the client switches
                    to ScrapingAnt (transport ``auto``), or raises
                    ImgflipBlockedError (transport ``direct``) so the task
                    fails loudly and Airflow retries it later, rather than
                    marking hundreds of frames "no results".
    retryable       other 5xx, timeouts, connection errors, a 200 without
                    any expected marker. Exponential backoff, full jitter.

Config (environment; empty means unset):
    IMGFLIP_CONTACT            appended to the user agent, e.g. a project URL
    IMGFLIP_HTML_DELAY_S       gap between page requests, default 2.0
    IMGFLIP_IMAGE_DELAY_S      gap between image requests, default 0.25
    IMGFLIP_TIMEOUT_S          default 30
    IMGFLIP_MAX_ATTEMPTS       per URL, default 4
    IMGFLIP_TRANSPORT          auto (default) | direct | scrapingant
    IMGFLIP_SCRAPINGANT_MAX_CREDITS   per client (one per task), default 1000

CLI:
    python -m modules.imgflip_client search "distracted boyfriend"
    python -m modules.imgflip_client template 112126428
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import quote_plus, urlparse
from urllib.robotparser import RobotFileParser

import requests

log = logging.getLogger("imgflip_client")

SITE = "https://imgflip.com"
USER_AGENT = "MemeAtlas-Research-Indexer/1.0"

SEARCH_MARKERS = ('id="mt-boxes-wrap"',)
TEMPLATE_MARKERS = ('id="mtm-title"',)

_PERMANENT_STATUSES = {404, 410}
_BLOCKED_STATUSES = {403, 429}
_CHALLENGE_MARKERS = ("just a moment", "cf-chl", "challenge-platform",
                      "attention required")
_IMAGE_TYPES = ("image/", "video/mp4")


class ImgflipBlockedError(RuntimeError):
    """imgflip refuses us and no fallback is left — stop the task."""


def search_url(query: str, page: int = 1) -> str:
    url = f"{SITE}/memesearch?q={quote_plus(query)}"
    return url if page <= 1 else f"{url}&page={int(page)}"


# ---------------------------------------------------------------------------
# Config & results
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ImgflipConfig:
    contact: str = ""
    html_delay_s: float = 2.0
    image_delay_s: float = 0.25
    timeout_s: float = 30.0
    max_attempts: int = 4
    backoff_base_s: float = 2.0
    backoff_max_s: float = 120.0
    blocked_after: int = 3
    transport: str = "auto"
    scrapingant_max_credits: int = 1000

    @property
    def user_agent(self) -> str:
        return f"{USER_AGENT} (+{self.contact})" if self.contact else USER_AGENT

    @classmethod
    def from_env(cls) -> "ImgflipConfig":
        def num(name: str, default: float) -> float:
            raw = os.getenv(name, "").strip()
            return float(raw) if raw else default

        transport = os.getenv("IMGFLIP_TRANSPORT", "").strip().lower() or "auto"
        if transport not in ("auto", "direct", "scrapingant"):
            raise ValueError(f"IMGFLIP_TRANSPORT must be auto|direct|scrapingant, "
                             f"not {transport!r}")
        return cls(
            contact=os.getenv("IMGFLIP_CONTACT", "").strip(),
            html_delay_s=num("IMGFLIP_HTML_DELAY_S", 2.0),
            image_delay_s=num("IMGFLIP_IMAGE_DELAY_S", 0.25),
            timeout_s=num("IMGFLIP_TIMEOUT_S", 30.0),
            max_attempts=int(num("IMGFLIP_MAX_ATTEMPTS", 4)),
            transport=transport,
            scrapingant_max_credits=int(num("IMGFLIP_SCRAPINGANT_MAX_CREDITS", 1000)),
        )


@dataclass
class PageResult:
    url: str
    ok: bool
    html: str | None = None
    final_url: str | None = None        # after redirects (template ids -> slugs)
    status_code: int | None = None
    error: str | None = None
    error_kind: str | None = None       # permanent|end_of_results|retryable|blocked
    transport: str = "direct"
    attempts_used: int = 0
    elapsed_s: float = 0.0
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def as_doc(self) -> dict[str, Any]:
        """Kwargs for template_store.save_page — the client<->store contract."""
        return {"url": self.url, "ok": self.ok, "html": self.html,
                "final_url": self.final_url, "status_code": self.status_code,
                "error": self.error, "error_kind": self.error_kind,
                "transport": self.transport, "attempts_used": self.attempts_used,
                "fetched_at": self.fetched_at}


@dataclass
class ImageResult:
    url: str
    ok: bool
    content: bytes | None = None
    content_type: str | None = None
    status_code: int | None = None
    error: str | None = None
    error_kind: str | None = None
    attempts_used: int = 0


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------

class ImgflipClient:
    """One per task. Holds what politeness needs across calls: when the last
    request went out, how many refusals came in a row, whether we switched
    to ScrapingAnt, and how many credits that has cost."""

    def __init__(self, cfg: ImgflipConfig, session: requests.Session | None = None,
                 *, scrapingant_fetch: Callable[[str], Any] | None = None,
                 sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic):
        self.cfg = cfg
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": cfg.user_agent})
        self._scrapingant_fetch = scrapingant_fetch
        self._sleep = sleep
        self._clock = clock
        self._last: dict[str, float] = {}
        self._robots: RobotFileParser | None = None
        self.consecutive_blocks = 0
        self.transport = "scrapingant" if cfg.transport == "scrapingant" else "direct"
        self.credits_used = 0
        self.requests = {"html": 0, "image": 0}

    # -- politeness ---------------------------------------------------------

    def _wait(self, kind: str) -> None:
        gap = self.cfg.html_delay_s if kind == "html" else self.cfg.image_delay_s
        last = self._last.get(kind)
        if last is not None:
            remaining = gap - (self._clock() - last)
            if remaining > 0:
                self._sleep(remaining)
        self._last[kind] = self._clock()

    def _backoff(self, attempt: int, retry_after: float | None = None) -> None:
        if retry_after is not None:
            self._sleep(min(self.cfg.backoff_max_s, max(0.0, retry_after)))
            return
        cap = min(self.cfg.backoff_max_s, self.cfg.backoff_base_s * (2 ** (attempt - 1)))
        self._sleep(cap * (0.5 + random.random() / 2))

    def allowed(self, url: str) -> bool:
        """robots.txt, read once per client. An unreadable robots.txt allows
        everything but the paths we know imgflip disallows."""
        if self._robots is None:
            rp = RobotFileParser()
            try:
                resp = self.session.get(f"{SITE}/robots.txt", timeout=self.cfg.timeout_s)
                lines = resp.text.splitlines() if resp.status_code == 200 else []
            except requests.RequestException:
                lines = []
            rp.parse(lines or ["User-agent: *", "Disallow: /orig/", "Disallow: /browse/"])
            self._robots = rp
        return self._robots.can_fetch(USER_AGENT, url)

    def _note_block(self, url: str, detail: str) -> None:
        self.consecutive_blocks += 1
        log.warning("imgflip refused %s (%s) — %d in a row", url, detail,
                    self.consecutive_blocks)
        if self.consecutive_blocks < self.cfg.blocked_after:
            return
        if (self.cfg.transport == "auto" and self.transport == "direct"
                and self._scrapingant_fetch is not None):
            log.warning("Switching to ScrapingAnt for the rest of this task "
                        "(budget %d credits)", self.cfg.scrapingant_max_credits)
            self.transport = "scrapingant"
            self.consecutive_blocks = 0
            return
        raise ImgflipBlockedError(
            f"imgflip refused {self.consecutive_blocks} requests in a row "
            f"(last: {url}: {detail}); transport={self.transport}")

    # -- pages --------------------------------------------------------------

    def fetch_page(self, url: str, *, markers: tuple[str, ...],
                   end_of_results_on_500: bool = False) -> PageResult:
        if not self.allowed(url):
            return PageResult(url=url, ok=False, error="disallowed by robots.txt",
                              error_kind="permanent")
        started = self._clock()
        last_error, last_status, last_kind = "unknown", None, "retryable"
        for attempt in range(1, self.cfg.max_attempts + 1):
            if self.transport == "scrapingant":
                result = self._via_scrapingant(url, markers, attempt, started)
                if result is not None:
                    return result
                last_error, last_kind = "scrapingant: no usable page", "retryable"
                continue

            self._wait("html")
            self.requests["html"] += 1
            try:
                resp = self.session.get(url, timeout=self.cfg.timeout_s)
            except requests.RequestException as exc:
                last_error, last_status = f"transport: {exc.__class__.__name__}", None
                log.warning("[%d/%d] %s — %s", attempt, self.cfg.max_attempts, url,
                            last_error)
                if attempt < self.cfg.max_attempts:
                    self._backoff(attempt)
                continue

            status = resp.status_code
            body = resp.text if resp.content else ""
            if status == 200 and any(m in body for m in markers):
                self.consecutive_blocks = 0
                return PageResult(url=url, ok=True, html=body, final_url=resp.url,
                                  status_code=200, attempts_used=attempt,
                                  elapsed_s=self._clock() - started)
            if status in _PERMANENT_STATUSES:
                return PageResult(url=url, ok=False, status_code=status,
                                  error=f"{status}", error_kind="permanent",
                                  attempts_used=attempt,
                                  elapsed_s=self._clock() - started)
            if status == 500 and end_of_results_on_500:
                return PageResult(url=url, ok=False, status_code=500,
                                  error="500 past the last page",
                                  error_kind="end_of_results", attempts_used=attempt,
                                  elapsed_s=self._clock() - started)
            challenged = any(m in body[:4096].lower() for m in _CHALLENGE_MARKERS)
            if status in _BLOCKED_STATUSES or challenged:
                last_error, last_status, last_kind = (
                    f"{status}{' challenge' if challenged else ''}", status, "blocked")
                self._note_block(url, last_error)        # may switch or raise
                if attempt < self.cfg.max_attempts:
                    self._backoff(attempt, _retry_after(resp))
                continue
            last_error, last_status, last_kind = (
                f"{status}" if status != 200 else "200 without an expected marker",
                status, "retryable")
            log.warning("[%d/%d] %s — %s", attempt, self.cfg.max_attempts, url, last_error)
            if attempt < self.cfg.max_attempts:
                self._backoff(attempt)

        return PageResult(url=url, ok=False, status_code=last_status, error=last_error,
                          error_kind=last_kind, transport=self.transport,
                          attempts_used=self.cfg.max_attempts,
                          elapsed_s=self._clock() - started)

    def _via_scrapingant(self, url: str, markers: tuple[str, ...], attempt: int,
                         started: float) -> PageResult | None:
        if self._scrapingant_fetch is None:
            raise ImgflipBlockedError("transport=scrapingant but no ScrapingAnt "
                                      "fetcher was configured")
        if self.credits_used >= self.cfg.scrapingant_max_credits:
            raise ImgflipBlockedError(
                f"ScrapingAnt budget of {self.cfg.scrapingant_max_credits} credits "
                "for this task is spent")
        fetched = self._scrapingant_fetch(url)
        if fetched.ok:
            self.credits_used += 1          # ScrapingAnt bills successes only
            if any(m in (fetched.html or "") for m in markers):
                return PageResult(url=url, ok=True, html=fetched.html, final_url=url,
                                  status_code=200, transport="scrapingant",
                                  attempts_used=attempt,
                                  elapsed_s=self._clock() - started)
            return None
        if fetched.error_kind == "permanent":
            return PageResult(url=url, ok=False, status_code=fetched.status_code,
                              error=fetched.error, error_kind="permanent",
                              transport="scrapingant", attempts_used=attempt,
                              elapsed_s=self._clock() - started)
        return None

    # -- images -------------------------------------------------------------

    def fetch_image(self, url: str) -> ImageResult:
        """An image from imgflip's CDN (or KYM's), always directly."""
        last_error, last_status = "unknown", None
        for attempt in range(1, self.cfg.max_attempts + 1):
            self._wait("image")
            self.requests["image"] += 1
            try:
                resp = self.session.get(url, timeout=self.cfg.timeout_s)
            except requests.RequestException as exc:
                last_error, last_status = f"transport: {exc.__class__.__name__}", None
                if attempt < self.cfg.max_attempts:
                    self._backoff(attempt)
                continue
            status = resp.status_code
            ctype = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
            if status == 200 and ctype.startswith(_IMAGE_TYPES) and resp.content:
                return ImageResult(url=url, ok=True, content=resp.content,
                                   content_type=ctype, status_code=200,
                                   attempts_used=attempt)
            if status in _PERMANENT_STATUSES or (status == 200 and ctype == "text/html"):
                return ImageResult(url=url, ok=False, status_code=status,
                                   error=f"{status} {ctype}", error_kind="permanent",
                                   attempts_used=attempt)
            last_error, last_status = f"{status} {ctype}", status
            if attempt < self.cfg.max_attempts:
                self._backoff(attempt, _retry_after(resp) if status == 429 else None)
        return ImageResult(url=url, ok=False, status_code=last_status, error=last_error,
                           error_kind="retryable", attempts_used=self.cfg.max_attempts)


def _retry_after(resp: requests.Response) -> float | None:
    raw = resp.headers.get("Retry-After", "").strip()
    return float(raw) if raw.isdigit() else None


def scrapingant_fetcher() -> Callable[[str], Any] | None:
    """A ScrapingAnt page fetcher when a key is configured, else None — so
    ``auto`` degrades to ``direct`` on a machine without one."""
    if not os.getenv("SCRAPINGANT_API_KEY", "").strip():
        return None
    from modules import scrapingant_client as sac

    cfg = sac.ScrapeConfig.from_env()
    session = sac.make_session()
    return lambda url: sac.fetch_html(session, url, cfg)


def make_client(session: requests.Session | None = None) -> ImgflipClient:
    return ImgflipClient(ImgflipConfig.from_env(), session,
                         scrapingant_fetch=scrapingant_fetcher())


def is_imgflip(url: str) -> bool:
    return (urlparse(url).hostname or "").lower().endswith("imgflip.com")


# ---------------------------------------------------------------------------
# Thin CLI
# ---------------------------------------------------------------------------

def _cli(argv: list[str] | None = None) -> int:
    from modules import imgflip_parse as ip

    parser = argparse.ArgumentParser(description="Fetch and parse imgflip pages.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("search")
    s.add_argument("query")
    s.add_argument("--page", type=int, default=1)
    t = sub.add_parser("template")
    t.add_argument("template_id", type=int)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    client = make_client()
    if args.cmd == "search":
        res = client.fetch_page(search_url(args.query, args.page),
                                markers=SEARCH_MARKERS, end_of_results_on_500=args.page > 1)
        out: Any = ip.parse_search(res.html) if res.ok else res.as_doc()
    else:
        res = client.fetch_page(ip.template_page_url(args.template_id),
                                markers=TEMPLATE_MARKERS)
        out = dict(ip.parse_template_page(res.html), final_url=res.final_url) \
            if res.ok else res.as_doc()
    print(json.dumps(out, indent=2, default=str))
    return 0 if res.ok else 1


if __name__ == "__main__":
    sys.exit(_cli())
