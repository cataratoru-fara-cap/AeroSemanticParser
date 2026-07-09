"""
scrapingant_client.py — ScrapingAnt fetch client for the KYM pipeline
======================================================================
Pure HTTP library + thin CLI, mirroring the kym_discover philosophy:
no Airflow imports, no Mongo imports, no module-level mutable state.
The DAG glues this to dom_store; this file only knows how to turn a
URL into HTML via ScrapingAnt.

Why ScrapingAnt / why browser=false
-----------------------------------
KYM is fully server-side rendered: the plain HTML response already
contains the complete DOM we care about. Rendering it in a headless
browser only executes the ad/analytics JS and costs 10 credits instead
of 1. So every request here goes through

    GET https://api.scrapingant.com/v2/general?url=...&browser=false

which costs exactly 1 API credit (~40k credits for the full corpus).
ScrapingAnt does not bill failed requests, so retries are free.

The API key is sent as the ``x-api-key`` HTTP *header*, never as a
query parameter, so it cannot leak into request logs or tracebacks.

Error taxonomy (drives both in-process retries and dom_store's
"should we ever try this URL again" bookkeeping):

    auth       401/403            -> raise ScrapingAntAuthError. A bad
                                     key or exhausted plan must kill the
                                     task loudly, not "fail" 500 URLs.
    permanent  400/404/405/422    -> upstream page is gone / request is
                                     malformed. Recorded, never retried.
    retryable  409/423/429/5xx,   -> concurrency limit, anti-bot
               timeouts, conn errs,  detection, transient infra. Retried
               suspiciously thin body  with exponential backoff + jitter.

Config comes from the environment (set in docker-compose / .env):

    SCRAPINGANT_API_KEY        required
    SCRAPINGANT_PROXY_TYPE     datacenter (default) | residential
    SCRAPINGANT_TIMEOUT_S      per-request API timeout, default 30
    SCRAPINGANT_MAX_ATTEMPTS   in-process tries per URL, default 4
    SCRAPINGANT_REQUEST_DELAY_S  politeness gap between requests, default 0.5

CLI (smoke test a URL without Airflow):

    python -m modules.scrapingant_client https://knowyourmeme.com/memes/doge \
        --out /tmp/doge.html
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Iterator

import requests

log = logging.getLogger("scrapingant_client")

API_ENDPOINT = "https://api.scrapingant.com/v2/general"

# ScrapingAnt HTTP status -> classification. Anything not listed and >= 500,
# plus transport-level errors, is treated as retryable.
_AUTH_STATUSES = {401, 403}
_PERMANENT_STATUSES = {400, 404, 405, 422}
_RETRYABLE_STATUSES = {409, 423, 429}


class ScrapingAntAuthError(RuntimeError):
    """API key invalid or plan exhausted — abort the batch, don't loop."""


# ---------------------------------------------------------------------------
# Config & result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScrapeConfig:
    """Immutable per-run configuration; build once, pass explicitly."""

    api_key: str
    browser: bool = False            # False => the 1-credit HTML call
    proxy_type: str = "datacenter"   # 'residential' costs 25x — last resort
    api_timeout_s: int = 30          # ScrapingAnt-side execution budget
    max_attempts: int = 4            # in-process tries per URL
    backoff_base_s: float = 2.0
    backoff_max_s: float = 60.0
    request_delay_s: float = 0.5     # politeness gap between sequential URLs
    min_content_bytes: int = 2048    # smaller than any real KYM page

    @property
    def http_timeout_s(self) -> float:
        """requests-side timeout: API budget plus transport headroom."""
        return self.api_timeout_s + 15

    @classmethod
    def from_env(cls) -> "ScrapeConfig":
        key = os.getenv("SCRAPINGANT_API_KEY", "").strip()
        if not key:
            raise ScrapingAntAuthError(
                "SCRAPINGANT_API_KEY is not set — export it in .env / "
                "docker-compose before running the scrape DAG."
            )
        return cls(
            api_key=key,
            proxy_type=os.getenv("SCRAPINGANT_PROXY_TYPE", "datacenter"),
            api_timeout_s=int(os.getenv("SCRAPINGANT_TIMEOUT_S", "30")),
            max_attempts=int(os.getenv("SCRAPINGANT_MAX_ATTEMPTS", "4")),
            request_delay_s=float(os.getenv("SCRAPINGANT_REQUEST_DELAY_S", "0.5")),
        )


@dataclass
class FetchResult:
    """Outcome of one URL, regardless of how many attempts it took."""

    url: str
    ok: bool
    html: str | None = None
    status_code: int | None = None      # status ScrapingAnt returned to us
    error: str | None = None
    error_kind: str | None = None       # 'permanent' | 'retryable' | None
    attempts_used: int = 0
    elapsed_s: float = 0.0
    fetched_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))

    def as_doc(self) -> dict[str, Any]:
        """Kwargs for dom_store.save_result — the only client<->store contract."""
        return {
            "url": self.url,
            "ok": self.ok,
            "html": self.html,
            "status_code": self.status_code,
            "error": self.error,
            "error_kind": self.error_kind,
            "attempts_used": self.attempts_used,
            "fetched_at": self.fetched_at,
        }


# ---------------------------------------------------------------------------
# Session & single-URL fetch
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    """Plain pooled session. Retries are handled explicitly in fetch_html
    (urllib3's Retry can't distinguish ScrapingAnt's 423 semantics)."""
    return requests.Session()


def _looks_like_page(html: str, cfg: ScrapeConfig) -> bool:
    """Cheap sanity check: a real KYM page is big and starts with markup.
    A tiny or tagless body is a proxy hiccup worth one more attempt."""
    if len(html.encode("utf-8", errors="ignore")) < cfg.min_content_bytes:
        return False
    return "<html" in html[:2048].lower()


def _backoff_sleep(attempt: int, cfg: ScrapeConfig) -> None:
    """Exponential backoff with full jitter; attempt is 1-based."""
    cap = min(cfg.backoff_max_s, cfg.backoff_base_s * (2 ** (attempt - 1)))
    time.sleep(cap * (0.5 + random.random() / 2))


def fetch_html(session: requests.Session, url: str,
               cfg: ScrapeConfig) -> FetchResult:
    """
    Fetch one URL through ScrapingAnt, retrying transient failures
    in-process. Returns a FetchResult either way; raises only
    ScrapingAntAuthError (a broken key must stop the whole batch).
    """
    params = {
        "url": url,
        # Explicit on every call: the default has flip-flopped across API
        # versions, and browser=true silently 10x-es the credit cost.
        "browser": "false" if not cfg.browser else "true",
        "proxy_type": cfg.proxy_type,
        "timeout": str(cfg.api_timeout_s),
    }
    headers = {"x-api-key": cfg.api_key}

    started = time.monotonic()
    last_error, last_status = "unknown", None

    for attempt in range(1, cfg.max_attempts + 1):
        try:
            resp = session.get(API_ENDPOINT, params=params, headers=headers,
                               timeout=cfg.http_timeout_s)
        except requests.RequestException as exc:
            last_error, last_status = f"transport: {exc.__class__.__name__}", None
            log.warning("[%d/%d] %s — %s", attempt, cfg.max_attempts, url,
                        last_error)
            if attempt < cfg.max_attempts:
                _backoff_sleep(attempt, cfg)
            continue

        status = resp.status_code
        if status == 200:
            # KYM serves UTF-8; only override when the proxy omits a charset,
            # where requests would otherwise fall back to ISO-8859-1.
            if "charset" not in resp.headers.get("Content-Type", "").lower():
                resp.encoding = "utf-8"
            html = resp.text
            if _looks_like_page(html, cfg):
                return FetchResult(
                    url=url, ok=True, html=html, status_code=200,
                    attempts_used=attempt,
                    elapsed_s=time.monotonic() - started,
                )
            last_error, last_status = "thin_body", 200
            log.warning("[%d/%d] %s — 200 but body looks empty (%d bytes)",
                        attempt, cfg.max_attempts, url, len(html))
            if attempt < cfg.max_attempts:
                _backoff_sleep(attempt, cfg)
            continue

        detail = _error_detail(resp)
        if status in _AUTH_STATUSES:
            raise ScrapingAntAuthError(
                f"ScrapingAnt returned {status} ({detail}) — check "
                "SCRAPINGANT_API_KEY / remaining credits.")

        if status in _PERMANENT_STATUSES:
            return FetchResult(
                url=url, ok=False, status_code=status,
                error=f"{status}: {detail}", error_kind="permanent",
                attempts_used=attempt, elapsed_s=time.monotonic() - started,
            )

        # 409 (concurrency), 423 (anti-bot), 429, 5xx, anything else
        last_error, last_status = f"{status}: {detail}", status
        log.warning("[%d/%d] %s — retryable %s", attempt, cfg.max_attempts,
                    url, last_error)
        if attempt < cfg.max_attempts:
            _backoff_sleep(attempt, cfg)

    return FetchResult(
        url=url, ok=False, status_code=last_status, error=last_error,
        error_kind="retryable", attempts_used=cfg.max_attempts,
        elapsed_s=time.monotonic() - started,
    )


def _error_detail(resp: requests.Response) -> str:
    """ScrapingAnt errors arrive as {'detail': ...}; degrade gracefully."""
    try:
        return str(resp.json().get("detail", ""))[:200]
    except Exception:
        return resp.text[:200]


# ---------------------------------------------------------------------------
# Batch iteration
# ---------------------------------------------------------------------------

def iter_fetch(session: requests.Session, urls: Iterable[str],
               cfg: ScrapeConfig,
               should_stop: Callable[[], bool] | None = None,
               ) -> Iterator[FetchResult]:
    """
    Sequentially fetch ``urls``, yielding each FetchResult as it lands so
    the caller can persist incrementally (a crash loses at most one page).
    Sleeps cfg.request_delay_s between requests; parallelism is the
    orchestrator's job (mapped tasks), not this function's.
    """
    first = True
    for url in urls:
        if should_stop is not None and should_stop():
            log.info("iter_fetch stopping early on request")
            return
        if not first:
            time.sleep(cfg.request_delay_s)
        first = False
        yield fetch_html(session, url, cfg)


# ---------------------------------------------------------------------------
# Thin CLI — smoke-test URLs without Airflow or Mongo
# ---------------------------------------------------------------------------

def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch URLs via ScrapingAnt (browser=false, 1 credit).")
    parser.add_argument("urls", nargs="+")
    parser.add_argument("--out", help="Write the first successful HTML here.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = ScrapeConfig.from_env()
    session = make_session()

    wrote, failures = False, 0
    for res in iter_fetch(session, args.urls, cfg):
        if res.ok:
            size = len(res.html or "")
            print(f"OK   {res.url}  {size} bytes  "
                  f"{res.attempts_used} attempt(s)  {res.elapsed_s:.1f}s")
            if args.out and not wrote:
                with open(args.out, "w", encoding="utf-8") as fh:
                    fh.write(res.html or "")
                print(f"     wrote {args.out}")
                wrote = True
        else:
            failures += 1
            print(f"FAIL {res.url}  [{res.error_kind}] {res.error}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(_cli())