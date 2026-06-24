"""
Airflow DAG: KnowYourMeme scraper — layout-drift-resistant
===========================================================
Architecture:
  Task 1  collect_meme_urls   — BS4 crawl of listing pages (fast, free)
  Task 2  scrape_meme_details — LLM extraction per detail page (drift-proof)
  Task 3  validate_records    — Pydantic validation (catches LLM hallucinations)
  Task 4  load_to_postgres    — upsert with full audit trail

Why LLM for extraction?
  KYM changes its HTML layout regularly (new sections, renamed classes,
  A/B tests, template variability across meme types).  CSS/XPath selectors
  break silently.  An LLM reading the rendered text understands *what* a
  "year of origin" or "spread platform" is regardless of where it lives in
  the DOM — the prompt is the stable contract, not the HTML structure.

Why BS4 for URL collection?
  The listing pages (/memes/all/page/N) have a stable anchor pattern that
  almost never changes.  Using an LLM here would burn tokens on trivially
  simple link extraction.  BS4 with a simple href filter is correct here.

Why Pydantic validation?
  LLMs occasionally hallucinate structure: returning a string where an int
  is expected, inventing status values, or returning an empty dict when the
  page 404s.  Pydantic catches this before it reaches Postgres, routing bad
  records to a dead-letter log rather than silently writing corrupt data.

Required Airflow connections / variables:
  Conn:  postgres_kym        (Postgres, points at your meme_db)
  Var:   KYM_PAGES           (int, listing pages per run, default 3)
  Var:   KYM_MODEL           (str, LLM model string, default openai/gpt-4o-mini)
  Env:   OPENAI_API_KEY      (in docker-compose.yml or Airflow env)
"""

from __future__ import annotations

import json
import time
import os
import logging
from datetime import datetime, timedelta

from pydantic import BaseModel, field_validator, model_validator, ValidationError

from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from modules import MemeRecord

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

KYM_CONN_ID = "postgres_kym"
KYM_TABLE   = "memes"
KYM_DEAD_TABLE = "memes_dead_letter"

# ─── DAG ─────────────────────────────────────────────────────────────────────

@dag(
    dag_id="kym_LLM_meme_scraper",
    description="lLM based Layout-drift-resistant KnowYourMeme scraper",
    schedule="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["scraping", "memes", "kym"],
)
def kym_LLM_scraper_dag():

    # ── Task 1: collect meme URLs via BS4 (fast, no LLM cost) ────────────────
    @task()
    def _collect_meme_urls() -> list[str]:
        """
        Scrape listing pages with plain requests + BeautifulSoup.
        The /memes/all/page/N URL pattern and <a href="/memes/..."> anchor
        structure are stable enough for BS4.  Save the LLM for the hard part.
        """
        import requests
        from bs4 import BeautifulSoup

        num_pages = int(Variable.get("KYM_PAGES", default_var=3))
        headers   = {"User-Agent": "Mozilla/5.0 (compatible; KYMScraper/1.0; research)"}
        seen: set[str] = set()

        for page in range(1, num_pages + 1):
            url = f"https://knowyourmeme.com/memes/all/page/{page}"
            log.info("Fetching listing page %d: %s", page, url)
            try:
                resp = requests.get(url, headers=headers, timeout=20)
                resp.raise_for_status()
            except Exception as exc:
                log.warning("Listing page %d failed: %s", page, exc)
                time.sleep(2)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.find_all("a", href=True)
            page_urls = [
                "https://knowyourmeme.com" + a["href"]
                for a in links
                if (
                    a["href"].startswith("/memes/")
                    and not a["href"].startswith("/memes/all")
                    and not a["href"].startswith("/memes/search")
                    # Exclude sub-pages: images, videos, comments
                    and a["href"].count("/") == 2
                )
            ]
            new = [u for u in page_urls if u not in seen]
            seen.update(new)
            log.info("Page %d: found %d new URLs (total %d)", page, len(new), len(seen))
            time.sleep(1.5)

        result = list(seen)
        log.info("Total unique meme URLs: %d", len(result))
        return result

    # ── Task 2: extract meme data with LLM ───────────────────────────────────
    @task()
    def _scrape_meme_details(urls: list[str]) -> list[dict]:
        """
        For each URL, fetch the raw page HTML, then send it to the LLM
        with a structured extraction prompt.

        Why this is layout-drift-resistant:
          We pass the full rendered text to the LLM, not a pre-parsed
          BeautifulSoup tree.  The prompt describes *what to extract by
          meaning*, not where to find it by CSS class.  When KYM moves
          the "year" field from a sidebar to an infobox, the LLM still
          finds it.  A CSS selector would silently return None.

        The LLM is used ONLY for extraction, not for URL collection — this
        keeps the expensive API calls focused on the value-add task.
        """
        import requests
        from scrapegraphai.graphs import SmartScraperGraph

        model   = Variable.get("KYM_MODEL", default_var="openai/gpt-4o-mini")
        headers = {"User-Agent": "Mozilla/5.0 (compatible; KYMScraper/1.0; research)"}

        llm_config = {
            "llm": {
                "api_key": os.environ.get("OPENAI_API_KEY", ""),
                "model": model,
            },
            "verbose": False,
            "headless": True,
            "loader_kwargs": {"requests_per_second": 1},
        }

        # Extraction prompt — describes fields by meaning, not by DOM location.
        # This is the stable contract that survives layout changes.
        EXTRACTION_PROMPT = """
        Extract structured knowledge about this internet meme from the page.
        Return ONLY a JSON object with these fields (use null for missing data):

        {
          "name":   string — the meme's official title,
          "status": string — one of: confirmed, submission, deadpool,
          "type":   string — e.g. image macro, video, catchphrase, exploitable,
          "year":   integer — the year the meme originated (4-digit year only),
          "origin": string — the platform or event where it originated (e.g. 4chan, Twitter, YouTube),
          "tags":   array of strings — the topic tags listed on the page,
          "about":  string — a concise 2-3 sentence description of what the meme is,
          "spread": string — how and where it spread online,
          "views":  integer — page view count if visible on the page
        }

        Do NOT invent information. If a field is not present on the page, return null.
        Return ONLY the JSON object, no markdown fences or extra text.
        """

        raw_results: list[dict] = []
        BATCH_SIZE = 5

        for i in range(0, len(urls), BATCH_SIZE):
            batch = urls[i : i + BATCH_SIZE]
            log.info("Scraping batch %d/%d: %d URLs", i // BATCH_SIZE + 1,
                     -(-len(urls) // BATCH_SIZE), len(batch))

            for url in batch:
                try:
                    graph = SmartScraperGraph(
                        prompt=EXTRACTION_PROMPT,
                        source=url,
                        config=llm_config,
                    )
                    result = graph.run()
                    if isinstance(result, str):
                        result = json.loads(result)
                    result["url"] = url
                    raw_results.append(result)
                    log.info("OK: %s → name=%s", url, result.get("name"))
                except Exception as exc:
                    log.warning("LLM extraction failed for %s: %s", url, exc)
                    raw_results.append({"url": url, "_error": str(exc)})
                time.sleep(1)

            time.sleep(2)

        return raw_results

    # ── Task 3: validate with Pydantic ───────────────────────────────────────
    @task()
    def _validate_records(raw_records: list[dict]) -> dict:
        """
        Run every raw LLM output through the MemeRecord schema.

        Valid records   → passed to load_to_postgres via XCom
        Invalid records → written to memes_dead_letter for inspection

        This is the critical layer that makes LLM extraction safe for
        production: the LLM might hallucinate field values or return
        unexpected structure; Pydantic catches that before it hits Postgres.
        """
        hook = PostgresHook(postgres_conn_id=KYM_CONN_ID)
        _ensure_tables(hook)

        valid:   list[dict] = []
        invalid: list[dict] = []

        for raw in raw_records:
            url = raw.get("url", "unknown")
            if "_error" in raw:
                invalid.append({"url": url, "reason": raw["_error"], "raw": json.dumps(raw)})
                continue
            try:
                record = MemeRecord(**raw)
                valid.append(record.model_dump())
            except ValidationError as exc:
                log.warning("Validation failed for %s: %s", url, exc)
                invalid.append({
                    "url": url,
                    "reason": str(exc),
                    "raw": json.dumps(raw, default=str),
                })

        # Write invalid records to dead-letter table for inspection
        if invalid:
            log.warning("%d records failed validation → dead-letter table", len(invalid))
            for rec in invalid:
                try:
                    hook.run(
                        f"""
                        INSERT INTO {KYM_DEAD_TABLE} (url, reason, raw_json, created_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (url) DO UPDATE
                          SET reason=EXCLUDED.reason, raw_json=EXCLUDED.raw_json, created_at=NOW()
                        """,
                        parameters=(rec["url"], rec["reason"][:1000], rec["raw"]),
                    )
                except Exception as e:
                    log.error("Dead-letter insert failed: %s", e)

        log.info("Validation: %d valid, %d invalid", len(valid), len(invalid))
        return {"valid": valid, "invalid_count": len(invalid)}

    # ── Task 4: Prepare data for SQL (small Python task) ─────────────────────
    @task
    def _prepare_load_data(validation_result: dict) -> list[dict]:
        """Just pass through the valid records (or do any final serialization)."""
        records = validation_result["valid"]
        log.info("Preparing %d records for bulk upsert", len(records))
        return records   # list of dicts works great with XCom

    # ── Task 5: Bulk upsert with @task.sql ───────────────────────────────────
    @task.sql(conn_id=KYM_CONN_ID)
    def _load_to_postgres(records: list[dict]) -> dict:
        """
        Bulk upsert using Postgres jsonb_to_recordset.
        This is layout-drift-resistant friendly and very efficient.
        """
        # The SQL below will be executed by Airflow's SQLExecuteQueryOperator
        # `records` will be available as a Jinja-templated parameter
        sql = """
        WITH source_data AS (
            SELECT *
            FROM jsonb_to_recordset(%(records)s::jsonb) AS x(
                url        TEXT,
                name       TEXT,
                status     TEXT,
                type       TEXT,
                year       INTEGER,
                origin     TEXT,
                tags       JSONB,
                about      TEXT,
                spread     TEXT,
                views      BIGINT
            )
        )
        INSERT INTO {{ params.table }} 
            (url, name, status, type, year, origin, tags, about, spread, views, scraped_at)
        SELECT 
            url, name, status, type, year, origin, tags, about, spread, views, NOW()
        FROM source_data
        ON CONFLICT (url) DO UPDATE SET
            name       = EXCLUDED.name,
            status     = EXCLUDED.status,
            type       = EXCLUDED.type,
            year       = EXCLUDED.year,
            origin     = EXCLUDED.origin,
            tags       = EXCLUDED.tags,
            about      = EXCLUDED.about,
            spread     = EXCLUDED.spread,
            views      = EXCLUDED.views,
            scraped_at = NOW();
        """

        # You can also return a summary if needed
        return {
            "upserted": len(records),  # approximate; for exact count use RETURNING + Python post-processing
            "validation_rejects": 0   # you can pass this from validation_result if desired
        }

    # ── Wire ─────────────────────────────────────────────────────────────────
    urls       = _collect_meme_urls() # pyright: ignore[reportArgumentType]
    raw        = _scrape_meme_details(urls) # pyright: ignore[reportArgumentType]
    validated  = _validate_records(raw) # pyright: ignore[reportArgumentType]
    prepared_load_data = _prepare_load_data(validated) # pyright: ignore[reportArgumentType]
    load_to_postgres = _load_to_postgres(prepared_load_data) # pyright: ignore[reportArgumentType]

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _ensure_tables(hook: PostgresHook) -> None:
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {KYM_TABLE} (
            id         SERIAL PRIMARY KEY,
            url        TEXT UNIQUE NOT NULL,
            name       TEXT,
            status     TEXT,
            type       TEXT,
            year       INTEGER,
            origin     TEXT,
            tags       JSONB DEFAULT '[]',
            about      TEXT,
            spread     TEXT,
            views      BIGINT,
            scraped_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_memes_name   ON {KYM_TABLE} (name);
        CREATE INDEX IF NOT EXISTS idx_memes_status ON {KYM_TABLE} (status);
        CREATE INDEX IF NOT EXISTS idx_memes_year   ON {KYM_TABLE} (year);
        CREATE INDEX IF NOT EXISTS idx_memes_tags   ON {KYM_TABLE} USING GIN (tags);

        CREATE TABLE IF NOT EXISTS {KYM_DEAD_TABLE} (
            id         SERIAL PRIMARY KEY,
            url        TEXT UNIQUE NOT NULL,
            reason     TEXT,
            raw_json   TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)


dag_instance = kym_LLM_scraper_dag()