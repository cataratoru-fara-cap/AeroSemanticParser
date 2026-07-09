"""
kym_scrape_dag.py — Airflow DAG over scrapingant_client + dom_store
====================================================================
Top-level orchestration ONLY. All fetch logic lives in
modules/scrapingant_client.py; all persistence in modules/dom_store.py.
State between tasks lives in MongoDB; XCom carries only URL strings and
small stats dicts.

Pipeline:
    select_urls    dom_store.pending_urls(): urls ⋈ doms — never-tried,
                   retryable failures, then stale (lastmod / refetch window)
    chunk_urls     split into mapped-task workloads
    scrape_chunk   (mapped) re-filter already-done, stream-fetch via
                   ScrapingAnt browser=false (1 credit/page), persist each
                   DOM the moment it lands
    summarize      corpus-level tallies from Mongo

Two retry tiers, as in the old Playwright batch task:
    * in-process — scrapingant_client backoff on 409/423/5xx/timeouts
    * Airflow    — task retries; scrape_chunk re-filters against Mongo
                   first, so a retried chunk re-buys nothing it already has
Failed URLs stay in `doms` with scrape_status='failed' (the dead-letter
record); non-permanent ones are re-queued automatically on the next run.

Trigger-time params:
    batch_size    URLs per DAG run (0 = everything pending) — the credit
                  throttle; 1000 ≈ 1000 credits
    chunk_size    URLs per mapped task
    namespaces    comma-separated filter, e.g. "memes" ("" = all)
    refetch_days  re-scrape OK pages older than N days (0 = never)
    confirmed_only  restrict to sitemap-confirmed URLs
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.sdk import Param, dag, task

from modules import dom_store
from modules import scrapingant_client as sac

log = logging.getLogger(__name__)

# How many mapped chunks may hit the ScrapingAnt API at once. Their
# concurrency cap is plan-dependent (free tier is low); each chunk is
# sequential internally with cfg.request_delay_s between requests, so
# aggregate rate ≈ N_parallel / delay. Bump alongside the plan.
MAX_PARALLEL_SCRAPE_TASKS = 2

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="kym_scrape",
    schedule=None,  # run after discovery; wire an Asset trigger later if wanted
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "kym", "scrape"],
    params={
        "batch_size": Param(1000, type="integer", minimum=0,
                            description="URLs this run (0 = all pending). "
                                        "browser=false => ~1 credit per URL."),
        "chunk_size": Param(50, type="integer", minimum=1,
                            description="URLs per mapped scrape task"),
        "namespaces": Param("", type="string",
                            description="Comma-separated, e.g. 'memes' "
                                        "('' = all namespaces)"),
        "refetch_days": Param(0, type="integer", minimum=0,
                              description="Re-scrape OK pages older than N "
                                          "days (0 = never refetch)"),
        "confirmed_only": Param(True, type="boolean"),
    },
)
def kym_scrape():

    # -- Phase 1: decide what to fetch --------------------------------------
    @task
    def select_urls(params: dict | None = None) -> list[str]:
        p = params or {}
        namespaces = [s.strip() for s in (p.get("namespaces") or "").split(",")
                      if s.strip()]
        urls = dom_store.pending_urls(
            limit=p.get("batch_size", 0),
            namespaces=namespaces or None,
            confirmed_only=p.get("confirmed_only", True),
            refetch_older_than_days=p.get("refetch_days", 0),
        )
        log.info("Selected %d URLs to scrape", len(urls))
        return urls

    @task
    def chunk_urls(urls: list[str], params: dict | None = None) -> list[list[str]]:
        size = (params or {}).get("chunk_size", 50)
        chunks = [urls[i:i + size] for i in range(0, len(urls), size)]
        log.info("Split %d URLs into %d chunks of ≤%d",
                 len(urls), len(chunks), size)
        return chunks

    # -- Phase 2: fetch & persist (one mapped task per chunk) ----------------
    @task(
        max_active_tis_per_dagrun=MAX_PARALLEL_SCRAPE_TASKS,
        execution_timeout=timedelta(hours=1),
        retries=2,
    )
    def scrape_chunk(chunk: list[str]) -> dict:
        cfg = sac.ScrapeConfig.from_env()

        todo = dom_store.filter_unscraped(chunk)  # retry-idempotency
        skipped = len(chunk) - len(todo)
        if not todo:
            return {"ok": 0, "failed": 0, "kept_ok": 0, "skipped": skipped}

        session = sac.make_session()
        results = (r.as_doc() for r in sac.iter_fetch(session, todo, cfg))
        tallies = dom_store.save_results(results)  # streams: durable per page

        tallies["skipped"] = skipped
        log.info("Chunk done — %s (≈%d credits)", tallies, tallies["ok"])
        return tallies

    # -- Phase 3: corpus-level summary ---------------------------------------
    @task(trigger_rule="none_failed")
    def summarize(chunk_stats: list[dict]) -> dict:
        run_totals: dict[str, int] = {}
        for s in chunk_stats:
            for k, v in s.items():
                run_totals[k] = run_totals.get(k, 0) + v
        corpus = dom_store.scrape_stats()
        log.info("SCRAPE RUN COMPLETE — run=%s corpus=%s", run_totals, corpus)
        return {"run": run_totals, "corpus": corpus}

    urls = select_urls()
    chunks = chunk_urls(urls)
    stats = scrape_chunk.expand(chunk=chunks)
    summarize(stats)


kym_scrape()