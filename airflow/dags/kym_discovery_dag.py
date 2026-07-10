"""
kym_discovery_dag.py — Airflow DAG over the refactored kym_discover library
============================================================================
State between tasks lives in MongoDB (via kym_store); XCom carries only
small data: stats dicts and the serialised Taxonomy.

Layout expectation:
    dags/
      kym_discovery_dag.py      <- this file
      include/
        __init__.py
        kym_discover.py         <- pure discovery library + thin CLI
        kym_store.py            <- JSON + Mongo persistence

Because the library no longer relies on module globals or in-place index
mutation, every task simply: loads what it needs from Mongo, calls a pure
function with an explicit Taxonomy/CrawlConfig, and upserts the returned
records. No state re-application hacks.

Trigger-time params:
    max_category_pages : 0 = unlimited (default), N = cap pages per listing
    sitemap_only       : true = skip the listing crawl (Phase 2)
    json_snapshot      : path on a shared volume to also dump kym_urls.json
                         ("" = skip)
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.sdk import Param, dag, task

from modules import kym_discover as kd
from modules import kym_store as store

log = logging.getLogger(__name__)

# How many listing crawls may hit KYM at once. Each mapped task self-rate-
# limits via CrawlConfig.crawl_delay, but N parallel crawlers multiply the
# aggregate request rate — keep this low to stay polite.
MAX_PARALLEL_LISTING_CRAWLS = 2

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _config_from_params(params: dict | None) -> kd.CrawlConfig:
    """Build a CrawlConfig from trigger-time params."""
    max_pages = (params or {}).get("max_category_pages") or 0
    if max_pages > 0:
        return kd.CrawlConfig(max_pages_per_listing=max_pages)
    return kd.DEFAULT_CONFIG


@dag(
    dag_id="kym_discovery",
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "kym", "discovery"],
    params={
        "max_category_pages": Param(0, type="integer", minimum=0,
                                    description="0 = unlimited"),
        "sitemap_only": Param(False, type="boolean"),
        "json_snapshot": Param("", type="string",
                               description="Path for a kym_urls.json dump, '' to skip"),
    },
)
def kym_discovery():

    # -- Phase 1: sitemaps -----------------------------------------------
    @task(execution_timeout=timedelta(hours=2))
    def discover_sitemaps(params: dict | None = None) -> dict:
        cfg = _config_from_params(params)
        session = kd.make_session()
        existing = store.mongo_load_index()
        changed, stats = kd.fetch_all_sitemaps(session, existing,
                                               kd.DEFAULT_TAXONOMY, cfg)
        if changed:
            store.mongo_upsert(changed.values())
        return stats

    # -- Phase 1b: taxonomy inference --------------------------------------
    @task
    def infer_taxonomy() -> dict:
        urls = store.mongo_known_urls()
        return kd.infer_taxonomy(urls).to_dict()

    # -- Phase 2: listing crawl (one mapped task per listing) ---------------
    @task
    def build_crawl_args(taxonomy: dict, params: dict | None = None) -> list[dict]:
        """Emit one arg-dict per listing; empty list => Phase 2 skipped."""
        if params and params["sitemap_only"]:
            log.info("sitemap_only=True — skipping listing crawl")
            return []
        tax = kd.Taxonomy.from_dict(taxonomy)
        return [{"path": p, "confirmed": c} for p, c in tax.listings]

    @task(
        max_active_tis_per_dagrun=MAX_PARALLEL_LISTING_CRAWLS,
        execution_timeout=timedelta(hours=6),
        retries=2,
    )
    def crawl_listing(listing: dict, taxonomy: dict,
                      params: dict | None = None) -> dict:
        cfg = _config_from_params(params)
        tax = kd.Taxonomy.from_dict(taxonomy)
        session = kd.make_session()
        known = store.mongo_known_urls()

        new_records = kd.crawl_listing(session, known, listing["path"],
                                       listing["confirmed"], tax, cfg)
        if new_records:
            store.mongo_upsert(new_records.values())
        return {"path": listing["path"], "added": len(new_records)}

    # -- Summary / optional JSON snapshot -----------------------------------
    @task(trigger_rule="none_failed")
    def summarize(params: dict | None = None) -> dict:
        index = store.mongo_load_index()
        summary = kd.summarize_index(index)
        log.info("DISCOVERY COMPLETE — %s", summary)

        snapshot = (params or {}).get("json_snapshot") or ""
        if snapshot:
            from pathlib import Path
            store.save_json(Path(snapshot), index)
        return summary

    sitemap_stats = discover_sitemaps()
    taxonomy = infer_taxonomy()
    crawl_args = build_crawl_args(taxonomy)
    crawls = crawl_listing.partial(taxonomy=taxonomy).expand(listing=crawl_args)

    sitemap_stats >> taxonomy
    crawls >> summarize()


kym_discovery()
