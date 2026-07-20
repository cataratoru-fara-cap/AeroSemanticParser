"""
kym_parse_dag.py — Airflow DAG over modules/kym_parse.py + modules/parse_store.py
===================================================================================
Top-level orchestration ONLY. All parsing logic lives in modules/kym_parse.py
(no Mongo, no Airflow imports); all persistence in modules/parse_store.py
(the only place this DAG touches the database). State between tasks lives
in MongoDB; XCom carries only URL strings and small stats dicts.

Pipeline:
    select_urls   parse_store.pending_urls(): urls ⋈ doms ⋈ entries — never
                  parsed, or stale (DOM content changed / parser upgraded /
                  corpus policy changed), unless force_reparse=True
    chunk_urls    split into mapped-task workloads
    parse_chunk   (mapped) stream pages one at a time off the Mongo cursor
                  (parse_store.iter_html -> dom_store.iter_html_for), parse
                  each, grade it against CorpusPolicy, upsert immediately —
                  per-page durable, peak memory ~one page, and nothing is
                  discarded for being incomplete, only labelled
    summarize     corpus-level tallies from Mongo

A page that fails schema validation (missing url/title/category/status/
origin/tags — the well-formedness gate) is persisted as a dead-letter record
in the dedicated `parse_failures` collection (kept out of `entries` so that
collection stays schema-pure), with the error message/type and the same
staleness stamps as entries docs — queryable for debugging
(db.parse_failures.find()) and NOT re-queued until the parser version or the
DOM content changes (parse failures are deterministic; blind retries would
fail identically). A later successful parse of the url deletes its
dead-letter record. Should be near-zero on confirmed memes; any failure here
is worth inspecting by hand.

Trigger-time params:
    batch_size     URLs per DAG run (0 = everything pending)
    chunk_size     URLs per mapped task
    namespaces     comma-separated filter, e.g. "memes" ("" = all)
    confirmed_only restrict to sitemap-confirmed URLs
    force_reparse  ignore staleness checks, re-parse every candidate
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.sdk import Param, dag, task
from pydantic import ValidationError

from modules import kym_parse
from modules import parse_store as store
from modules.kym_models import CORPUS_POLICY_VERSION, DEFAULT_CORPUS_POLICY

log = logging.getLogger(__name__)

# Parsing is pure CPU/DOM work (no outbound HTTP), so this can run higher
# concurrency than the scrape stage's ScrapingAnt-credit-bound tasks.
MAX_PARALLEL_PARSE_TASKS = 4

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="kym_parse",
    schedule=None,  # run after scrape; wire an Asset trigger later if wanted
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "kym", "parse"],
    params={
        "batch_size": Param(0, type="integer", minimum=0,
                            description="URLs this run (0 = all pending)"),
        "chunk_size": Param(200, type="integer", minimum=1,
                            description="URLs per mapped parse task"),
        "namespaces": Param([], type="array", items={"type": "string"},
                    description="One namespace per line, e.g. 'memes'. Empty = all."),
        "confirmed_only": Param(True, type="boolean"),
        "force_reparse": Param(False, type="boolean",
                               description="Ignore staleness checks; re-parse everything selected"),
    },
)
def kym_parse_dag():

    # -- Phase 1: decide what to parse --------------------------------------
    @task
    def select_urls(params: dict | None = None) -> list[str]:
        p = params or {}
        namespaces = store.clean_namespaces(p.get("namespaces"))
        urls = store.pending_urls(
            namespaces=namespaces or None,
            confirmed_only=p.get("confirmed_only", True),
            current_parser_version=kym_parse.PARSER_VERSION,
            current_policy_version=CORPUS_POLICY_VERSION,
            force_reparse=p.get("force_reparse", False),
            limit=p.get("batch_size", 0),
        )
        log.info("Selected %d URLs to parse (parser=%s policy=%s)",
                 len(urls), kym_parse.PARSER_VERSION, CORPUS_POLICY_VERSION)
        return urls

    @task
    def chunk_urls(urls: list[str], params: dict | None = None) -> list[list[str]]:
        size = (params or {}).get("chunk_size", 200)
        chunks = [urls[i:i + size] for i in range(0, len(urls), size)]
        log.info("Split %d URLs into %d chunks of ≤%d",
                 len(urls), len(chunks), size)
        return chunks

    # -- Phase 2: parse & persist (one mapped task per chunk) ----------------
    @task(
        max_active_tis_per_dagrun=MAX_PARALLEL_PARSE_TASKS,
        execution_timeout=timedelta(minutes=30),
        retries=2,
    )
    def parse_chunk(chunk: list[str]) -> dict:
        if not chunk:
            return {"ready": 0, "incomplete": 0, "parse_failed": 0, "skipped": 0}

        # Everything below is lazy end-to-end: iter_html streams one
        # decompressed page at a time off the Mongo cursor, parse_entry
        # consumes it, and save_parsed upserts each doc as the generator
        # yields — so each page is durable the moment it's parsed and its
        # HTML/soup are released before the next page is fetched. Peak
        # memory ≈ one page, independent of chunk_size. Do NOT "optimise"
        # this into a list; materializing a chunk of multi-MB pages is
        # what OOM-killed the first run.
        counters = {"seen": 0}
        failures: list[dict] = []  # tiny (url + error string) — safe to buffer

        def parsed_stream():
            for url, html, sha in store.iter_html(chunk):
                counters["seen"] += 1
                try:
                    entry = kym_parse.parse_entry(html, url=url)
                except (ValidationError, ValueError) as exc:
                    failures.append({
                        "url": url, "dom_content_sha256": sha,
                        "error": str(exc), "error_type": type(exc).__name__})
                    log.warning("Parse failed for %s: %s", url, exc)
                    continue
                yield entry, sha

        tallies = store.save_parsed(
            parsed_stream(), DEFAULT_CORPUS_POLICY, kym_parse.PARSER_VERSION,
            CORPUS_POLICY_VERSION)

        # Dead-letter records: queryable via db.parse_failures.find({}) —
        # a kept-separate collection so `entries` stays schema-pure. They
        # carry the same staleness stamps as ok docs, so select_pending
        # won't re-queue them until the parser version or the DOM content
        # actually changes. The namespace lookup is cheap here — only the
        # (typically few) failed urls in this chunk, not the whole batch —
        # and it clusters failures by source: e.g. a run of namespace=
        # 'editorials' failures means an editorial URL slipped through the
        # confirmed-meme filter, not that the parser itself is broken.
        if failures:
            ns_by_url = store.namespaces_for(f["url"] for f in failures)
            for f in failures:
                f["namespace"] = ns_by_url.get(f["url"])
        store.save_failures(failures, kym_parse.PARSER_VERSION,
                            CORPUS_POLICY_VERSION)

        tallies["parse_failed"] = len(failures)
        tallies["skipped"] = len(chunk) - counters["seen"]
        log.info("Chunk done — %s", tallies)
        return tallies

    # -- Phase 3: corpus-level summary ---------------------------------------
    @task(trigger_rule="none_failed")
    def summarize(chunk_stats: list[dict]) -> dict:
        run_totals: dict[str, int] = {}
        for s in chunk_stats:
            for k, v in s.items():
                run_totals[k] = run_totals.get(k, 0) + v
        corpus = store.parse_stats()
        log.info("PARSE RUN COMPLETE — run=%s corpus=%s", run_totals, corpus)
        return {"run": run_totals, "corpus": corpus}
    
    # -- Phase 4: persist summary + render plots -----------------------------
    @task(trigger_rule="none_failed")
    def plot_summary(summary: dict, run_id: str | None = None) -> list[str]:
        from modules import summary_plots, summary_store
        summary_store.save_summary(stage="parse", dag_id="kym_parse",
                                   run_id=run_id or "manual", summary=summary)
        history = summary_store.load_history("scrape")
        paths = summary_plots.render_all("parse", summary, history)
        return [str(p) for p in paths]

    urls = select_urls()
    chunks = chunk_urls(urls)
    stats = parse_chunk.expand(chunk=chunks)
    summarry = summarize(stats)
    plot_summary(summarry)


kym_parse_dag()