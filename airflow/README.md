# AeroSemanticParser — KYM pipeline

Scrapes and parses [Know Your Meme](https://knowyourmeme.com) into a
structured corpus, orchestrated by Airflow, with a Streamlit dashboard
over the results.

```
kym_discovery  ──▶  kym_scrape  ──▶  kym_parse        (each triggers the next)
   urls              doms             entries
                                      parse_failures

                     run_summaries  ◀── every stage records its run
                            │
                            ▼
                     dashboard (:8501)
```

## Quick start

```bash
cp .env.example .env            # fill in secrets; AIRFLOW_UID must be `id -u`
cp config/airflow.cfg.example config/airflow.cfg
docker compose build
docker compose up -d
```

Everything browser-facing goes through one reverse proxy on **port 8080**
(`proxy/Caddyfile`). The network in front of this host drops most ports and
8080 is one that gets through, so the other services are routed through it
rather than published on ports of their own:

| Service | URL | Notes |
|---|---|---|
| Airflow | http://&lt;host&gt;:8080/ | DAG triggering and logs |
| **Dashboard** | **http://&lt;host&gt;:8080/dashboard/** | corpus analytics (read-only) |
| mongo-express | http://&lt;host&gt;:8080/mongo/ | raw collection browser; login is `MONGO_EXPRESS_USER` / `MONGO_EXPRESS_PASSWORD` in `.env` |
| pgAdmin | http://&lt;host&gt;:8080/pgadmin/ | Airflow metadata DB; login is `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` in `.env` |
| Flower | http://localhost:5555 | `--profile flower` |

The dashboard (8501), mongo-express (8081) and pgAdmin (5050) also listen
on `127.0.0.1` only, for SSH tunnels and debugging — note the dashboard's prefix still
applies there: `http://localhost:8501/dashboard/`.

## The three stages

Each DAG has the same shape: `select → chunk → mapped work → summarize →
record_summary`. Work is chunked into mapped tasks so a failure retries a
chunk rather than the run, and every chunk re-filters against Mongo first,
so a retry never redoes finished work (this matters most in `kym_scrape`,
where ScrapingAnt bills per request).

**`kym_discovery`** (monthly) — crawls sitemaps, then listing pages, into
`urls`. Upserts are idempotent: `Confirmed` is monotonic and `last_scraped`
is never clobbered.

**`kym_scrape`** — fetches pending URLs via ScrapingAnt into `doms`, HTML
zlib-compressed. Two retry tiers (in-process backoff, then Airflow task
retries). A failed refetch never destroys a previously good DOM. Failures
carry an error *kind*; `permanent` ones (400/404/405/422) are never
re-queued.

**`kym_parse`** — parses stored DOMs into validated `KYMEntryScrape` rows in
`entries`, grading each against `CorpusPolicy` as `ready` or `incomplete`.
Nothing is discarded for being incomplete — it is *labelled*, so a policy
change re-grades the corpus without re-parsing. Pages that fail schema
validation land in `parse_failures`, a dead-letter collection carrying the
same staleness stamps as `entries`, so a deterministic failure is not
retried until the parser version or the page content actually changes.

### Streaming is load-bearing

A KYM page is multi-MB decompressed and roughly 10× that inside
BeautifulSoup. `parse_store.iter_html` streams one page at a time off the
Mongo cursor, and `dom_store.content_shas` projects *only* the hash.
Materialising either into a list OOM-killed the first run. The comments
saying so are warnings, not trivia.

## Layout

```
dags/
  kym_{discovery,scrape,parse}_dag.py   orchestration only — no logic
  modules/
    mongo_base.py        shared client/_id/UTC plumbing for the stores
    mongo_store.py       owns `urls`      ← kym_store.py is its facade
    dom_store.py         owns `doms`
    parse_store.py       owns `entries`, `parse_failures`
    summary_store.py     owns `run_summaries`
    kym_discover.py      pure discovery library + CLI   (no Mongo, no Airflow)
    scrapingant_client.py pure fetch library + CLI       (no Mongo, no Airflow)
    kym_parse.py         pure HTML → model + CLI         (no Mongo, no Airflow)
    kym_models.py        the entry schema and CorpusPolicy
    kg/                  pure KG libraries — build, census, metrics,
                         semantics  (no Mongo, no Airflow)
  kg_config/             curated KG inputs, tracked: the entry-type taxonomy,
                         the YARRRML mapping, the morph-kgc ini template
  tests/                 pytest; conftest.py puts dags/ on sys.path
dashboard/               Streamlit app, its own image
```

The layering rule, which is worth keeping: **pure library ← store module
owning exactly one collection ← DAG**. A library never imports Mongo or
Airflow; a DAG never issues a query. One collection has one owner module,
so a schema change has one place to edit.

## Tests

```bash
python -m pytest            # from airflow/ — 74 tests, no network, no real Mongo
```

Mongo is faked with `mongomock`; HTTP is faked with stub sessions. `pytest.ini`
puts `dags/` on the path, so tests run the same way inside and outside the
container.

## Dashboard

Read-only, its own small image, served at `/dashboard/` through the proxy. Two kinds of panel,
deliberately distinguished:

- **current state** — aggregated live from `urls`/`doms`/`entries`/
  `parse_failures`, so it includes work finished since the last DAG run;
- **trend** — read from `run_summaries`, because the live collections know
  what is true now, not what was true in July.

Its colours are a validated palette (see `dashboard/lib/theme.py`), not a
taste call: categorical hues in fixed order and never cycled, magnitude
bars on a single sequential hue, reserved status colours that always ship
with an icon and a written label, and a data table behind every chart.

## Post-parse work

`dags/modules/kg/` (build, census, metrics, semantics) holds the pure KG
libraries. It was called `helpers/`, which named *how* it ran rather than
what it did. The modules are **still hand-run against `entries`** — the
`kym_kg` DAG that orchestrates them is in progress — but they now obey the
same layering rule as the rest of the tree, so a library here imports
neither Mongo nor Airflow.

Four modules are explicitly transitional and named so you can tell:
`census_tags.py` folds into `census.py`, `export_pg.py` and `export_rml.py`
fold into a single `serialize.py`, and `_legacy_store.py` is replaced by a
conventional `modules/kg_store.py`.

Curated inputs live in `dags/kg_config/` and are tracked in git. They used
to sit in `data/`, which is gitignored — which is how a reviewed 104-line
taxonomy ended up retyped by hand into a Python constant.

## Conventions worth not breaking

- Collection names come from `MONGODB_*` env vars set in `docker-compose.yml`
  — the dashboard reads the same names, so they stay single-sourced.
- `config/airflow.cfg` and `.env` are gitignored. Their `.example` twins are
  tracked. Anything docker-compose sets as `AIRFLOW__*` wins over the cfg
  file, so do not set it in both.
- `_PIP_ADDITIONAL_REQUIREMENTS` reinstalls on every container start. Real
  dependencies belong in `requirements.txt`.
