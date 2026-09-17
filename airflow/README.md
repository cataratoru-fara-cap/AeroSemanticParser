# AeroSemanticParser — KYM pipeline

Scrapes and parses [Know Your Meme](https://knowyourmeme.com) into a
structured corpus, orchestrated by Airflow, with a Streamlit dashboard
over the results.

```
kym_discovery ──▶ kym_scrape ──▶ kym_parse ──▶ kym_kg        (each triggers the next)
   urls            doms           entries        kg_nodes / kg_edges / kg_builds
                                  parse_failures data/kg/builds/<build_id>/
                                                     graph.nt  rml_data/*.csv
                                                     kg_view_*.csv  manifest.json
                                                        ▲
                                     kym_kg_validate ───┘  (weekly: re-derive the
                                                            RDF via RML, diff it)

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
| **SPARQL** | **http://&lt;host&gt;:8080/sparql/kg/query** | the KG's RDF store (Fuseki), **read-only** through the proxy — admin and update paths are refused there; `?query=SELECT…` |
| Neo4j Browser | http://localhost:7474 (tunnel) | the KG's property graph; not proxied — the Browser opens its own Bolt connection, so tunnel **both** ports: `ssh -L 7474:localhost:7474 -L 7687:localhost:7687 <host>`, user `neo4j` / `NEO4J_PASSWORD` |
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
  kym_{discovery,scrape,parse,kg}_dag.py   orchestration only — no logic
  kym_kg_validate_dag.py                   the RDF diff gate, its own DAG
  modules/
    mongo_base.py        shared client/_id/UTC plumbing for the stores
    mongo_store.py       owns `urls`      ← kym_store.py is its facade
    dom_store.py         owns `doms`
    parse_store.py       owns `entries`, `parse_failures`
    kg_store.py          owns `kg_nodes`, `kg_edges`, `kg_builds` (generational)
    summary_store.py     owns `run_summaries`
    kym_discover.py      pure discovery library + CLI   (no Mongo, no Airflow)
    scrapingant_client.py pure fetch library + CLI       (no Mongo, no Airflow)
    kym_parse.py         pure HTML → model + CLI         (no Mongo, no Airflow)
    kym_models.py        the entry schema and CorpusPolicy
    kg/                  pure KG libraries (no Mongo, no Airflow):
      build.py             one entry → nodes/edges — the only producer
      taxonomy.py          the curated entry-type taxonomy, validated
      census.py            frequency + co-occurrence over a corpus field
      serialize.py         one stream → graph.nt + RML CSVs + view CSVs
      rdf.py               canonical N-Triples serializer
      ntdiff.py            memory-bounded set diff of two .nt files
      metrics.py           IMKG-comparable graph statistics (pure stdlib)
      semantics.py         LLM definition-embedding analysis of types
  kg_config/             curated KG inputs, tracked: the entry-type taxonomy,
                         the YARRRML mapping, the MemeAtlas ontology
                         (memeatlas.ttl), MODEL.md (the IMKG crosswalk),
                         the morph-kgc ini template
  tests/                 pytest; conftest.py puts dags/ on sys.path
dashboard/               Streamlit app, its own image
```

The layering rule, which is worth keeping: **pure library ← store module
owning exactly one collection ← DAG**. A library never imports Mongo or
Airflow; a DAG never issues a query. One collection has one owner module,
so a schema change has one place to edit.

## Tests

```bash
python -m pytest            # from airflow/ — no network, no real Mongo
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

The **Knowledge graph** page leads with a third question: is the graph that
is live the graph we think it is. It reads the published build through
`kg_builds/current` — every KG query is build-scoped, so nothing there can
show a mixture of generations — and reports whether the four stores point at
the same build and whether the last RDF gate agreed. It deliberately does
**not** connect to Neo4j or Fuseki: that would put two more drivers in this
image for data the DAG already recorded in the run summary at publish time.

Its colours are a validated palette (see `dashboard/lib/theme.py`), not a
taste call: categorical hues in fixed order and never cycled, magnitude
bars on a single sequential hue, reserved status colours that always ship
with an icon and a written label, and a data table behind every chart.

## The KG stage

**MemeAtlas extends IMKG.** Where IMKG models something, its terms are
used as is (`m4s:MediaFrame`, `m4s:title`, `m4s:tag`, `kymt:` entry-type
classes, `skos:broader` for series), so IMKG queries run here. The rest of
the parsed record — sections, links, references, images, regions, corpus
grading — uses `mk:` terms declared in `dags/kg_config/memeatlas.ttl`.
`dags/kg_config/MODEL.md` has the full crosswalk. Origin and Spread
sections are not yet modelled; they are left to the event-extraction task.

**`kym_kg`** (triggered by parse) — lifts `entries` into a knowledge graph
and publishes it in every representation at once:

- **Neo4j** — the property graph: typed nodes (`Frame`, `TagConcept`, …)
  and relationships whose types are the edge vocabulary verbatim
  (`hasTag`, `partOfSeries`, `hasSection`, `subTypeOf`, …), carrying every
  parsed property;
- **Fuseki** — the RDF graph, queryable over SPARQL at `/sparql/`, with
  the vocabulary in the named graph `urn:memeatlas:ontology`;
- **files** under `data/kg/builds/<build_id>/` — `graph.nt`, the CSVs the
  RML mapping reads, Cosmograph/Gephi view CSVs, and a `manifest.json` of
  per-file hashes; `data/kg/current` points at the published build;
- **Mongo** `kg_nodes`/`kg_edges`/`kg_builds` — the working copy and the
  **authority**: `kg_builds/current` is the one pointer the others follow.

A store whose password is not configured is skipped, so the stage degrades
to Mongo + files rather than failing.

Every file is a projection of **one** node/edge stream from `kg/build.py`,
so the representations cannot disagree about which edges exist. They used
to: the RDF was derived by a second copy of the loop and carried 14,563
`relatesToMeme` triples the property graph did not, for two months, and
nothing compared them.

**`kym_kg_validate`** (weekly, or by hand) is the comparison. It re-derives
the RDF through an independent path — the YARRRML mapping in
`dags/kg_config/`, compiled by yatter, materialised by morph-kgc — and
diffs it against `graph.nt`. Steady state is *equal modulo four provenance
predicates*; one content triple either way is a failure. It flags the
build; it does not un-publish it.

### The KG stage publishes atomically

Mongo here is standalone, so there are no multi-document transactions.
Instead every build writes only into its own `build_id` namespace and
never touches the published generation — a reader cannot see a half-built
graph because it lives where nobody is reading. Publishing is: files
pointer first, then one single-document write to `kg_builds/current`,
which is atomic even standalone. Authority moves last, so once it says
"published" everything else already is. Re-running `publish` converges.

No query against `kg_nodes`/`kg_edges` is valid without a `build_id`
filter; every index leads on it so an unfiltered scan is visibly wrong.

To get the current graph: Mongo readers take `kg_builds.findOne({_id:
"current"}).build_id` and filter on it; file consumers follow
`data/kg/current` (or read `data/kg/CURRENT`) to a self-describing
`manifest.json`.

### Curated inputs are tracked

`dags/kg_config/` holds the reviewed entry-type taxonomy, the YARRRML
mapping, the MemeAtlas ontology and the morph-kgc ini template. They used to sit in `data/`, which
is gitignored — which is how a 104-line reviewed taxonomy ended up retyped
by hand into a Python constant, and how that constant came to include an
edge the taxonomy had marked "sample before promoting". `kg/taxonomy.py`
now refuses a file that contradicts itself.

## Conventions worth not breaking

- Collection names come from `MONGODB_*` env vars set in `docker-compose.yml`
  — the dashboard reads the same names, so they stay single-sourced.
- `config/airflow.cfg` and `.env` are gitignored. Their `.example` twins are
  tracked. Anything docker-compose sets as `AIRFLOW__*` wins over the cfg
  file, so do not set it in both.
- `_PIP_ADDITIONAL_REQUIREMENTS` reinstalls on every container start. Real
  dependencies belong in `requirements.txt`.
