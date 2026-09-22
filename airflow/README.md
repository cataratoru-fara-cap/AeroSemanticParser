# AeroSemanticParser — KYM pipeline

Scrapes and parses [Know Your Meme](https://knowyourmeme.com) into a
structured corpus, orchestrated by Airflow, with a Streamlit dashboard
over the results.

```
kym_discovery ─▶ kym_scrape ─▶ kym_parse ─▶ kym_events ─▶ kym_kg   (each triggers the next)
   urls           doms          entries      events         kg_nodes / kg_edges / kg_builds
                                parse_       event_         data/kg/builds/<build_id>/
                                failures     failures          graph.nt  rml_data/*.csv
                                             data/kg/events/   kg_view_*.csv  manifest.json
                                             (LLM, JSONL)            ▲
                                              kym_kg_validate ───────┘  (weekly: re-derive
                                                                        the RDF via RML, diff it)

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

## The stages

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

**`kym_events`** (triggered by parse) — extracts spatio-temporal events
from every Origin and Spread section (36,011 of them), one LLM call per
section through `modules/openwebui_client.py`, validated against
`dags/kg_config/event_extraction_schema.json`. **Extractive only**: the
model sees numbered sentences and answers with sentence numbers — the
evidence text is copied from the page by the pipeline, there is no
model-written summary, and the model does not even date anything: it
returns the date WORDS and the pipeline parses them, taking a missing year
from an earlier sentence and resolving "that same day" against the nearest
earlier dated event (`mk:dateBasis`, `mk:dateAnchoredTo`). Every value the
model does return (date words, place, actors) is **grounded**: resolved to
the span of the section it names and stored in the section's own words, so
a value worded differently is corrected rather than lost and a value with
nothing behind it is not stored at all. An independent `audit()` refuses to
store a record that fails. Links, `[n]` citations, photos and embedded posts are attached
by position (parser 1.6.1), never by the model. Nothing is truncated or
capped. Each result lands twice as it arrives — a line in
`data/kg/events/<extract_id>/chunk-*.jsonl` and a doc in `events` (the
authority) — and a section is re-asked only when its text or media, the
prompt, the schema or the extraction contract changes. **One call at a
time**: the lab hosts serve requests FIFO with no load balancer, so
parallel calls only queue. Measured on a **1,528-section random sample**
(2026-09-22, extraction 2.6.0): 5,579 events, 73% of them dated, **zero
dead letters and zero audit violations**, 2.1 s p50 per section, so the
full 36,011 take ~21 hours of host time — more if other lab users are
queued ahead. Of the sentences that state a date, 94% end up carried by a
dated event; the rest are mostly dates inside reported content ("According
to the post, Aquaman was born on July 12th, 2025"), which must NOT become
event dates. Beyond `audit()`, the sample was swept for every invariant a
record should hold internally — precision against date format, relative
chains pointing backwards in time, citations present in the text they are
attached to, values that name nobody — and that sweep is what found four
of the five bugs fixed on 2026-09-22 (see `kg/events.py`'s changelog). Run the backfill
with `trigger_kg=false`, and build the graph once at the end. The model is a
policy, not a name: never a reasoning model; `ministral-3:14b` on
ollama-ccdd by default (`kg/events.py`, "Model policy"). Four models were
measured on the same 99 sections before settling there — a bigger one is
not better at this, and `llama3.3:70b` is worse in the way that matters
(`kg/events.py`, "Why not a bigger model").

**Reviewing the event layer.** The events are a model's reading, so the
dashboard has one page that WRITES: `:8080/dashboard/` → *Review*. Draw the
sample with `python -m modules.kg.review draw` and read the number back
with `... review report`; `dags/modules/kg/review.py` explains the three
strata and why they are scored separately, and `dashboard/lib/review.py`
explains why that page is allowed to write when nothing else in the
dashboard is.

### Streaming is load-bearing

A KYM page is multi-MB decompressed and roughly 10× that inside
BeautifulSoup. `parse_store.iter_html` streams one page at a time off the
Mongo cursor, and `dom_store.content_shas` projects *only* the hash.
Materialising either into a list OOM-killed the first run. The comments
saying so are warnings, not trivia.

## Layout

```
dags/
  kym_{discovery,scrape,parse,events,kg}_dag.py   orchestration only — no logic
  kym_kg_validate_dag.py                   the RDF diff gate, its own DAG
  modules/
    mongo_base.py        shared client/_id/UTC plumbing for the stores
    mongo_store.py       owns `urls`      ← kym_store.py is its facade
    dom_store.py         owns `doms`
    parse_store.py       owns `entries`, `parse_failures`
    event_store.py       owns `events`, `event_failures`
    kg_store.py          owns `kg_nodes`, `kg_edges`, `kg_builds` (generational)
    summary_store.py     owns `run_summaries`
    kym_discover.py      pure discovery library + CLI   (no Mongo, no Airflow)
    scrapingant_client.py pure fetch library + CLI       (no Mongo, no Airflow)
    openwebui_client.py  lab LLM/embedding client + CLI  (no Mongo, no Airflow)
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
      semantics.py         LLM definition-embedding analysis of types (CLI)
      events.py            LLM event extraction from Origin/Spread (+ CLI)
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
`dags/kg_config/MODEL.md` has the full crosswalk. As of 5.1.0 every field
the parser extracts reaches the graph: the Origin and Spread sections,
deferred until then, are `m4s:origin` and `m4s:spread`. What those
narratives say *happened* is a separate layer (6.0.0): `kym_events`
extracts dated, placed events with named actors, and each becomes an
`mk:Event` node linked from its frame by `mk:hasEvent` — the one kind of
node in the graph that is a model's reading rather than parsed fact, and
marked as such (`mk:extractionModel`, `mk:sourceText`). MODEL.md's
"Events: drawn from EventKG, not copied from it" has what was taken from
EventKG and what deliberately was not.

**`kym_kg`** (triggered by events) — lifts `entries`, and the events
extracted from them, into a knowledge graph
and publishes it in every representation at once:

- **Neo4j** — the property graph: typed nodes (`Frame`, `TagConcept`, …)
  and relationships whose types are the edge vocabulary verbatim
  (`hasTag`, `partOfSeries`, `citesExternal`, `subTypeOf`, …), carrying every
  parsed property. A node is only something other things can share (a frame,
  a type, a URL, an image). Section text sits on the frame, and what belongs
  to one link, citation or image showing sits on the edge;
- **Fuseki** — the RDF graph, queryable over SPARQL at `/sparql/`, with
  the vocabulary in the named graph `urn:memeatlas:ontology`. Edge details
  such as anchor and citation text are RDF-star annotations
  (`<< ?f mk:citesExternal ?u >> mk:citationText ?t`);
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

### Model calls go through one client

Every LLM or embedding call uses `dags/modules/openwebui_client.py`,
never raw HTTP. The lab's Open WebUI hosts each need their own API key and
serve different models, and they go down independently. The client
handles all of that:

- **Hosts:** tried in priority order, ollama-ccdd first, then ollama-ui.
- **Model choice:** discovered at runtime from each host's model list.
  The requested model is tried on every host before any other model.
  Only then does it fall back to a model of the same size tier
  (sm/md/lg/xl) and the same specialization (general/vision/coding).
  A caller can instead ask for a specialization directly.
- **Pinning:** once a purpose has succeeded, it stays on that model's
  digest for the whole run. It may change host, but never model.

`python -m modules.openwebui_client probe --model <name>` shows both
inventories and the order models would be tried in. The client's
docstring has the full rules and the env vars.

`kg/semantics.py` (describe → embed → analyze) records the model name,
digest and embedding size in every file it writes. A resumed run stays on
the model that produced the existing output, or stops.

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
