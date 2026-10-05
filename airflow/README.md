# AeroSemanticParser — KYM pipeline

Scrapes and parses [Know Your Meme](https://knowyourmeme.com) into a
structured corpus, orchestrated by Airflow, with a Streamlit dashboard
over the results.

```
kym_discovery ─▶ kym_scrape ─▶ kym_parse ─▶ kym_entities ─▶ kym_entity_curation ─▶ kym_events ───┐
   urls           doms          entries      entities         entity_curation         events     │
                                parse_       (NLP + local     (rules + LLM judge:     (LLM,      │
                                failures     Wikidata         which links reach       JSONL)     │
                                             lexicon)         the graph, 6.5.0)                  │
   ┌─────────────────────────────────────────────────────────────────────────────────────────────┘
   └▶ kym_templates ─▶ kym_template_entities ─▶ kym_frame_image_entities ─▶ kym_wikidata_statements ─┐
      imgflip_pages     template_entities        frame_image_entities        wikidata_statements     │
      imgflip_templates (VLM, JSONL; 6.4.0)      (VLM, JSONL; 7.1.0)         wikidata_labels         │
      frame_templates                                                        (dump; 7.1.0)           │
   ┌─────────────────────────────────────────────────────────────────────────────────────────────────┘
   └▶ kym_kg                              each triggers the next
      kg_nodes / kg_edges / kg_builds
      data/kg/builds/<build_id>/
      graph.nt  rml_data/*.csv  kg_view_*.csv
        ▲
        └── kym_kg_validate  (weekly: re-derive the RDF via RML, diff it)

                     run_summaries  ◀── every stage records its run
                            │
                            ▼
                     dashboard (:8501)
```

Every stage selects only what is missing or stale (new or changed text, or
a new version of its code, prompt, schema or lexicon), so the monthly chain does
the month's new and changed frames and nothing else; kym_kg's gate skips the
build when nothing that affects the graph moved. Each trigger param
(`trigger_curation`, `trigger_events`, `trigger_templates`,
`trigger_template_entities`, `trigger_kg`) can cut the chain for a manual
or backfill run.

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
| **SPARQL** | **http://&lt;host&gt;:8080/sparql/kg/query** | the KG's RDF store (Fuseki), **read-only** through the proxy — update and admin paths are refused there, except the GETs Fuseki's own UI needs to list the dataset (so http://&lt;host&gt;:8080/sparql/ works as a query page); `?query=SELECT…` |
| **Neo4j Browser** | **http://&lt;host&gt;:8080/browser/** | the KG's property graph; connects to `bolt://<host>:8080` (Bolt over WebSocket, pre-filled), user `neo4j` / `NEO4J_PASSWORD`. **Not read-only** — Community has no read-only users. The HTTP Query API is at `http://<host>:8080/db/neo4j/query/v2`. Every build is kept as a generation, so filter on the published one: `MATCH (p:KGPointer {name:'current'}) MATCH (f:Frame {build_id: p.build_id}) …` |
| Flower | http://localhost:5555 | `--profile flower` |

The dashboard (8501), mongo-express (8081), pgAdmin (5050), Fuseki (3030)
and Neo4j (7474, 7687) also listen on `127.0.0.1` only, for SSH tunnels and
debugging — note the dashboard's prefix still applies there:
`http://localhost:8501/dashboard/`.

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

**`kym_entities`** (triggered by parse) — recognises the named entities
(and the common-noun concepts) in every frame's title, tags and About
section with spaCy, and links each to a **Wikidata** item, as IMKG did with
DBpedia Spotlight (`m4s:fromAbout`, `m4s:fromTags`, plus `mk:fromTitle`).
The linking runs against a local lexicon built from the **full Wikidata
dump**, not an API: `python -m modules.kg.wikidata build` streams the
~156 GB dump (a dated `wikidata-YYYYMMDD-all.json.gz`, checksum-verified —
never `latest-all`, which moves weekly) into a SQLite file (`WIKIDATA_LEXICON`,
default `data/wikidata/lexicon.sqlite`; download and build steps in
`dags/modules/kg/wikidata.py`). Local and deterministic: the corpus links
in minutes, and a frame is re-linked only when its text, the linker, the
lexicon or the spaCy model changes. Every link is grounded to the
characters it came from and keeps the features it was scored on, so
curation (below) needs no re-run. A curated sense list,
`dags/kg_config/entity_senses.yaml`, fixes the words the ranker gets wrong
("series" is never the maths series; "a series of" links nothing; "X" is
Twitter only where the text says so). With no lexicon the stage links
nothing and triggers the next one anyway. MODEL.md's "Entities" section
has what was taken from IMKG and what was changed.

**`kym_entity_curation`** (triggered by entities) — decides which of those links reach
the graph (6.5.0, gap 09): most of what the linker finds is incidental
("hair", "mug", "popularity"). Title links are always kept; About and tag
links go through local rules first (`kg/curation.py`,
`dags/kg_config/entity_curation.yaml`: platforms kept, body parts and
measures dropped, links the title or a tag agrees with kept, whole tags
that name something kept), then an **LLM judge** reads the entry and
gives each undecided item a role — subject, source, format, platform, or
incidental / wrong sense (ministral-3:14b; a link read only from the About
must be kept by it under two differently worded prompts). Meme formats
("image macro", "copypasta") are kept and generic About words ("image",
"TikToker") dropped by list, before the judge. Dropped links stay in
`entity_curation` with their reason; kept ones carry it into the graph
(`mk:relevanceBasis`). Until a frame is judged only its rule-kept links
reach the graph. Measured with `python -m modules.kg.entity_review draw`
/ `score` against the bar: of the kept, ≥ 0.85 relevant; of the dropped,
≤ 0.15.

**`kym_events`** (triggered by curation) — extracts spatio-temporal events
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
of the five bugs fixed on 2026-09-22 (see `kg/events.py`'s changelog). Run a backfill
with `trigger_templates=false`, and build the graph once at the end. The model is a
policy, not a name: never a reasoning model; `ministral-3:14b` on
ollama-ccdd by default (`kg/events.py`, "Model policy"). Four models were
measured on the same 99 sections before settling there — a bigger one is
not better at this, and `llama3.3:70b` is worse in the way that matters
(`kg/events.py`, "Why not a bigger model"). Extraction 3.2.0 (2026-09-25)
came out of reading the 127 review sections, re-extracted seven times, and a
60-entry holdout: 94 of the 127 now clean, the residual being the model's
judgement rather than this code — gap 08 has the numbers and the caveat
that the reader was Claude, not a person. Extraction 4.0.0 (2026-09-29)
puts EVERY sentence of Origin and Spread in an event — 3.2.0's full run had
left 15.3% of them out — and `audit()` refuses a record that does not. In the graph (6.3.0) each frame's
events are one chain, Origin then Spread, in the order the page tells them
(`mk:nextInStory`) — page order, not time order; time order is in the dates.

**`kym_templates`** (triggered by events) — finds the **imgflip meme templates** each
frame is made with. Eligible: every `meme`, plus any frame that links to
imgflip or has a KYM *Template* section (18,479). For each, imgflip's
public search is queried with the title (`/memesearch`, server-rendered, no
login; page 1 always, later pages while they stay relevant), directly at
~1 page/s with a research user agent — ScrapingAnt only if imgflip starts
refusing us (`modules/imgflip_client.py`; imgflip's internal JSON endpoint
is never used). Candidates are scored on name (IMKG's difflib ratio and
ordered word containment), on their picture against the frame's own KYM
images, on search rank and imgflip's featured flag; a frame's own KYM
"Meme Generator" link is ground truth (782 frames). **Near-identical
uploads are one template** — resizes, re-encodes, mirror images (pHash +
dHash on the thumbnails, borders trimmed, `kg/visual.py`); the others are
kept in `imgflip_templates` as its duplicates and never reach the graph.
Each frame keeps **0 templates** (with the reason) **or 1 to 10, most
varied first**. Selection is one global task over every searched frame, a
function of what is stored — a new threshold re-selects without a request.
Kept templates get their `/memetemplate` details and blank image. Measured
on 50 + 50 random frames (2026-09-28, read by Claude from contact sheets,
`kg/template_review.py`): ~0.95 of kept templates relevant, ~6.7 s and
~2 page requests per frame; `kg/templates.py` has the calibration and the
residual (a crop of the same photo still counts as its own template).

**`kym_template_entities`** (triggered by templates) — reads each kept template's image
with the lab's vision model (**qwen3-vl:32b**, one call at a time, like
`kym_events`): named people and characters, animals, objects, logos,
artworks and printed text, each with a box; every box and name is checked
before anything is stored (`kg/template_entities.py`,
`kg_config/template_entity_schema.json`). Each region is linked to Wikidata
through the same local lexicon as `kym_entities`, preferring what the
frame's own text already links. A deterministic 1% is also read with no
context, to measure how often the frame's title put a name in the model's
mouth (on the pilot the context-only names were all right, so they are
kept). In the graph: every named entity and printed text, plus the three
largest generic ones; everything stays in `template_entities`. A new
lexicon re-links without re-reading.

**`kym_frame_image_entities`** (triggered by template entities) — reads each
entry's own image (its `og:image`) the same way, with the same model, checks
and linker, told what the entry is (its title, category and the start of its
About) so it can name what it sees (`kg/frame_images.py`). The same regions
reach the graph, linked from the frame itself with IMKG's `m4s:fromImage`:
the edge the IMKG paper's SpongeBob query reads. ~6.6 s a frame, so a full
pass is two days of the lab GPU and a month's new frames under an hour.

**`kym_wikidata_statements`** (triggered by frame image entities) — reads every
truthy, item-valued statement of every Wikidata item a frame's text, a
template's image or a frame's image links to, from the dated dump the lexicon
was built from, plus the labels of the statement values the lexicon cannot
name (`kg/wikidata_statements.py`). `kym_kg` makes them edges named by their
property (`P31`; `wdt:P31` in RDF): what the paper's people, films and gender
queries read. Only items not yet read from that dump are read, so a monthly
run reads the dump only when new items were linked (the first full run took
about 3 h).

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
  kym_*_dag.py           orchestration only — no logic: discovery, scrape,
                         parse, entities, entity_curation, events, templates,
                         template_entities, frame_image_entities,
                         wikidata_statements, kg
  kym_kg_validate_dag.py the RDF diff gate, its own DAG
  modules/
    mongo_base.py        shared client/_id/UTC plumbing for the stores
    mongo_store.py       owns `urls`      ← kym_store.py is its facade
    dom_store.py         owns `doms`
    parse_store.py       owns `entries`, `parse_failures`
    event_store.py       owns `events`, `event_failures`
    review_store.py      owns `event_reviews`
    entity_store.py      owns `entities`
    entity_curation_store.py     owns `entity_curation`, `entity_curation_failures`
    template_store.py    owns `imgflip_pages`, `imgflip_templates`,
                         `frame_templates`, `template_assignments`
    template_entity_store.py     owns `template_entities`, `template_entity_failures`
    frame_image_store.py owns `frame_image_entities`, `frame_image_entity_failures`
    wikidata_statement_store.py  owns `wikidata_statements`, `wikidata_labels`
    kg_store.py          owns `kg_nodes`, `kg_edges`, `kg_builds` (generational)
    summary_store.py     owns `run_summaries`
    kym_discover.py      pure discovery library + CLI   (no Mongo, no Airflow)
    scrapingant_client.py pure fetch library + CLI       (no Mongo, no Airflow)
    openwebui_client.py  lab LLM/embedding client + CLI  (no Mongo, no Airflow)
    imgflip_client.py    polite imgflip HTTP client + CLI (no Mongo, no Airflow)
    imgflip_parse.py     imgflip pages → plain dicts      (no Mongo, no Airflow)
    template_search.py   glue: imgflip_client + template_store + kg/templates
    kym_parse.py         pure HTML → model + CLI         (no Mongo, no Airflow)
    kym_models.py        the entry schema and CorpusPolicy
    kg/                  KG libraries — no Airflow, and no Mongo except inside
                         a CLI's own import:
      build.py             one entry → nodes/edges — the only producer
      taxonomy.py          the curated entry-type taxonomy, validated
      origin.py            the infobox `origin` → canonical platform concepts
      tag_normalize.py     plural folding for tags
      census.py            frequency + co-occurrence over a corpus field
      cooccurs.py          statistical coOccursWith edges from a census
      siblings.py          sharesSameSeries edges between a series' frames
      serialize.py         one stream → graph.nt + RML CSVs + view CSVs
      rdf.py               canonical N-Triples serializer
      loaders.py           generational load/publish/prune: Neo4j and Fuseki
      ntdiff.py            memory-bounded set diff of two .nt files
      metrics.py           IMKG-comparable graph statistics (pure stdlib)
      semantics.py         LLM definition-embedding analysis of types (CLI)
      events.py            LLM event extraction from Origin/Spread (+ CLI)
      review.py            drawing and scoring the event review (CLI)
      wikidata.py          the Wikidata dump -> a local entity lexicon (+ CLI)
      wikidata_statements.py  the linked items' statements, from that dump
      entities.py          NER + linking of title/tags/About to Wikidata (+ CLI)
      curation.py          which linked entities matter to the meme
      entity_review.py     measuring entity curation (CLI)
      templates.py         which imgflip templates fit a frame
      visual.py            perceptual hashes for near-duplicate templates
      template_review.py   contact sheets for template selections (CLI)
      template_entities.py what a template's image shows, linked to Wikidata
      frame_images.py      what an entry's own image shows, linked to Wikidata
  kg_config/             curated KG inputs, tracked: the taxonomies and
                         curation rules, the model output schemas, the
                         YARRRML mapping, the MemeAtlas ontology
                         (memeatlas.ttl), MODEL.md (the IMKG crosswalk),
                         the morph-kgc ini template, the Neo4j Browser
                         stylesheet
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
It does see Fuseki's volume, mounted read-only for sizes alone: the page
shows how much disk the TDB2 database takes and how much the host has left,
because that store once grew unwatched to 34 GB (gap 04).

The derived layers have a page each, in pipeline order after Parse:
**Entities** (what the linker found; what curation keeps and why, the
judge's roles, which frequently linked items survive), **Events**
(coverage, re-extraction progress with its pace, and the newest events
read against their evidence) and **Templates** (which frames found
templates and why the others did not, the pool, the image reading with its
pace, the blind audit, the templates read last). The Overview has one
progress line per derived layer.

On the Events and Review pages, what an event extracted is marked in the
sentences it came from: the date words, the places and the people or
accounts, each with its own underline style and a written legend, matched
the way the event audit matches (`dashboard/lib/highlight.py`). On Review
the marks appear only after the recall question is answered.

Its colours are a validated palette (see `dashboard/lib/theme.py`), not a
taste call: categorical hues in fixed order and never cycled, magnitude
bars in one colour (one series is one colour; a darker-where-bigger ramp on
unordered categories would repeat the bar length in hue), reserved status
colours that always ship with an icon and a written label, and a data table
behind every chart. `dashboard/tests/` covers the highlighter; every page
can be run headless with Streamlit's `AppTest` inside the dashboard image.

## The KG stage

Querying the live graph (Neo4j and SPARQL), and what the 6.5.0 graph
measures against IMKG: [`KG_QUERIES.md`](KG_QUERIES.md).

**MemeAtlas extends IMKG.** Where IMKG models something, its terms are
used as is (`m4s:MediaFrame`, `m4s:title`, `m4s:tag`, `kymt:` entry-type
classes, `skos:broader` for series and, since 7.0.0, `rdfs:seeAlso` between
the frames of one series), so IMKG queries run here. The rest of
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
EventKG and what deliberately was not. The other derived layer (6.1.0) is
the Wikidata links from `kym_entities`: the items are Wikidata's own
resources (`http://www.wikidata.org/entity/Q…`, labelled, never typed),
and the frame's `m4s:fromAbout` / `m4s:fromTags` / `mk:fromTitle` edges to
them are a linker's reading — each mention annotated with how sure it was
(`mk:linkScore`) and how it was found (`mk:linkMethod`). The template
layer (6.4.0) adds `mk:MemeTemplate` nodes (`mk:template/<imgflip id>`,
with IMKG's `imgflip:templateId`), linked from each frame that selected
them by `mk:hasTemplate` and back by IMKG's `m4s:templateOf`, annotated
with the fit (`mk:templateScore`); what a template's image shows is linked
with IMKG's `m4s:fromImage`, each region annotated with its box
(`mk:boundingBox`, a Media Fragments literal) and the model that read it.

**`kym_kg`** (triggered by the statements stage) — lifts `entries`, the events
extracted from them, their Wikidata links, their templates, what the
templates' and their own images show, and the linked items' Wikidata
statements, into a knowledge graph and publishes it in every representation
at once:

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

After a publish, `prune` keeps `keep_builds` generations (default 2) in
every store, and `compact_fuseki` then compacts Fuseki's TDB2 database.
TDB2 never gives back the disk of triples it deletes or replaces: before
this step existed it had grown to 36 GB for 12M live triples, and the first
compaction took it to 3.8 GB with every triple kept (gap 04).

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
