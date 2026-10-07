# Chapter 9 — Keep it running: orchestration and quality (≈5 min)

Everything in `airflow/`: `docker-compose.yml` (15 services), `dags/` (12 DAGs),
`dashboard/` (Streamlit), `proxy/Caddyfile`.

## The story
Every month a robot repeats Doge's whole journey — but only for what is new or
changed. If anything stops half-way, running it again finishes the job without
redoing or losing work. That one property shaped every engineering decision.

## The monthly chain
kym_discovery (1st of the month) → kym_scrape → kym_parse → kym_entities →
kym_entity_curation → kym_events → kym_templates → kym_template_entities →
kym_frame_image_entities → kym_wikidata_statements → kym_kg. Each triggers the
next; a run started by hand never builds the graph unless asked (tested in
`tests/test_dag_chain.py`).

## The machine
- Airflow 3 (scheduler, Celery workers with Redis, Postgres for its history).
- MongoDB: every stage's output, one owner module per collection.
- Neo4j and Fuseki: the published graph. Files: builds, images.
- The lab's GPU servers (Ollama behind Open WebUI): ministral-3:14b for text,
  qwen3-vl:32b for images.
- One door: the campus firewall lets only port 8080 through, so Caddy serves the
  dashboard, Neo4j Browser, SPARQL and the admin tools behind it.

## Principles — the why
- **Layering:** pure library ← store module owning exactly one collection ← DAG
  (orchestration only). A DAG never issues a query; a library never touches a
  database; a schema change has one place to edit.
- **Stamps, not timestamps:** each output records the versions that produced it
  (parser, prompt, schema hash, model, lexicon, linker…). A stage selects what is
  missing or stale under the current stamps — changing the event prompt
  re-extracts events, it does not re-scrape pages.
- **Dead letters:** failures are stored with their reason and not retried until
  something relevant changes.
- **Models on record:** every output names the model that produced it; the
  curation judge never falls back to another model. (Event extraction still may,
  by design — recorded either way.)
- **Run summaries:** every run writes one; the dashboard reads them (progress,
  coverage, failures, timings).
- **Shared GPUs:** one call at a time on the lab's servers; backfills took days
  (35,358 stories, 26,868 template images, 23,438 entries' own images), so
  every step had to survive interruption.

## Quality
- 1,175 tests (~9k of the ~40k lines of Python): parsers on saved pages, model
  code on recorded answers, the DAG chain itself.
- Review loops for every model layer: sample, read, fix, re-read; contact sheets
  (templates), blind audits (images), a review page and a holdout (events),
  labelled samples against a bar set in advance (curation).
- 14 technical gaps logged in `airflow/Technical gaps …/`: what is weak, what it
  costs, the fix. Seven are closed (02–07, and 14 — one entry, one address,
  closed this week), seven open (01, 08–13).
- 96 commits since 24 June 2026.

## If someone asks
- *Could it run elsewhere?* It is Docker Compose on one machine; the GPU models
  are reached over HTTP, so another lab's Ollama/Open WebUI works by configuration.
- *What happens when KYM changes its layout?* Parse failures and missing fields
  rise on the dashboard; the stored HTML lets a fixed parser re-read everything.
- *How long does a monthly run take?* It depends on how many entries changed:
  the costly parts (vision, LLM) only touch new or changed entries. Check the
  run summaries (dashboard, Runs page) before quoting a number.

## Sources
`airflow/README.md` (The stages, Layout, Conventions), the gaps folder,
`tests/test_dag_chain.py`, the dashboard.
