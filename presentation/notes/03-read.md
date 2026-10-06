# Chapter 3 — Read: one page becomes one record (≈6 min)

DAG: `kym_parse`. Modules: `kym_parse.py` (pure), `kym_models.py` (schema and
CorpusPolicy), store `parse_store.py` (entries, parse_failures).

## The story
A person reading Doge's page sees a title, a picture, a box of facts, the
story underneath. The parser does the same and writes a tidy record — the same
fields for every page. Every later step works from that record; nobody reads
HTML again.

## Doge's record
title Doge · category meme, status confirmed · year 2010 · origin Tumblr ·
region Japan · types animal, character, exploitable, image macro, slang ·
series parent Interior Monologue Captioning · 22 tags · 25 sections (About,
Origin, Spread, Identity, Dogecoin, …) with 77 links and 36 images · 49
external references.

## Numbers
- 24,291 entries, all parsed by parser 1.7.0.
- Categories: memes 18,738 · events 2,149 · subcultures 1,493 · people 1,279 ·
  sites 455 · cultures 177. These become IMKG's classes (`kym:Meme`, …).
- 4,515 have every field (18.6%). Missing: region 18,604 (77%), Spread 5,751,
  Origin 5,609, entry type 4,563, About 2,205, year 770, tags 66.
- 709 dead letters: photos 553, videos 116, editorials 40 — not entries.

## Design decisions — the why
- **A pure parser.** HTML in, typed record out; no Mongo, no Airflow. That is
  what makes it testable on saved pages (`dags/tests/fixtures/doge.html`).
- **A versioned schema.** Each record carries `parser_version`,
  `schema_version`, `corpus_policy_version`. A new parser re-parses; an
  unchanged page with the same parser is not re-read.
- **Incomplete is a status, not a rejection.** The corpus policy: nothing is
  discarded for being incomplete. Missing fields are listed (`corpus_missing`);
  status is `ready` or `incomplete`. Tags were made "gated" rather than required
  (2026-07-16).
- **Dead letters.** Non-entries fail the schema and go to `parse_failures` with
  the reason, so `entries` stays schema-pure and nothing is lost.
- **Tried and dropped: clustering page layouts.** Early on, DOMs were clustered
  structurally to discover KYM's layouts. An explicit parser with fixtures was
  easier to trust and to fix; the clustering code was removed. Worth saying:
  removing an approach is engineering too.
- **Checked against IMKG.** The category class casing (`kym:Meme`) was verified
  against IMKG's mapping on every corpus page (gap 02).

## If someone asks
- *How do you know the parser is right?* Fixtures (saved pages) with expected
  records in the test suite; every parser change runs them. Parser gaps found in
  review are logged (gap 03: aliases, scraped_at, the template image — fixed in
  1.7.0).
- *What about KYM layout changes?* A changed layout shows up as parse failures
  or missing fields on the dashboard; the stored HTML lets the fixed parser
  re-read everything.

## Sources
`airflow/dags/modules/kym_parse.py`, `kym_models.py`, the Parse dashboard page,
gaps 02 and 03.
