# Chapter 5 — Tell the story: Origin and Spread become events (≈6 min)

DAG: `kym_events`. Modules: `kg/events.py` (prompt, schema, audit),
`kg/review.py` (review sampling), stores `event_store.py`, `review_store.py`.
Schema: `kg_config/event_extraction_schema.json`.

## The story
KYM tells each meme's history in two sections, Origin and Spread. A language
model reads each section and turns its sentences into events — what happened,
when, where, who — each keeping the sentence it came from.

## Doge's story (16 events, in page order)
2005-06-24 Homestar Runner says "d-o-g-e" · 2010-10-28 a Reddit /r/Ads post ·
Apr 2012 a Tumblr audio "adventure game" · (no date) the blog Your Daily Doge ·
2012-05-07 a YouTube fake Pokémon battle · (no date) 4chan /v/ doge threads ·
May 2012 Tumblr "Polite Doge" · Aug 2012 the blog F--k Yeah Doge · 2012 Shiba
Confessions, "shibes" · Dec 2012 /r/DogsIWannaHug · Dec 2012 Cheezburger
"Schnauze" · 2013-01-08 /r/Doge created · May 2013 /r/dailydoge · Jul 2013 the
blog shibe-doge · 2013-07-29 a 4chan /s4s/ thread with 600 replies · 2013-11-20
YouTube's Comic Sans easter egg.

The one-sentence example: "On May 7th, YouTuber KwandaoRen66 uploaded a video
…" → 2012-05-07 (day; the year from the sentence before), YouTube (platform),
KwandaoRen66 (actor), confirmed. (The 2010 /r/Ads title contains a swear word —
avoided on the slide.)

## Numbers
- 36,673 sections (Origin 18,458, Spread 18,215) → 142,787 events; 18,773
  entries have events (77.3% of entries; 18,827 have a story at all), 7.6 each.
- Dates: 64% to the day, 6% to the month, 5% to the year, 26% undated. Places:
  71% a platform. Certainty: confirmed 140,998, unconfirmed 1,738, disputed 50,
  debunked 1.
- Model: ministral-3:14b on the lab's Ollama (ollama-ccdd); 14 failures.

## Design decisions — the why
- **Why a language model.** The story is free prose; no rule-based parser gets
  "the following week" or "In response, …" right.
- **Constrained output.** One call per section, JSON constrained by a schema,
  validated before it is written. Versioned: prompt, schema hash, extraction
  version, model — a change re-extracts only what it affects.
- **Every sentence belongs to an event (extraction 4.0.0).** The full 3.2.0 run
  showed 15.3% of all sentences in no event (43.5% of sections), mostly
  reception and background the old prompt told the model to drop. Prompt 9: each
  sentence starts an event or continues the previous one; an audit refuses any
  record with a sentence left out. Coverage 84.9% → 100% on the 236 review
  sections, 83.6% → 100% on a 116-section holdout never used for tuning.
  Dated sentences ending up in a dated event: ~95–97%, unchanged.
- **Dates keep their precision and their words.** "Sometime in April 2012" is a
  month, not 1 April; `date_text` and `date_basis` record where the date came
  from.
- **Page order, not time order.** Events are chained with `mk:nextInStory` in
  the order the page tells them. KYM tells flashbacks on purpose: 30 of 753
  dated neighbours step back in time. Time order is in the dates.
- **Drawn from EventKG, not copied.** SEM's what/when/where/who skeleton and
  EventKG's rule that every statement stays traceable to its source; aligned in
  `memeatlas.ttl`, but no per-statement named graphs and no cross-source event
  registry. Events are the first IRIs MemeAtlas mints from a model's reading
  (`mk:event/<id>`) — a major version, 6.0.0.

## If someone asks
- *How good is it?* Coverage is measured (above). Correctness of individual
  events is checked by the review loop (the dashboard's Review page and an
  automated reading), not a full human gold standard — gap 08 stays open and
  says so.
- *Why not one event per page?* Events are shared across pages in principle
  ("Elon Musk tweets about Dogecoin" appears in several entries); cross-frame
  event identity is an open question (chapter 11).
- *Why does the chart fall after 2020?* Documentation lag: KYM writes a meme up
  once it has lasted.

## Sources
`kg/events.py`, gap 08 (the coverage update of 2026-09-29), MODEL.md "Events:
drawn from EventKG, not copied from it".
