# `kym:<Category>` casing is a guess, not a verified match to IMKG

**Status:** closed 2026-09-30 — verified: MemeAtlas's `kym:<Category>` IRIs are byte-equal to IMKG's for all six categories. See "Resolution" below.

## What

`airflow/dags/modules/kg/rdf.py::category_class()` renders a frame's category
as `kym:<Category>` by capitalizing the parser's lowercase category string
(`meme` → `Meme`, `subculture` → `Subculture`). This follows the one example
given in the IMKG paper (`kym:Meme`), but IMKG's own mapping output / raw
category values are not published anywhere I could check byte-for-byte.

## Why it matters

If IMKG's real class IRIs use different casing, or a different word entirely
for some category (e.g. maybe IMKG uses `kym:Person` vs ours guessing from
`person`, or has no class for `culture`/`site` at all), then:
- A SPARQL query written against real IMKG data and pointed at MemeAtlas
  would silently return zero rows instead of an error — the worst kind of
  mismatch.
- The "an IMKG query runs unchanged against MemeAtlas" claim in
  `kg/rdf.py`'s docstring and `MODEL.md` would be false for category-based
  queries specifically, while being true for everything else.

## TODO

- [x] Get a sample of real IMKG-produced data. No need to ask: IMKG's own
      repository (github.com/riccardotommasini/imkg) publishes the scraper,
      the mappings and scraped sample data.
- [x] Compare against `category_class()` for `meme`, `person`, `event`,
      `site`, `culture`, `subculture` — identical (below).
- [x] Fix the mapping if they differ — they don't. `category_class()` is now
      an explicit table of the six verified names (`rdf.CATEGORY_CLASSES`)
      rather than `str.capitalize()`, so an unexpected category gets no class
      instead of a guessed one.
- [x] `rdf.py`'s docstring and `MODEL.md` (deliberate difference 3) state
      the verified mapping instead of the caveat.

## Resolution (2026-09-30)

How IMKG produces the class, from its repository:

1. **Where the value comes from.** Its Scrapy spider
   (`scraping/tasks/scraper/memes/spiders/KnowYourMeme.py`) reads
   `category = entry_body.xpath('aside/dl/a/text()').get()` — the text of
   the category badge at the top of the entry's sidebar, as the page shows
   it. The spider itself tests `if category == "Meme"`, so the value is
   capitalised.
2. **How it becomes a class.** `kym/mappings/kym.media.frames.yaml` maps
   it verbatim: `[a, kym:$(category)~iri]`, with
   `kym: "https://knowyourmeme.com/memes/"`.
3. **What the data holds.** The published sample
   (`scraping/tasks/scraper/memes/kym/1.json`) has `"category": "Meme"` (8)
   and `"Person"` (2).

What KYM shows, measured on every page MemeAtlas has stored (23,879 of the
23,882 entries; the badge is
`<a href="/categories/meme" class="entry-category-badge">Meme</a>`):

| Parser category | Badge text | Pages | MemeAtlas emits |
|---|---|---|---|
| meme | Meme | 18,391 | `kym:Meme` |
| event | Event | 2,121 | `kym:Event` |
| subculture | Subculture | 1,479 | `kym:Subculture` |
| person | Person | 1,271 | `kym:Person` |
| site | Site | 446 | `kym:Site` |
| culture | Culture | 171 | `kym:Culture` |

The parser's category (from the URL path) and the badge agree on 23,879 of
23,879 pages, and the badge text is exactly what `category_class()` emits.
An IMKG query on `?f a kym:Meme` (or any of the six) therefore returns the
same frames against MemeAtlas. `tests/test_kg_rdf.py` pins the six names.

One residual caveat, not worth more work: IMKG scraped its data years
before MemeAtlas did, and KYM could have used other badge words then. Its
sample has the two most common ones (Meme, Person) exactly as they are
today; the other four are not in the sample, so they are verified against
IMKG's code path, not against its data.
