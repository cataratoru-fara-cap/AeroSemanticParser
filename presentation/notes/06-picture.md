# Chapter 6 — See the picture: templates, and what their images show (≈7 min)

DAGs: `kym_templates` → `kym_template_entities`. Modules: `imgflip_client.py`,
`imgflip_parse.py`, `template_search.py`, `kg/templates.py` (scoring, choice),
`kg/visual.py` (perceptual hashes), `kg/template_entities.py` (vision reading,
grounding, linking), `kg/template_review.py` (contact sheets); stores
`template_store.py`, `template_entity_store.py`.

## The story
Most memes are a blank picture people caption. imgflip, the meme generator,
calls them templates. For each entry we find its templates, merge the copies,
keep the best and most varied, and let a vision model say what each picture
shows — linked to Wikidata, like the text.

## Doge at this step
- imgflip searched for "doge": 120 candidates over 3 result pages; copies merged;
  8 kept: Doge (KYM's own "Meme Generator" link → certain, R = 1.0), Doge 2,
  Average Fan vs. Average Enjoyer, Buff Doge vs. Cheems, doge then and now,
  Dogecoin, Swole Doge vs Cheems, Multi Doge (scores 0.61–0.75).
- Vision: on Buff Doge vs. Cheems the model names Doge (Q15613810) and Cheems,
  linked to Balltze (Q110983191), the real dog behind Cheems. On Dogecoin, Elon
  Musk (Q317521) holding Doge.

## Numbers
- 18,144 entries searched; 8,001 got templates (44%); 8,660 got no result
  (most likely entries that are not picture memes — people, events, sites);
  1,483 only below-threshold candidates.
- 145,498 candidates seen → 10,261 near-copies merged → 26,868 templates kept;
  28,397 entry–template links (27,654 by search, 743 from KYM's own link).
  Per entry: 1 to 10 (3,012 entries have exactly one; 935 have ten).
- 26,818 images read, 50 failures (38 the model repeating itself, 11 cut off,
  1 bad image). 116,549 regions: object 40,164 · person 30,603 · text 27,426 ·
  character 9,657 · animal 5,026 · artwork 2,717 · logo 956. 20,660 named.
  48,218 links reach the graph.

## Design decisions — the why
- **Scored, not matched by hand.** IMKG joined 96 frames to 241 templates by hand
  and difflib ≥ 0.85. MemeAtlas scores every candidate: name (difflib and
  ordered word containment; word order enforced — "History Drunk" is not "Drunk
  History"), the picture against the entry's own images, search rank, imgflip's
  featured flag. KYM's own link is ground truth.
- **Measured both ways.** Precision ≈ 0.95 on 50 + 50 random frames' contact
  sheets — **read by Claude, not yet by a person: say it**. Recall with KYM's
  link *hidden*, on 150 linked frames: the linked template is in the top 10 for
  78.7%, first for 51.3% (misses: imgflip returned nothing for 15 titles; person
  frames with dozens of right templates; reordered names).
- **"The same picture" is a perceptual hash** (pHash, dHash on 250 px
  thumbnails): merges resizes, re-encodes, mirror images, letterboxes; keeps
  crops and edits apart (your rule). Known limit: a re-crop counts as its own
  template.
- **Most varied first.** MMR ranking, so ten templates are not ten copies of
  one scene.
- **Politeness is a constraint.** Only imgflip's public HTML pages, ~1 request
  per second, robots.txt respected; never the internal JSON search endpoint,
  which their terms rule out, though it would cut requests ~40×. The paid API
  would cost ~$130 per pass.
- **Vision with guard rails.** qwen3-vl:32b, grammar-constrained JSON (boxes on
  a 0–1000 grid, kind, name, printed text, ≤ 12 regions); every box and name
  checked before storage. Spike fixes before the pilot: the model filed its JSON
  under "thinking", unbounded lists, "named" set on generic things.
- **Blind audit.** The entry's title could put a name in the model's mouth, so a
  deterministic sample is re-read with no context: 204 of 240 named things (85%)
  confirmed blind; on the pilot, names only context gave were all right — so
  context names are kept (your decision, 2026-09-29).
- **Only what says something reaches the graph:** named regions, printed text
  that names something, and the three largest generic regions. "a man" is in
  6,800 templates — the noise this rule contains. Everything stays in Mongo.

## If someone asks
- *Why imgflip and not KYM's own images?* KYM's images are examples of a meme;
  imgflip's templates are its reusable blank. 7.1.0 now also reads each entry's
  own image (chapter 11).
- *Cost?* No paid API: imgflip public pages; the GPU is the lab's. Gap 10
  estimated the first full pass at ~32 h of search, ~15–30 h of details and days
  of vision, run in parallel.

## Sources
gap 10 (the imgflip template layer), `kg/templates.py`, `kg/visual.py`,
`kg/template_entities.py`, MODEL.md "Templates: imgflip, beyond IMKG".
