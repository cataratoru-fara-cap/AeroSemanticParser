# Chapter 10 — What comes next (≈3 min, then questions)

## The story
7.1.0 is built and running: the vision model now reads each entry's own image,
and every linked Wikidata item brings its facts. Together they make IMKG's own
published questions (its paper's Table 4) answerable on MemeAtlas, with the
queries word for word, because the vocabulary was kept.

## 7.1.0 in numbers (2026-10-05)
- Frame images: 24,291 entries' own images, read by qwen3-vl:32b with the same
  checks as the templates (about two days of the lab GPU; running now, ends
  ~7 Oct).
- Wikidata statements: 753,854 truthy, item-valued statements of 32,494 linked
  items, 1,183 properties, from the same dump as the lexicon. Edges named by
  their property (P31…), `wdt:P31` in RDF; values become nodes (one hop).
- A private test build (not published) already had 1,030,361 nodes and 3.2M
  edges and answered three of the four questions; the fourth needs the image
  run.

## IMKG's four questions (Table 4, with the answers from IMKG's notebook)
| question | IMKG (2023) |
|---|---|
| memes that show SpongeBob | 130 |
| the most meme-able people | Trump (145), Kyle Craven (72), Kanye West (56) |
| memes based on films | 413 |
| sex or gender of people | male 10,333, female 2,798 (the paper prints 2,865) |
**Update this slide when 7.1.0 is published** — re-run `scripts/extract.py` and
`scripts/figures.py`, and fill the MemeAtlas column.

## Open questions (each logged in the gaps folder)
- **The mk: namespace** (gap 01) sits under meme4.science, IMKG's domain: it
  needs IMKG's authors' agreement before a public release.
- **Things Wikidata does not know** (gap 11): 22,614 recognised names in frame
  text, and a fifth of named people/characters in template images, have no item
  (Morty Smith, Springtrap…). Options range from a larger lexicon to minting
  MemeAtlas's own items.
- **One event, many pages:** two entries narrating the same happening make two
  events; cross-frame event identity is open (and the natural link to EventKG's
  own events).
- **Old addresses** (after gap 14, closed this week): a moved entry's frame
  is the address KYM gives today — Doge's is `/sensitive/memes/doge`, IMKG's was
  `/memes/doge`. Neo4j lists the old address (`also_at`); RDF does not
  (`owl:sameAs` would — a model decision).
- Also open: incoming Wikidata statements to a meme's own item (IMKG had them),
  the NSFW placeholder hub (gap 12).

## Closing line
"From a 2023 snapshot to a graph that renews itself every month." Then thank
Riccardo and the lab, and leave the final map on screen during questions.
