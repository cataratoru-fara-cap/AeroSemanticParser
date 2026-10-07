# Chapter 11 — 7.1.0 and next (≈3 min, then questions)

## The story
7.1.0 is live (published 7 October): the vision model now reads each entry's own
image, and every linked Wikidata item brings its facts. Together they make IMKG's
own published questions (its paper's Table 4) answerable on MemeAtlas, with the
queries word for word, because the vocabulary was kept.

## 7.1.0 in numbers (KG 7.1.0, build kg_20261007T101123Z_manual)
- Entries' own images: 23,438 of the 23,477 entries' pictures read by
  qwen3-vl:32b with the same checks as the templates (two days of the lab GPU;
  34 failed twice — the model repeating itself — and are logged). 105,954
  regions, 17,737 named. Linked like the text: 39,544 edges to 8,951 Wikidata
  items on 20,541 entries (87.5%). Blind audit: 159 of 175 names found again
  without the page's context. Doge's picture: a dog, and flowers.
- Wikidata statements: 696,194 truthy, item-valued statements of 28,300 linked
  items, 1,168 properties, from the same dump as the lexicon (14 Sep 2026). Edges
  named by their property (P31…), `wdt:P31` in RDF; their values are nodes
  (Wikidata items 26,897 → 272,304).
- The graph: 1,034,968 nodes, 3,185,248 edges, 9,672,008 triples (7.0.0:
  795,711 / 2,564,089 / 8,790,369).

## IMKG's four questions (Table 4; IMKG's answers from its own notebook)
| question | IMKG (2023) | MemeAtlas 7.1.0 |
|---|---|---|
| memes that show SpongeBob | 130 | 230 in their own picture; 229 through their templates; 353 either |
| the most meme-able people | Trump (145), Kyle Craven (72), Kanye West (56) | Trump (951), Biden (201), Musk (165); Kanye West 7th (143) |
| memes based on films | 413 | 808 entries, 450 films (Avengers: Endgame 41, Infinity War 33, Star Wars IV 31) |
| sex or gender of people | male 10,333, female 2,798 (the paper prints 2,865) | male 5,683, female 2,077 |

- *Why does MemeAtlas find more SpongeBob?* Twice the entries, and the model
  names the character actually in the picture. IMKG's examples: "Are You
  Feeling It Now Mr. Krabs" shows Mr. Krabs (SpongeBob is in its templates);
  "Bold and Brash" shows Squidward.
- *Where is Kyle Craven?* He is Bad Luck Brian. IMKG got him from Google
  Vision, which matches a photo to the web pages that name it; our model reads
  the photo as "man". He is linked 6 times, from text.
- *Why fewer people by gender?* MemeAtlas imports the facts of what memes link
  to, one step out; IMKG also imported every statement to and from each meme's
  own Wikidata item. The shape is the same: about three men to one woman (73%
  against IMKG's 79%).
- Queries in SPARQL and Cypher, with the full answers: `airflow/KG_QUERIES.md`.

## Open questions (each logged in the gaps folder)
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
