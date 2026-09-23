# The entity layer links everything it recognises, relevant or not

**Status:** open (2026-09-22) — this is the planned NEXT TASK on the entity
layer, not a defect found in passing.

## What it is

KG 6.1.0 recognises entities in each frame's title, tags and About section
with NLP (spaCy NER, noun chunks, proper-noun runs) and links them to
Wikidata through a local lexicon built from the full dump
(`modules/kg/wikidata.py`, `modules/kg/entities.py`, `kym_entities`). It is
recall-oriented on purpose, as DBpedia Spotlight was for IMKG: it links a
common noun as readily as a name. So a frame about a reaction image of a
man in a t-shirt holding a mug in a field gets `m4s:fromAbout` edges to
**t-shirt**, **mug**, **field** and **hair** — real Wikidata items, really
on the page, and almost never what the meme is ABOUT. They are incidental
detail of the picture being described, not part of the media frame's
semantic representation.

Measured on the whole corpus (2026-09-23: full lexicon from dump
20260914, linker 1.1.0, threshold 0.45): **282,914 links over 23,882
frames** (11.8 per frame; 52 frames have none) to **30,449 distinct
items**, 17,365 of them linked from a single frame. By field: About
174,267, tags 91,111, titles 17,536. **59% of the About links (102,852 of
174,267) come from spans with no proper noun** — common-noun concepts,
not named entities. The items linked from the most frames: *series*
(4,849), Twitter, TikTok, *image macro*, YouTube, *catchphrase*, *song*,
X, *parody*, *viral video*, *game*, Reddit, *Americans*, *photograph*,
Instagram, *film*, *popularity*, 4chan, *man*. Platforms are arguably
relevant; *popularity*, *man*, *photograph* are the noise this gap is
about.

Worse than irrelevant, some hubs are WRONG: *series* is Q170198, the
mathematical series ("infinite sum"), 5,531 mentions, from KYM's stock
phrase "X is a series of …". The right sense (series of creative works,
Q7725310) is not named "series" in Wikidata, so the maths item wins
unopposed; and in "a series of videos" the word is a quantifier that
names nothing. Likewise *game* (1,763 About mentions) is Q11410, games in
general, where KYM almost always means a video game. (First measured on
404 frames against a 5 GB partial lexicon at 0.50: 969 links, two thirds
of the About links common nouns — *meme*, *animation*, *male*, *vein*
from "in the same vein".)

## Why it matters

A consumer querying "which memes involve dogs" wants Doge, not every page
whose About mentions a dog in passing. Hubs like *meme* and *male* connect
thousands of unrelated frames, which distorts any graph measure (degree,
paths, communities) computed over the entity layer, and would dominate
IMKG-style Wikidata expansion if that is added later (gap: 1-hop
statements, MODEL.md "Not yet modelled").

## What is already in place to fix it cheaply

The linker was built so that curation never has to re-run NLP. Every link
in the `entities` collection keeps:

* `features` — `prior` (Wikipedia popularity), `context` (overlap between
  the item's description and the frame's text), `exact`, `type` (NER
  agreement), `kym` (the item is itself on KYM), `clarity` (lead over the
  runner-up);
* `method` (`kym_id` / `title` / `tag` / `ner` / `propn` / `noun_chunk`),
  `proper` (the span had a proper noun), `ner_label`, `candidates`,
  `margin`;
* the exact span (`field`, `start`, `end`), so position is available too;
* `nil` — named spans nothing was found for (lexicon coverage).

The graph carries `mk:linkScore`, `mk:linkMethod` and `mk:nerLabel` on
each mention, so even a SPARQL consumer can filter today.

## What fixing it looks like (to be decided)

Candidate methods, roughly cheapest first — probably combined:

1. **Rules over what is stored.** Keep named entities (`ner`/`propn`,
   `proper`), tags, titles and `kym_id` links; keep a concept only when it
   is also a tag, or recurs, or its `context` feature is high. Cheap and
   explainable; misses concepts that matter and are said once.
2. **Class-based filtering.** Walk the item's P31/P279 (already in the
   lexicon) and keep or drop by class: creative works, people, fictional
   characters, organisations, platforms, events, memes — versus body
   parts, garments, household objects, colours. A curated allow/deny list
   of classes in `kg_config/`, like the origin and tag lists.
3. **Salience against the corpus.** tf-idf-style: an item mentioned by
   thousands of frames (*meme*, *male*, *text*) says little about any one
   of them. Needs only the `entities` collection.
4. **Relevance against the frame.** Score each linked item's description
   (or its Wikidata neighbourhood) against the frame's own About/Origin
   with sentence embeddings — `modules/openwebui_client.py` already serves
   embedding models — and keep the top-k.
5. **An LLM judge** over (frame, candidate items), extractive like the
   event layer (it may only choose among the items, never add one). Most
   accurate, most expensive; the event layer's review tooling shows how to
   measure it.

Whatever is chosen must be measured the way the event layer is: a drawn
sample, a human verdict per link, a Wilson interval (see gap 08 and
`modules/kg/review.py`).

## The two re-checks, done (2026-09-23, full lexicon)

* **Threshold re-read → 0.45** (`MIN_LINK_SCORE`, linker 1.1.0). 403
  random frames re-linked, 25 hand-judged links per band, judged on
  whether the item is the right referent — not whether it matters:
  0.40–0.45 12/25, 0.45–0.50 18/25, 0.50–0.55 18/25, 0.55–0.60 22/25,
  0.60–0.70 23/25, 0.70+ 23/25. Estimated ~82% of links right at 0.45
  (~86% at 0.50, with ~30% fewer links). One judge, n = 25 per band, so
  the intervals are wide (18/25 is 52–86%). On the partial lexicon
  0.45–0.50 had been ~40%: the missing senses were the problem.
* **Own item found for 2,882 of 23,882 frames (12.1%)** through the KYM
  slug (P13484). Wikidata has 3,744 items with a slug and 290 with KYM's
  numeric ID (P6760, IMKG's join), only 251 of those without a slug — so
  parsing the numeric ID would add at most ~250 frames.

## What the wrong links are (from the same 150)

Patterns that cross every score band, so no threshold removes them — they
belong to a linker fix or to this curation step:

* **a common word's other sense** — "speech" (a public address) → vocal
  communication; "game" → games in general; a TikTok "sound" → acoustic
  wave; "in conjunction with" → the part of speech; "series" (above);
* **a nationality read as its language** — "Danish MP", "English
  musician" → the languages (NER says NORP; the type feature does not
  steer away from LANGUAGE items);
* **a fragment of a longer name** — "Warcraft" out of *World of Warcraft*,
  "Zoo" out of *Higashiyama Zoo*, "Kyojin" out of *Shingeki no Kyojin*;
* **a capitalised ordinary word → a work** — "People also use…" →
  *People* magazine, "Portrait" → a band, "powers" → a TV series;
* two one-offs: the context feature sends the tag "meme" to a footballer
  nicknamed Meme on football pages; the own-item rule (any alias of the
  frame's own item scores 1.0) sends the tag "terminator" to Arnold
  Schwarzenegger — otherwise that rule is ~98% right (Tardar Sauce →
  Grumpy Cat, Groom Lake → Area 51).
