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

Measured on 404 random frames (against a lexicon built from the first 5 GB
of the dump, threshold 0.50): 969 links, 311 distinct items, and **two
thirds of the About links (431 of 641) are common-noun concepts**, not
named entities. The most frequent: *meme* (178), *animation*, *male*,
*text*, *illustration*, *phrase*, *vein* (from "in the same vein"),
*object*, *article*, *screenshot*. Some are useful (*selfie*, *fandom*,
*kaiju*, *political correctness*); most are noise.

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

## Two things to redo when this is picked up

* **Re-read the threshold on the full lexicon.** `MIN_LINK_SCORE = 0.50`
  is provisional: the calibration lexicon lacked many right senses
  (politics, cake, Twitter, Ohio — the dump is not in QID order), so wrong
  senses often won unopposed and precision was underestimated. Rough
  precision by band there: 0.45–0.50 ≈ 40%, 0.50–0.60 ≈ 70%, ≥ 0.60 ≈ 90%.
* **Check how often the frame's own item is found.** Frames join
  Wikidata on the KYM slug (P13484, "Know Your Meme slug"). Only 5 of the
  404 sampled frames found their item, but the calibration lexicon held
  just 282 slugs, so the real rate is unknown until the full lexicon is
  built. IMKG joined on P6760, KYM's *numeric* ID, which the parser does
  not extract. If slug coverage turns out thin and the page exposes the
  number, adding it to the parser would make the certain `kym_id` link
  more common (`Lexicon.by_kym_id` is already in place).
