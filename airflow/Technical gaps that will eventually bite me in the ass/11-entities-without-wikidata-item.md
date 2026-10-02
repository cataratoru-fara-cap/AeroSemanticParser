# Entities with no Wikidata item never reach the graph, and the graph does not say what an item is

**Status:** open (2026-10-02). Found after the KG 6.5.0 publish, while
checking whether every entity in the graph is a Wikidata entity. It is,
and that is the gap: whatever has no Wikidata item is left out.

## What it is

Every entity in the published graph is a Wikidata item: 26,897 items,
reached by 225,552 edges (`mk:fromTitle` 18,258, `m4s:fromTags` 77,640,
`m4s:fromAbout` 85,077, `m4s:fromImage` 44,577). A name the pipeline
recognises but cannot link to an item stays in Mongo (`entities.nil`,
`template_entities`) and appears nowhere in the KG.

Measured on 6.5.0 (2026-10-02):

- **Frame text.** 22,614 recognised spans have no item:
  - by spaCy label: PERSON 9,508, ORG 7,927, WORK_OF_ART 2,870, GPE 676;
  - the frequent "people" are often meme names that spaCy read as people:
    "Hood Irony" 34, "Instagram Story" 20, "Jiafei" 11, "Dark Brandon" 10.
- **Template images.** The 115,683 regions on kept templates:

  | Region | In the graph | No Wikidata item | Linked, left out by design |
  |---|---|---|---|
  | Named person or character | 16,045 | **4,418** (~2,100 distinct names) | 2 |
  | Printed text | 3,286 | 23,938 | 51 |
  | Generic object | 27,377 | 23,415 | 17,151 (only the three largest per template go in) |

  Printed text and generic objects without an item are expected. The
  named regions are the loss that matters: about one named region in
  five (4,418 of 20,465) says who is in the picture and is dropped.

## Why the named ones miss

Spot-checked on ten frequent names, two causes:

1. **Not in our lexicon.** The lexicon (`modules/kg/wikidata.py`, dump
   wikidata-20260914, 17.3M items) keeps an item only if it has an English
   or multilingual Wikipedia article, a KYM ID, or subclass links. So
   Morty Smith, Drew Scanlon, Springtrap, Cereal Guy and Hoss Delgado have
   no entry, and the linker has nothing to match. Whether Wikidata holds
   items for them without an article was not checked: that needs the dump,
   not the API.
2. **In the lexicon, but the linker did not commit.** The same name is
   linked in some templates and not in others: Shrek in 38 and not in 18,
   the American flag in 69 and not in 61, Coca-Cola in 2 and not in 12.
   Some refusals are right: the lexicon's "Pomni" (Q7001204) is a different
   item.

## And in RDF, an item is only a label

A Wikidata IRI in the graph carries `rdfs:label` and nothing else: no
`rdf:type`, no P31, no description. Neo4j has the description too
(`WikidataEntity.description`). So "every person in the graph" or "every
fictional character" cannot be asked of the graph alone; it needs a
federated query to Wikidata's endpoint.

## Why it matters

- The template layer's most useful reading is who is in the picture, and
  a fifth of it is lost.
- Many of the missing names are meme characters (Cereal Guy, Eggdog,
  Cromulon) that Wikidata may never have, but KYM does: the right referent
  may be a frame in our own graph.
- Without types, the entity layer cannot be filtered or compared by kind,
  which the IMKG-style analyses will want.

## What fixing looks like

1. **Measure first.** Draw a sample of named template regions and frame
   PERSON/ORG spans with no item, and sort each: has a Wikidata item
   (checked in the dump), is a KYM frame, or is noise. That decides which
   of the fixes below is worth its cost.
2. **Link to our own frames.** Match unlinked names against frame titles
   and aliases (parser 1.7.0 reads them). A new edge to the frame (e.g.
   "depicts frame") would be a MINOR graph version.
3. **Widen the lexicon.** Also admit items with no Wikipedia article for
   chosen classes (fictional characters, video game characters, internet
   personalities). That means a lexicon rebuild from the dump (hours) and a
   re-link (minutes). The new `lexicon_version` re-runs the curation rules;
   the judge is asked only about new items.
4. **Linker misses on short names.** Measure the template linker's recall
   on a labelled sample, then tune its threshold and margin for
   one- or two-word names.
5. **Types in RDF.** Write each item's P31 classes (and its description)
   from the lexicon into the graph, offline. That is cheap, and a MINOR
   version.
