# The MemeAtlas data model — an extension of IMKG

MemeAtlas extends **IMKG**, the Internet Meme Knowledge Graph (Tommasini,
Ilievski & Wijesiriwardene, ESWC 2023,
[github.com/riccardotommasini/imkg](https://github.com/riccardotommasini/imkg)).

- Where IMKG already models something, MemeAtlas uses IMKG's term **as is**,
  so a SPARQL query written for IMKG's Know Your Meme layer runs on MemeAtlas
  unchanged.
- Everything IMKG does not model uses the `mk:` namespace. Every `mk:` term is
  declared in [`memeatlas.ttl`](memeatlas.ttl), and aligned to IMKG, SKOS,
  schema.org or PROV where a match exists.

What MemeAtlas reused from IMKG is its **vocabulary**: namespaces, class and
predicate names, and conventions such as frame IRIs being the live KYM URL
and series being expressed with `skos:broader`. It did not reuse IMKG's
mapping files or data:

- The graph is built from MemeAtlas's own scrape and parser.
- The graph is serialised in Python (`modules/kg/rdf.py`).
- A separately hand-written YARRRML mapping
  ([`kg_mapping.yarrrml.yml`](kg_mapping.yarrrml.yml)) rebuilds the same graph
  with yatter + morph-kgc. `kym_kg_validate` compares the two graphs triple by
  triple.

## Namespaces

| Prefix | IRI | Owner |
|---|---|---|
| `m4s:` | `https://meme4.science/` | IMKG |
| `kym:` | `https://knowyourmeme.com/memes/` | IMKG: category classes |
| `kymt:` | `https://knowyourmeme.com/types/` | IMKG: entry-type classes |
| `mk:` | `https://meme4.science/atlas/` | MemeAtlas |
| `skos:`, `rdfs:`, `rdf:`, `xsd:` | W3C | |

The `mk:` IRI is a sub-path of IMKG's own domain. **IMKG's authors must agree
to it before publication.** If they don't, changing it takes one constant in
`rdf.py` plus the prefix lines in the mapping and ontology files.

## Crosswalk

Each row maps a node or edge name in `modules/kg/build.py` (the name used in
Neo4j and Mongo) to its RDF term.

### Terms reused from IMKG

| Property-graph element | RDF | Parsed field |
|---|---|---|
| node `frame` | `a m4s:MediaFrame` and `a kym:<Category>` | `url`, `category` |
| `frame.label` | `m4s:title` (and `rdfs:label`) | `title` |
| `frame.status` | `m4s:status` | `status` |
| `frame.year` | `m4s:year` (`xsd:integer`) | `year` |
| `frame.from` | `m4s:from` | `origin` (the infobox platform) |
| `frame.about` | `m4s:about` | the About section's text |
| `frame.origin_text` (5.1.0) | `m4s:origin` | the Origin section's text |
| `frame.spread_text` (5.1.0) | `m4s:spread` | the Spread section's text |
| `frame.added` | `m4s:added` (`xsd:dateTime`) | `kym_added` |
| `frame.last_updated` | `m4s:last_update_source` (`xsd:dateTime`) | `kym_last_updated` |
| edge `hasEntryType` | `rdf:type kymt:<slug>` | `entry_type` |
| edge `hasTag` | `m4s:tag "<tag>"` | `tags` |
| edge `partOfSeries` | `skos:broader` plus the inverse `skos:narrower` | `series_parent` |
| edge `fromAbout` (6.1.0) | `m4s:fromAbout` → a Wikidata item | entities recognised in the About section (see [Entities](#entities-linked-to-wikidata-as-imkg-did-61)) |
| edge `fromTags` (6.1.0) | `m4s:fromTags` → a Wikidata item | `tags`, each looked up whole |
| edge `hasTemplate`, inverse (6.4.0) | `m4s:templateOf` (template → frame; IMKG's mapping) | `frame_templates.selected` (see [Templates](#templates-imgflip-beyond-imkg-640)) |
| `template.template_id` (6.4.0) | `imgflip:templateId` (a plain literal, as IMKG writes it) | imgflip's id |
| edge `fromImage` (6.4.0) | `m4s:fromImage` → a Wikidata item | what a template's image shows (`template_entities`) |

### MemeAtlas extensions

| Property-graph element | RDF | Aligned to | Parsed field |
|---|---|---|---|
| node `entry_type_concept` | `kymt:<slug> a rdfs:Class, skos:Concept`; `skos:inScheme mk:EntryTypeScheme`; `skos:prefLabel` | | the entry-type vocabulary |
| edge `subTypeOf` | `rdfs:subClassOf` between `kymt:` classes | | `entry_type_taxonomy.yaml` |
| edge `hasRegion` | `mk:region "<name>"` | | `region` |
| edge `relatesToMeme` | `mk:relatesToMeme` | ⊑ `rdfs:seeAlso` | every KYM link on the page or in its references |
| edge `citesExternal` | `mk:citesExternal` | ⊑ `rdfs:seeAlso` | every non-KYM link on the page or in its references |
| `frame.description` | `mk:description` | ⊑ `schema:description` | `meta.description` |
| `frame.aliases` | `skos:altLabel` | | `aliases` |
| `frame.section_texts` | `mk:sectionText "<heading>\n\n<paragraphs>"` | ⊑ `schema:articleBody` | `sections[]` with text, except the three narrative kinds, each of which has an IMKG term of its own |
| `frame.corpus_status`, `corpus_missing` | `mk:corpusStatus`, `mk:corpusMissing` | | corpus grading |
| `frame.parser_version`, `parsed_at`, `scraped_at` | `mk:parserVersion`, `mk:parsedAt`, `mk:scrapedAt` | ⊑ PROV | provenance |
| node `image` | `<file url> a mk:Image`; `mk:width`, `mk:height` | `mk:Image` ⊑ `schema:ImageObject` | `og_image`, `template_image_url`, `sections[].images[]`, `og:image:width/height` |
| edge `hasImage` | `mk:hasImage` | ⊑ `schema:image` | |
| node `origin_concept` (5.0.0) | `mk:origin/<slug> a skos:Concept`; `skos:inScheme mk:OriginScheme`; `skos:prefLabel` | | `origin`, canonicalized (see below) |
| edge `hasOrigin` (5.0.0) | `mk:hasOrigin` | | curated aliases + a fallback slug, `kg_config/origin_taxonomy.yaml` |
| edge `subTypeOf` (origin) (5.0.0) | `rdfs:subClassOf` between `mk:origin/` concepts | | `origin_taxonomy.yaml`, platform-like slugs only |
| node `badge_concept` (5.0.0) | `mk:badge/<slug> a skos:Concept`; `skos:inScheme mk:BadgeScheme`; `skos:prefLabel` | | the badge vocabulary |
| edge `hasBadge` (5.0.0) | `mk:badge` (an `owl:ObjectProperty` as of 5.0.0 — was a literal in 4.0.0) | | `badges` |
| node `event` (6.0.0) | `mk:event/<id> a mk:Event` | `mk:Event` ⊑ `sem:Event`, ⊑ `schema:Event` | `events` collection (see [Events](#events-drawn-from-eventkg-not-copied-from-it)) |
| edge `hasEvent` (6.0.0) | `mk:hasEvent` | | |
| `event.source_text` | `mk:sourceText` | | the verbatim sentences it was read from, copied from the page by the pipeline (the model only points at sentence numbers). Since extraction 4.0.0 these are ALL of the event's sentences — the happening and what describes, counts or comments on it — and a section's events together cover every one of its sentences |
| `event.source_section` | `mk:sourceSection` (`origin` / `spread`) | | |
| `event.date_start`, `date_end` | `mk:eventStart`, `mk:eventEnd` (`xsd:dateTime`) | ⊑ `sem:hasBeginTimeStamp`, `sem:hasEndTimeStamp` | `date` at `date_precision`, as an interval |
| `event.date_precision`, `date_text` | `mk:datePrecision`, `mk:dateText` | | |
| `event.date_basis` (6.0.0) | `mk:dateBasis` (`stated` / `relative`) | | how the date was arrived at — the model never dates anything, it returns the words and the pipeline parses them |
| edge `eventDateAnchor` (6.0.0) | `mk:dateAnchoredTo` | | for a relative date ("that same day"), the earlier event it was counted from |
| edge `nextInStory` (6.3.0) | `mk:nextInStory` | not aligned — page order, not time order | from each event to the one its frame tells next: Origin then Spread read as one story, one chain per frame, derived from sentence positions (`build.story_order`) |
| `event.locations` (6.2.0; was `location`), `location_type` | `mk:eventLocation`, one triple per place; `mk:locationType` | not aligned (see below) | the platform AND the venue on it ("Facebook", "the Star Wars Sithposting shitposting group") |
| `event.certainty` | `mk:certainty` | | the source's own hedging |
| `event.actors` | `mk:eventActor`, one triple each | ⊑ `sem:hasActor` | |
| `event.extraction_model`, `extraction_version` | `mk:extractionModel`, `mk:extractionVersion` | ⊑ `prov:wasGeneratedBy` | which model, under which contract |
| edge `eventLink` (6.0.0) | `mk:eventLink` | ⊑ `rdfs:seeAlso` | a hyperlink inside the event's sentences (parser 1.6.0 link positions) |
| edge `eventCitation` (6.0.0) | `mk:eventCitation` | ⊑ `rdfs:seeAlso` | the reference an `[n]` marker in the event's sentences cites |
| edge `eventEmbed` (6.0.0) | `mk:eventEmbed` | ⊑ `rdfs:seeAlso` | an embedded post shown right after a paragraph narrating the event (parser 1.6.0 embeds) |
| edge `eventImage` (6.0.0) | `mk:eventImage` | ⊑ `schema:image` | a photo shown right after a paragraph narrating the event |
| node `wikidata_entity` (6.1.0) | `<http://www.wikidata.org/entity/Q…>` with its `rdfs:label`; **no class** | | the `entities` collection (see [Entities](#entities-linked-to-wikidata-as-imkg-did-61)) |
| edge `fromTitle` (6.1.0) | `mk:fromTitle` → a Wikidata item | | entities recognised in the title, or the item whose KYM slug (P13484) is this page |
| node `template` (6.4.0) | `mk:template/<imgflip id> a mk:MemeTemplate`; `rdfs:label`; `skos:altLabel`; `mk:fileFormat` | `mk:MemeTemplate` ⊑ `schema:CreativeWork` | `imgflip_templates` (name, "also called" names, format) |
| edge `hasTemplate` (6.4.0) | `mk:hasTemplate` (frame → template), emitted with IMKG's `m4s:templateOf` back | `owl:inverseOf m4s:templateOf` | `frame_templates.selected` |
| edge `templateImage` (6.4.0) | `mk:templateImage` → `mk:Image` | ⊑ `schema:image` | the blank image (still templates only) |
| edge `imgflipPage` (6.4.0) | `mk:imgflipPage` → the imgflip page | ⊑ `rdfs:seeAlso` | the `/meme/` URL IMKG's imgflip memes point to |


### `category` (5.0.0): no further work

`category` is already fully handled by the reused-from-IMKG row above
(`a kym:<Category>`, from the parsed `category` field): the real corpus has
only 6 flat values (meme, event, subculture, person, site, culture), with
no nesting left in the value itself once the URL path that produced it is
discarded. Noted here explicitly so it isn't mistaken for an oversight.

### `origin_concept` (5.0.0): canonicalizes ALL of `origin`, not just platforms

`frame.from`/`m4s:from` (the infobox "Origin" line) is not a platform
field — the real corpus holds genuine platforms (Twitter, YouTube, 4chan),
countries (United States, Japan), franchises (The Simpsons, Avengers:
Endgame), companies (Nintendo, Valve), games (Elden Ring) and people
(Donald Trump), with real duplication in every category, not just platform
casing ("United States" / "USA" / "America"; "Avengers: Endgame" /
"Avengers: Endgame (Film)"). `kg/origin.py`'s curated alias map in
`kg_config/origin_taxonomy.yaml` canonicalizes all of it into one
`origin_concept` node per distinct referent; unreviewed long-tail values
fall back to a deterministic slugify rather than a lossy generic "Other".
The curated `subTypeOf` hierarchy on top (imageboard / social-network /
video-platform / …) covers **only** the subset of canonical slugs that are
actually platforms — a country, franchise, company or person is
deduplicated but deliberately left with no forced parent.

### Occurrences: edge properties / RDF-star annotations

What belongs to one *mention* of a target, rather than to the frame or the
target, is an `occurrences` entry on the frame-level edge. In Neo4j it
becomes index-aligned list properties on the relationship, and in RDF an
annotation on the quoted edge:

```turtle
<frame> mk:citesExternal <url> .
<< <frame> mk:citesExternal <url> >> mk:citationText "The Doge article" ;
                                     mk:citationIndex 3 .
```

| Occurrence field | RDF-star annotation | Neo4j list property | On edges | Parsed field |
|---|---|---|---|---|
| `anchor_text` | `mk:anchorText` | `anchor_texts` | `relatesToMeme`, `citesExternal` | `sections[].links[].text` |
| `in_section` | `mk:inSection` | `in_sections` | all three | `sections[].heading` |
| `citation_text` | `mk:citationText` | `citation_texts` | `relatesToMeme`, `citesExternal` | `external_references[].text` |
| `citation_index` | `mk:citationIndex` (`xsd:integer`) | `citation_indexes` | `relatesToMeme`, `citesExternal` | `external_references[].index` |
| `site_name` | `mk:siteName` | `site_names` | `relatesToMeme`, `citesExternal` | `additional_references[].name` |
| `role` | `mk:imageRole` (`page` / `section`) | `roles` | `hasImage` | `og_image` / `sections[].images[]` |
| `alt_text` | `mk:altText` | `alt_texts` | `hasImage` | `sections[].images[].alt` |
| `caption` | `mk:caption` ⊑ `schema:caption` | `captions` | `hasImage` | `sections[].images[].caption` |
| `mention_text` (6.1.0) | `mk:mentionText` | `mention_texts` | `fromTitle`, `fromTags`, `fromAbout` | the words on the page the item was recognised in |
| `link_score` (6.1.0) | `mk:linkScore` (`xsd:decimal`) | `link_scores` (`-1.0` = absent) | `fromTitle`, `fromTags`, `fromAbout` | the linker's score, 0–1 |
| `link_method` (6.1.0) | `mk:linkMethod` | `link_methods` | `fromTitle`, `fromTags`, `fromAbout` | how the span was found (`kym_id`, `title`, `tag`, `ner`, `propn`, `noun_chunk`) |
| `ner_label` (6.1.0) | `mk:nerLabel` | `ner_labels` | `fromTitle`, `fromTags`, `fromAbout` | the spaCy entity type, when there was one |
| `relevance_basis` (6.5.0) | `mk:relevanceBasis` | `relevance_bases` | `fromTitle`, `fromTags`, `fromAbout` | why curation kept the link: `title`, `own_item`, `platform`, `format`, `title_agrees`, `tag_and_text`, `tag_named`, `judge` |
| `template_score` (6.4.0) | `mk:templateScore` (`xsd:decimal`) | `template_scores` (`-1.0` = absent) | `hasTemplate` | how well the template fits the frame, 0–1 (1.0 = the frame's own KYM link) |
| `template_match` (6.4.0) | `mk:templateMatch` | `template_matches` | `hasTemplate` | `kym_reference` or `search` |
| `mention_text`, `link_score`, `link_method` (6.4.0) | as above | as above | `fromImage` | the region's name (or its printed text), the linker's score, `vlm_named` / `vlm_generic` / `frame_agree` / `ner` / `propn` / `title` |
| `depiction_kind` (6.4.0) | `mk:depictionKind` | `depiction_kinds` | `fromImage` | person, character, animal, object, text, logo, artwork, other |
| `bounding_box` (6.4.0) | `mk:boundingBox` | `bounding_boxes` | `fromImage` | the region, `xywh=percent:x,y,w,h` (W3C Media Fragments) |
| `detected_by` (6.4.0) | `mk:detectedBy` ⊑ `prov:wasGeneratedBy` | `detected_bys` | `fromImage` | the vision model that read it |

- Neo4j lists are aligned by position: entry *i* of every list is the same
  mention. A missing value is `""`, or `-1` for `citation_indexes`, because a
  Neo4j list cannot hold null. `occurrence_count` gives the length.
- RDF-star annotations are a set. When one frame mentions the same target
  twice, RDF keeps both anchor texts but not which heading went with which;
  the property graph keeps the pairing. This affects 1,542 (frame, target)
  pairs.

SPARQL-star, on Fuseki:

```sparql
PREFIX mk: <https://meme4.science/atlas/>
SELECT ?url ?citation WHERE {
  << <https://knowyourmeme.com/memes/doge> mk:citesExternal ?url >> mk:citationText ?citation
}
```

These nodes appear only in the property graph:

- `frame_stub`: a KYM page that is linked to but was not scraped.
- `external_ref`: a non-KYM URL.
- `tag_concept`, `region_concept`: in RDF, tags and regions are literals, as
  IMKG makes tags.

`coOccursWith` (`kg/census.py` + `kg/cooccurs.py`, tags only as of 5.0.1
— entry_type's statistical edges were judged needless alongside its
curated `subTypeOf` hierarchy and removed) is property-graph-only,
unconditionally: `tag_concept` has no IRI in RDF (`hasTag`'s object is a
plain `m4s:tag` literal, matching IMKG's own convention), so a
`coOccursWith` edge has no RDF projection at all — not a literal-object
triple, not an RDF-star annotation, both need a resource on at least one
side. Not declared as an `mk:` term at all. See "Not yet modelled".

Build provenance is written into every graph as `mk:currentBuild`, with the
predicates `mk:buildId`, `mk:snapshotAt`, `mk:kgBuildVersion` and
`mk:taxonomyVersion`.

## What becomes a node

A node (in RDF, a resource with its own IRI) is something other things can
share: a media frame, an entry type, a tag, a region, a URL, an image file.
What belongs to one frame is a literal on it (`m4s:about`, `mk:sectionText`),
which is also how IMKG keeps About, Origin and Spread. What belongs to one
mention of a target is an occurrence on the edge (see above).

MemeAtlas therefore mints no IRIs of its own for page content:

- Frames keep IMKG's convention: the live KYM URL.
- Images are identified by the image file's URL.
- Entry types are `kymt:<slug>`.

Before 4.0.0, sections, body links and references were nodes under
`https://meme4.science/atlas/entry/<sha1(url)>/…`. On the real corpus:
- about 88k of the 135k section nodes held no text;
- every Link node had exactly one `hasLink` in and one `linksTo` out, and
  every Reference one `hasReference` in and one `refersTo` out — a detour
  around edges that already existed;
- image captions sat on the shared image node, so pages overwrote each other.

Dissolving them in 4.0.0 took the published graph from 985,481 to 476,794
nodes, from 1,738,366 to 855,932 edges, and from 4,177,478 to 2,632,847
triples, with no parsed data dropped. The IMKG-comparable core is unchanged:
348,751 nodes and 712,796 edges, identical to 3.0.0.

### …and the one exception, 6.0.0: events

A node is **also** warranted when a thing has several attributes that must
stay grouped — a reified statement, where spreading the attributes across
the parent would lose which value goes with which. An extracted event has
ten co-varying attributes (what, a start and an end, a precision, where,
what kind of where, who, how certain, which sentence, which model). As
literals on the frame, nothing would say which date went with which
summary — the same defect that made 4.0.0 move image captions off the
shared image node.

So `mk:event/<id>` is a node. It is:

- the first IRI MemeAtlas mints for page-derived content since 4.0.0, and
  the first minted for anything other than a concept;
- the first node in the graph that is **derived rather than parsed** — a
  language model's reading of a sentence. Every event carries
  `mk:extractionModel` and `mk:sourceText` so a consumer can always tell
  model output from scraped fact and trace it to the prose it came from.

The id is `<sha1(frame url)[:12]>-<sha1(url, section, source text,
summary)[:10]>`: content-addressed (re-extracting unchanged text mints the
same IRI), frame-scoped (the IRI shows which entry it belongs to), and
never all-digit (the hyphen stops pandas reading the column as a number
inside morph-kgc). It is minted once, in `kg/events.py`, and stored.

## Deliberate differences from IMKG

1. **Typed literals.** `m4s:year` is `xsd:integer`, so SPARQL range filters
   work; IMKG leaves it untyped. `m4s:added` and `m4s:last_update_source` are
   `xsd:dateTime`, because IMKG's mapping uses `xsd:timestamp`, which is not an
   XSD datatype.
2. **The type hierarchy uses `rdfs:subClassOf`, not `skos:broader`.** IMKG
   already uses `skos:broader` between frames for "Part of a series on". Using
   the same predicate between entry types would give it two meanings.
3. ~~**`kym:<Category>` capitalisation**~~ — **not a difference** (verified
   2026-09-30, gap 02). IMKG's KnowYourMeme spider takes the category from
   the page's category badge (`aside/dl/a/text()`) and its mapping emits
   `kym:$(category)~iri`; its sample data has `Meme` and `Person`. On all
   23,879 corpus pages the badge reads exactly the class name MemeAtlas
   emits: `kym:Meme`, `kym:Event`, `kym:Subculture`, `kym:Person`,
   `kym:Site`, `kym:Culture` (`rdf.CATEGORY_CLASSES`). Kept in this list so
   the numbering the other sections cite stays stable.
4. **`mk:relatesToMeme` is broader than IMKG's `rdfs:seeAlso`.** IMKG's
   `rdfs:seeAlso` covers only the Related Entries box. `mk:relatesToMeme`
   covers every KYM link on the page, and is declared a sub-property of
   `rdfs:seeAlso`.
5. **`mk:badge` changed type in 5.0.0.** It was `owl:DatatypeProperty` (a
   literal) in 4.0.0; badges are now `badge_concept` resources, so it is
   `owl:ObjectProperty`. A documented ontology break, not a silent one —
   there was exactly one distinct badge value in the whole corpus
   ("Sensitive") at the time of the change, so this cost nothing to fix.

6. **Events have no IMKG counterpart.** IMKG keeps Origin and Spread as
   literals only; MemeAtlas keeps those literals *unchanged* (`m4s:origin`,
   `m4s:spread`, still datatype properties) and adds the event layer
   beside them under `mk:`, rather than re-typing IMKG's terms into object
   properties. The `mk:` namespace grows by 20 terms (see gap 01).

7. **Wikidata items are their canonical entity IRIs** (6.1.0),
   `http://www.wikidata.org/entity/Q42`. IMKG emitted the HTML page URL,
   `https://www.wikidata.org/wiki/Q42` (kym.media.frames.textual.enrichment
   in its repository), which names a document *about* the item and which
   no Wikidata dump or query uses. With the entity IRI, a federated
   `SERVICE <https://query.wikidata.org/sparql>` joins with no rewriting.
   The predicates, `m4s:fromAbout` and `m4s:fromTags`, are IMKG's own.
8. **Frame and template are linked with `m4s:templateOf` and
   `mk:hasTemplate`, never `m4s:sameAs`** (6.4.0). IMKG's mapping declares
   `<template> m4s:templateOf <frame>`, but its published `linkage.nt`
   writes `<frame> m4s:sameAs <template>` — a template is not the same thing
   as a media frame, so MemeAtlas follows the mapping, and emits the
   inverse too.
9. **A template may belong to several frames** (6.4.0; Gabi, 2026-09-28).
   IMKG gave each template exactly one frame. Here each frame that selects
   a template has its own edge, with its own `mk:templateScore`.
10. **Templates get MemeAtlas IRIs** (6.4.0): `mk:template/<imgflip id>`,
   not the imgflip page URL IMKG's memes point to. The page is linked
   (`mk:imgflipPage`) and IMKG's `imgflip:templateId` is kept as a plain
   literal, so both graphs still join — on the id, or in one hop.

## Events: drawn from EventKG, not copied from it

EventKG (built on SEM, the Simple Event Model) is where the shape came from.
What was taken and what was not:

| EventKG idea | MemeAtlas 6.0.0 |
|---|---|
| SEM's what / when / where / who | **Taken** — `mk:sourceText` / `mk:eventStart`+`mk:eventEnd`+`mk:datePrecision` / `mk:eventLocation`+`mk:locationType` / `mk:eventActor`, each aligned to its `sem:` term in `memeatlas.ttl` |
| Every statement traceable to its source | **Taken** — `mk:sourceText` (checked to be a real substring of the prose the model saw), `mk:sourceSection`, `mk:extractionModel` |
| Timestamps on every event | **Changed** — an interval at the precision the source gave, plus the precision itself. "early 2013" spans 2013, rather than becoming 2013-01-01 |
| — | **Added** — `mk:certainty`: the source's own hedging (confirmed / disputed / unconfirmed / debunked), so a rumour stays a rumour |
| Dates as the extractor states them | **Changed.** The model returns only the date WORDS, grounded in the event's own sentences (KYM states a year once and then omits it, so "June 16th, 2025" is resolved to the page's "June 16th" — never to a different date the sentences state); the pipeline parses them, takes a missing year from the most recent DATE the section states before the event's own date words (a year TOKEN is not a year context: the pilot read years out of quoted captions, festival names and "it didn't establish the year 2026"), and resolves "that same day" / "the following day" against the nearest earlier dated event — recording `mk:dateBasis` and `mk:dateAnchoredTo`. Words that name no day ("shortly after") leave the event undated rather than dated by guess |
| Free-text event descriptions | **Refused.** The model writes no text at all: it points at sentences by number, and every value it returns (date words, place, actors) is *grounded* — resolved to the span of the section it names and stored in the section's own words, never kept as the model wrote it. See `kg/events.py`, "Extractive only" and "grounding" |
| Per-statement named graphs (`eventKG-s:`) | **Not taken.** The graph must stay ground (`ntdiff` raises on blank nodes), `graph` is a reserved morph-kgc column, and each build already *is* a named graph |
| One event shared by many sources | **Not in 6.0.0.** EventKG merges on Wikipedia/Wikidata anchors; there are none here, and a wrong merge destroys information where a missing one only omits a link. Because ids are frame-scoped and content-addressed, a later linking pass can add `mk:sameEventAs` edges without re-minting an IRI |
| Emitting `sem:` terms | **Not taken** — aligned in the ontology only, like `schema:` and `prov:` (pinned by `test_event_terms_align_to_sem_without_emitting_it`) |
| Previous / next event (DBpedia `dbo:previousEvent`/`followingEvent`, Wikidata "follows"/"followed by") | **Changed (6.3.0).** Those are the order of a SERIES in time. `mk:nextInStory` is the order a frame's page TELLS its events in, Origin then Spread — how people narrate a meme's evolution, "x happened, then during the following week y happened". It places the ~21% of events no date could be given, and it is deliberately not aligned to any temporal relation: KYM tells flashbacks ("But a month earlier, ..."), and 30 of 753 dated neighbours in the 2026-09-25 samples step back in time. Time order is `mk:eventStart` |

The pipeline is its own stage, `kym_events`, between parse and kg:
`kg/events.py` (one LLM call per Origin/Spread section, validated against
`kg_config/event_extraction_schema.json`) → a JSONL artifact under
`data/kg/events/` → the `events` collection (`modules/event_store.py`, one
doc per frame and section) → `kg/build.py` as data.

## Entities: linked to Wikidata, as IMKG did (6.1.0)

IMKG enriched each KYM media frame with the Wikidata entities its text
names: it sent the About section and the tags to **DBpedia Spotlight**
(confidence 0.5), mapped the DBpedia resources it returned to Wikidata
QIDs, and emitted `m4s:fromAbout` / `m4s:fromTags` from the frame to each
(`kym/tags/spotlight.py`, `kym/KYM.Enrichment.ipynb`,
`kym/mappings/kym.media.frames.textual.enrichment.yaml` in its
repository). It joined the frame *itself* to Wikidata on KYM's own ID, and
pulled Wikidata statements between the linked items from downloaded dumps
through KGTK.

| IMKG | MemeAtlas 6.1.0 |
|---|---|
| About and tags annotated | **Taken**, with IMKG's predicates verbatim — plus the **title** (`mk:fromTitle`), which IMKG did not link |
| DBpedia Spotlight, a remote API, then DBpedia → Wikidata | **Replaced** by local NLP (spaCy: NER, noun chunks, proper-noun runs) and a lexicon built from the **full Wikidata dump** (`kg/wikidata.py`): the same answer every time for the same dump, no quota, no second knowledge base in between |
| Object `https://www.wikidata.org/wiki/Q…` | **Changed** to the entity IRI — Deliberate difference 7 |
| Frame ↔ item on KYM's numeric ID (P6760) | **Changed** to the KYM slug (P13484): the numeric ID is not on the page and the parser does not extract it; the slug is the URL's last segment. The item found this way is the title's entity at score 1.0 (`mk:linkMethod "kym_id"`), and wins every other span that could name it |
| Confidence threshold, not recorded | **Recorded** per mention (`mk:linkScore`, `mk:linkMethod`, `mk:nerLabel`, RDF-star on the quoted edge) |
| 1-hop Wikidata statements between linked items (KGTK) | **Not in 6.1.0** — see Not yet modelled |
| Google Vision labels → `m4s:fromImage` | **Not in 6.1.0** |

How a link is made (`kg/entities.py` has the reasoning at length):

- **Recognition.** Candidate spans are spaCy's named entities (never a
  date, time or amount), its noun chunks and every suffix of one, runs of
  proper nouns, the whole title, and each whole tag.
- **Lookup.** The lexicon holds every item with an English or `mul` label
  and at least one Wikipedia article — or a KYM identifier, whatever its
  sitelinks — minus Wikimedia bookkeeping (disambiguation pages,
  categories, lists, scholarly articles). Longest span first; a span that
  links claims its characters.
- **Ranking and acceptance.** Candidates are ranked on popularity, context
  overlap with the frame's text, label match, NER-type agreement (a hint,
  never a veto) and whether the item is itself on KYM; the winner's score
  adds its lead over the runner-up. Under `MIN_LINK_SCORE` (0.45) nothing
  is linked; at that line about 82% of links name the right item (hand
  judged per score band, `kg/entities.py`). Whether the item MATTERS to
  the meme is a separate question — [Curation](#curation-only-relevant-links-650).
- **Senses** (1.2.0). A curated list, `kg_config/entity_senses.yaml`,
  corrects the hubs the ranker gets wrong: "series" is a series of creative
  works, never the maths series, and "a series of" links nothing; "game" is
  a video game; "X" is Twitter only where the text says so ("on X", "X
  (formerly Twitter)") and never the snowclone placeholder; "sound" and
  "number" link nothing. The file's hash is a linker stamp, so editing it
  re-links.
- **Grounded.** Every mention stores the exact characters it came from;
  `audit()` refuses a record that does not match its page.

A Wikidata item is a node other frames share, like an image — but its IRI
is Wikidata's, so MemeAtlas mints nothing for it and asserts no class on
it: it gets its `rdfs:label` (the one it was linked under) and nothing
else. The node exists in Mongo and Neo4j with its description too.

These links are **derived** — a linker's reading, like events — and
recall-oriented: a common noun links as readily as a name ("hair",
"mug"). The graph carries only the ones curation keeps.

The pipeline is its own stage, `kym_entities`, between parse and events:
`kg/entities.py` over the lexicon at `WIKIDATA_LEXICON` → the `entities`
collection (`modules/entity_store.py`, one doc per frame, with every
link's features for curation) → `kym_entity_curation` → `kg/build.py` as
data.

### Curation: only relevant links (6.5.0)

IMKG emitted every Spotlight annotation above its confidence threshold.
MemeAtlas links more (noun chunks, not only names), and most of what it
links is incidental — *popularity*, *man*, *face*, *television program*.
6.5.0 emits a link only when it is **relevant** to the meme: its subject,
the people, characters or things it shows; a source work, franchise, game
or event; the kind of meme it is (image macro, copypasta, snowclone…); or
a platform or community it came from or spread on. The rest stay in Mongo
with their verdict — never deleted, only not emitted.

Title links are always kept. About and tag links go through two steps
(`kg/curation.py`, stage `kym_entity_curation`, collection
`entity_curation`); the basis of each kept link is `mk:relevanceBasis`:

1. **Local rules**, first match wins, over `kg_config/entity_curation.yaml`
   and the lexicon's P31/P279:

   | Rule | Verdict (basis) |
   |---|---|
   | the title field; the item is this meme (KYM slug, or the entry's own item) | keep (`title`, `own_item`) |
   | a listed non-topic item (popularity, time, word, Wikipedia…) | drop |
   | a platform: listed, or an instance of a platform class, through the whole class tree | keep (`platform`) |
   | a meme format (image macro, reaction image, copypasta, snowclone, parody …) | keep (`format`) |
   | the title also links it | keep (`title_agrees`) |
   | both a tag and the About link it | keep (`tag_and_text`) |
   | an About mention of a DIRECT instance/subclass of a denied class (anatomy, measure, mathematical concept) | drop |
   | a whole tag whose item has a capitalised label — a name, by Wikidata's convention of lower-case common nouns | keep (`tag_named`) |
   | an About mention of a generic word ("image", "social media", "man", "TikToker", "United States") | drop |
   | anything else | the judge |

   Denied classes match on direct parents only: Wikidata's upper ontology
   is tangled enough that *image macro* is, a few hops up, a "measure".

2. **An LLM judge** reads the entry (title, tags, Origin, About) and the
   numbered undecided items — each with its description and the sentence
   it was read from — and gives each a ROLE: `subject`, `source`,
   `format`, `platform` (kept) or `incidental`, `wrong_sense` (dropped),
   under a JSON schema that requires every number exactly once
   (`kg_config/entity_curation_schema.json`). The judge is
   ministral-3:14b; an item read only from the About that it keeps is
   kept only if the same model, asked again with a differently worded
   prompt, keeps it too — two readings agreeing drop most of what one
   keeps by chance. Chosen by two bake-offs and a review of the live run
   (`kg/curation.py` has the tables): projected over the corpus, kept
   0.875 relevant, dropped 0.126 relevant. Verdicts and roles are
   stored per item with the models, digests and prompt version; a rule
   edit never re-asks.

Until a frame's judge has answered, only its rule-kept links reach the
graph. A frame whose links changed since it was curated (a re-link)
falls back to its title links until the rules run again.

## Templates: imgflip, beyond IMKG (6.4.0)

IMKG's imgflip layer is **meme instances**: it scraped the memes users made
from imgflip's 1,765 most-used templates (1.3M memes, each with
`imgflip:templateId` and `imgflip:template` → the template's `/meme/` page),
and linked templates to KYM frames mostly by hand — Wikidata's P6760 for
276 seeds, 326 KYM → imgflip links filtered by hand, difflib title matches
at ≥ 0.85, and manual mapping: 96 frames ↔ 241 templates. It never
described the templates themselves, and never read their images.

| IMKG | MemeAtlas 6.4.0 |
|---|---|
| Meme instances, top templates only | **Blank templates**, found per frame by searching imgflip with its title (`kym_templates`) |
| Frame ↔ template by hand and difflib ≥ 0.85 | **Scored**: name (difflib and ordered word containment), the template's picture against the frame's own KYM images, search rank, imgflip's featured flag; the frame's own KYM "Meme Generator" link is ground truth. Tuned and measured on contact sheets (`kg/templates.py`) |
| — | **Near-identical uploads are one template** (perceptual hashes, `kg/visual.py`); the representative is imgflip's featured upload, else the oldest |
| — | **0, or 1 to 10 per frame, most varied first** — Gabi's minimum and maximum; "nothing fits" is recorded with its reason |
| `m4s:templateOf` (mapping) / `m4s:sameAs` (data) | `m4s:templateOf` and `mk:hasTemplate` — Deliberate difference 8 |
| One frame per template | Several — Deliberate difference 9 |
| Template IRI = its imgflip page | `mk:template/<id>` + `imgflip:templateId` + `mk:imgflipPage` — Deliberate difference 10 |
| Google Vision on the KYM frame image → `m4s:fromImage` | The lab's vision model (qwen3-vl:32b) on the **template** image → `m4s:fromImage`, each region annotated with what it depicts, where (`mk:boundingBox`) and which model read it (`mk:detectedBy`) |

Like events and entity links, `m4s:fromImage` here is **derived** — a
model's reading. Only every named entity, printed text, and the three
largest generic regions per template reach the graph; the rest stays in
`template_entities` for curation (gap 09's problem, for images). What is
**never** emitted: the dropped near-duplicate uploads, candidates that were
not kept, imgflip popularity, and the featured flag (property graph only).

`mk:template/<id>` is the graph's second minted IRI for derived content,
after `mk:event/<id>`, and falls under gap 01 (the `mk:` namespace needs
IMKG's authors' agreement) like every other `mk:` term.

## Not yet modelled

- **The frame's own image entities** (IMKG's actual `m4s:fromImage` use:
  Google Vision on the KYM frame image). 6.4.0 reads imgflip templates; the
  same extractor could read each frame's og:image — about 18k more calls.
- **Wikidata statements between linked items.** IMKG added the Wikidata
  edges between the items in its graph (KGTK over dumps). The lexicon has
  P31/P279 already; the rest would need the claims table from the same
  dump, and a decision about which properties are worth importing.
- **Entities from Origin/Spread, and event actors as items.** Only title,
  tags and About are linked; `mk:eventActor` stays a literal.

- **Cross-frame event identity.** Two entries narrating the same real
  happening get two `mk:Event` IRIs. See the EventKG table above for why,
  and for why adding the links later needs no re-minting.
- **`mk:sourceSection` / `mk:sourceText` on the edge.** In 6.0.0 there is
  exactly one mention per (frame, event), so they live on the event node.
  Once events are shared across frames they describe how *one frame*
  narrated it, and move to an RDF-star annotation on `mk:hasEvent`.
- **The bare `date`** (`"2013"`, `"2013-05"`) is in Mongo and Neo4j but
  emits no triple: it is recoverable from `mk:eventStart` +
  `mk:datePrecision`, and a column of bare years is the one value in the
  RML surface that pandas could read as a number inside morph-kgc.
- **Platform locations as concepts.** `mk:eventLocation` is free text. The
  `platform` ones overlap heavily with `origin_concept`, whose alias map
  already canonicalises them; resolving them would make every event depend
  on `origin_taxonomy_version`, and would conflate "where the meme came
  from" with "where this happened". Deferred, deliberately.
- **Actors as resources.** `mk:eventActor` is a literal — usernames and
  handles, with no identity resolution behind them.
- **`coOccursWith` has no RDF representation, for any pair.** Originally
  (5.0.0) both `entry_type` and tags got statistical `coOccursWith`
  edges, with entry_type pairs reaching RDF and tag pairs not (tags have
  no IRI in RDF — `hasTag`'s object is a plain `m4s:tag` literal). 5.0.1
  removed entry_type's `coOccursWith` entirely (needless alongside its
  curated `subTypeOf` hierarchy), leaving tags as the only source — so
  the field is now uniformly property-graph-only. Promoting tags to RDF
  resources to change that was considered and declined: it would make a
  node's RDF "class-ness" depend on a runtime, frequency-ranked property
  instead of `kind` alone, and 5.0.0/5.0.1 already explicitly defer
  building any tag hierarchy. `coOccursWith` lives in Mongo and Neo4j
  only — never in `graph.nt`, never in a validated RML mapping, and not
  declared in `memeatlas.ttl`.
- **Pipeline bookkeeping** (`dom_content_sha256`, `corpus_policy_version`,
  `schema_version`) stays in `entries`. It describes the pipeline, not the
  meme.
- **`meta`.** Its other keys are either site-wide constants (`og:site_name`,
  `twitter:card`) or copies of fields already carried (`og:title`, `og:url`,
  `og:image`). The three description keys hold the same text in every entry.

## Where each rule is enforced

| Rule | Check |
|---|---|
| The mapping and `rdf.py` emit exactly the same predicates, classes and source CSVs, compared as full IRIs | `tests/test_kg_vocabulary.py` |
| Every `mk:` term that can be emitted is declared in `memeatlas.ttl`, and nothing is declared that is not emitted | `tests/test_kg_vocabulary.py` |
| The IMKG terms are used, and no `mk:` term shadows one | `tests/test_kg_vocabulary.py` |
| Every field the parser extracts from a real page reaches the graph | `tests/test_kg_build.py::FullRecordTests` |
| The two derivations produce the same triples on real data | `kym_kg_validate` |
| `sem:` is aligned to in the ontology and never emitted | `tests/test_kg_vocabulary.py` |
| An event's `source_text` is really in the section the model saw; its date matches its precision | `tests/test_kg_events.py` |
| Every sentence of Origin and Spread is in an event (extraction 4.0.0) | `tests/test_kg_events.py` (`CoverageTests`), and `audit()` refuses any record with a gap |
| A re-extraction replaces a section's events, never merges them; a failing section is not retried until something changes | `tests/test_event_store.py` |
| The lexicon keeps exactly what the filter says (no disambiguation pages; KYM-slug items kept without sitelinks; `mul` labels count) and frames join on P13484 | `tests/test_kg_wikidata.py` |
| Every entity mention is the page's own words at its offsets; the KYM-slug item wins; context separates senses; an NER label never vetoes | `tests/test_kg_entities.py` |
| A Wikidata item is an object only — never typed, never a predicate — and `fromAbout`/`fromTags` are IMKG's own | `tests/test_kg_vocabulary.py` |
| A re-link replaces a frame's mentions; every staleness stamp re-queues on its own | `tests/test_entity_store.py` |
| A template's two directions and IMKG's terms; the template IRI | `tests/test_kg_vocabulary.py` |
| Template nodes, edges and annotations; a template two frames chose is emitted identically; the build reads at its snapshot | `tests/test_kg_template_graph.py` |
| Duplicates: resize, re-encode, mirror and letterboxing are one picture, no chaining; selection is 0 or 1–10 | `tests/test_kg_visual.py`, `tests/test_kg_templates.py` |
| Every image region stored is a real box with a real name; the blind audit's arithmetic; what reaches the graph | `tests/test_kg_template_entities.py` |
