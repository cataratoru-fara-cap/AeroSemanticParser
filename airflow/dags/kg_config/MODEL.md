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
| `event.source_text` | `mk:sourceText` | | the verbatim sentences it was read from, copied from the page by the pipeline (the model only points at sentence numbers) |
| `event.source_section` | `mk:sourceSection` (`origin` / `spread`) | | |
| `event.date_start`, `date_end` | `mk:eventStart`, `mk:eventEnd` (`xsd:dateTime`) | ⊑ `sem:hasBeginTimeStamp`, `sem:hasEndTimeStamp` | `date` at `date_precision`, as an interval |
| `event.date_precision`, `date_text` | `mk:datePrecision`, `mk:dateText` | | |
| `event.date_basis` (6.0.0) | `mk:dateBasis` (`stated` / `relative`) | | how the date was arrived at — the model never dates anything, it returns the words and the pipeline parses them |
| edge `eventDateAnchor` (6.0.0) | `mk:dateAnchoredTo` | | for a relative date ("that same day"), the earlier event it was counted from |
| `event.location`, `location_type` | `mk:eventLocation`, `mk:locationType` | not aligned (see below) | |
| `event.certainty` | `mk:certainty` | | the source's own hedging |
| `event.actors` | `mk:eventActor`, one triple each | ⊑ `sem:hasActor` | |
| `event.extraction_model`, `extraction_version` | `mk:extractionModel`, `mk:extractionVersion` | ⊑ `prov:wasGeneratedBy` | which model, under which contract |
| edge `eventLink` (6.0.0) | `mk:eventLink` | ⊑ `rdfs:seeAlso` | a hyperlink inside the event's sentences (parser 1.6.0 link positions) |
| edge `eventCitation` (6.0.0) | `mk:eventCitation` | ⊑ `rdfs:seeAlso` | the reference an `[n]` marker in the event's sentences cites |
| edge `eventEmbed` (6.0.0) | `mk:eventEmbed` | ⊑ `rdfs:seeAlso` | an embedded post shown right after a paragraph narrating the event (parser 1.6.0 embeds) |
| edge `eventImage` (6.0.0) | `mk:eventImage` | ⊑ `schema:image` | a photo shown right after a paragraph narrating the event |
| node `wikidata_entity` (6.1.0) | `<http://www.wikidata.org/entity/Q…>` with its `rdfs:label`; **no class** | | the `entities` collection (see [Entities](#entities-linked-to-wikidata-as-imkg-did-61)) |
| edge `fromTitle` (6.1.0) | `mk:fromTitle` → a Wikidata item | | entities recognised in the title, or the item whose KYM slug (P13484) is this page |


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
3. **`kym:<Category>` capitalisation** follows the paper's `kym:Meme`. IMKG's
   raw category values are not published, so byte equality with its class
   IRIs has not been verified.
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
  adds its lead over the runner-up. Under `MIN_LINK_SCORE` (0.50,
  provisional) nothing is linked.
- **Grounded.** Every mention stores the exact characters it came from;
  `audit()` refuses a record that does not match its page.

A Wikidata item is a node other frames share, like an image — but its IRI
is Wikidata's, so MemeAtlas mints nothing for it and asserts no class on
it: it gets its `rdfs:label` (the one it was linked under) and nothing
else. The node exists in Mongo and Neo4j with its description too.

These links are **derived** — a linker's reading, like events — and
recall-oriented: a common noun links as readily as a name ("hair",
"mug"). Which links matter to the meme is the next task, gap 09.

The pipeline is its own stage, `kym_entities`, between parse and events:
`kg/entities.py` over the lexicon at `WIKIDATA_LEXICON` → the `entities`
collection (`modules/entity_store.py`, one doc per frame, with every
link's features for curation) → `kg/build.py` as data.

## Not yet modelled

- **Curation of the entity layer** (gap 09). Every recognised entity is
  linked; incidental common nouns dominate the About links.
- **Wikidata statements between linked items.** IMKG added the Wikidata
  edges between the items in its graph (KGTK over dumps). The lexicon has
  P31/P279 already; the rest would need the claims table from the same
  dump, and a decision about which properties are worth importing.
- **Entities from Origin/Spread, and event actors as items.** Only title,
  tags and About are linked; `mk:eventActor` stays a literal.
- **Image entities** (IMKG's `m4s:fromImage`, from Google Vision).

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
| A re-extraction replaces a section's events, never merges them; a failing section is not retried until something changes | `tests/test_event_store.py` |
| The lexicon keeps exactly what the filter says (no disambiguation pages; KYM-slug items kept without sitelinks; `mul` labels count) and frames join on P13484 | `tests/test_kg_wikidata.py` |
| Every entity mention is the page's own words at its offsets; the KYM-slug item wins; context separates senses; an NER label never vetoes | `tests/test_kg_entities.py` |
| A Wikidata item is an object only — never typed, never a predicate — and `fromAbout`/`fromTags` are IMKG's own | `tests/test_kg_vocabulary.py` |
| A re-link replaces a frame's mentions; every staleness stamp re-queues on its own | `tests/test_entity_store.py` |
