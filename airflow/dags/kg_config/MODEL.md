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
| `frame.added` | `m4s:added` (`xsd:dateTime`) | `kym_added` |
| `frame.last_updated` | `m4s:last_update_source` (`xsd:dateTime`) | `kym_last_updated` |
| edge `hasEntryType` | `rdf:type kymt:<slug>` | `entry_type` |
| edge `hasTag` | `m4s:tag "<tag>"` | `tags` |
| edge `partOfSeries` | `skos:broader` plus the inverse `skos:narrower` | `series_parent` |

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
| `frame.section_texts` | `mk:sectionText "<heading>\n\n<paragraphs>"` | ⊑ `schema:articleBody` | `sections[]` with text, except About (`m4s:about`) and the deferred Origin/Spread |
| `frame.corpus_status`, `corpus_missing` | `mk:corpusStatus`, `mk:corpusMissing` | | corpus grading |
| `frame.parser_version`, `parsed_at`, `scraped_at` | `mk:parserVersion`, `mk:parsedAt`, `mk:scrapedAt` | ⊑ PROV | provenance |
| node `image` | `<file url> a mk:Image`; `mk:width`, `mk:height` | `mk:Image` ⊑ `schema:ImageObject` | `og_image`, `template_image_url`, `sections[].images[]`, `og:image:width/height` |
| edge `hasImage` | `mk:hasImage` | ⊑ `schema:image` | |
| node `origin_concept` (5.0.0) | `mk:origin/<slug> a skos:Concept`; `skos:inScheme mk:OriginScheme`; `skos:prefLabel` | | `origin`, canonicalized (see below) |
| edge `hasOrigin` (5.0.0) | `mk:hasOrigin` | | curated aliases + a fallback slug, `kg_config/origin_taxonomy.yaml` |
| edge `subTypeOf` (origin) (5.0.0) | `rdfs:subClassOf` between `mk:origin/` concepts | | `origin_taxonomy.yaml`, platform-like slugs only |
| node `badge_concept` (5.0.0) | `mk:badge/<slug> a skos:Concept`; `skos:inScheme mk:BadgeScheme`; `skos:prefLabel` | | the badge vocabulary |
| edge `hasBadge` (5.0.0) | `mk:badge` (an `owl:ObjectProperty` as of 5.0.0 — was a literal in 4.0.0) | | `badges` |


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

## Not yet modelled

- **The Origin and Spread sections** (`DEFERRED_SECTION_KINDS` in
  `build.py`) are left to the event-extraction task. They will map to IMKG's
  `m4s:origin` and `m4s:spread`. Links inside them already feed
  `mk:relatesToMeme` and `mk:citesExternal`.
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
