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
| edge `relatesToMeme` | `mk:relatesToMeme` | ⊑ `rdfs:seeAlso` | every KYM link on the page |
| edge `citesExternal` | `mk:citesExternal` | ⊑ `rdfs:seeAlso` | every non-KYM link on the page |
| `frame.description` | `mk:description` | ⊑ `schema:description` | `meta.description` |
| `frame.badges` | `mk:badge` | | `badges` |
| `frame.aliases` | `skos:altLabel` | | `aliases` |
| `frame.corpus_status`, `corpus_missing` | `mk:corpusStatus`, `mk:corpusMissing` | | corpus grading |
| `frame.parser_version`, `parsed_at`, `scraped_at` | `mk:parserVersion`, `mk:parsedAt`, `mk:scrapedAt` | ⊑ PROV | provenance |
| node `section` | `a mk:Section`; `mk:sectionKind`, `mk:heading`, `mk:position`, `mk:headingLevel`, `mk:text` | | `sections[]` |
| edge `hasSection` | `mk:hasSection` | | |
| node `link` | `a mk:Link`; `mk:anchorText` | | `sections[].links[]` |
| edges `hasLink`, `linksTo` | `mk:hasLink`, `mk:linksTo` | | |
| node `reference` | `a mk:ExternalReference` or `a mk:AdditionalReference` (both ⊑ `mk:Reference`); `mk:citationIndex`, `mk:citationText`, `mk:siteName` | | `external_references[]`, `additional_references[]` |
| edges `hasReference`, `refersTo` | `mk:hasReference`, `mk:refersTo` | | |
| node `image` | `<file url> a mk:Image`; `mk:altText`, `mk:caption`, `mk:width`, `mk:height` | `mk:Image` ⊑ `schema:ImageObject`, `mk:caption` ⊑ `schema:caption` | `og_image`, `template_image_url`, `sections[].images[]`, `og:image:width/height` |
| edge `hasImage` | `mk:hasImage` | ⊑ `schema:image` | |

These nodes appear only in the property graph:

- `frame_stub`: a KYM page that is linked to but was not scraped.
- `external_ref`: a non-KYM URL.
- `tag_concept`, `region_concept`: in RDF, tags and regions are literals, as
  IMKG makes tags.

Build provenance is written into every graph as `mk:currentBuild`, with the
predicates `mk:buildId`, `mk:snapshotAt`, `mk:kgBuildVersion` and
`mk:taxonomyVersion`.

## IRIs MemeAtlas mints

```
https://meme4.science/atlas/entry/<sha1(url)>/section/<i>
https://meme4.science/atlas/entry/<sha1(url)>/section/<i>/link/<j>
https://meme4.science/atlas/entry/<sha1(url)>/reference/<j>
https://meme4.science/atlas/entry/<sha1(url)>/additional-reference/<j>
```

- `sha1(url)` is the entry's `_id` in Mongo.
- `<i>` is the position among all parsed sections, including the deferred
  ones below, so an IRI stays the same once those are modelled.
- Images are identified by the URL of the image file.

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

## Not yet modelled

- **The Origin and Spread sections** (`DEFERRED_SECTION_KINDS` in
  `build.py`) are left to the event-extraction task. They will map to IMKG's
  `m4s:origin` and `m4s:spread`. Links inside them already feed
  `mk:relatesToMeme` and `mk:citesExternal`.
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
