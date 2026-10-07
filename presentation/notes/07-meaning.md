# Chapter 7 — Give it meaning: ontology, taxonomies, folksonomy (≈6 min)

Config: `airflow/dags/kg_config/memeatlas.ttl` (the ontology),
`entry_type_taxonomy.yaml`, `origin_taxonomy.yaml`, `tag_normalization_exceptions.yaml`,
`MODEL.md` (the crosswalk to IMKG). Modules: `kg/taxonomy.py`, `kg/origin.py`,
`kg/census.py`, `kg/cooccurs.py`, `kg/tag_normalize.py`. Data:
`airflow/data/kg_census_*.json`, `kg_type_definitions.json`,
`kg_type_semantics_report.json`.

## The story
Before weaving the graph, the words it is written in. Three kinds of
vocabulary, each built differently — and the difference is the point:
- an **ontology** says which kinds of things exist and which arrows join them;
- **taxonomies** say what is a kind of what — decided by a person;
- the **folksonomy** is the tags people chose — kept as they are, and measured.

## The ontology — how it was derived
1. **IMKG's words, as they are.** Where IMKG already models something, its term
   is used verbatim: `m4s:MediaFrame`, `m4s:title`/`about`/`origin`/`spread`,
   the `kym:` category classes and `kymt:` entry-type classes, `skos:broader` for
   a series, `rdfs:seeAlso` for siblings, `m4s:fromAbout`/`fromTags`/`fromImage`,
   `m4s:templateOf`, `m4s:tag`. So IMKG's own queries run unchanged.
2. **New terms only where IMKG has none** (`mk:`, `https://meme4.science/atlas/`),
   each declared in `memeatlas.ttl` and aligned to a standard where one fits:
   `mk:Event` ⊑ `sem:Event` (the Simple Event Model, EventKG's) and
   `schema:Event`; `mk:MemeTemplate` ⊑ `schema:CreativeWork`; `mk:Image` ⊑
   `schema:ImageObject`; citations and links ⊑ `rdfs:seeAlso`; provenance ⊑ PROV.
   Aligned, never emitted: the graph itself carries only IMKG's terms and ours.
3. **A node only for what is shared** (4.0.0): entries, types, tags, URLs,
   images, Wikidata items. What belongs to one entry is a value on it; what
   belongs to one mention (anchor text, a link's score) annotates the arrow with
   RDF-star. Events (6.0.0) are the one exception — other things point at them.
4. **Checked.** `tests/test_kg_vocabulary.py`: every `mk:` term the serializer
   can emit is declared, nothing declared is unused, IMKG's terms are used and
   never shadowed. `kym_kg_validate` re-derives the RDF a second way, through
   the YARRRML mapping, and compares. 14 deliberate differences from IMKG are
   written down in MODEL.md, each with its reason (typed literals, entity IRIs
   for Wikidata items, siblings derived from the series, …).

## The taxonomies — how they were derived
**Entry types** (KYM's own controlled list: 119 types — exploitable, image
macro, catchphrase …). "Meaning first, statistics as audit":
- *Statistics*: a census over the corpus (`kg/census.py`) — 583 pairs of types
  that appear on the same entries, with pointwise mutual information and
  containment. Two types that co-occur far more than chance may be related;
  two that never co-occur though they should may be one idea filed twice.
- *Meaning*: a language model wrote a definition of each type (prompt v1; 9
  rewritten by hand); each definition embedded with `qwen3-embedding:0.6b`.
  Similar meaning + no co-occurrence = a substitution candidate
  ("controversy" vs "viral-debate": cosine 0.71, expected together ~20 times,
  observed 0).
- *A person filed every candidate pair* into buckets
  (`entry_type_taxonomy.yaml`): is-a where both agree (5); is-a by meaning where
  the statistics cannot see it (8); real relations of the wrong type — causal or
  part-of (5: a trial follows a crime; a song is part of an album); rejected (3:
  anime and manga co-occur because franchises span both, they are not one
  thing); contested, to sample (2); adjective-like qualifiers with no parent
  (3: ai-generated, historical-figure, shock-media); missing umbrellas (comics,
  game, screen media).
- Only the two is-a buckets become edges: **13 `subTypeOf`** edges
  (`rdfs:subClassOf` between the `kymt:` classes — not `skos:broader`, which
  IMKG already uses between frames). `kg/taxonomy.py` refuses a pair filed in
  two buckets: it once shipped an edge the record called contested.

**Origins** (the infobox's free-text "Origin"): platforms, but also countries,
films, companies, people. The head of the distribution (the top ~200 raw values,
most entries by volume) was read and canonicalised by hand: 300 spellings → 202
origins ("Twitter", "X", "twitter.com" → twitter); the long tail keeps its own
slug, never a lossy "Other". The is-a tree covers platforms only: 42 edges under
12 umbrellas (social network, imageboard, video platform, meme site …). A
country, a film or a person is de-duplicated but never given a fake parent. No
statistics here: one origin per entry, so two origins never co-occur — every
edge is judgement (`evidence: curated`).

## The folksonomy — how it is treated
Tags are free keywords from KYM's editors and users: how people describe
memes, and noisy. What MemeAtlas does:
- keeps the editors' words (in RDF, IMKG's own `m4s:tag` literal);
- folds only what is certainly one tag: plurals (exploitable 905 + exploitables
  825; catchphrase 664 + catchphrases 660; meme 1,061 + memes 641 — raw census),
  with a do-not-fold list seeded by reading every top-300 tag the rule would
  touch (news, politics, star wars, the simpsons … — 12 so far, the list only
  grows);
- measures what goes together: a tag census counts pairs on the same entry, and
  frequent pairs become `coOccursWith` edges (gaming–video games 396,
  twitter–x 201, anime–manga 189, donald trump–politics 161) — 4,741 of them,
  in the property graph only. A threshold on a count, deliberately a different
  relation from the taxonomy's is-a. (The same co-occurrence was tried for
  entry types and removed in 5.0.1: next to a curated taxonomy it was noise.)
- 104,251 distinct tags as written → 101,882 tag concepts in the graph.

## If someone asks
- *Why not learn the taxonomy automatically?* The statistics alone proposed
  edges that are wrong on meaning (anime ⊑ manga, cartoon ⊑ tv-show); the
  meaning alone cannot be checked. Together, with a person deciding, every edge
  has a recorded reason.
- *Why not a taxonomy of tags?* 100k free keywords with no editor — a census and
  co-occurrence are honest about what they are; a hand-made tree over them would
  not be.
- *Is the ontology published?* `memeatlas.ttl` is loaded into Fuseki as its own
  named graph (`urn:memeatlas:ontology`) and copied into every build.

## Sources
`MODEL.md` (Crosswalk, What becomes a node, Deliberate differences), the YAML
headers of the curated files, gap notes 06 and 07, `kg/taxonomy.py` docstring.
