# Chapter 7 — Weave the graph: one build, every representation (≈8 min)

DAG: `kym_kg` (and `kym_kg_validate`). Modules: `kg/build.py` (the only
producer), `kg/serialize.py`, `kg/rdf.py`, `kg/loaders.py`, `kg/metrics.py`,
`kg/siblings.py`; store `kg_store.py`. Config: `kg_config/memeatlas.ttl`
(ontology), `kg_mapping.yarrrml.yml` (RML mapping), `MODEL.md` (crosswalk).

## The story
Everything the previous steps learned about Doge becomes arrows from Doge. The
same graph is published in two languages — RDF for the Semantic Web, a property
graph for Neo4j — from one build, checked across every store before it goes
live.

## Doge as a graph (the first KG_QUERIES query, drawn from the live graph)
Up: its series (Interior Monologue Captioning → Image Macros → Memes). Upper
left: its children (Doge 2 / Caesar, Ironic Doge Memes, Shiba Inus / Shibes, The
Death of Kabosu / Doge). Left, dashed: its 9 siblings (including its own
"sensitive" twin: in 7.0.0 Doge is two frames — fixed since, chapter 2; in 7.1.0
the twin is gone and Doge's frame is the `/sensitive/` address). Right: Wikidata items. Lower right: its 8
templates and what their images show — some are the same items the text names
(Shiba Inu, Doge): text and picture agree. Bottom: 16 events in one chain. Lower
left: 5 entry types and 21 tags. In total 379 edges touch Doge.

## Numbers (KG 7.0.0, build kg_20261002T170459Z_manual, published 2 Oct 2026)
795,711 nodes · 2,564,089 edges · 26 relation types · 8,790,369 RDF triples.
Nodes by kind: external pages 257,031 · images 202,390 · events 142,787 · tags
101,883 · Wikidata items 26,897 · templates 26,868 · frames 24,291 · frame
stubs 7,631 · origins 5,703 · entry types 119 · regions 110 · badges 1.
Biggest relations: sharesSameSeries 679,392 (derived) · citesExternal 244,297 ·
hasTag 225,943 · citesMediaFrame 215,295 · hasImage 192,518 · hasEvent 142,787 ·
nextInStory 124,014.

## Design decisions — the why
- **IMKG's words, kept.** Where IMKG has a term, it is used verbatim: m4s:title,
  m4s:about/origin/spread, m4s:fromAbout/fromTags/fromImage, kym:<Category>
  classes, skos:broader for the series, rdfs:seeAlso for siblings,
  m4s:templateOf. MemeAtlas's `mk:` terms only where IMKG has none (templates,
  events, curation, citations). 13 deliberate differences are written down in
  MODEL.md (typed literals, Wikidata entity IRIs instead of wiki URLs, siblings
  derived from the series, …).
- **What becomes a node (4.0.0).** A node is something other things can share;
  what belongs to one entry is a value on it; what belongs to one mention is on
  the edge. Sections, links and references used to be nodes — 88k of 135k
  section nodes held no text, every link node was a detour. Dissolving them:
  985,481 → 476,794 nodes, 1.74M → 856k edges, no data lost. Events are the one
  exception (things point at them).
- **One producer, many representations.** `build.py` is the only code that
  makes nodes and edges; `serialize.py` writes N-Triples, the RML CSVs and the
  view CSVs from the same stream; loaders fill Neo4j and Fuseki.
- **Generational builds, verified, then published.** Each build has an id;
  stores keep the live build and the previous one for rollback. `verify` checks
  that Mongo, the files, Fuseki and Neo4j agree on every count; `publish` flips
  pointers — followers first, Mongo (the authority) last — and reads them back:
  a torn publish fails loudly.
- **An independent second derivation.** `kym_kg_validate` re-derives the RDF
  from the CSVs through the YARRRML mapping (yatter → morph-kgc) and diffs it
  with graph.nt: 8,790,365 vs 8,790,369 — equal but for the 4 provenance triples.
  Two implementations agreeing is the strongest check that the mapping
  documentation is true.
- **Staleness and versions.** The build stamps every input version; nothing
  changed → nothing rebuilt. Semantic versions: renaming a term (relatesToMeme →
  citesMediaFrame) is major (7.0.0); a new layer is minor (7.1.0).

## If someone asks
- *Why both RDF and a property graph?* RDF for interoperability (IMKG, Wikidata,
  SPARQL federation); Neo4j for exploration, visual browsing and graph analytics.
  One build feeds both so they cannot drift.
- *What is RDF-star?* A statement about a statement: the link "Doge's tags
  mention dog" carries its mention text and score.
- *Where does it run?* Fuseki (TDB2) serves SPARQL, Neo4j 5 Community serves
  Cypher, both behind the one open port.

## Sources
MODEL.md (What becomes a node; Deliberate differences), `kg/build.py` version
history, `airflow/README.md` "The KG stage".
