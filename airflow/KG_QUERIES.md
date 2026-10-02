# Querying the knowledge graph, and what 6.5.0 shows

The live graph is **KG 7.0.0**, build `kg_20261002T170459Z_manual`, published
2026-10-02. It adds four layers to the IMKG-style core: events (6.0.0),
Wikidata links from each frame's text (6.1.0, curated in 6.5.0), imgflip
templates and what their images show (6.4.0). The conclusions below were
measured on 6.5.0; [what 7.0.0 changed](#what-700-changed) comes first.

- **Neo4j Browser:** `http://<host>:8080/browser/` (the property graph; needs
  the neo4j password).
- **SPARQL:** `http://<host>:8080/sparql/` (read-only; the default graph is the
  live build).

## What 7.0.0 changed

Two changes to the core:

- **The frames of one series are linked to each other**, as Riccardo asked
  when reviewing the model. Until 6.5.0 two frames of a series were
  connected only through the parent they share. 7.0.0 adds an edge for
  every two frames with the same series parent:
  679,392 pairs from 2,077 series. In Neo4j it is `sharesSameSeries`, one
  relationship per pair. In RDF it is `rdfs:seeAlso`, in both directions
  (1,358,784 triples). That is IMKG's own term for an entry's siblings, so
  IMKG's queries run unchanged and now get the whole series (query 4).
- **`relatesToMeme` is renamed `citesMediaFrame`** (`mk:citesMediaFrame`),
  beside `citesExternal`. A query that names the old term finds nothing.
  A link now counts only when it points at a KYM entry. 2,841 links to
  other KYM pages (photos, videos, news, editorials, listings, profiles,
  forums, `/login`, searches) are `citesExternal` now, and their 1,945
  `FrameStub`s are `ExternalRef`s.

The whole graph has 795,711 nodes (unchanged), 2,564,089 edges (+679,392),
26 relation types and 8,790,369 triples. The siblings are left out of the
IMKG-comparable core: they are derived from `partOfSeries` and connect
nothing the core does not already connect.

## How the graph sits in Neo4j

Every node has the label `KGNode` plus one for its kind (`Frame`, `FrameStub`,
`WikidataEntity`, `Template`, `Event`, …). Every node and relationship
carries the `build_id` of its build. Two builds are kept: the live one and
the one before it, for rollback. **Always resolve the live build first**,
through `(:KGPointer {name: 'current'})`, as every query below does.

| Relationship | From → to | Properties worth reading |
|---|---|---|
| `fromTitle`, `fromTags`, `fromAbout` | `Frame` → `WikidataEntity` | `relevance_bases` (why curation kept it), `mention_texts`, `link_scores` |
| `hasTemplate` | `Frame` → `Template` | `template_scores`, `template_matches` |
| `fromImage` | `Template` → `WikidataEntity` | `depiction_kinds`, `bounding_boxes`, `mention_texts` |
| `partOfSeries` | `Frame` → its series parent (`Frame`, or `FrameStub` when the parent is not in the corpus) | — |
| `sharesSameSeries` | `Frame` — `Frame`, two frames with the same series parent (7.0.0). Stored once per pair, so match it without an arrow: `(f)-[:sharesSameSeries]-(s)` | — |
| `citesMediaFrame` | `Frame` → a KYM entry it links to, on its page or in its references (`Frame`, or `FrameStub` when not in the corpus). Called `relatesToMeme` until 7.0.0 | `anchor_texts`, `in_sections`, `citation_texts` |
| `citesExternal` | `Frame` → any other page it links to, on another site or on KYM (`ExternalRef`) | the same |

A relationship's properties are lists, one entry per mention, index-aligned:
position *i* of `mention_texts` and of `relevance_bases` describe the same
mention. A `FrameStub` has no title, only its KYM URL (`id`).

## The queries

### 1. One frame as a graph

For Neo4j Browser, which draws every path returned. It shows the frame's
Wikidata items, its templates and what each template's image shows, its
chain of series parents (up to five levels), and the other frames of its
series. Change the title on the first line. (A frame of a big series has
hundreds of siblings, TikTok's 542; drop that line for those.)

```cypher
WITH 'Tung Tung Tung Sahur' AS title
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id, label: title})
RETURN f,
  COLLECT { MATCH p = (f)-[:fromTitle|fromTags|fromAbout]->(:WikidataEntity) RETURN p } AS frame_entities,
  COLLECT { MATCH p = (f)-[:hasTemplate]->(:Template)-[:fromImage]->(:WikidataEntity) RETURN p }
    + COLLECT { MATCH p = (f)-[:hasTemplate]->(t:Template) WHERE NOT (t)-[:fromImage]->() RETURN p } AS templates,
  COLLECT { MATCH p = (f)-[:partOfSeries*1..5]->(:KGNode) RETURN p } AS series_parents,
  COLLECT { MATCH p = (f)-[:sharesSameSeries]-(:Frame) RETURN p } AS siblings
```

For "Tung Tung Tung Sahur" this draws 22 Wikidata links, 10 templates with
the items their images show, the series chain Italian Brainrot / AI
Italian Animals → Brain Rot / Brainrot → Internet Slang → The Internet,
and its 13 siblings in Italian Brainrot.

### 2. One row per frame

For the table view. Each row has the frame's series parents, each
Wikidata item with its field and the reason curation kept it, the templates,
and the items seen in the templates' images.

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id})
RETURN f.label AS frame,
       f.id AS url,
       COLLECT { MATCH (f)-[:partOfSeries]->(p) RETURN coalesce(p.label, p.id) } AS series_parents,
       COLLECT { MATCH (f)-[r:fromTitle|fromTags|fromAbout]->(e:WikidataEntity)
                 RETURN e.qid + ' ' + e.label + ' (' + type(r) + ', ' + r.relevance_bases[0] + ')' } AS wikidata,
       COLLECT { MATCH (f)-[:hasTemplate]->(t:Template)
                 RETURN t.label + ' #' + toString(t.template_id) } AS templates,
       COLLECT { MATCH (f)-[:hasTemplate]->(:Template)-[:fromImage]->(e:WikidataEntity)
                 RETURN DISTINCT e.qid + ' ' + e.label } AS seen_in_templates
ORDER BY frame
LIMIT 50
```

A sample row: *Distracted Boyfriend* has parent *Object Labeling*. Its
Wikidata links include `Q55691704 distracted boyfriend meme (fromTitle,
title)` and `Q6002242 image macro (fromTags, format)`. It has 10 templates,
whose images show man, woman and a backpack. Fifty rows take about 7 s. For
the whole corpus, drop the `LIMIT` or add
`WHERE f.label STARTS WITH '…'`.

### 3. A frame's siblings, and which of them its page links to

One row per sibling, and whether either page links to the other.

```cypher
WITH 'Tung Tung Tung Sahur' AS title
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id, label: title})-[:sharesSameSeries]-(s:Frame)
RETURN s.label AS sibling,
       EXISTS { (f)-[:citesMediaFrame]->(s) } AS links_to_it,
       EXISTS { (s)-[:citesMediaFrame]->(f) } AS linked_from_it
ORDER BY sibling
```

Tung Tung Tung Sahur has 13 siblings. Its page and theirs link 8 of them,
either way; the other 5 are linked only by the series. The links between
pages and the series are different relations: only 3,602 of the 679,392
sibling pairs are linked by `citesMediaFrame`.

### 4. A frame's series, as IMKG queries it (SPARQL)

IMKG's own term for an entry's siblings, `rdfs:seeAlso`, unchanged.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX m4s:  <https://meme4.science/>
SELECT ?sibling ?title WHERE {
  <https://knowyourmeme.com/memes/tung-tung-tung-sahur> rdfs:seeAlso ?sibling .
  ?sibling m4s:title ?title .
}
```

### 5. Which build is live (SPARQL)

```sparql
SELECT ?version ?build WHERE {
  <https://meme4.science/atlas/currentBuild>
      <https://meme4.science/atlas/kgBuildVersion> ?version ;
      <https://meme4.science/atlas/buildId> ?build .
}
```

## Conclusions

### Every entity in the graph is a Wikidata item, and that leaves things out

All 26,897 entities in the graph are Wikidata items, reached by 225,552
edges:

| Edge | Count |
|---|---|
| `fromTitle` | 18,258 |
| `fromTags` | 77,640 |
| `fromAbout` | 85,077 |
| `fromImage` | 44,577 |

Each item has its label. In RDF it has nothing else: no type (P31) and no
description (Neo4j also has the description). So "every person in the graph"
needs a federated query to Wikidata.

What is recognised but has no Wikidata item stays in Mongo and never reaches
the graph:

- **Frame text:** 22,614 names: 9,508 PERSON, 7,927 ORG, 2,870
  WORK_OF_ART. The frequent "people" are often meme names that spaCy read
  as people ("Hood Irony", "Dark Brandon").
- **Template images** (the 115,683 regions on kept templates):
  - printed text: 23,938 regions with no item (expected);
  - generic objects: 23,415 with no item, plus 17,151 linked but left out
    by design (only the three largest per template go in);
  - **named people and characters: 4,418 with no item**, about 2,100
    distinct names and a fifth of all named regions.

The named misses have two causes:

1. **The lexicon holds only items with a Wikipedia article**, a KYM ID or
   subclass links (17.3M items). Morty Smith, Drew Scanlon, Springtrap,
   Cereal Guy and Hoss Delgado have no entry.
2. **The linker did not commit.** Shrek is linked in 38 templates and not in
   18; the American flag in 69 and not in 61.

This is technical gap 11
([entities without a Wikidata item](Technical%20gaps%20that%20will%20eventually%20bite%20me%20in%20the%20ass/11-entities-without-wikidata-item.md)),
with the fixes and their costs.

### The graph by the numbers

Measured with `kg/metrics.py` on 2026-10-02.

**The IMKG-comparable core** (frames, entry types, tags, series, links):
the KYM row of IMKG's Table 2.

| | 5.0.1 (18 Sep) | 6.5.0 | IMKG, KYM row |
|---|---|---|---|
| Nodes | 346,439 | 364,210 | 167,662 |
| Edges | 711,277 | 738,614 | 914,941 |
| Edges, triple-equivalent | — | 921,122 | — |
| Relation types | 6 | 6 | 18 |
| Average degree | 4.11 | 4.06 | 10.91 |
| Frames | 23,882 | 24,291 | 12,585 |

IMKG is RDF, where every literal is an edge, so raw edge counts understate
MemeAtlas. The triple-equivalent row counts populated node attributes as
edges. Read that way, the 6.5.0 core matches IMKG's KYM subgraph for edges
(921k against 915k) over nearly twice the frames.

**The whole graph:**

| | 6.5.0 | IMKG, full |
|---|---|---|
| Nodes | 795,711 | 4,850,636 |
| Edges | 1,884,697 | 16,549,810 |
| Relation types | 25 | 836 |
| Average degree | 4.74 | 6.82 |

IMKG's full graph is dominated by triples it imported from Wikidata (hence
836 relation types). MemeAtlas links to Wikidata items but imports none of
their statements, so the gap is mostly that import, not coverage of memes.

**What the 6.x layers add**, per frame (24,291 frames):

| Frames with | Count | Share |
|---|---|---|
| A Wikidata link from their text | 24,174 | 99.5% |
| — from the title / tags / About | 15,245 / 22,925 / 21,693 | |
| An imgflip template | 8,368 | 34.4% |
| — whose image shows a Wikidata item | 7,793 | 32.1% |
| Events | 18,773 | 77.3% (7.6 events each on average, median 7) |
| All three | 8,158 | 33.6% |
| None of them | 65 | 0.3% |

- Of the 26,897 Wikidata items, 20,885 come only from frame text, 1,637
  only from template images, and 4,375 from both.
- 87% of the 26,868 templates show at least one linked item, and 2,616
  templates are kept by more than one frame.

**Integrity**, on the core:

- the RDF re-derived independently through the YARRRML mapping
  (`kym_kg_validate`) equals the published `graph.nt`: 7,431,581 triples
  against 7,431,585, the 4 extra being build provenance;
- no isolated nodes, duplicate edges, self-loops, dangling edges or series
  cycles;
- 2 connected components, as in 5.0.1;
- 28.3% of series parents are not frames in the corpus (9,576 `FrameStub`s);
- 18.8% of frames have no entry type, and 0.3% have no tags;
- the longest series chain is 7 steps: Bonk Cheems → Cheems → Dogelore →
  Ironic Doge Memes → Doge → Interior Monologue Captioning → Image Macros →
  Memes.

### Findings worth acting on

- **The NSFW placeholder is a hub.** KYM's cover image for NSFW content
  (`image-covers/nsfw.png`) is one image node: 2,553 frames link to it as
  their image and 3,125 events cite it. It ranks sixth in PageRank across
  the whole graph. It is a placeholder, not an image, and should not be a
  node. The real URL is in the stored page, so the parser can fix it
  ([gap 12](Technical%20gaps%20that%20will%20eventually%20bite%20me%20in%20the%20ass/12-nsfw-placeholder-image-is-a-node.md)).
- **Generic regions dominate the template images.** "man" is shown by 6,800
  templates and ranks fourth in PageRank. That comes from the rule that
  keeps the three largest generic regions per template.
- **The core metrics published with 6.5.0 were distorted, and are now
  recomputed.** Since 6.4.0 each template's imgflip page is an
  `external_ref` node, so the core counted 26,745 of them as isolated
  nodes. That lowered the core's average degree and split it into 26,747
  components. The core now counts a node other than a frame only if a core
  edge reaches it. The build's `metrics.json` was recomputed: the numbers
  above are the corrected ones.
- **Entry-type semantics:** with the 409 new frames, two more pairs of
  entry types read as near-synonyms that are never used together
  (art ~ viral-video, media-host ~ participatory-media). The complementary
  pairs are unchanged.

## How these were computed

From `airflow/dags`, with the venv's Python and `MONGODB_URI` pointing at
Mongo:

```bash
# IMKG comparison: the core (what kym_kg runs after every publish) and the whole graph
python -m modules.kg.metrics --scope core --out ../data/kg/current/metrics.json
python -m modules.kg.metrics --scope full --out ../data/kg/current/metrics_full.json

# Censuses
python -m modules.kg.census --field entry_type --output ../data/kg_census_entry_type.json
python -m modules.kg.census --field tags --output ../data/kg_census_tags.json

# Entry-type semantics (definitions and embeddings are reused when unchanged)
python -m modules.kg.semantics describe --census ../data/kg_census_entry_type.json --out ../data/kg_type_definitions.json
python -m modules.kg.semantics embed --definitions ../data/kg_type_definitions.json --out ../data/kg_type_embeddings.json
python -m modules.kg.semantics analyze --embeddings ../data/kg_type_embeddings.json \
    --census ../data/kg_census_entry_type.json --out ../data/kg_type_semantics_report.json
```

The RDF re-derivation check runs as the `kym_kg_validate` DAG: weekly
against the live build, or by hand with `{"build_id": "…"}`.

The 5.0.1-era files are kept in `data/archive/2026-10-02_pre-6.5.0/`.
