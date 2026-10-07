# Querying MemeAtlas, and what 7.1.0 answers

The live graph is **KG 7.1.0**, build `kg_20261007T101123Z_manual`, published
2026-10-07. It adds two layers to 7.0.0, so that the queries of IMKG's paper
run on MemeAtlas as they are written: what each entry's own image shows, and
the Wikidata statements of every item the graph links to. It is also the first
graph that holds each Know Your Meme entry once (gap 14).

- **Neo4j Browser:** `http://<host>:8080/browser/` (the property graph; needs
  the neo4j password).
- **SPARQL:** `http://<host>:8080/sparql/` (read-only; the default graph is the
  live build). Fuseki has no query timeout: keep to bounded queries like the
  ones below.

## IMKG's four questions

IMKG's paper (Tommasini, Ilievski, Wijesiriwardene, ESWC 2023) shows what its
graph makes possible with four queries (its Table 4), written as KGTK Kypher
match clauses. The full queries and their answers are in IMKG's notebook
`KGTK Use Cases.ipynb`. None of them ran on MemeAtlas 7.0.0: three need
Wikidata statements (is a human, is a film, sex or gender) and one needs what
the meme's own image shows. 7.1.0 adds both, in IMKG's terms
(`m4s:fromImage` on the meme, `wdt:P31`, `wdt:P21`), so each query
translates word for word.

| Question | IMKG (2023) | MemeAtlas 7.1.0 |
|---|---|---|
| Memes showing SpongeBob | 130 | 230 (353 counting their templates) |
| The most meme-able people | Trump (145), Kyle Craven (72), Kanye West (56) | Trump (951), Biden (201), Musk (165) |
| Memes based on films | 413 | 808 (450 films) |
| People by sex or gender | male 10,333, female 2,798 | male 5,683, female 2,077 |

IMKG's column is its notebook's output. The paper's Table 4 prints 51 film
memes and 2,865 women; its text says 413.

Each question below is given three ways: IMKG's Kypher as published, SPARQL
for Fuseki, and Cypher for Neo4j Browser. The SPARQL and the Cypher give the
same answers (checked on 7.1.0, 2026-10-07).

### 1. Memes that show SpongeBob

IMKG (Kypher):

```
(h)-[:`m4s:fromImage`]->(:Q83279),
(h)-[:`rdf:type`]->(:`kym:Meme`)
return: count(distinct h)
```

SPARQL:

```sparql
PREFIX m4s: <https://meme4.science/>
PREFIX kym: <https://knowyourmeme.com/memes/>
PREFIX wd:  <http://www.wikidata.org/entity/>
SELECT (COUNT(DISTINCT ?h) AS ?memes) WHERE {
  ?h m4s:fromImage wd:Q83279 ;
     a kym:Meme .
}
```

Cypher:

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (h:Frame {build_id: cur.build_id, category: 'meme'})-[:fromImage]->(:WikidataEntity {qid: 'Q83279'})
RETURN count(DISTINCT h) AS memes
```

**230 memes**, against IMKG's 130. IMKG read each meme's picture with Google
Vision; MemeAtlas reads it with the vision model that reads the templates
(`qwen3-vl:32b`) and links what it names with the same linker as the text.
The model names the character actually shown. Of IMKG's three examples,
*Are You Feeling It Now Mr. Krabs* shows Mr. Krabs (SpongeBob is in its
templates), *Bold and Brash* shows Squidward, and *Big Meaty Claws* has no
frame in 7.1.0.

MemeAtlas also reads each meme's imgflip templates. Through them 229 memes
show SpongeBob; counting both, 353:

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (h:Frame {build_id: cur.build_id, category: 'meme'})
WHERE EXISTS { (h)-[:fromImage]->(:WikidataEntity {qid: 'Q83279'}) }
   OR EXISTS { (h)-[:hasTemplate]->(:Template)-[:fromImage]->(:WikidataEntity {qid: 'Q83279'}) }
RETURN count(h) AS memes
```

### 2. The most meme-able people

IMKG (Kypher):

```
(h)-[]->(person),
(h)-[:`rdf:type`]->(:`kym:Meme`),
(person)-[:P31]->(:Q5)
return: person, count(h) as c   order by c desc   limit 3
```

SPARQL:

```sparql
PREFIX kym:  <https://knowyourmeme.com/memes/>
PREFIX wd:   <http://www.wikidata.org/entity/>
PREFIX wdt:  <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?person ?name (COUNT(?h) AS ?c) WHERE {
  ?person wdt:P31 wd:Q5 .
  ?h ?p ?person ;
     a kym:Meme .
  OPTIONAL { ?person rdfs:label ?name }
}
GROUP BY ?person ?name
ORDER BY DESC(?c)
LIMIT 3
```

Cypher:

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (h:Frame {build_id: cur.build_id, category: 'meme'})-[r]->(person:WikidataEntity)
      -[:P31]->(:WikidataEntity {qid: 'Q5'})
RETURN person.qid, person.label, count(r) AS c, count(DISTINCT h) AS memes
ORDER BY c DESC LIMIT 10
```

The SPARQL takes about 3 minutes in Fuseki (the open `?p` has to be looked up
for every human); the Cypher, a second. Like IMKG, `c` counts every arrow from a
meme to the person, so a meme that
names Trump in its title, its tags and its picture counts three times.

| Person | c | Memes |
|---|---|---|
| Donald Trump | 951 | 393 |
| Joe Biden | 201 | 88 |
| Elon Musk | 165 | 68 |
| Adolf Hitler | 159 | 73 |
| Jesus Christ | 148 | 71 |
| Drake | 147 | 65 |
| Kanye West | 143 | 72 |
| Barack Obama | 118 | 69 |
| Taylor Swift | 112 | 40 |
| Hillary Clinton | 106 | 50 |

Kyle Craven, IMKG's second, is the face of *Bad Luck Brian*. MemeAtlas links
him 6 times, from two entries. Its vision model reads Bad Luck Brian's photo
as "man". IMKG's 72 came from Google Vision, which matches a photo to the web
pages that name it.

### 3. Memes based on films

IMKG (Kypher):

```
(h)-[:`m4s:fromAbout`]->(t),
(t)-[:P31]->(:Q11424)
return: count(distinct h)
```

SPARQL:

```sparql
PREFIX m4s: <https://meme4.science/>
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT (COUNT(DISTINCT ?h) AS ?frames) (COUNT(DISTINCT ?t) AS ?films) WHERE {
  ?t wdt:P31 wd:Q11424 .
  ?h m4s:fromAbout ?t .
}
```

Cypher:

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (h:Frame {build_id: cur.build_id})-[:fromAbout]->(t:WikidataEntity)
      -[:P31]->(:WikidataEntity {qid: 'Q11424'})
RETURN count(DISTINCT h) AS frames, count(DISTINCT t) AS films
```

**808 entries** whose About names one of 450 films, against IMKG's 413 (as in
IMKG's query, any entry, not only memes). The most named: *Avengers: Endgame*
(41 entries), *Avengers: Infinity War* (33), *Star Wars: Episode IV* (31),
*Joker* (24), *The Batman* (21), *Barbie* (16). IMKG's first example holds:
*Hitler's Downfall Parodies* is about *Downfall*. Its second does not:
*Cat Transcendence*'s About links cat, Instagram and flower, not the film
*The Prophecy*.

### 4. People by sex or gender

IMKG (Kypher):

```
()-[]->(person),
(person)-[:P21]->(gender)
return: gender, count(distinct person)
```

SPARQL:

```sparql
PREFIX wdt:  <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?gender ?name (COUNT(DISTINCT ?person) AS ?people) WHERE {
  ?person wdt:P21 ?gender .
  FILTER EXISTS { ?x ?p ?person }
  OPTIONAL { ?gender rdfs:label ?name }
}
GROUP BY ?gender ?name
ORDER BY DESC(?people)
```

Cypher:

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (person:WikidataEntity {build_id: cur.build_id})-[:P21]->(gender:WikidataEntity)
WHERE EXISTS { ()-->(person) }
RETURN gender.qid, gender.label, count(DISTINCT person) AS people
ORDER BY people DESC
```

| Sex or gender | MemeAtlas 7.1.0 | IMKG |
|---|---|---|
| male | 5,683 | 10,333 |
| female | 2,077 | 2,798 |
| male organism | 92 | 170 |
| trans woman | 42 | 20 |
| non-binary | 41 | 14 |
| female organism | 31 | 67 |

Then 19 more values, each held by fewer than 40 people. The share is close to
IMKG's: 73% of the men and women are men (IMKG 79%). MemeAtlas has fewer
people because it imports less of Wikidata. It takes the statements of every
item its memes link to, one step out. IMKG also took every statement to and
from each meme's own Wikidata item, and the statements between items already
in its graph.

### The most common items and relations (the paper's Table 3)

The ten items most often found by each source.

| IMKG `fromImage` | MemeAtlas `fromImage` (the meme's picture) | IMKG `fromAbout` | MemeAtlas `fromAbout` |
|---|---|---|---|
| font | man (6,780) | image macro | TikTok (2,901) |
| image | woman (2,908) | 4chan | Twitter (2,798) |
| Internet meme | microphone (458) | Internet meme | image macro (1,615) |
| Know Your Meme | girl (439) | catchphrase | YouTube (1,558) |
| meme | cat (410) | YouTube | catchphrase (1,471) |
| art | dog (373) | parody | song (1,345) |
| happiness | car (327) | Japanese | video game (1,176) |
| gesture | hand (326) | Tumblr | X (1,072) |
| illustration | Donald Trump (309) | meme | parody (1,005) |
| fictional character | boy (274) | United States of America | viral video (963) |

Google Vision labels what kind of picture it is ("font", "image",
"illustration"). The vision model in MemeAtlas lists what is in it, and its
generic regions ("man", "woman") are kept only for the three largest per
picture. The About columns agree on the platforms and genres.

The paper also names the most common Wikidata relations: P31 *instance of*
(59k), P136 *genre* (34k), P106 *occupation* (28k). In MemeAtlas: P31
(40,434), P106 (30,039), P161 *cast member* (27,226), P1343 *described by
source* (21,938), P527 *has part* (19,578), P136 (17,749).

## What 7.1.0 changed

Three changes, all additive for queries except Doge's address:

- **Each entry once (gap 14).** KYM moved 811 entries into its "sensitive"
  section and left the old addresses up, so 813 entries were two frames in
  7.0.0. In 7.1.0 a frame is the address KYM gives the entry today, and its
  old addresses are listed on it: 834 frames carry 835 old addresses in
  `also_at`. Doge's frame is now `https://knowyourmeme.com/sensitive/memes/doge`
  (IMKG and 7.0.0: `/memes/doge`). Look a frame up by either address:

  ```cypher
  WITH 'https://knowyourmeme.com/memes/doge' AS url
  MATCH (cur:KGPointer {name: 'current'})
  MATCH (f:Frame {build_id: cur.build_id}) WHERE f.id = url OR url IN f.also_at
  RETURN f.id, f.also_at
  ```

  RDF has no `also_at`; use the new IRI. Saying the old IRI is the same entry
  (`owl:sameAs`) is a modelling decision, still open.
- **What each entry's own image shows.** The vision model read 23,438 of the
  23,477 entries' images (34 failed twice and are logged). What it names is
  linked to Wikidata like the text: 39,544 `fromImage` edges from frames to
  8,951 items, on 20,541 frames. Doge's picture shows a dog and a flower.
  In Neo4j `fromImage` now starts from a `Frame` as well as a `Template`.
- **Wikidata's statements.** Every linked item brings its truthy, item-valued
  statements, from the same dump as the lexicon (14 September 2026): 696,194
  statements over 1,168 properties. Their values are nodes too, with labels,
  so the graph's Wikidata items grew from 26,897 to 272,304 (28,374 of them
  linked from a frame or a template). In Neo4j an edge's type is the
  property, as in IMKG: `(p)-[:P31]->(:WikidataEntity {qid: 'Q5'})`. In RDF
  it is `wdt:P31`.

| | 7.0.0 | 7.1.0 |
|---|---|---|
| Nodes | 795,711 | 1,034,968 |
| Edges | 2,564,089 | 3,185,248 |
| RDF triples | 8,790,369 | 9,672,008 |
| Relation types | 26 | 26, and 1,168 Wikidata properties |
| Frames | 24,291 | 23,477 |
| Wikidata items | 26,897 | 272,304 |

Counts that hang off frames fell with the duplicates: events 142,787 to
137,496, sibling pairs 679,392 to 631,311.

## How the graph sits in Neo4j

Every node has the label `KGNode` plus one for its kind (`Frame`, `FrameStub`,
`WikidataEntity`, `Template`, `Event`, …). Every node and relationship
carries the `build_id` of its build. Older builds are kept beside the live
one, for rollback. **Always resolve the live build first**,
through `(:KGPointer {name: 'current'})`, as every query below does.

| Relationship | From → to | Properties worth reading |
|---|---|---|
| `fromTitle`, `fromTags`, `fromAbout` | `Frame` → `WikidataEntity` | `relevance_bases` (why curation kept it), `mention_texts`, `link_scores` |
| `hasTemplate` | `Frame` → `Template` | `template_scores`, `template_matches` |
| `fromImage` | `Frame` or `Template` → `WikidataEntity`: what the entry's own image (7.1.0) or the template's image shows | `depiction_kinds`, `bounding_boxes`, `mention_texts` |
| `P31`, `P21`, … one type per Wikidata property (7.1.0) | `WikidataEntity` → `WikidataEntity`: the item's truthy statements, one step out | — |
| `partOfSeries` | `Frame` → its series parent (`Frame`, or `FrameStub` when the parent is not in the corpus) | — |
| `sharesSameSeries` | `Frame` — `Frame`, two frames with the same series parent (7.0.0). Stored once per pair, so match it without an arrow: `(f)-[:sharesSameSeries]-(s)` | — |
| `citesMediaFrame` | `Frame` → a KYM entry it links to, on its page or in its references (`Frame`, or `FrameStub` when not in the corpus). Called `relatesToMeme` until 7.0.0 | `anchor_texts`, `in_sections`, `citation_texts` |
| `citesExternal` | `Frame` → any other page it links to, on another site or on KYM (`ExternalRef`) | the same |

A `Frame` whose entry KYM moved lists its old addresses in `also_at` (7.1.0).

A relationship's properties are lists, one entry per mention, index-aligned:
position *i* of `mention_texts` and of `relevance_bases` describe the same
mention. A `FrameStub` has no title, only its KYM URL (`id`).

### Colours and sizes in Neo4j Browser

Out of the box every node is the same colour. The Browser colours a node by
the label highest in its styling list, and `KGNode`, which every node has,
starts at the top. Import the shared stylesheet once:
in the graph styling panel choose **Upload GraSS styles**, then pick
[`dags/kg_config/neo4j_browser.grass`](dags/kg_config/neo4j_browser.grass) and
**Import**. It puts `KGNode` at the bottom of the list, so each kind's own
style wins. The style is kept per browser; change any colour or size in the
same panel.

| Kind | Colour | Size | Caption |
|---|---|---|---|
| `Frame` | blue `#3987e5` | largest | title |
| `FrameStub` | dark blue `#184f95` (a frame not in the corpus) | small | KYM URL |
| `WikidataEntity` | orange `#d95926` | large | label |
| `Template` | aqua `#199e70` | large | name |
| `Event` | violet `#4a3aa7` | medium | the sentence it was read from |
| `EntryTypeConcept`, `OriginConcept` | pink `#e87ba4` | medium | label |
| `TagConcept`, `RegionConcept`, `BadgeConcept` | pink `#e87ba4` | small | label |
| `Image`, `ExternalRef` | light grey `#b5b4ad` | smallest | URL |

Frame, Wikidata item and template link to each other, so they take the
three hues that stay apart from each other under protanopia, deuteranopia and
normal vision, on both the light and the dark theme. With events (violet) and
concepts (pink) the five hues still pass every pair on the light theme, the
Browser's default (checked with the data-viz palette validator; the same
colours as the talk's slides). On the dark theme the violet sits below 3:1
against the background, so there an event is told by its caption. The
concepts share one pink and are told apart by size and caption, as are images
and external links.

## More queries

### One frame as a graph

For Neo4j Browser, which draws every path returned. It shows the frame's
Wikidata items, what its picture shows, its templates and what each
template's image shows, its chain of series parents (up to five levels), and
the other frames of its series. Change the title on the first line. (A frame
of a big series has hundreds of siblings, TikTok's 542; drop that line for
those.)

```cypher
WITH 'Tung Tung Tung Sahur' AS title
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id, label: title})
RETURN f,
  COLLECT { MATCH p = (f)-[:fromTitle|fromTags|fromAbout]->(:WikidataEntity) RETURN p } AS frame_entities,
  COLLECT { MATCH p = (f)-[:fromImage]->(:WikidataEntity) RETURN p } AS picture,
  COLLECT { MATCH p = (f)-[:hasTemplate]->(:Template)-[:fromImage]->(:WikidataEntity) RETURN p }
    + COLLECT { MATCH p = (f)-[:hasTemplate]->(t:Template) WHERE NOT (t)-[:fromImage]->() RETURN p } AS templates,
  COLLECT { MATCH p = (f)-[:partOfSeries*1..5]->(:KGNode) RETURN p } AS series_parents,
  COLLECT { MATCH p = (f)-[:sharesSameSeries]-(:Frame) RETURN p } AS siblings
```

For "Tung Tung Tung Sahur" this draws 22 Wikidata links, the 2 items its
picture shows, 10 templates with the items their images show, the series
chain Italian Brainrot / AI Italian Animals → Brain Rot / Brainrot → Internet
Slang → The Internet, and its 13 siblings in Italian Brainrot.

### One row per frame

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
       COLLECT { MATCH (f)-[:fromImage]->(e:WikidataEntity) RETURN e.qid + ' ' + e.label } AS in_its_picture,
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

### A frame's siblings, and which of them its page links to

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
pages and the series are different relations: only 3,448 of the
631,311 sibling pairs are linked by `citesMediaFrame`.

### A frame's series, as IMKG queries it (SPARQL)

IMKG's own term for an entry's siblings, `rdfs:seeAlso`, unchanged.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX m4s:  <https://meme4.science/>
SELECT ?sibling ?title WHERE {
  <https://knowyourmeme.com/memes/tung-tung-tung-sahur> rdfs:seeAlso ?sibling .
  ?sibling m4s:title ?title .
}
```

### What a linked item is, from Wikidata (Cypher)

The statements of one item, with the property's id as the edge type:

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (e:WikidataEntity {build_id: cur.build_id, qid: 'Q22686'})-[r]->(v:WikidataEntity)
WHERE type(r) =~ 'P[0-9]+'
RETURN type(r) AS property, collect(v.label)[..5] AS values, count(*) AS n
ORDER BY n DESC LIMIT 15
```

### Which build is live (SPARQL)

```sparql
SELECT ?version ?build WHERE {
  <https://meme4.science/atlas/currentBuild>
      <https://meme4.science/atlas/kgBuildVersion> ?version ;
      <https://meme4.science/atlas/buildId> ?build .
}
```

## Conclusions

### Every entity is a Wikidata item, and Wikidata now says what it is

The 28,374 items that frames and templates link to are all Wikidata items,
reached by 258,851 edges:

| Edge | From | Count |
|---|---|---|
| `fromTitle` | frame | 17,668 |
| `fromTags` | frame | 74,870 |
| `fromAbout` | frame | 82,192 |
| `fromImage` | frame (7.1.0) | 39,544 |
| `fromImage` | template | 44,577 |

Until 7.0.0 an item had only its label (and, in Neo4j, its description), so
"every person in the graph" needed a federated query to Wikidata. From 7.1.0
each item brings its statements: 5,504 items are humans
(`(e)-[:P31]->(:WikidataEntity {qid: 'Q5'})`).

What is recognised but has no Wikidata item stays in Mongo and never reaches
the graph:

- **Frame text** (measured on 6.5.0): 22,614 names: 9,508 PERSON, 7,927 ORG,
  2,870 WORK_OF_ART. The frequent "people" are often meme names that spaCy
  read as people ("Hood Irony", "Dark Brandon").
- **Template images** (6.5.0, the 115,683 regions on kept templates):
  - printed text: 23,938 regions with no item (expected);
  - generic objects: 23,415 with no item, plus 17,151 linked but left out
    by design (only the three largest per template go in);
  - **named people and characters: 4,418 with no item**, about 2,100
    distinct names and a fifth of all named regions.
- **The entries' own images** (7.1.0): of 58,967 things named in 23,438
  pictures, 7,203 found no item.

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

Measured with `kg/metrics.py` after each publish.

**The IMKG-comparable core** (frames, entry types, tags, series, links):
the KYM row of IMKG's Table 2.

| | 5.0.1 (18 Sep) | 6.5.0 (2 Oct) | 7.1.0 (7 Oct) | IMKG, KYM row |
|---|---|---|---|---|
| Nodes | 346,439 | 364,210 | 363,393 | 167,662 |
| Edges | 711,277 | 738,614 | 711,616 | 914,941 |
| Edges, triple-equivalent | — | 921,122 | 891,677 | — |
| Relation types | 6 | 6 | 6 | 18 |
| Average degree | 4.11 | 4.06 | 3.92 | 10.91 |
| Frames | 23,882 | 24,291 | 23,477 | 12,585 |

IMKG is RDF, where every literal is an edge, so raw edge counts understate
MemeAtlas. The triple-equivalent row counts populated node attributes as
edges. Read that way, the 7.1.0 core comes close to IMKG's KYM subgraph for
edges (892k against 915k) over 1.87 times the frames. 7.1.0 is a little
smaller than 6.5.0 because it holds each entry once.

**The whole graph:**

| | 7.1.0 | IMKG, full |
|---|---|---|
| Nodes | 1,034,968 | 4,850,636 |
| Edges | 3,185,248 | 16,549,810 |
| Relation types | 26, and 1,168 Wikidata properties | 836 |
| Average degree | 6.16 | 6.82 |

IMKG's full graph is mostly imgflip: 1.3 million memes and their captions,
and the triples it imported from Wikidata (hence 836 relation types).
MemeAtlas reads imgflip's templates, not its memes. Since 7.1.0 it imports
the statements of every item it links to, which brings its relation types
past IMKG's.

**What the layers add**, per frame (23,477 frames):

| Frames with | Count | Share |
|---|---|---|
| A Wikidata link from their text | 23,359 | 99.5% |
| An imgflip template | 8,001 | 34.1% |
| A Wikidata item in their own picture (7.1.0) | 20,541 | 87.5% |
| Events | 18,096 | 77.1% (7.6 events each on average) |
| All four | 6,822 | 29.1% |
| None of them | 12 | 0.05% |

- Of the 28,374 linked Wikidata items, 17,271 come only from frame text,
  3,115 only from pictures (1,477 of them only from the entries' own
  pictures, new in 7.1.0), and 7,988 from both.
- 87% of the 26,868 templates show at least one linked item, and 2,616
  templates are kept by more than one frame (6.5.0).

**Integrity**, on the core:

- the RDF re-derived independently through the YARRRML mapping
  (`kym_kg_validate`) equals the published `graph.nt`: 9,672,004 triples
  against 9,672,008, the 4 extra being build provenance;
- no isolated nodes, duplicate edges, self-loops, dangling edges or series
  cycles;
- 2 connected components, as since 5.0.1;
- 26.0% of series parents are not frames in the corpus (1,060 of 4,074);
- 18.8% of frames have no entry type, and 0.3% have no tags;
- the longest series chain is 7 steps: Bonk Cheems → Cheems → Dogelore →
  Ironic Doge Memes → Doge → Interior Monologue Captioning → Image Macros →
  Memes.

### Findings worth acting on

- **The NSFW placeholder is a hub.** KYM's cover image for NSFW content
  (`image-covers/nsfw.png`) is one image node: 2,484 frames link to it as
  their image and 3,044 events cite it (7.1.0). It is a placeholder, not an
  image, and should not be a node. The real URL is in the stored page, so the
  parser can fix it
  ([gap 12](Technical%20gaps%20that%20will%20eventually%20bite%20me%20in%20the%20ass/12-nsfw-placeholder-image-is-a-node.md)).
- **Generic regions dominate the images.** "man" is shown by 6,800 templates
  and 6,780 entries' own pictures. That comes from the rule that keeps the
  three largest generic regions per picture.
- **Old addresses in RDF.** A moved entry's old IRI (Doge's `/memes/doge`,
  IMKG's) is listed in Neo4j (`also_at`) but not in RDF. `owl:sameAs` would
  say it; it is a modelling decision, still open.
- **Entry-type semantics** (6.5.0): two pairs of entry types read as
  near-synonyms that are never used together (art ~ viral-video,
  media-host ~ participatory-media). The complementary pairs are unchanged.

## How these were computed

The four questions and Table 3, from `airflow/dags` with the venv's Python:
the queries above, in Neo4j Browser or against Fuseki. The rest:

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

