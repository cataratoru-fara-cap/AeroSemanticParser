# The queries behind the figures

The graph figures and numbers in the deck come from these queries, run by
`scripts/extract.py` against one build (the live one by default). Below they are
written for **Neo4j Browser** (`http://<host>:8080/browser/`): each resolves the
live build through the `KGPointer` first, as `KG_QUERIES.md` explains. Import
`airflow/dags/kg_config/neo4j_browser.grass` first, so the colours match the
slides.

They refine the queries of `airflow/KG_QUERIES.md`: the frame is a parameter, the
series is followed to any depth, and the story, templates and siblings are drawn
together.

**Doge's address.** IMKG and KG 7.0.0 have Doge at `/memes/doge`. From 7.1.0 a
frame is the address KYM gives the entry today, `/sensitive/memes/doge`, and
lists the old address as `also_at` (gap 14). So every query below finds Doge by
either address: `WHERE f.id = url OR url IN f.also_at`. (On 7.0.0 Neo4j warns
that `also_at` does not exist yet; the query still answers.)

## Doge as a graph (chapter 7, "everything we learned")

```cypher
WITH 'https://knowyourmeme.com/memes/doge' AS url
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id}) WHERE f.id = url OR url IN f.also_at
RETURN f,
  COLLECT { MATCH p = (f)-[:fromTitle|fromTags|fromAbout]->(:WikidataEntity) RETURN p } AS items,
  COLLECT { MATCH p = (f)-[:hasTemplate]->(:Template)-[:fromImage]->(:WikidataEntity) RETURN p } AS templates,
  COLLECT { MATCH p = (f)-[:partOfSeries*1..10]->(:KGNode) RETURN p } AS ancestors,
  COLLECT { MATCH p = (:Frame)-[:partOfSeries]->(f) RETURN p } AS children,
  COLLECT { MATCH p = (f)-[:sharesSameSeries]-(:Frame) RETURN p } AS siblings,
  COLLECT { MATCH p = (f)-[:hasEvent]->(:Event) RETURN p } AS events,
  COLLECT { MATCH p = (f)-[:hasEntryType]->() RETURN p } AS types
```

The whole neighbourhood, by kind (the "379 edges" figure):

```cypher
WITH 'https://knowyourmeme.com/memes/doge' AS url
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id}) WHERE f.id = url OR url IN f.also_at
MATCH (f)-[r]-(x)
RETURN type(r) AS relation, startNode(r) = f AS outgoing, x.kind AS kind, count(*) AS n
ORDER BY n DESC
```

## Doge's family (chapter 9)

```cypher
WITH 'https://knowyourmeme.com/memes/doge' AS url
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id}) WHERE f.id = url OR url IN f.also_at
MATCH p = (d:Frame {build_id: cur.build_id})-[:partOfSeries*1..10]->(f)
RETURN [n IN nodes(p) | n.label] AS chain
ORDER BY length(p) DESC
```

## Series and links are different relations

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (a:Frame {build_id: cur.build_id})-[:sharesSameSeries]->(c)
RETURN count(*) AS pairs,
       sum(CASE WHEN EXISTS { (a)-[:citesMediaFrame]->(c) } OR EXISTS { (c)-[:citesMediaFrame]->(a) }
                THEN 1 ELSE 0 END) AS linked_by_page
```
679,392 pairs, 3,602 linked (KG 7.0.0).

## What each entry carries

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id})
WITH f, EXISTS { (f)-[:fromTitle|fromTags|fromAbout]->() } AS wd,
        EXISTS { (f)-[:hasTemplate]->() } AS tpl, EXISTS { (f)-[:hasEvent]->() } AS ev
RETURN count(*) AS frames, sum(CASE WHEN wd THEN 1 ELSE 0 END) AS with_wikidata,
       sum(CASE WHEN tpl THEN 1 ELSE 0 END) AS with_template, sum(CASE WHEN ev THEN 1 ELSE 0 END) AS with_events,
       sum(CASE WHEN wd AND tpl AND ev THEN 1 ELSE 0 END) AS with_all,
       sum(CASE WHEN NOT wd AND NOT tpl AND NOT ev THEN 1 ELSE 0 END) AS with_none
```

## When and where memes are born

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id})-[:hasOrigin]->(o)
WHERE o.label IN ['4chan', 'youtube', 'tumblr', 'reddit', 'twitter', 'tiktok']
RETURN f.year AS year, o.label AS platform, count(*) AS memes
ORDER BY year, platform
```

## What the graph talks about most

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id})-[:fromTitle|fromTags|fromAbout]->(e:WikidataEntity)
RETURN e.qid, e.label, count(DISTINCT f) AS entries ORDER BY entries DESC LIMIT 10;

MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id})-[:hasTemplate]->(t:Template)
RETURN t.label, t.template_id, count(DISTINCT f) AS entries ORDER BY entries DESC LIMIT 10;
```

## The graph of the graph (chapter 7)

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (a:KGNode {build_id: cur.build_id})-[r]->(c:KGNode)
RETURN a.kind AS from, type(r) AS relation, c.kind AS to, count(*) AS edges
ORDER BY edges DESC
```
About a minute on the whole graph.

## One entry, one address (chapter 2, gap 14)

Two frames with one title — the duplicates, on KG 7.0.0:

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id}) WHERE f.id CONTAINS '/memes/'
WITH f.label AS title, collect(f.id) AS addresses WHERE size(addresses) > 1
RETURN count(*) AS entries_twice, collect(addresses)[..5] AS examples
```
813 on KG 7.0.0; 0 from 7.1.0. From 7.1.0, the frames that absorbed an old
address, and where their links now arrive:

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (f:Frame {build_id: cur.build_id}) WHERE f.also_at IS NOT NULL
RETURN count(f) AS frames, sum(size(f.also_at)) AS old_addresses,
       sum(CASE WHEN f.id CONTAINS '/sensitive/' THEN 1 ELSE 0 END) AS kept_in_sensitive
```
The decisions themselves are in Mongo (`urls.duplicate_of`), on the dashboard's
Scrape page, and in the `kym_scrape` run summary.

## The vocabulary at work (chapter 7, "Give it meaning")

Where memes are born, by kind of platform. The ontology gives the words: IMKG's
class `kym:Meme` and our `mk:hasOrigin`, declared in `memeatlas.ttl`. The origin
taxonomy (`origin_taxonomy.yaml`) does the grouping: Twitter, Tumblr and
Instagram are each a `social-network`, through `rdfs:subClassOf`; without it the
question needs a hand-written list of sites. Run on 7.0.0: social networks 5,567
memes, video platforms 3,699, imageboards 818, meme sites 282 (12 kinds). About
14 s in Fuseki.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mk:   <https://meme4.science/atlas/>
PREFIX kym:  <https://knowyourmeme.com/memes/>
SELECT ?kind (COUNT(DISTINCT ?meme) AS ?memes)
       (GROUP_CONCAT(DISTINCT ?siteName; separator=", ") AS ?platforms)
WHERE {
  ?site rdfs:subClassOf ?kind .          # the taxonomy: a platform under its kind
  ?kind a skos:Concept .
  ?meme mk:hasOrigin ?site ;             # our term
        a kym:Meme .                     # IMKG's class
  ?site skos:prefLabel ?siteName .
}
GROUP BY ?kind
ORDER BY DESC(?memes)
```

The same in Neo4j Browser, where the taxonomy is the `subTypeOf` edge (0.1 s):

```cypher
MATCH (cur:KGPointer {name: 'current'})
MATCH (site:OriginConcept {build_id: cur.build_id})-[:subTypeOf]->(kind:OriginConcept)
MATCH (meme:Frame {category: 'meme'})-[:hasOrigin]->(site)
RETURN kind.label AS kind, count(DISTINCT meme) AS memes,
       collect(DISTINCT site.label) AS platforms
ORDER BY memes DESC
```

## SPARQL, as IMKG would ask (chapter 9, "Try it")

At `http://<host>:8080/sparql/` (the default graph is the live build). RDF has
no `also_at`: from 7.1.0 use Doge's new IRI,
`<https://knowyourmeme.com/sensitive/memes/doge>`.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX m4s:  <https://meme4.science/>
SELECT ?sibling ?title WHERE {
  <https://knowyourmeme.com/memes/doge> rdfs:seeAlso ?sibling .
  ?sibling m4s:title ?title .
}
```

Fuseki has no query timeout: keep SPARQL demos to small, bound queries like this
one, and never run a heavy one while a graph build is loading or compacting.
