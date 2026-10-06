# Chapter 4 — Link: names become Wikidata items, and only those that matter stay (≈7 min)

DAGs: `kym_entities` → `kym_entity_curation`. Modules: `kg/wikidata.py` (the
lexicon), `kg/entities.py` (recognition and linking), `kg/curation.py` (rules and
the judge), `kg/entity_review.py` (measurement); stores `entity_store.py`,
`entity_curation_store.py`. Config: `kg_config/entity_senses.yaml`,
`kg_config/entity_curation.yaml`.

## The story
The computer underlines the names in the text and looks each one up in
Wikidata, where every thing has a worldwide ID. Recognising names is the easy
part; deciding which ones *matter to the meme* is the hard part, and it was
measuring that changed the design.

## Doge at this step — real decisions
| mention | Wikidata item | kept? | why |
|---|---|---|---|
| title "Doge" | Q15613810 Doge (Internet meme) | kept | the title; also its KYM ID in Wikidata → score 1.0 |
| tag/About "Shiba Inu(s)" | Q39315 dog breed | kept | a tag and the text agree |
| "Kabosu" | Q23486479 Japanese dog, meme celebrity | kept | a tag and the text agree |
| "Tumblr", "Reddit" | platforms | kept | a platform |
| tag "dogecoin" | Q15377916 | kept | a whole tag names it |
| "monologues" | Q261197 monologue | kept | the judge: its format |
| tag "doges" | Q202691 doge, head of state in Italian city-states | dropped | judge: wrong sense |
| "Sato" (Atsuko Sato) | Q7426333 Puerto Rican slang for a mixed-breed dog | dropped | judge: wrong sense |
| "Donald Trump" | Q22686 | dropped | judge: incidental (DOGE, the US department) |
| "meme", "internet", "iconography", "plans" | generic items | dropped | judge: incidental |

35 mentions found for Doge, 23 kept, each with its reason.

## Numbers
- 305,359 mentions in 24,242 entries → 31,085 distinct Wikidata items. By field:
  About 191,060 · tags 95,822 · title 18,477. By method: noun chunk 124,166 ·
  tag 95,822 · NER 60,457 · proper-noun run 19,952 · KYM ID 2,940 · title 2,022.
- Curation keeps 209,827 (68.7%), drops 95,532.
  - Kept by: a tag and the text agree 54,052 · LLM judge 42,114 · platform 26,623 ·
    the title agrees 26,478 · a whole tag names it 23,207 · named by the title
    18,477 · a meme format 14,178 · the meme's own item 4,698.
  - Dropped by: LLM judge 67,455 · too generic 21,375 · deny list (item) 3,454 ·
    deny list (class) 3,248. Judge roles: incidental 48,930 · subject 24,214 ·
    source 13,701 · wrong sense 7,959 · format 4,330 · platform 781.
- The lexicon: 17.3M Wikidata items, 22.6M names, built from the dated dump
  wikidata-20260914 (156 GB, 121.7M lines) into a 3.3 GB SQLite file in about an
  hour. Filters: at least one sitelink, English and multilingual labels, ten
  excluded classes (Wikimedia pages such as disambiguations and categories,
  scholarly articles).

## Design decisions — the why
- **At home, not a web service.** IMKG used DBpedia Spotlight (remote, rate
  limited, answers drift) then mapped DBpedia → Wikidata. MemeAtlas: spaCy
  (`en_core_web_sm`) finds candidate spans — NER, noun chunks and their
  suffixes, proper-noun runs, the whole title, each whole tag — and a local
  lexicon links them. Same dump, same answer; no quota; no second KB between.
  (NOTES.md records the suggestion to download all of Wikidata rather than call
  an API.)
- **Evidence on every link.** Score, method, NER label, and (after curation) the
  relevance basis. In RDF these are RDF-star annotations on the link itself
  (`<< kym:doge m4s:fromTags wd:Q144 >> mk:linkScore 0.616`).
- **The meme's own item** via Wikidata's Know Your Meme property (P13484, the
  slug): certain, score 1.0, and it wins over every other span that could name it.
- **IMKG's predicates verbatim:** `m4s:fromAbout`, `m4s:fromTags`; the title
  link is MemeAtlas's `mk:fromTitle` (IMKG never linked titles).
- **Threshold 0.45** (linker 1.1.0), lowered from 0.50 after measuring.

## Measuring changed the design (gap 09)
- First full run (2026-09-23): 282,914 links. 59% of About links came from spans
  with no proper noun — common-noun concepts ("popularity", "man",
  "photograph"). Some hubs were *wrong*: KYM's stock phrase "X is a series of …"
  linked the mathematical series (Q170198) 5,531 times; "game" linked games in
  general where KYM means video games.
- Fixes: a curated sense list (linker 1.2.0), then curation as its own stage —
  rules first (first match wins), then ministral-3:14b judges the rest, asked
  with two differently worded prompts.
- The bar was set **before** measuring: of kept links ≥ 0.85 relevant, of dropped
  ≤ 0.15. A first review (200 links) failed on the dropped side (0.25), which
  produced curation 1.1.0. Now: **0.875 kept / 0.126 dropped**.
- **Say the caveat:** those figures are projected over the corpus from 353
  labelled items; the holdout review follows the full judge run.

## If someone asks
- *Why not a big neural linker (REL, ReFinED, BLINK)?* Considered (NOTES.md);
  the lexicon approach is reproducible and fast, and curation fixed precision
  where it mattered. A heavier linker remains an option if recall tops out.
- *What about names Wikidata does not have?* Gap 11: 22,614 recognised names
  in frame text have no item (Hood Irony, Dark Brandon read as people…), and a
  fifth of named people/characters in template images (Morty Smith,
  Springtrap). They stay in Mongo, not in the graph.
- *Who judged the judge?* The review loop: labelled samples, a bar fixed in
  advance, a holdout.

## Sources
`dags/modules/kg/entities.py`, `kg/curation.py`, gap 09, gap 11, MODEL.md
"Entities: linked to Wikidata, as IMKG did".
