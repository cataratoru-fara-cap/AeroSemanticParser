# Chapter 10 — The graph in numbers (live build, KG 7.1.0) (≈5 min)

Every number comes from `data/live.json` (scripts/extract.py on the live build)
and `data/stages.json` (the dashboard's functions). The queries are listed in
`queries.md`.

## MemeAtlas next to IMKG (the fair comparison is IMKG's KYM part)
| | MemeAtlas 7.1.0 | IMKG, KYM part |
|---|---|---|
| KYM entries | **23,477** | 12,585 |
| nodes (core) | 363,393 | 167,662 |
| edges (core) | 711,616 | 914,941 |
| edges counted the RDF way | **891,677** | 914,941 |
| relation types (core) | 6 | 18 |
| average degree (core) | 3.92 | 10.91 |
| rebuilt | every month | once |

Why "the RDF way": IMKG is RDF, where every literal (about, year, status…) is an
edge; MemeAtlas's property graph keeps literals on nodes. Counting populated node
attributes as edges ("triple-equivalent"), the core matches IMKG's density over
1.87× the entries. Do not compare raw edge counts or degrees without saying this.

## What each entry carries
Wikidata link 99.5% · events 77.1% · a template 34.1% · a Wikidata item in its
own picture 87.5% (new in 7.1.0) · all four 29.1% · none 12 entries. Templates
exist for picture memes, not for people or events.

## When memes are born (said over "Where memes are born"; no slide of its own)
Entries by the year their meme started: rising from the late 1990s to a peak in
2019 (1,814), then falling — mostly documentation lag (KYM writes a meme up once
it has lasted). 929 entries start before 1995.

## Where memes are born (origin platform, canonicalised: "Twitter / X", "X", "twitter.com" → twitter)
Peaks: 4chan 2010 · Tumblr 2014 · YouTube and Reddit 2019 · Twitter 2020 ·
TikTok 2022. Totals 2005–2025: Twitter 3,821 · YouTube 2,182 · TikTok 1,741 ·
Reddit 1,281 · Tumblr 739 · 4chan 724.

## What the graph talks about most (backup slide)
Wikidata items by entries naming them: Twitter 3,599, TikTok 3,205, image macro
2,401, YouTube 2,178, catchphrase 2,032, video game, viral video, parody, song,
Reddit. Templates shared by most entries: Trollface 9, Soyboy Vs Yes Chad 8,
Woman Yelling At Cat 8, Drake Hotline Bling 8, Roll Safe 7, then five each.

## Doge's family
Ancestors: Doge → Interior Monologue Captioning → Image Macros → Memes.
Descendants: 20 chains, the deepest Bonk (Cheems) → Cheems → Dogelore → Ironic
Doge Memes → Doge — the longest series chain of the whole graph (7 steps to
Memes). Siblings (7.0.0, Riccardo's request): 631,311 pairs share a series; only
3,448 (0.55%) link to each other on their pages — series and links are different
relations, and the graph keeps both. This slide's text is small: zoom in (pdfpc
has a zoom) rather than read it from the back.

## Is the graph sound? (on chapter 9's "Checked, measured, versioned" slide)
0 isolated nodes · 0 duplicate edges · 0 cycles in a series · 2 connected
components · the RDF re-derived through the mapping equals the published RDF.
Worth knowing: 26.0% of series parents (1,060 of 4,074) are not in the corpus
(stubs); 18.8% of entries have no entry type on KYM.

## Findings worth saying
- **One entry, two addresses (found preparing this talk, fixed — gap 14,
  chapter 2):** 813 entries were two frames in 7.0.0, at the old address and at
  the one KYM moved them to (mostly into its `/sensitive/` section); Doge's twin
  even appeared as its sibling. Fixed at collection: 7.1.0 holds each entry once,
  so its numbers are about 3% below 7.0.0's.
- **The NSFW placeholder image is a hub** (gap 12): KYM's cover for NSFW content
  is one image node: 2,484 frames and 3,044 events link to it (7.1.0).
- **Generic regions:** "a man" in 6,800 templates and 6,780 entries' own
  pictures.

## Live demo (optional, the "Try it" backup slide)
Neo4j Browser at `:8080/browser/` with `dags/kg_config/neo4j_browser.grass`
imported (colours as in the slides). SPARQL at `:8080/sparql/`: the rdfs:seeAlso
query IMKG would write returns Doge's whole series. **Do not run heavy SPARQL
during a graph build** (Fuseki has no query timeout).

## If someone asks
- *Why is the average degree lower than IMKG's?* Counting convention (above).
- *How many relation types in the whole graph?* 26 of MemeAtlas's own, and from
  7.1.0 each Wikidata property of the imported statements is a relation too
  (1,168): 1,194 in all, against IMKG's 836.
