# Chapter 1 — Where it started: IMKG, 2023 (≈5 min)

## The story
When you arrived there was IMKG: a paper (ESWC 2023, Tommasini, Ilievski,
Wijesiriwardene), a GitHub repository (riccardotommasini/imkg) and one data
release on Zenodo (v0.9). It was the first knowledge graph of Internet memes,
and it was built once. Your brief was to make it a *process*: re-runnable,
incremental, reading every page in depth, and still speaking IMKG's language.

## What a knowledge graph is (for the lay part of the room)
Things are dots, facts are arrows. The example is IMKG's own figure 7 (One Does
Not Simply Walk into Mordor), redrawn. "The meme's text mentions Mordor",
"Mordor appears in The Lord of the Rings", "Tolkien wrote it". Because arrows
connect, the computer can answer questions nobody wrote down ("memes about
Tolkien's work?") by following them.

## IMKG in numbers (Table 2 of the paper)
| | nodes | edges | relations | frames | memes |
|---|---|---|---|---|---|
| KYM part | 167,662 | 914,941 | 18 | 12,585 | 12,585 |
| imgflip part | 4,698,912 | 15,129,606 | 10 | — | 1,326,032 |
| Wikidata subset | 85,917 | 504,781 | 805 | 242 | 242 |
| **IMKG** | **4,850,636** | **16,549,810** | **836** | 12,585 | 1,338,617 |

Most of IMKG's size is imgflip memes and their captions. The fair comparison for
MemeAtlas is the KYM part (chapter 10).

## How it was built (section 4 of the paper)
1. **Model** — media frames (KYM entries), memes (imgflip instances), templates.
2. **Collect** — a Selenium crawler over all of KYM (about two days), imgflip
   memes, 556 Wikidata memes as seeds.
3. **Enrich** — DBpedia Spotlight on About/Origin/Spread and captions (confidence
   ≥ 0.5, then DBpedia → Wikidata); Google Vision on the frame image;
   KGTK over Wikidata dumps for background statements.
4. **Integrate** — RML mappings to RDF; KYM ↔ Wikidata on 276 memes (P6760);
   KYM ↔ imgflip by hand plus ~60 title matches above 85% similarity; 96 frames
   linked to 241 templates.

## "What I found" — say it fairly
IMKG did what a research paper needs. These are the properties a *living* graph
needs, which is what you were asked to build:
- a one-off snapshot, while KYM changes daily;
- most of each page unread: links, references, the story;
- bridges made by hand (276 memes, 96 frames) do not scale to new entries;
- a link carries no score and no reason.
The authors wrote the outlook themselves: "recurrent releases of the KGs … to
handle scalability in terms of volume and velocity".

## The three goals (they come back in every chapter)
1. A pipeline, not a snapshot — monthly, incremental, reproducible.
2. Every page, read in depth — and say how sure we are of every derived fact.
3. Compatible with IMKG — its vocabulary kept word for word.

## If someone asks
- *Did you reuse IMKG's code?* No: the crawler and enrichment were rebuilt
  (Airflow DAGs, local NLP, a local Wikidata lexicon). What was kept is IMKG's
  *model and vocabulary* (m4s:, kym:, skos:broader, rdfs:seeAlso,
  m4s:fromAbout/fromTags/fromImage, m4s:templateOf).
- *Why not just re-run IMKG?* Its crawler targets KYM's layout of 2022, and a
  re-run would still be a snapshot. (Do not claim the crawler is broken unless
  you have tried it.) Using outside services where they do the job was a sound
  choice for a paper; it is not one of the things to fix.
- *Is MemeAtlas a replacement?* An extension: same vocabulary, more depth,
  continuous. IMKG's imgflip memes (1.3M instances) are not re-collected.

## Sources
Paper text: IMKG ESWC 2023 (Fig. 4 pipeline, Fig. 7 example, Table 2, §6
outlook). `airflow/dags/kg_config/MODEL.md` — the crosswalk to IMKG.
