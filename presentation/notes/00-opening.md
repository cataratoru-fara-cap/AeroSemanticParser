# Opening — title, Meet Doge, the map (≈3 min)

## The story
One meme, followed through the whole system. Doge is known by everyone in the
room, it is a rich entry (a series of 20 descendant memes, 16 dated events, 8
templates, 23 curated Wikidata links), and its story touches every layer of the
graph. Every chapter opens with what happens to Doge at that step.

## What to land
- A computer can find pages about Doge but cannot *answer* questions about it,
  because the facts are not connected. A knowledge graph connects them.
- The map is both the agenda and the architecture: each node is a stage of the
  pipeline, each arrow is data flowing. The dashed box is the machine (Airflow)
  that runs every stage every month.

## Slide by slide
1. **Title.** The picture is the thesis: a meme in the middle, everything known
   about it around it, each colour a kind of knowledge (blue entries, orange
   Wikidata items, aqua templates, yellow events, green types and tags). The
   colours are the same in every graph picture of the talk, and in Neo4j Browser
   with the shared stylesheet.
2. **Meet Doge.** Ask the room. Then the three questions. The answer to each is
   a chapter: who (Link), where it started (Events), what grew out of it (Graph:
   its family).
3. **The map.** Read it once, left to right. Point at the dashed box: everything
   inside runs every month by itself.

## About the zoom
Each chapter starts from this map and zooms into its node; at the end it zooms
back out. The zoom is a sequence of slides that advance by themselves in Okular,
Acrobat and pdfpc (each step has a PDF page duration). If a viewer does not
honour durations, click through, or build with `\gmanimatefalse` in `deck.tex`
(two steps per zoom).

## If someone asks
- *Why Doge?* Universally known, one of the richest entries, and it sits in the
  middle of the longest series chain in the graph (Bonk Cheems → … → Memes).
- *Is the image fine to show?* It is KYM's own cover image of the entry, cached
  by the pipeline; for an internal talk this is fair use — say "image: Know Your
  Meme" if you publish the slides.
