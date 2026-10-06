# Opening — title, Meet Doge, the map (≈3 min)

## The story
One meme, followed through the whole system. Doge is known by everyone in the
room, it is a rich entry (a series of 20 descendant memes, 16 dated events, 8
templates, 23 curated Wikidata links), and its story touches every layer of the
graph. Every chapter opens with what happens to Doge at that step, and Doge's
small photo walks the map from chapter to chapter.

## What to land
- A computer can find pages about Doge but cannot *answer* questions about it,
  because the facts are not connected. A knowledge graph connects them.
- The map is both the agenda and the architecture: each node is a stage of the
  pipeline, each arrow is data flowing. The dashed box is the machine (Airflow)
  that runs every stage every month. Every slide of the talk lives inside one of
  these nodes.

## Slide by slide
1. **Title.** The picture is the thesis: a meme in the middle, everything known
   about it around it, each colour a kind of knowledge (blue entries, orange
   Wikidata items, aqua templates, violet events, pink types and tags). The
   colours are the same in every graph picture of the talk, and in Neo4j
   Browser with the shared stylesheet.
2. **Meet Doge.** Ask the room. Then the three questions. The answer to each is
   a chapter: who (Link), where it started (Events), what grew out of it (Graph:
   its family).
3. **The map.** Read it once, left to right. Point at the dashed box: everything
   inside runs every month by itself. Pink, at the top: the vocabulary (chapter 7).

## About the camera
The whole talk is one picture: the map, with every chapter's slides inside its
node. One click = one camera move, played by itself:
- **into a chapter**: the camera flies into the node and stops on its icon, its
  title and its slides as numbered thumbnails with captions --- the chapter's
  plan, on screen, to lean on;
- **along the track**: from one slide to the next, a short pan;
- **to the next chapter**: out and back in, in one motion, Doge walking along.
The moves are pages that advance by themselves (PDF page durations, 0.045 s
each) in pdfpc, Okular and Acrobat; the stop at the end waits for your click.
Any forward key plays the next move: a move and its stop share one slide number,
so pdfpc's page keys and its Shift+Page "user slide" keys both land on the
move's first page and let it run. Clicking during a move only skips ahead within
it. In a viewer that ignores durations, build with `\gmanimatefalse` in
`deck.tex`: no motion pages, one page per stop.

## If someone asks
- *Why Doge?* Universally known, one of the richest entries, and it sits in the
  middle of the longest series chain in the graph (Bonk Cheems → … → Memes).
- *Is the image fine to show?* It is KYM's own cover image of the entry, cached
  by the pipeline; for an internal talk this is fair use — say "image: Know Your
  Meme" if you publish the slides.
