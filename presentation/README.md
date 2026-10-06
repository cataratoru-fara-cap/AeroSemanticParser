# MemeAtlas — the talk

A one-hour Beamer deck about the project: where it started (IMKG, 2023), every
stage of the pipeline told through one meme (Doge), the vocabulary the graph is
written in, and the live graph in numbers.

The whole talk is one picture: the pipeline map. Each chapter is a node of it,
and the chapter's slides live *inside* that node, small, on a numbered track. A
camera moves over the picture:

- **into a chapter** --- it stops on the node's icon and title, with the chapter's
  slides as captioned thumbnails: the plan of the chapter, on screen;
- **along the track** --- slide to slide, a short pan;
- **to the next chapter** --- out and back in, in one motion (van Wijk and Nuij's
  smooth zoom-and-pan path), while Doge's photo walks the map.

Each move is a run of pages that advance by themselves; the slide it lands on is
the real, vector slide.

## Build

`make` (Docker, the `texlive/texlive` image; no local TeX needed). It builds in
two passes:

1. `scripts/world.py` reads the map (`chapters/registry.tex`) and the tour (the
   chapter files) and writes `figures/generated/world.tex`: where every chapter
   and slide sits, and every page of every camera move.
2. `thumbs.tex` → `thumbs.pdf`: every slide, one page each --- what the camera flies
   over, each page included in vector.
3. The decks:

| file | what |
|---|---|
| `slides.pdf` | the deck, for the screen: the camera moves by itself |
| `presenter.pdf` | the same with your notes on a second screen, for pdfpc |
| `notes.pdf` | one page per stop followed by its notes, to read and refine |
| `stops.pdf` | one page per stop, no motion --- quick to proof |

Edit a slide, run `make` again: `thumbs.pdf` is rebuilt, so the camera never flies
over an old version of it. A full build takes about six minutes. Overleaf can
build `slides.tex` (pdfLaTeX) but not `thumbs.pdf`; without it the camera flies
over grey placeholders.

## Present

One key press (or click) per move: the move plays by itself and stops on the
next slide. A move is a run of pages that advance on their own (each shows for
0.045 s), so the viewer has to draw each page within that time --- or have drawn
it beforehand.

- **pdfpc** (recommended): `pdfpc --notes=right presenter.pdf` --- slides on the
  projector, notes and the next slide on your screen. It draws every page ahead
  of time when it starts, so the moves play smoothly. A move and the stop it lands
  on share one slide number, so any forward key plays the next move.
- **Okular**: `slides.pdf`, in presentation mode (View → Presentation,
  Ctrl+Shift+P). Outside presentation mode Okular ignores the page durations and
  every page of a move needs its own key press. Okular draws a page only when it
  is due and drops it if it is not ready in time: a moving page takes about 90 ms
  to draw at 1080p (Okular's renderer, measured on one 3.9 GHz core), so a move
  shows about every second page. For smooth moves, set Settings → Configure Okular
  → Performance → Memory usage to **Greedy** and give it a minute or two after
  starting the presentation: it then draws every page ahead and plays the moves
  from memory (about 8 GB at 1080p; Okular allows itself half the machine's).
- **Acrobat** (full screen): `slides.pdf`; moves play by themselves.
- **Other viewers** may ignore the page durations; then set `\gmanimatefalse` in
  `deck.tex`: no motion pages, one page per stop.

## Edit

- **The tour**: in `chapters/NN-*.tex`, `\gmenter{chapter}` enters a node,
  `\gmgo{id}{caption}` announces the frame that follows as a slide of that node
  (the caption labels its thumbnail), `\gmoverview{caption}` shows the whole map,
  `\gmepilogue` starts the slides outside any node. A node holds 1 to 6 slides.
  Slides must not use overlays (one page each).
- **Notes on the slides** (what pdfpc shows): the `\note{…}` of each frame; a
  `\note` right after `\gmenter` or `\gmoverview` belongs to that view.
- **Explanations per chapter** (the story, numbers and their sources, the
  reasoning, likely questions, caveats to say out loud): `notes/NN-*.md`.
- **Title, name, date**: the TODO lines in `deck.tex`.
- **Facts not from the graph** (IMKG's paper, review measurements, code counts):
  `chapters/facts.tex`, each with its source.
- **The map** (chapters, positions, colours, icons, arrows): `chapters/registry.tex`.
  **The camera** (sizes, speeds, how far it pulls back): `scripts/world.py`.
  **Colours, fonts, slide parts, drawing the world**: `theme/beamerthememeatlas.sty`.
- **Type sizes**: 11 pt body text, 10 pt at the smallest, titles on one line ---
  readable from the back of a room.

## Numbers and figures

Nothing on the slides is typed by hand except `chapters/facts.tex`. The rest is
generated from the live system:

```bash
# from presentation/, with the Airflow stack running
docker compose -f ../airflow/docker-compose.yml exec -T airflow-worker \
    python - [build_id] < scripts/extract.py > data/live.json      # the graph, Doge
docker cp scripts/stages.py kym_dashboard:/tmp/stages.py
docker exec -w /app kym_dashboard python /tmp/stages.py > data/stages.json   # the stages
python3 scripts/figures.py     # -> figures/generated/*.tex, figures/img/*
```

`extract.py` reads the live build unless given a build id. **For 7.1.0**, once it
is published: run the three commands, rebuild, then fill the MemeAtlas column of
"IMKG's four questions" (chapter 11) and update `chapters/facts.tex` (7.1.0
section). Doge's frame moves with 7.1.0 (gap 14, chapter 2): it is
`/sensitive/memes/doge`, listed by KYM on 9 July and fetched on 9 September.
`extract.py` finds it by either address, and the step-1 slide prints whichever it
found. The dates written in that slide's notes and in `notes/02-collect.md`
(18 June, 10 July, 364 KB, 79 KB) are the old address's, so re-read them. `figures.py` needs the standard library and PyYAML (it counts the curated taxonomy
files); Pillow, if installed, resizes the images. The queries are listed, in Neo4j Browser form, in
`queries.md`.

## Layout

```
deck.tex                the document: preamble, then \input of every chapter
slides|presenter|notes|stops|thumbs.tex   the build wrappers (see Build)
theme/beamerthememeatlas.sty        colours, fonts, drawing the world, the tour, slide parts
chapters/registry.tex   the map: chapters (number, title, colour, place, icon) and arrows
chapters/NN-*.tex       one file per chapter: the tour and its slides, notes inside each frame
chapters/facts.tex      hand-maintained facts, with sources
notes/NN-*.md           the explanations, chapter by chapter
data/                   live.json, stages.json — extracted, committed with the deck
figures/generated/      numbers.tex, every chart and graph drawing, world.tex (the camera)
figures/img/            Doge's image, his eight templates, IMKG's example meme
scripts/                extract.py, stages.py, figures.py, world.py
queries.md              the queries behind the figures, for Neo4j Browser
```

The meaning chapter's numbers (taxonomy buckets, origins, tag folding) are counted
by `figures.py` from the curated files in `../airflow/dags/kg_config/` and the
censuses in `../airflow/data/kg_census_*.json`.

## Images

`figures/img/` holds Know Your Meme's cover image of Doge, eight imgflip templates
and IMKG's own example meme (One Does Not Simply, imgflip template 61579), cached
by the pipeline. They belong to those sites (see the repository's licence note) —
fine for an internal talk; credit them if the slides are published.
