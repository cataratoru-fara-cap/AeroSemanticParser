# MemeAtlas — the talk

A one-hour Beamer deck about the project: where it started (IMKG, 2023), every
stage of the pipeline told through one meme (Doge), and the live graph in
numbers. The talk is shaped like a graph: each chapter is a node of the pipeline
map, entered with a camera zoom and left with the reverse; inside a chapter, a
mini-map in the corner shows where you are.

## Build

Three outputs from one source:

| file | what |
|---|---|
| `slides.tex` → `slides.pdf` | the deck, for the screen |
| `presenter.tex` → `presenter.pdf` | the deck with your notes on a second screen, for pdfpc |
| `notes.tex` → `notes.pdf` | one page per slide followed by its notes, to read and refine |
| `review.tex` → `review.pdf` | one page per slide, no notes (zooms collapsed) — quick to proof |

With TeX Live (2023 or later; Beamer, TikZ, pgfplots, FiraSans, fontawesome5):

```bash
latexmk -pdf slides.tex
latexmk -pdf presenter.tex
latexmk -pdf notes.tex
```

No TeX on the machine? Docker: `make` (see the Makefile) runs the same in the
`texlive/texlive` image. Overleaf works too: upload the folder, set the main file
to `slides.tex`, compiler pdfLaTeX.

## Present

- **pdfpc** (recommended): `pdfpc --notes=right presenter.pdf` — slides on the
  projector, notes and the next slide on your screen. Zooms play by themselves.
- **Okular / Acrobat** (full screen): `slides.pdf`; zooms play by themselves.
- **Other viewers** may ignore the page durations the zooms use; then each zoom
  is a few clicks. Or set `\gmanimatefalse` in `deck.tex`: every zoom becomes two
  slides (start, end).

Doge's family tree and the graph of the graph have small text: use the viewer's
zoom on them, or talk over the shape.

## Edit

- **Notes on the slides** (what pdfpc shows): the `\note{…}` at the end of each
  frame in `chapters/NN-*.tex`.
- **Explanations per chapter** (the story, numbers and their sources, the
  reasoning, likely questions, caveats to say out loud): `notes/NN-*.md`.
- **Title, name, date**: the TODO lines in `deck.tex`.
- **Facts not from the graph** (IMKG's paper, review measurements, code counts):
  `chapters/facts.tex`, each with its source.
- **Colours, fonts, the map, the zoom**: `theme/beamerthememeatlas.sty`. The
  chapters and their place on the map: `chapters/registry.tex`.

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
"IMKG's four questions" (chapter 10) and update `chapters/facts.tex` (7.1.0
section). Doge's frame moves with 7.1.0 (gap 14, chapter 2): it is
`/sensitive/memes/doge`, listed by KYM on 9 July and fetched on 9 September.
`extract.py` finds it by either address, and the step-1 slide prints whichever it
found. The dates written in that slide's notes and in `notes/02-collect.md`
(18 June, 10 July, 364 KB, 79 KB) are the old address's, so re-read them. `figures.py` needs only the standard library (Pillow, if installed,
resizes the images). The queries are listed, in Neo4j Browser form, in
`queries.md`.

## Layout

```
deck.tex                the document: preamble, then \input of every chapter
slides|presenter|notes|review.tex   the build wrappers
theme/beamerthememeatlas.sty        colours, fonts, the map, the zoom, slide parts
chapters/registry.tex   the chapters: number, title, colour, place on the map
chapters/NN-*.tex       one file per chapter, notes inside each frame
chapters/facts.tex      hand-maintained facts, with sources
notes/NN-*.md           the explanations, chapter by chapter
data/                   live.json, stages.json — extracted, committed with the deck
figures/generated/      numbers.tex and every chart and graph drawing
figures/img/            Doge's image and his eight templates
scripts/                extract.py, stages.py, figures.py
queries.md              the queries behind the figures, for Neo4j Browser
```

## Images

`figures/img/` holds Know Your Meme's cover image of Doge and eight imgflip
templates, cached by the pipeline. They belong to those sites (see the
repository's licence note) — fine for an internal talk; credit them if the
slides are published.
