# Build the talk without a local TeX installation (Docker), and refresh its data.
#
#   make          everything: the world, the thumbnails, then the four PDFs
#   make stops    one page per stop, no motion — quick to proof (needs the thumbnails)
#
# The deck is built in two passes. thumbs.pdf holds every slide, one page each; the camera
# then flies over those pages (included in vector) and lands on the real slide. Re-run
# `make` after editing any slide, or the camera flies over the old page of it.
UID := $(shell id -u)
GID := $(shell id -g)
RUN = docker run --rm -u $(UID):$(GID) -v $(CURDIR):/w -w /w texlive/texlive:latest
TEX = $(RUN) latexmk -pdf -interaction=nonstopmode -halt-on-error
COMPOSE = docker compose -f ../airflow/docker-compose.yml
SOURCES = deck.tex theme/beamerthememeatlas.sty chapters/*.tex figures/generated/*.tex

all: slides.pdf presenter.pdf notes.pdf stops.pdf

# where every chapter and slide sits, and the camera between them
figures/generated/world.tex: scripts/world.py deck.tex chapters/*.tex
	python3 scripts/world.py

thumbs.pdf: thumbs.tex $(SOURCES) figures/generated/world.tex
	$(TEX) thumbs.tex

slides.pdf presenter.pdf notes.pdf stops.pdf: %.pdf: %.tex $(SOURCES) figures/generated/world.tex thumbs.pdf
	$(TEX) $<

stops: stops.pdf

# the live graph and the stages -> data/, then the figures
data:
	$(COMPOSE) exec -T airflow-worker python - < scripts/extract.py > data/live.json
	docker cp scripts/stages.py kym_dashboard:/tmp/stages.py
	docker exec -w /app kym_dashboard python /tmp/stages.py > data/stages.json

figures:
	python3 scripts/figures.py

clean:
	rm -f *.aux *.log *.nav *.out *.snm *.toc *.fls *.fdb_latexmk *.vrb *.synctex.gz

.PHONY: all stops data figures clean
