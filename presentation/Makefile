# Build the talk without a local TeX installation (Docker), and refresh its data.
TEX = docker run --rm -v $(CURDIR):/w -w /w texlive/texlive:latest latexmk -pdf -interaction=nonstopmode -halt-on-error
COMPOSE = docker compose -f ../airflow/docker-compose.yml

all: slides.pdf presenter.pdf notes.pdf

%.pdf: %.tex deck.tex theme/beamerthememeatlas.sty chapters/*.tex figures/generated/*.tex
	$(TEX) $<

# the live graph and the stages -> data/, then the figures
data:
	$(COMPOSE) exec -T airflow-worker python - < scripts/extract.py > data/live.json
	docker cp scripts/stages.py kym_dashboard:/tmp/stages.py
	docker exec -w /app kym_dashboard python /tmp/stages.py > data/stages.json

figures:
	python3 scripts/figures.py

clean:
	rm -f *.aux *.log *.nav *.out *.snm *.toc *.fls *.fdb_latexmk *.vrb *.synctex.gz

.PHONY: all data figures clean
