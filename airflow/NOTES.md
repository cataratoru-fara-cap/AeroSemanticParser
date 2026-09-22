# Scratch notes

Carried over from `technology notes.txt`, unchanged in substance — the
original was a bare comma-separated line, which is why nobody could tell
whether it was a shopping list or a decision.

## Technologies to review

- **OWL / SKOS** — ontology + thesaurus vocabularies for the KG stage.
  SKOS is the lighter fit for the KYM tag/entry-type taxonomy;
  OWL if the type semantics need real inference.
- **EventKG** — reviewed for the 6.0.0 event layer, and used as a model
  rather than a template. MemeAtlas takes SEM's what/when/where/who
  skeleton and EventKG's rule that every statement stays traceable to its
  source, aligns `mk:Event` and its properties to SEM in `memeatlas.ttl`
  without emitting `sem:` terms, and does NOT adopt EventKG's per-statement
  named graphs or its cross-source event registry. The reasons, term by
  term, are in `dags/kg_config/MODEL.md` ("Events: drawn from EventKG, not
  copied from it"). Aligning KYM events against EventKG's own events
  (Wikidata-anchored) is still open, and would be the natural first
  consumer of cross-frame event identity.
- **Wikidata / DBpedia Spotlight / spaCy** — now in use for the 6.1.0
  entity layer. IMKG linked About text and tags with the DBpedia Spotlight
  API and mapped the results to Wikidata; MemeAtlas does the recognition
  locally with spaCy (`en_core_web_sm`) and the linking against a lexicon
  built from the full Wikidata JSON dump (`kg/wikidata.py`), following the
  suggestion to download all of Wikidata rather than call an API. Still to
  review for the curation step (gap 09): class-based filtering over
  P31/P279, corpus salience, embedding relevance, and heavier linkers
  (REL, ReFinED, BLINK) if the lexicon approach tops out.
- **Streamlit** — now in use: the analytics dashboard (`dashboard/`).
- **Railway** — hosting option, not evaluated yet.

Add to this file rather than starting another scratch file.
