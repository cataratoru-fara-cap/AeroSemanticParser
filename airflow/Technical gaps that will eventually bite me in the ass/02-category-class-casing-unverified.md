# `kym:<Category>` casing is a guess, not a verified match to IMKG

**Status:** open — low risk, cheap to verify, just hasn't been checked against real IMKG output.

## What

`airflow/dags/modules/kg/rdf.py::category_class()` renders a frame's category
as `kym:<Category>` by capitalizing the parser's lowercase category string
(`meme` → `Meme`, `subculture` → `Subculture`). This follows the one example
given in the IMKG paper (`kym:Meme`), but IMKG's own mapping output / raw
category values are not published anywhere I could check byte-for-byte.

## Why it matters

If IMKG's real class IRIs use different casing, or a different word entirely
for some category (e.g. maybe IMKG uses `kym:Person` vs ours guessing from
`person`, or has no class for `culture`/`site` at all), then:
- A SPARQL query written against real IMKG data and pointed at MemeAtlas
  would silently return zero rows instead of an error — the worst kind of
  mismatch.
- The "an IMKG query runs unchanged against MemeAtlas" claim in
  `kg/rdf.py`'s docstring and `MODEL.md` would be false for category-based
  queries specifically, while being true for everything else.

## TODO

- [ ] Get a sample of real IMKG-produced triples (their published dataset, or
      ask Riccardo directly — same conversation as the namespace question) and
      check the actual `kym:` class IRIs they emit for a few known categories.
- [ ] Compare against `category_class()`'s output for the same categories:
      `meme`, `person`, `event`, `site`, `culture`, `subculture` (the full set
      `modules/kg/build.py::_CATEGORY_PATH_PATTERNS` can produce).
- [ ] If they differ, fix the mapping in `category_class()` — it's a single
      small function, not structural — and note the real crosswalk in
      `kg_config/MODEL.md`.
- [ ] Either way, once verified, update the docstring in `rdf.py` (currently:
      *"IMKG's raw category values are not published, so exact byte-equality
      with IMKG's class IRIs is unverified"*) to state the confirmed mapping
      instead of the caveat.
