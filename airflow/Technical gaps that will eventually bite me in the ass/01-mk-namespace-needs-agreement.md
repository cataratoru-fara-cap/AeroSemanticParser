# `mk:` namespace needs Riccardo's sign-off

**Status:** open — blocks publishing the ontology as final, doesn't block using it internally.

## What

MemeAtlas's extension namespace is `mk: = https://meme4.science/atlas/`, declared
in `airflow/dags/kg_config/memeatlas.ttl` and used throughout
`airflow/dags/modules/kg/rdf.py`. That IRI is a sub-path of `m4s: =
https://meme4.science/`, which is IMKG's own namespace
(github.com/riccardotommasini/imkg, Tommasini/Ilievski/Wijesiriwardene, ESWC
2023).

## Why it matters

Minting an extension namespace under someone else's domain without asking is
a courtesy problem now and a correctness problem later: if IMKG's authors
publish their own `atlas/` sub-path, or object to the positioning, every
`mk:`-prefixed IRI MemeAtlas has ever emitted — in Fuseki, in any exported
`.nt` file, in the paper — needs to change. The earlier this is settled the
fewer published artifacts need a namespace migration.

Riccardo already asked once (in this project) whether his IMKG mappings were
reused. That's the natural opening to also raise this.

## TODO

- [ ] Ask Riccardo Tommasini directly: is `https://meme4.science/atlas/` an
      acceptable namespace for MemeAtlas as an IMKG extension, or does he want
      it hosted elsewhere (e.g. under MemeAtlas's own top-level domain instead
      of a path under `meme4.science`)?
- [ ] If he objects, the fix is contained: one constant
      (`PREFIXES["mk"]` in `airflow/dags/modules/kg/rdf.py`) plus the matching
      `@prefix mk:` line in `airflow/dags/kg_config/memeatlas.ttl` and
      `airflow/dags/kg_config/kg_mapping.yarrrml.yml`. No node/edge logic
      changes. Re-run `kym_kg` with `force_rebuild=true` after.
- [ ] Once agreed (in either direction), record the decision in
      `airflow/dags/kg_config/MODEL.md` under "Namespaces" so it stops being a
      TODO and becomes documented fact.
