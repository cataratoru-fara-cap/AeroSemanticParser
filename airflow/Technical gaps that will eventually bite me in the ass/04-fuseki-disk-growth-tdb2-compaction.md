# Fuseki/TDB2 never reclaims space on a build swap — already at 2.8G and will keep growing

**Status:** open — not urgent (330G free on host), but growth is unbounded without action.

## What

Each `kym_kg` publish does a Graph Store Protocol `PUT` of the full
`graph.nt` into both the build's own named graph
(`urn:memeatlas:build:<id>`) and the default graph (`modules/kg/loaders.py`).
`prune` deletes old build-named graphs via `fuseki_prune`, but **TDB2 does
not physically reclaim disk when a graph's triples are deleted or replaced**
— it's an append-only/MVCC store under the hood, and space is only
reclaimed by an explicit offline (or online, with `tdb2.tdbcompact`)
compaction pass, which this project never runs.

Measured directly on the running container after one build cycle:
```
docker exec kym_fuseki du -sh /fuseki/databases
2.8G
```
for a dataset whose live content (one default graph + one build graph + the
158-triple ontology graph, since `keep_builds` retains 2 generations) is
~4.2M triples × 2 ≈ well under that on-disk footprint already — the gap is
dead space from the *previous* smaller-vocabulary build (808K-triple
generation) that got superseded and pruned but not compacted.

## Why it matters

Every `kym_kg` run now writes ~4.2M triples (up from ~800K pre-IMKG-remodel,
since the graph now carries the full parsed record). At `keep_builds=2`, disk
grows by roughly one build's worth of dead TDB2 space per run, forever,
until something compacts it. On a 330G-free host this is not an emergency,
but it's the kind of thing that looks fine for months and then isn't.

## TODO

- [ ] Add a periodic (e.g. weekly, alongside `kym_kg_validate`'s existing
      `@weekly` schedule) `tdb2.tdbcompact` step — either as a task in a DAG
      that shells into the Fuseki container, or as a scheduled compose
      `exec`. Fuseki supports online compaction via
      `POST /$/compact/<dataset>` in recent versions (verify against
      `stain/jena-fuseki:5.1.0`, the pinned image) — check whether it's
      available before reaching for an offline job that needs Fuseki stopped.
- [ ] Once a compaction path is chosen, measure before/after disk use to
      confirm it actually reclaims the space (not just a hypothetical fix).
- [ ] Add disk usage of the Fuseki volume to whatever host-monitoring already
      exists (or note that none exists, if that's the case) so growth is
      visible before it's a problem — not just discoverable by someone
      running `du` by hand.
- [ ] Reconsider `keep_builds` default (currently 2) once graph size is
      ~5x what it was — retaining 2 generations of a 4.2M-triple graph is a
      bigger commitment now than it was of an 800K-triple one.
