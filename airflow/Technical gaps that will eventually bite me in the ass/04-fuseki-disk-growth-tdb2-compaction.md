# Fuseki/TDB2 never reclaims space on a build swap — already at 2.8G and will keep growing

**Status:** closed 2026-09-30 — `kym_kg` compacts Fuseki after every prune (`compact_fuseki`); first compaction 36.0 GB -> 3.8 GB with every triple kept; the dashboard shows the store's size and the host's free disk. See "Resolution" below.

**Update 2026-09-18 — 6.0.0 makes this grow faster.** The event layer adds,
at full extraction coverage, on the order of 120k `mk:Event` nodes at ~10
triples each: **roughly +1.3M triples on a ~2.7M-triple graph (+45%)**,
all of it PUT twice per publish (build graph + default graph). 5.1.0 adds
a little more on its own (`m4s:origin` / `m4s:spread` text on every frame
that has those sections, plus their images and anchor texts). Nothing
about the compaction answer below changes; its urgency does.

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

- [x] A compaction step. Fuseki 5.1.0 (the pinned image) has online
      compaction — `POST /$/compact/<dataset>?deleteOld=true`, an async task
      polled at `/$/tasks/<id>` (checked in the image's own
      `ActionCompact`/`DatabaseOps` classes) — so no offline job and no
      stopped Fuseki. `loaders.fuseki_compact()`; `kym_kg` runs it as
      `compact_fuseki` after every `prune`, rather than weekly: the dead
      space is made by the publish's PUTs and the prune's DELETEs, so that
      is when to give it back.
- [x] Measured before/after (below): it reclaims the space.
- [x] Disk visible: there is no host monitoring on this machine, so the
      dashboard's Knowledge Graph page now shows the TDB2 database's size on
      disk, its generation directories, and the host's free space (Fuseki's
      volume mounted read-only into the dashboard, sizes only).
- [x] `keep_builds` reconsidered — kept at 2 (below).

## Resolution (2026-09-30)

**What had happened.** The database was 34 GB (36.0 GB in bytes), not 2.8 GB:
every build since 09-18 PUT its graph twice (its named graph and the
default graph) and every prune deleted graphs, and TDB2 kept all of it. Live
content was 11,946,478 triples: the published 09-18 build twice (default +
named graph, 2,672,281 each), two newer builds that were loaded but never
published (09-21: 3,256,688; 09-22: 3,345,007 — prune keeps the newest
`keep_builds` builds plus the published one), and the 221-triple ontology.

**The first compaction** (run through `loaders.fuseki_compact`, the code
the DAG uses):

| | Before | After |
|---|---|---|
| TDB2 database | 36.0 GB (`Data-0001`) | 3.8 GB (`Data-0002`) |
| Live triples | 11,946,478 | 11,946,478 — same count in every graph |
| Host disk free | 124 GiB | 154 GiB |
| Time | | 9 min 23 s; Fuseki stayed up (its admin API answered every poll) |

One thing to know: after the switch, Fuseki's JVM kept the deleted
`Data-0001` files memory-mapped (4,267 mappings, 40 open descriptors), so
the host did not get the space back at the moment the task finished; it
came back within ~3 minutes, once the JVM garbage-collected them (query
traffic was running). A Fuseki restart releases them at once — it was
restarted afterwards, came back healthy on the compacted database, and the
public `/sparql/` route answered. Nothing to do about it in the DAG.

**`keep_builds` stays 2.** What one more retained generation costs, measured:
~1 GB in Fuseki (compacted TDB2 is ~0.32 GB per million triples; a build is
~3.3M), ~0.45 GB in Mongo (`kg_nodes`/`kg_edges` with indexes) and ~1.8 GB
of files under `data/kg/builds/` — about 3.3 GB, against 154 GB free. The
previous generation is the rollback (re-point at it), which is worth that.
Revisit if the host drops under ~50 GB free: the dashboard now shows it.

**Still true, by design:** a build that is loaded but not published (run
with `publish=false`, or failing verification) stays in Fuseki as a named
graph until a later publish's prune — that is why three builds were there.
