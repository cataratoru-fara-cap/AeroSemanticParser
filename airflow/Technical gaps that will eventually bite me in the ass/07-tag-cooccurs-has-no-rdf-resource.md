# tag-tag `coOccursWith` has no RDF representation

**Status:** closed 2026-09-18 — superseded, not fixed. The asymmetry this file described no longer exists; see "What changed" below.

## What (as originally written, 2026-09-17)

KG 5.0.0 added statistical `coOccursWith` edges for both `entry_type_concept`
and `tag_concept` pairs (`kg/cooccurs.py`, fed by `kg/census.py`'s
co-occurrence output). `entry_type_concept` pairs got full RDF treatment —
`mk:coOccursWith` triples, both directions, `owl:SymmetricProperty`
declared. `tag_concept` pairs did not reach RDF at all, because
`tag_concept` has no IRI in RDF (`hasTag`'s object is a plain `m4s:tag`
literal, matching IMKG's own convention).

## What changed (2026-09-18)

Curator feedback on the published 5.0.0 graph: entry_type's `coOccursWith`
was judged needless statistical noise alongside the curated `subTypeOf`
hierarchy it already has, and was removed entirely (`kym_kg_dag.py`'s
`write_cooccurs_edges` no longer computes it; `rdf.py`, `serialize.py` and
`kg_mapping.yarrrml.yml`'s entry_type-side RDF/RML plumbing for it was
removed too, since it became permanently unreachable code, not just
data-dependent). Tags keep `coOccursWith` — they have no curated
alternative — so the field is now the **only** source of `coOccursWith`
edges, and the asymmetry this file was written about (two concept kinds,
different RDF treatment) simply no longer exists: `coOccursWith` is
uniformly property-graph-only now, a plain design fact rather than a
tension between two inconsistent halves.

The two options this file originally offered ("promote tag_concept to an
RDF resource" or "leave it property-graph-only permanently") are effectively
moot for the same reason: there's no longer an entry_type side to compare
against, so there's no pressure toward parity. If tag-tag co-occurrence over
SPARQL is ever wanted, that's a fresh cost/benefit decision, not a
continuation of this one.

## If this resurfaces

- If entry_type's `coOccursWith` is ever reinstated, the RDF/RML machinery
  it needs (predicate declaration, ontology term, YARRRML mapping,
  `EDGE_TYPE_TO_RML_FILE` entry) is fully described in the KG 5.0.0
  implementation (git history around commit introducing `kg/cooccurs.py`)
  and would need to be re-added, along with the `rdf.py`/`serialize.py`
  guard that keeps tag pairs excluded even then.
- If tag-tag co-occurrence needs to be queryable over SPARQL some day, see
  this file's original "two ways to close it" section in git history for
  the tradeoffs already considered (promoting `tag_concept` to a resource
  vs. accepting it as an analytics-only feature).
