"""
kg — pure knowledge-graph libraries for the post-parse stage.

Renamed from `helpers`, which described *how* the code was run (by hand)
rather than what it does. Everything here obeys the project's layering
rule — **pure library <- store <- DAG** — so nothing in this package may
import Mongo or Airflow. Persistence belongs to `modules/kg_store.py`;
orchestration to `dags/kym_kg_dag.py`.

    build.py       one `entries` doc -> (nodes, edges). The ONLY producer;
                   both the RDF and property-graph projections derive from
                   it, so the two representations cannot drift.
    census.py      frequency + co-occurrence census over a corpus field.
    metrics.py     IMKG-comparable graph statistics. Pure stdlib by design
                   — no numpy, deliberately (see its module docstring).
    semantics.py   definition-embedding analysis of entry types (LLM).

Transitional, absorbed by later phases — named honestly so nobody mistakes
them for the destination:

    census_tags.py    -> folds into census.py (same shape, different keys)
    export_pg.py      -> folds into serialize.py
    export_rml.py     -> folds into serialize.py
    _legacy_store.py  -> replaced by modules/kg_store.py

Curated inputs these read live in `dags/kg_config/`, tracked in git.
They used to live in `data/`, which is gitignored — which is exactly how a
104-line reviewed taxonomy ended up retyped by hand into a Python tuple.
"""
