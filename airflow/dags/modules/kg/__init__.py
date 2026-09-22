"""
kg — pure knowledge-graph libraries for the post-parse stage.

Renamed from `helpers`, which described *how* the code was run (by hand)
rather than what it does. Everything here obeys the project's layering
rule — **pure library <- store <- DAG** — so nothing in this package may
import Mongo or Airflow. Persistence belongs to `modules/kg_store.py`;
orchestration to `dags/kym_kg_dag.py`.

    build.py       one `entries` doc -> (nodes, edges). The ONLY producer;
                   every representation below is a projection of its output,
                   so the representations cannot disagree about which edges
                   exist.
    taxonomy.py    the curated entry_type taxonomy (kg_config/*.yaml), loaded,
                   validated, and turned into skos:broader concept edges.
    census.py      frequency + co-occurrence census over a corpus field.
    serialize.py   one build's (nodes, edges) -> graph.nt + the RML CSVs +
                   the property-graph view CSVs + a manifest, atomically.
    rdf.py         (nodes, edges) -> canonical N-Triples. The product path.
    ntdiff.py      memory-bounded set diff of two N-Triples files. Powers
                   the gate that checks the RML path against rdf.py.
    metrics.py     IMKG-comparable graph statistics. Pure stdlib by design
                   — no numpy, deliberately (see its module docstring).
    semantics.py   definition-embedding analysis of entry types (LLM).
    events.py      Origin/Spread narrative -> spatio-temporal event rows
                   (LLM), per kg_config/event_extraction_schema.json. The
                   rows are persisted by modules/event_store.py and handed
                   back to build.py as data, so this module stays pure and
                   build.py never imports an HTTP client.

    wikidata.py    a downloaded Wikidata JSON dump -> a local SQLite lexicon
                   (labels, aliases, popularity, classes, KYM slugs), and
                   the read-only lookups over it. Stdlib only.
    entities.py    title, tags and About -> Wikidata entities: spaCy NER +
                   noun chunks, looked up in that lexicon, scored, grounded
                   to their characters. Persisted by modules/entity_store.py
                   and handed to build.py as data, like events. Neither
                   reaches the network: the dump is downloaded once, by hand.

The two LLM modules are the only ones here that reach the network, and
they do it exclusively through modules/openwebui_client.py. Both take the
client as an argument rather than constructing one, so importing either
reads no configuration and contacts nothing.

Retired here, with their behaviour verified equivalent on the live corpus
before removal: kg_census_tags.py (into census.py), kg_export.py and
kg_export_rml.py (into serialize.py), and the original kg_store.py (replaced
by modules/kg_store.py, which owns the collections properly).

Curated inputs these read live in `dags/kg_config/`, tracked in git.
They used to live in `data/`, which is gitignored — which is exactly how a
104-line reviewed taxonomy ended up retyped by hand into a Python tuple.
"""
