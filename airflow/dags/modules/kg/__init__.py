"""
helpers — post-parse knowledge-graph modules (hand-run, not orchestrated).

Nothing in this package is wired into a DAG. kg_build/kg_store/kg_census/
kg_export/kg_export_rml/kg_metrics/kg_type_semantics are run by hand
against the `entries` collection the parse stage produces. Treat them as
staging for a future KG DAG, not as part of the live pipeline.
"""
