"""
Knowledge-graph stage — what was published, and whether the four copies agree.

The question this page answers is different from the parse page's. Parse
asks "why are entries not corpus-ready"; this asks "is the graph that is
live the graph we think it is": which build is published, whether the RDF
re-derivation agreed with it, whether every store points at the same build,
and what the taxonomy withheld.
"""

from __future__ import annotations

import streamlit as st

from lib import charts, components as ui, data
from lib.theme import PLOTLY_CONFIG, active_palette

st.set_page_config(page_title="Knowledge graph · KYM", page_icon="🕸️", layout="wide")
pal = active_palette()

reachable, err = data.ping()
if not reachable:
    st.error(f"Cannot reach MongoDB.\n\n```\n{err}\n```")
    st.stop()

st.title("Knowledge graph")
st.caption("`kym_kg`, triggered by parse · owns `kg_nodes`, `kg_edges` and "
           "`kg_builds` · publishes into Neo4j, Fuseki and `data/kg/`")

kg = data.kg_state()
rows = data.run_history("kg")
runs = [r for r in rows if not r["summary"].get("skipped")]
latest = runs[-1]["summary"] if runs else {}
skipped = len(rows) - len(runs)

if not kg["build_id"]:
    ui.empty_state("No published KG build yet.",
                   "Trigger the `kym_kg` DAG. Nothing is published until the "
                   "build passes verification, so an empty page here means "
                   "'not yet', never 'half done'.")
    st.stop()

# -- the headline: what is live ----------------------------------------------
left, right = st.columns([1, 2], gap="large")
with left:
    ui.hero("Published build", kg["nodes"], pal,
            caption=f"`{kg['build_id']}` · {kg['nodes']:,} nodes, "
                    f"{kg['edges']:,} edges"
                    + (f" · published {kg['published_at']:%Y-%m-%d %H:%M} UTC"
                       if kg["published_at"] else ""))
with right:
    ui.stat_tiles([
        {"label": "Frames", "value": kg["frames"],
         "help": "Scraped KYM entries in the graph — every other node hangs off one."},
        {"label": "Edges", "value": kg["edges"]},
        {"label": "RDF triples", "value": kg["manifest_counts"].get("triples"),
         "help": "Lines in graph.nt, and what Fuseki holds. More than edges: "
                 "each frame also carries type/label/category/status triples."},
        {"label": "Generations kept", "value": kg["generations"],
         "help": "Builds retained after prune. Rollback is re-pointing at the previous one."},
    ], pal)

st.divider()

# -- agreement: the reason this stage exists ----------------------------------
st.subheader("Do the four copies agree?")
st.caption(
    "One build, four stores. Mongo's `kg_builds/current` is the authority; files, "
    "Fuseki and Neo4j follow it, and `publish` reads every pointer back. The RDF "
    "gate re-derives the graph through the RML mapping with morph-kgc and diffs it "
    "against the in-process serialization — the two used to differ by 14,563 "
    "edges without anyone noticing.")

val = kg["validation"]
pointers = (latest.get("build") or {}).get("pointers") or {}
integrity = latest.get("integrity") or {}
items = []
if pointers:
    agree = all(v == kg["build_id"] for v in pointers.values())
    items.append(("good" if agree else "critical",
                  "stores agree on the build" if agree else "stores DISAGREE",
                  sum(1 for v in pointers.values() if v == kg["build_id"])))
if val is None:
    items.append(("warning", "RDF gate not yet run", 0))
else:
    items.append(("good" if val.get("equal") else "critical",
                  "RDF re-derivation agrees" if val.get("equal")
                  else "RDF re-derivation DIVERGES", val.get("rml_triples") or 0))
items.append(("good" if integrity.get("dangling_edge_targets", 0) == 0 else "critical",
              "dangling edge targets", integrity.get("dangling_edge_targets", 0)))
items.append(("good" if integrity.get("series_cycles_found", 0) == 0 else "critical",
              "series cycles", integrity.get("series_cycles_found", 0)))
ui.status_row(items, pal)

store_rows = []
stores = latest.get("stores") or {}
for name, key in (("Mongo (authority)", "mongo"), ("files", "files"),
                  ("Fuseki (RDF)", "fuseki"), ("Neo4j (property graph)", "neo4j")):
    held = stores.get(key) or {}
    store_rows.append({
        "store": name,
        "points at": pointers.get(key) or ("—" if key in ("fuseki", "neo4j") and key not in stores
                                           else pointers.get(key, "—")),
        "holds": (f"{held.get('triples'):,} triples" if held.get("triples") else
                  f"{held.get('nodes'):,} nodes / {held.get('edges'):,} edges"
                  if held.get("nodes") else
                  (f"{kg['nodes']:,} nodes / {kg['edges']:,} edges" if key == "mongo" else "")),
    })
ui.data_table(store_rows, "Show per-store pointers")
if val is not None and not val.get("equal"):
    st.error(f"The last RDF gate found content divergence on "
             f"`{', '.join(val.get('content_divergence') or [])}`. The build stays "
             f"published — the gate flags, it does not veto — but this needs looking at: "
             f"`{val.get('report')}`.")

st.divider()

# -- composition -----------------------------------------------------------------
st.subheader("What the graph is made of")
st.caption("One quantity across categories, so a single sequential hue. `frame_stub` "
           "nodes are entries referenced but not (yet) scraped; `external_ref` are "
           "outbound citations, which is why they dominate.")
c1, c2 = st.columns(2, gap="large")
with c1:
    st.markdown("**Nodes by kind**")
    if kg["nodes_by_kind"]:
        st.plotly_chart(charts.magnitude_bars(kg["nodes_by_kind"], pal, value_name="nodes"),
                        config=PLOTLY_CONFIG, width="stretch")
with c2:
    st.markdown("**Edges by type**")
    if kg["edges_by_type"]:
        st.plotly_chart(charts.magnitude_bars(kg["edges_by_type"], pal, value_name="edges"),
                        config=PLOTLY_CONFIG, width="stretch")
ui.data_table(
    [{"kind": k, "nodes": v} for k, v in kg["nodes_by_kind"].items()]
    + [{"type": k, "edges": v} for k, v in kg["edges_by_type"].items()],
    "Show composition")

if integrity:
    st.markdown("**Semantic attachment**")
    frames = kg["frames"] or 1
    m1, m2 = st.columns(2, gap="large")
    with m1:
        ui.meter("Frames with at least one entry type",
                 frames - integrity.get("frames_without_entry_type", 0), frames, pal,
                 good_above=0.85, warn_above=0.6)
    with m2:
        ui.meter("Series parents that are scraped frames (not stubs)",
                 frames - integrity.get("unresolved_series_parents", 0)
                 if integrity.get("unresolved_series_parents") is not None else frames,
                 frames, pal, good_above=0.8, warn_above=0.5)
    st.caption(
        f"Longest `partOfSeries` chain: **{integrity.get('series_chain_max_depth', '—')}** · "
        f"unresolved series parents (stubs): **{integrity.get('unresolved_series_parents', '—'):,}** · "
        f"isolated nodes: **{integrity.get('isolated_nodes', '—')}**")

st.divider()

# -- taxonomy --------------------------------------------------------------------
st.subheader("Entry-type taxonomy")
tax = latest.get("taxonomy") or {}
st.caption(
    "The curated record in `dags/kg_config/entry_type_taxonomy.yaml`. Only "
    "`broader_confirmed` and `broader_semantic_only` become `skos:broader` edges; "
    "the other buckets are decisions *not* to encode something, carried here so "
    "they stay visible rather than becoming folklore.")
if tax.get("buckets"):
    t1, t2 = st.columns([2, 1], gap="large")
    with t1:
        st.plotly_chart(charts.magnitude_bars(tax["buckets"], pal, value_name="pairs"),
                        config=PLOTLY_CONFIG, width="stretch")
    with t2:
        ui.stat_tiles([{"label": "broader edges encoded", "value": tax.get("edges_encoded")},
                       {"label": "pairs withheld", "value": tax.get("withheld_pairs")}], pal)
        missing = tax.get("slugs_missing_from_census") or []
        if missing:
            st.warning(f"Taxonomy slugs absent from the corpus: `{'`, `'.join(missing)}` — "
                       f"their edges were dropped from this build, not silently skipped.")
        else:
            st.caption("Every slug the taxonomy names exists in the corpus.")
    ui.data_table([{"bucket": k, "pairs": v} for k, v in tax["buckets"].items()],
                  "Show taxonomy buckets")
else:
    ui.empty_state("No taxonomy report in the latest run summary.")

st.divider()

# -- generations -------------------------------------------------------------------
st.subheader("Builds")
builds = data.kg_builds()
if builds:
    ui.data_table(
        [{"build": b["build_id"], "state": b["state"],
          "published": "●" if b["published"] else "",
          "created": b["created_at"].strftime("%Y-%m-%d %H:%M") if b["created_at"] else "—",
          "triples": b["triples"], "RDF gate": b["rdf_diff"] or "not run"}
         for b in builds],
        f"Show the {len(builds)} retained generation(s)")
if kg["legacy_docs"]:
    st.caption(
        f"**{kg['legacy_docs']:,} documents** in `kg_nodes`/`kg_edges` predate the "
        f"generational store and carry no `build_id`. They are invisible to every "
        f"query on this page and are never pruned — dead weight that is safe to drop; "
        f"the current build already contains everything they held.")

st.divider()

# -- across runs ---------------------------------------------------------------------
st.subheader("Across runs")
if skipped:
    st.caption(f"{skipped} of {len(rows)} runs were skipped by the staleness gate "
               f"(nothing that affects the graph had changed) and are omitted below.")
if len(runs) < 2:
    ui.empty_state(
        f"Only {len(runs)} completed build recorded — a trend needs at least 2.",
        "Every `kym_kg` run that builds adds one document to `run_summaries`.")
else:
    tab_graph, tab_edges, tab_run = st.tabs(["Graph size", "Edges by type", "Per-run work"])
    with tab_graph:
        series = {name: data.scalar_series(runs, path)
                  for name, path in [("nodes", "graph.nodes"), ("edges", "graph.edges"),
                                     ("frames", "graph.frames"), ("triples", "exports.triples")]}
        st.plotly_chart(charts.trend_lines({k: v for k, v in series.items() if v}, pal,
                                           value_name="count"),
                        config=PLOTLY_CONFIG, width="stretch")
    with tab_edges:
        types = sorted({t for r in runs for t in (r["summary"].get("graph", {})
                                                   .get("edges_by_type") or {})})
        series = {t: data.scalar_series(runs, f"graph.edges_by_type.{t}") for t in types}
        series = {k: v for k, v in series.items() if v}
        st.plotly_chart(charts.trend_lines(series, pal, value_name="edges"),
                        config=PLOTLY_CONFIG, width="stretch")
        note = charts.dropped_series_note(series)
        if note:
            st.caption(note)
    with tab_run:
        series = {name: data.scalar_series(runs, f"run.{key}")
                  for name, key in [("entries", "entries"), ("nodes written", "nodes_written"),
                                    ("edges written", "edges_written"),
                                    ("stubs materialized", "stubs_materialized")]}
        st.plotly_chart(charts.trend_lines({k: v for k, v in series.items() if v}, pal,
                                           value_name="this run"),
                        config=PLOTLY_CONFIG, width="stretch")
    ui.data_table(
        [{"run": r["run_id"], "when": r["created_at"].strftime("%Y-%m-%d %H:%M UTC"),
          "build": r["summary"].get("build", {}).get("build_id"),
          "nodes": r["summary"].get("graph", {}).get("nodes"),
          "edges": r["summary"].get("graph", {}).get("edges"),
          "triples": r["summary"].get("exports", {}).get("triples")}
         for r in reversed(runs)],
        "Show every completed build")
