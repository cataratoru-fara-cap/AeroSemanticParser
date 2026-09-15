"""
app.py — KYM pipeline dashboard, Overview page.

Answers the question you actually open a dashboard to answer: is the
pipeline healthy, and where is the corpus losing pages?

Reads two things (see lib/data.py): live collections for current state,
`run_summaries` for trend. Nothing here writes.
"""

from __future__ import annotations

import streamlit as st

from lib import charts, components as ui, data
from lib.theme import PLOTLY_CONFIG, active_palette

st.set_page_config(page_title="KYM Pipeline", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")

pal = active_palette()

reachable, err = data.ping()
if not reachable:
    st.error(f"Cannot reach MongoDB at the configured `MONGODB_URI`.\n\n```\n{err}\n```")
    st.caption("The dashboard is read-only; this is a connection problem, "
               "not a data problem. Check the `mongo` service is up and on "
               "the same Docker network.")
    st.stop()

st.title("KYM pipeline")
st.caption("discovery → scrape → parse · live corpus state and per-run history")

disc = data.discovery_state()
scr = data.scrape_state()
prs = data.parse_state()

# ---------------------------------------------------------------------------
# Hero + KPI row — single values, so tiles and a figure, not charts
# ---------------------------------------------------------------------------

left, right = st.columns([1, 2], gap="large")

with left:
    ui.hero("Corpus-ready entries", prs["entries_ready"], pal,
            caption=f"of {prs['entries_total']:,} parsed "
                    f"({prs['entries_ready'] / prs['entries_total']:.1%})"
                    if prs["entries_total"] else None)

with right:
    ui.stat_tiles([
        {"label": "URLs discovered", "value": disc["total"],
         "help": "Every URL in the `urls` collection, confirmed or not."},
        {"label": "Confirmed", "value": disc["confirmed"],
         "help": "Sitemap-confirmed entry pages — the scrape candidate set."},
        {"label": "DOMs stored", "value": scr["doms_ok"],
         "help": "Pages fetched successfully and held in `doms`."},
        {"label": "Parsed", "value": prs["entries_total"],
         "help": "Rows in `entries`, ready or incomplete."},
    ], pal)

st.divider()

# ---------------------------------------------------------------------------
# The funnel — where the corpus loses pages
# ---------------------------------------------------------------------------

st.subheader("Where the corpus loses pages")
st.caption("Each stage's input is the stage above it. A big drop is either "
           "a backlog (work not yet done) or a loss (work that failed) — the "
           "stage pages tell you which.")

stages = [
    ("Discovered", disc["total"]),
    ("Confirmed", disc["confirmed"]),
    ("Scraped OK", scr["doms_ok"]),
    ("Parsed", prs["entries_total"]),
    ("Corpus-ready", prs["entries_ready"]),
]
st.plotly_chart(charts.funnel(stages, pal), config=PLOTLY_CONFIG,
                width="stretch")

prev = None
funnel_rows = []
for name, value in stages:
    funnel_rows.append({
        "stage": name,
        "count": value,
        "share of discovered": f"{value / stages[0][1]:.1%}" if stages[0][1] else "—",
        "lost vs previous stage": f"{prev - value:,}" if prev is not None else "—",
    })
    prev = value
ui.data_table(funnel_rows, "Show funnel data")

st.divider()

# ---------------------------------------------------------------------------
# Stage health — three meters, one per stage boundary
# ---------------------------------------------------------------------------

st.subheader("Stage health")
c1, c2, c3 = st.columns(3, gap="large")

with c1:
    st.markdown("**Scrape coverage**")
    ui.meter("Confirmed URLs with a stored DOM",
             scr["doms_ok"], scr["urls_confirmed"], pal)
    ui.status_row([
        ("good", "OK", scr["doms_ok"]),
        ("warning", "Retryable", scr["failed_retryable"]),
        ("critical", "Permanent", scr["failed_permanent"]),
    ], pal)

with c2:
    st.markdown("**Parse coverage**")
    ui.meter("Stored DOMs turned into entries",
             prs["entries_total"], scr["doms_ok"], pal)
    ui.status_row([
        ("good", "Parsed", prs["entries_total"]),
        ("critical", "Dead-letter", prs["parse_failures"]),
    ], pal)

with c3:
    st.markdown("**Corpus readiness**")
    ui.meter("Entries passing the corpus policy",
             prs["entries_ready"], prs["entries_total"], pal,
             good_above=0.8, warn_above=0.4)
    ui.status_row([
        ("good", "Ready", prs["entries_ready"]),
        ("warning", "Incomplete", prs["entries_incomplete"]),
    ], pal)

st.divider()

# ---------------------------------------------------------------------------
# Freshness — is this thing actually running
# ---------------------------------------------------------------------------

st.subheader("Last run per stage")
fresh = data.stage_freshness()
if not fresh:
    ui.empty_state(
        "No run summaries recorded yet.",
        "Each DAG's `record_summary` task writes one document per run to "
        "`run_summaries`. Trigger a DAG and this fills in.")
else:
    st.dataframe(
        [{"stage": stage,
          "dag": v["dag_id"],
          "last run": v["created_at"].strftime("%Y-%m-%d %H:%M UTC")
                      if v["created_at"] else "—",
          "age": f"{v['age_hours']:.0f}h" if v["age_hours"] is not None else "—",
          "run id": v["run_id"]}
         for stage, v in sorted(fresh.items())],
        width="stretch", hide_index=True)

with st.sidebar:
    st.markdown("### About")
    st.caption(
        "**Current state** panels aggregate the live `urls`, `doms`, "
        "`entries` and `parse_failures` collections, so they include work "
        "finished since the last DAG run.\n\n"
        "**Trend** charts read `run_summaries` — one document per stage per "
        "run — because the live collections know what is true now, not what "
        "was true in July.\n\n"
        "Reads are cached for 60s. Use the button below to force a refresh.")
    if st.button("Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Light/dark follows your Streamlit theme "
               "(menu → Settings → Appearance).")
