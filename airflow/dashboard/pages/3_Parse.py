"""
Parse stage — the diagnostic page.

This is where the interesting question lives: 23,882 pages parse, but only
~4,300 clear the corpus policy. `missing_field_counts` says which field is
responsible, which is the difference between "the parser is broken" and
"KYM does not publish a region for most memes".
"""

from __future__ import annotations

import streamlit as st

from lib import charts, components as ui, data
from lib.theme import PLOTLY_CONFIG, active_palette

st.set_page_config(page_title="Parse · KYM", page_icon="🧩", layout="wide")
pal = active_palette()

reachable, err = data.ping()
if not reachable:
    st.error(f"Cannot reach MongoDB.\n\n```\n{err}\n```")
    st.stop()

st.title("Parse")
st.caption("`kym_parse`, triggered by scrape · owns `entries` and "
           "`parse_failures`")

state = data.parse_state()

ui.stat_tiles([
    {"label": "Entries", "value": state["entries_total"]},
    {"label": "Corpus-ready", "value": state["entries_ready"]},
    {"label": "Incomplete", "value": state["entries_incomplete"],
     "help": "Parsed fine, but missing a field the corpus policy requires. "
             "Kept and queryable — never discarded."},
    {"label": "Dead-letter", "value": state["parse_failures"],
     "help": "Failed schema validation. Held in `parse_failures`, not "
             "re-queued until the parser version or the page changes."},
], pal)

st.divider()

# -- the headline diagnostic ------------------------------------------------
st.subheader("Why entries are not corpus-ready")
st.caption(
    "Each bar counts entries missing that field. One entry can miss several, "
    "so these do not sum to the incomplete total. Read it as: fix the top bar "
    "and this many entries move toward ready.")

if state["missing_field_counts"]:
    left, right = st.columns([2, 1], gap="large")
    with left:
        st.plotly_chart(
            charts.magnitude_bars(state["missing_field_counts"], pal,
                                  value_name="entries missing it"),
            config=PLOTLY_CONFIG, width="stretch")
    with right:
        ui.meter("Entries passing the corpus policy",
                 state["entries_ready"], state["entries_total"], pal,
                 good_above=0.8, warn_above=0.4)
        top = max(state["missing_field_counts"].items(), key=lambda kv: kv[1])
        share = top[1] / state["entries_total"] if state["entries_total"] else 0
        st.caption(
            f"**`{top[0]}`** is the binding constraint: {top[1]:,} entries "
            f"({share:.0%}) lack it. If that field is not actually required "
            f"for the downstream KG, relaxing it in `CorpusPolicy` is a one-line "
            f"change that re-grades the corpus without re-parsing a single page.")
    ui.data_table(
        [{"missing field": k, "entries": v,
          "share of all entries":
              f"{v / state['entries_total']:.1%}" if state["entries_total"] else "—"}
         for k, v in sorted(state["missing_field_counts"].items(),
                            key=lambda kv: -kv[1])],
        "Show all missing fields")
else:
    ui.empty_state("No entries parsed yet.", "Trigger the `kym_parse` DAG.")

st.divider()

# -- failures ---------------------------------------------------------------
st.subheader("Parse failures")
st.caption(
    "A failure clustered under one namespace usually means a non-meme URL "
    "slipped through the confirmed filter — not that the parser is broken. "
    "That is a very different fix, which is why these are split two ways.")

c1, c2 = st.columns(2, gap="large")
with c1:
    st.markdown("**By error type**")
    if state["failure_type_counts"]:
        st.plotly_chart(
            charts.magnitude_bars(state["failure_type_counts"], pal,
                                  value_name="failures"),
            config=PLOTLY_CONFIG, width="stretch")
    else:
        st.caption("No failures recorded.")
with c2:
    st.markdown("**By namespace**")
    if state["failure_namespace_counts"]:
        st.plotly_chart(
            charts.magnitude_bars(state["failure_namespace_counts"], pal,
                                  value_name="failures"),
            config=PLOTLY_CONFIG, width="stretch")
    else:
        st.caption("No failures recorded.")

samples = data.failure_samples(100)
if samples:
    ui.data_table(
        [{**s, "failed_at": s["failed_at"].strftime("%Y-%m-%d %H:%M")
          if s["failed_at"] else "—"} for s in samples],
        f"Show the {len(samples)} most recent dead-letter records")

st.divider()

# -- corpus composition -----------------------------------------------------
st.subheader("Corpus composition")
c1, c2 = st.columns(2, gap="large")
with c1:
    st.markdown("**By category**")
    if state["by_category"]:
        st.plotly_chart(
            charts.magnitude_bars(state["by_category"], pal, value_name="entries"),
            config=PLOTLY_CONFIG, width="stretch")
with c2:
    st.markdown("**By KYM status**")
    if state["by_status"]:
        st.plotly_chart(
            charts.magnitude_bars(state["by_status"], pal, value_name="entries"),
            config=PLOTLY_CONFIG, width="stretch")

if state["parser_versions"]:
    st.caption(
        "Parser versions present in `entries`: "
        + ", ".join(f"`{k}` ({v:,})" for k, v in state["parser_versions"].items())
        + ". More than one means part of the corpus predates the current "
          "parser and is due for a re-parse — `pending_urls` picks that up "
          "automatically on the next run.")

st.divider()

st.subheader("Across runs")
rows = data.run_history("parse")
if len(rows) < 2:
    ui.empty_state(
        f"Only {len(rows)} parse run recorded — a trend needs at least 2.",
        "Every `kym_parse` run adds one document to `run_summaries`.")
else:
    tab_corpus, tab_missing, tab_run = st.tabs(
        ["Corpus totals", "Missing fields", "Per-run throughput"])
    with tab_corpus:
        series = {name: data.scalar_series(rows, f"corpus.{key}")
                  for name, key in [("total", "entries_total"),
                                    ("ready", "entries_ready"),
                                    ("incomplete", "entries_incomplete"),
                                    ("dead-letter", "parse_failures")]}
        st.plotly_chart(
            charts.trend_lines({k: v for k, v in series.items() if v}, pal,
                               value_name="entries"),
            config=PLOTLY_CONFIG, width="stretch")
    with tab_missing:
        fields = sorted(
            {f for r in rows
             for f in (r["summary"].get("corpus", {})
                       .get("missing_field_counts") or {})})
        series = {f: data.scalar_series(rows, f"corpus.missing_field_counts.{f}")
                  for f in fields}
        series = {k: v for k, v in series.items() if v}
        st.plotly_chart(
            charts.trend_lines(series, pal, value_name="entries missing it"),
            config=PLOTLY_CONFIG, width="stretch")
        note = charts.dropped_series_note(series)
        if note:
            st.caption(note)
    with tab_run:
        series = {name: data.scalar_series(rows, f"run.{key}")
                  for name, key in [("ready", "ready"), ("incomplete", "incomplete"),
                                    ("failed", "parse_failed"),
                                    ("skipped", "skipped")]}
        st.plotly_chart(
            charts.trend_lines({k: v for k, v in series.items() if v}, pal,
                               value_name="pages this run"),
            config=PLOTLY_CONFIG, width="stretch")
