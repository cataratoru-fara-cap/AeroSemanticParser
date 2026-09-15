"""Scrape stage — ScrapingAnt fetch outcomes and stored DOM coverage."""

from __future__ import annotations

import streamlit as st

from lib import charts, components as ui, data
from lib.theme import PLOTLY_CONFIG, active_palette

st.set_page_config(page_title="Scrape · KYM", page_icon="🌐", layout="wide")
pal = active_palette()

reachable, err = data.ping()
if not reachable:
    st.error(f"Cannot reach MongoDB.\n\n```\n{err}\n```")
    st.stop()

st.title("Scrape")
st.caption("`kym_scrape`, triggered by discovery · owns the `doms` collection")

state = data.scrape_state()

ui.stat_tiles([
    {"label": "DOMs stored", "value": state["doms_ok"]},
    {"label": "Failed", "value": state["doms_failed"]},
    {"label": "Corpus size",
     "value": round(state["content_bytes"] / 1_000_000_000, 2),
     "help": "Total uncompressed HTML in GB (stored zlib-compressed)."},
    {"label": "Avg page",
     "value": round(state["content_avg"] / 1024),
     "help": "Average uncompressed page size in KB."},
], pal)

st.divider()

c1, c2 = st.columns([1, 1], gap="large")

with c1:
    st.subheader("Coverage")
    st.caption("A single ratio against its limit — a meter, not a two-slice pie.")
    ui.meter("Confirmed URLs with a stored DOM",
             state["doms_ok"], state["urls_confirmed"], pal)
    backlog = state["urls_confirmed"] - state["doms_ok"] - state["doms_failed"]
    st.caption(
        f"**{max(backlog, 0):,}** confirmed URLs have never been attempted. "
        f"At roughly 1 ScrapingAnt credit per page (`browser=false`), that is "
        f"the remaining cost to complete the corpus.")

with c2:
    st.subheader("Fetch outcomes")
    st.caption("Reserved status colours, each with its own icon and label — "
               "the colour never carries the meaning alone.")
    ui.status_row([
        ("good", "OK", state["doms_ok"]),
        ("warning", "Retryable", state["failed_retryable"]),
        ("critical", "Permanent", state["failed_permanent"]),
    ], pal)
    st.caption(
        "Retryable failures are re-queued automatically on the next run "
        "(up to 3 attempts). Permanent ones — 400/404/405/422 — never are; "
        "they usually mean the URL should not have been in the candidate set.")

if state["by_error_kind"]:
    st.divider()
    st.subheader("Failures by kind")
    st.plotly_chart(
        charts.magnitude_bars(state["by_error_kind"], pal, value_name="DOMs"),
        config=PLOTLY_CONFIG, width="stretch")
    ui.data_table([{"error kind": k, "count": v}
                   for k, v in sorted(state["by_error_kind"].items(),
                                      key=lambda kv: -kv[1])],
                  "Show failure kinds")

st.divider()

st.subheader("Across runs")
rows = data.run_history("scrape")
if len(rows) < 2:
    ui.empty_state(
        f"Only {len(rows)} scrape run recorded — a trend needs at least 2.",
        "Every `kym_scrape` run adds one document to `run_summaries`.")
else:
    tab_corpus, tab_run = st.tabs(["Corpus totals", "Per-run throughput"])
    with tab_corpus:
        series = {name: data.scalar_series(rows, f"corpus.{key}")
                  for name, key in [("stored OK", "doms_ok"),
                                    ("failed", "doms_failed"),
                                    ("confirmed URLs", "urls_confirmed")]}
        st.plotly_chart(
            charts.trend_lines({k: v for k, v in series.items() if v}, pal,
                               value_name="DOMs"),
            config=PLOTLY_CONFIG, width="stretch")
    with tab_run:
        series = {name: data.scalar_series(rows, f"run.{key}")
                  for name, key in [("fetched OK", "ok"), ("failed", "failed"),
                                    ("kept existing", "kept_ok"),
                                    ("skipped", "skipped")]}
        st.plotly_chart(
            charts.trend_lines({k: v for k, v in series.items() if v}, pal,
                               value_name="pages this run"),
            config=PLOTLY_CONFIG, width="stretch")
    ui.data_table(
        [{"run": r["run_id"],
          "when": r["created_at"].strftime("%Y-%m-%d %H:%M UTC"),
          **{f"run.{k}": v for k, v in (r["summary"].get("run") or {}).items()},
          **{f"corpus.{k}": v for k, v in (r["summary"].get("corpus") or {}).items()}}
         for r in reversed(rows)],
        "Show every scrape run")
