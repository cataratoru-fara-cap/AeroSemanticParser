"""Discovery stage — what the sitemap crawl found, and how it is split."""

from __future__ import annotations

import streamlit as st

from lib import charts, components as ui, data
from lib.theme import PLOTLY_CONFIG, active_palette

st.set_page_config(page_title="Discovery · KYM", page_icon="🗺️", layout="wide")
pal = active_palette()

reachable, err = data.ping()
if not reachable:
    st.error(f"Cannot reach MongoDB.\n\n```\n{err}\n```")
    st.stop()

st.title("Discovery")
st.caption("`kym_discovery`, monthly · owns the `urls` collection")

state = data.discovery_state()

ui.stat_tiles([
    {"label": "URLs total", "value": state["total"]},
    {"label": "Confirmed", "value": state["confirmed"],
     "help": "Found in a sitemap. Only these are scrape candidates by default."},
    {"label": "Unconfirmed", "value": state["unconfirmed"],
     "help": "Found by crawling listings but not present in any sitemap."},
    {"label": "No lastmod", "value": state["lastmod_null"],
     "help": "No sitemap lastmod, so staleness can only be judged by a "
             "refetch window, not by the page's own change date."},
], pal)

st.divider()

# -- namespace magnitude ----------------------------------------------------
st.subheader("Corpus by namespace")
st.caption("One quantity measured across categories, so the bars use a single "
           "hue — darker means larger. Different hues would imply these are "
           "different kinds of thing.")

totals = {ns: v["confirmed"] + v["unconfirmed"]
          for ns, v in state["by_namespace"].items()}
if totals:
    st.plotly_chart(charts.magnitude_bars(totals, pal, value_name="URLs"),
                    config=PLOTLY_CONFIG, width="stretch")
    ui.data_table(
        [{"namespace": ns,
          "total": v["confirmed"] + v["unconfirmed"],
          "confirmed": v["confirmed"],
          "unconfirmed": v["unconfirmed"],
          "confirmed %": f"{v['confirmed'] / (v['confirmed'] + v['unconfirmed']):.1%}"
                         if (v["confirmed"] + v["unconfirmed"]) else "—"}
         for ns, v in sorted(state["by_namespace"].items(),
                             key=lambda kv: -(kv[1]["confirmed"] + kv[1]["unconfirmed"]))],
        "Show all namespaces")
else:
    ui.empty_state("No URLs discovered yet.", "Trigger the `kym_discovery` DAG.")

st.divider()

# -- confirmed split --------------------------------------------------------
st.subheader("Confirmed vs unconfirmed, per namespace")
st.caption("Part-to-whole, so one stacked bar per namespace. A namespace that "
           "is mostly unconfirmed was reached by crawling listings rather than "
           "by a sitemap — worth checking before it is scraped.")
if state["by_namespace"]:
    st.plotly_chart(
        charts.split_bars(state["by_namespace"], ("confirmed", "unconfirmed"), pal),
        config=PLOTLY_CONFIG, width="stretch")

st.divider()

# -- trend ------------------------------------------------------------------
st.subheader("Growth across runs")
rows = data.run_history("discovery")
if len(rows) < 2:
    ui.empty_state(
        f"Only {len(rows)} discovery run recorded — a trend needs at least 2.",
        "Every `kym_discovery` run adds one document to `run_summaries`.")
else:
    series = {
        "total": data.scalar_series(rows, "total"),
        "confirmed": data.scalar_series(rows, "confirmed"),
        "no lastmod": data.scalar_series(rows, "lastmod_null"),
    }
    series = {k: v for k, v in series.items() if v}
    st.plotly_chart(charts.trend_lines(series, pal, value_name="URLs"),
                    config=PLOTLY_CONFIG, width="stretch")
    ui.data_table(
        [{"run": r["run_id"],
          "when": r["created_at"].strftime("%Y-%m-%d %H:%M UTC"),
          **{k: r["summary"].get(k)
             for k in ("total", "confirmed", "lastmod_null")}}
         for r in reversed(rows)],
        "Show every discovery run")
