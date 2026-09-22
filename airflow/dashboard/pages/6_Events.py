"""
Events stage — how much of the narrative has been read, and how.

The event layer is the one part of the graph that is a language model's
reading rather than parsed fact (gap 08), so this page answers two
different questions. Coverage: how far the backfill has got, against the
entries that actually HAVE an Origin or Spread section. And character: what
the extracted events look like — how precisely dated, how hedged, which
model said them — plus a sample to check against the quotes they cite.
"""

from __future__ import annotations

import streamlit as st

from lib import charts, components as ui, data
from lib.theme import PLOTLY_CONFIG, active_palette

st.set_page_config(page_title="Events · KYM", page_icon="🗓️", layout="wide")
pal = active_palette()

reachable, err = data.ping()
if not reachable:
    st.error(f"Cannot reach MongoDB.\n\n```\n{err}\n```")
    st.stop()

st.title("Events")
st.caption("`kym_events`, triggered by parse · owns `events` and "
           "`event_failures` · one LLM call per Origin/Spread section")

state = data.event_state()

if not state["units_total"] and not state["failures_total"]:
    ui.empty_state(
        "No events extracted yet.",
        "Trigger `kym_events`. For the initial backfill, run it in batches "
        "with `trigger_kg=false` and build the graph once at the end.")
    st.stop()

ui.stat_tiles([
    {"label": "Events", "value": state["events_total"]},
    {"label": "Sections read", "value": state["units_total"],
     "help": "One unit = one Origin or Spread section = one LLM call."},
    {"label": "Dated", "value": state["events_total"] - state[
        "events_by_precision"].get("none", 0),
     "help": "Events the section dates, at the precision it gives. The rest "
             "are narrated with no day, month or year anywhere the pipeline "
             "can reach — undated rather than dated by guess."},
    {"label": "Read, nothing found", "value": state["zero_event_units"],
     "help": "Sections that describe rather than narrate. A correct answer, "
             "stored so the section is not asked again."},
    {"label": "Dead-letter", "value": state["failures_total"],
     "help": "Sections the model could not answer within the schema. Held in "
             "`event_failures`; not retried until the text, prompt, schema or "
             "extraction version changes."},
], pal)

st.divider()

# -- coverage ---------------------------------------------------------------
st.subheader("Backfill coverage")
left, right = st.columns([1, 2], gap="large")
with left:
    ui.meter("Origin/Spread sections extracted (target: 100%)",
             state["units_total"], state["units_possible"], pal,
             good_above=0.999, warn_above=0.5)
with right:
    st.caption(
        "The denominator is every Origin or Spread section that has text "
        f"({state['units_possible']:,} sections across "
        f"{state['frames_with_narrative']:,} entries). Sections are never "
        "truncated and events are never capped. Until this is near full, a missing `mk:hasEvent` in the "
        "graph can mean \"not read yet\" as easily as \"no events\" — which is "
        "why the backfill should finish before the event layer is published.")
    if state["last_extracted_at"]:
        st.caption(f"Last extraction: "
                   f"{state['last_extracted_at']:%Y-%m-%d %H:%M} UTC.")

st.divider()

# -- character of the extracted events --------------------------------------
st.subheader("What the events look like")
st.caption(
    "Every event carries its source's own hedging and the precision its date "
    "was actually given at. A column of nothing but `confirmed` is worth a "
    "second look: it is what the prose mostly says, and also what a model that "
    "ignores hedges would produce (gap 08).")

c1, c2, c3 = st.columns(3, gap="large")
with c1:
    st.markdown("**Certainty**")
    if state["events_by_certainty"]:
        st.plotly_chart(
            charts.magnitude_bars(state["events_by_certainty"], pal,
                                  value_name="events"),
            config=PLOTLY_CONFIG, width="stretch")
with c2:
    st.markdown("**Date precision**")
    if state["events_by_precision"]:
        st.plotly_chart(
            charts.magnitude_bars(state["events_by_precision"], pal,
                                  value_name="events"),
            config=PLOTLY_CONFIG, width="stretch")
with c3:
    st.markdown("**Kind of place**")
    if state["events_by_location_type"]:
        st.plotly_chart(
            charts.magnitude_bars(state["events_by_location_type"], pal,
                                  value_name="events"),
            config=PLOTLY_CONFIG, width="stretch")

ui.data_table(
    [{"breakdown": name, "value": k, "events": v}
     for name, counts in (("certainty", state["events_by_certainty"]),
                          ("date precision", state["events_by_precision"]),
                          ("kind of place", state["events_by_location_type"]))
     for k, v in counts.items()],
    "Show these counts as a table")

st.divider()

# -- provenance ---------------------------------------------------------------
st.subheader("Which model read what")
st.caption(
    "A model change does NOT re-queue sections already read (unlike "
    "`kg/semantics.py`, which refuses to mix models): each section is an "
    "independent record, and re-running tens of GPU-hours on every weights "
    "rotation would be absurd. The mix is allowed — and shown here, so it is "
    "never invisible.")
c1, c2 = st.columns(2, gap="large")
with c1:
    st.markdown("**Sections by model**")
    st.plotly_chart(
        charts.magnitude_bars(state["models_in_use"], pal, value_name="sections"),
        config=PLOTLY_CONFIG, width="stretch")
with c2:
    st.markdown("**Dead-letter by kind**")
    if state["failure_kind_counts"]:
        st.plotly_chart(
            charts.magnitude_bars(state["failure_kind_counts"], pal,
                                  value_name="sections"),
            config=PLOTLY_CONFIG, width="stretch")
        st.caption("`invalid` = the reply itself was not the schema's shape "
                   "(not JSON, no event list). A value the model added does "
                   "not land here — it is stripped and counted above. "
                   "`permanent` = the host refused the request outright.")
    else:
        st.caption("No failures recorded.")

samples = data.event_samples(40)
if samples:
    ui.data_table(samples, f"Check {len(samples)} recent events against their evidence")

st.divider()

# -- history ----------------------------------------------------------------
st.subheader("Across runs")
rows = data.run_history("events")
if len(rows) < 2:
    ui.empty_state(
        f"Only {len(rows)} event run recorded — a trend needs at least 2.",
        "Every `kym_events` run adds one document to `run_summaries`.")
else:
    tab_corpus, tab_run = st.tabs(["Corpus totals", "Per-run throughput"])
    with tab_corpus:
        series = {name: data.scalar_series(rows, f"corpus.{key}")
                  for name, key in [("events", "events_total"),
                                    ("sections read", "units_total"),
                                    ("dead-letter", "failures_total")]}
        st.plotly_chart(
            charts.trend_lines({k: v for k, v in series.items() if v}, pal,
                               value_name="count"),
            config=PLOTLY_CONFIG, width="stretch")
    with tab_run:
        series = {name: data.scalar_series(rows, f"run.{key}")
                  for name, key in [("sections read", "units"),
                                    ("events", "events"),
                                    ("failed", "failed"),
                                    ("skipped", "skipped")]}
        st.plotly_chart(
            charts.trend_lines({k: v for k, v in series.items() if v}, pal,
                               value_name="this run"),
            config=PLOTLY_CONFIG, width="stretch")
