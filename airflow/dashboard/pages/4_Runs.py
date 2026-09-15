"""Every recorded run, newest first — the audit view behind the charts."""

from __future__ import annotations

import json

import streamlit as st

from lib import components as ui, data
from lib.theme import active_palette

st.set_page_config(page_title="Runs · KYM", page_icon="📜", layout="wide")
pal = active_palette()

reachable, err = data.ping()
if not reachable:
    st.error(f"Cannot reach MongoDB.\n\n```\n{err}\n```")
    st.stop()

st.title("Run history")
st.caption("One document per stage per run, from `run_summaries`. This is the "
           "raw material every trend chart is derived from.")

rows = data.run_history(None, limit=500)
if not rows:
    ui.empty_state(
        "No run summaries recorded yet.",
        "Each DAG's `record_summary` task writes one document per run.")
    st.stop()

stages = sorted({r["stage"] for r in rows if r["stage"]})
picked = st.multiselect("Stage", stages, default=stages,
                        help="Filter the table and the drill-down below.")
shown = [r for r in reversed(rows) if r["stage"] in picked]

st.dataframe(
    [{"stage": r["stage"],
      "when": r["created_at"].strftime("%Y-%m-%d %H:%M UTC")
              if r["created_at"] else "—",
      "dag": r["dag_id"],
      "run id": r["run_id"],
      "keys": ", ".join(sorted((r["summary"] or {}).keys()))}
     for r in shown],
    width="stretch", hide_index=True)

st.divider()
st.subheader("Inspect one run")

if shown:
    labels = [f"{r['stage']} · "
              f"{r['created_at'].strftime('%Y-%m-%d %H:%M') if r['created_at'] else '?'}"
              f" · {r['run_id']}" for r in shown]
    choice = st.selectbox("Run", range(len(labels)),
                          format_func=lambda i: labels[i])
    st.json(shown[choice]["summary"], expanded=True)
    st.download_button(
        "Download this summary as JSON",
        data=json.dumps(shown[choice]["summary"], indent=2, default=str),
        file_name=f"{shown[choice]['stage']}_{shown[choice]['run_id']}.json",
        mime="application/json")
