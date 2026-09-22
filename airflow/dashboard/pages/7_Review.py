"""
Review — the one page that writes, and the only answer to gap 08.

Every `mk:Event` in the graph is a language model's reading of a sentence
of KYM prose. Automated checks prove nothing was ADDED to the page; they
cannot tell whether the reading is right, or whether anything was missed.
Only a person can, and this is where they do it.

Two things about the layout are deliberate:

  * **The recall question comes first.** You say which sentences narrate
    an event BEFORE the extraction is revealed. With the events on screen
    your attention anchors to them and misses stop being visible — that is
    the single biggest bias risk in reviewing your own system's output.
  * **Every field defaults to "ok".** A reviewer marks exceptions. Asking
    for 306 explicit confirmations would guarantee rubber-stamping.

Sampling, scoring and the confidence intervals live in
`dags/modules/kg/review.py`; this page only reads a drawn section and
writes a verdict (`lib/review.py` explains why writing is allowed here).
"""

from __future__ import annotations

import time

import streamlit as st

from lib import components as ui, review
from lib.theme import active_palette

st.set_page_config(page_title="Review · KYM", page_icon="🔍", layout="wide")
pal = active_palette()

st.title("Event review")
st.caption("gap 08 · one section at a time · verdicts land in `event_reviews`")

try:
    prog = review.progress()
except Exception as exc:                      # noqa: BLE001 - shown, not raised
    st.error(f"Cannot reach MongoDB.\n\n```\n{exc}\n```")
    st.stop()

if not prog["sections_total"]:
    ui.empty_state(
        "No review sample has been drawn yet.",
        "On the Airflow side: `python -m modules.kg.review draw`. It picks "
        "60 representative sections, 30 holding a pipeline-computed "
        "relative date, and every section with a non-confirmed event.")
    st.stop()

# -- where we are -----------------------------------------------------------
ui.stat_tiles([
    {"label": "Sections read", "value": prog["sections_done"],
     "help": f"of {prog['sections_total']} drawn"},
    {"label": "Representative",
     "value": f"{prog['by_sample']['representative']['done']}"
              f"/{prog['by_sample']['representative']['sections']}",
     "help": "The headline sample: a proportional draw across section kind "
             "and event density. Only this one's rate describes the layer."},
    {"label": "Relative dates",
     "value": f"{prog['by_sample']['relative']['done']}"
              f"/{prog['by_sample']['relative']['sections']}",
     "help": "Dates the PIPELINE computed by counting from another event. "
             "4% of events, and an error here runs down a chain."},
    {"label": "Non-confirmed",
     "value": f"{prog['by_sample']['unconfirmed']['done']}"
              f"/{prog['by_sample']['unconfirmed']['sections']}",
     "help": "Every section holding a hedged event — 51 of them, so this "
             "is a census rather than an estimate."},
], pal)
ui.meter("Sections reviewed", prog["sections_done"], prog["sections_total"], pal)
st.divider()

# -- pick a section ---------------------------------------------------------
left, right = st.columns([3, 2], gap="large")
with left:
    which = st.selectbox("Sample", ("all", *review.SAMPLES), index=0)
with right:
    reviewer = st.text_input("Reviewer", value=st.session_state.get("reviewer", ""),
                             placeholder="your name — recorded with each verdict")
    st.session_state["reviewer"] = reviewer

show_done = st.checkbox("Include sections already reviewed "
                        "(for a second, blind pass)", value=False)
rows = review.queue(which, include_done=show_done)
if not rows:
    st.success("Nothing left in this sample. Run "
               "`python -m modules.kg.review report` for the number.")
    st.stop()

labels = [f"{r['frame_url'].rsplit('/', 1)[-1][:46]} · {r['source_section']}"
          f"{'  ✓' if r.get('status') == 'done' else ''}" for r in rows]
idx = st.selectbox(f"Section ({len(rows)} to read)", range(len(rows)),
                   format_func=lambda i: labels[i])
doc = review.section(rows[idx]["_id"])
if doc is None:
    st.warning("That section is gone — redraw the sample.")
    st.stop()

st.session_state.setdefault("opened_at", {})
st.session_state["opened_at"].setdefault(doc["_id"], time.time())

st.subheader(f"{doc['frame_url'].rsplit('/', 1)[-1]} · {doc['source_section']}")
st.caption(f"{doc.get('heading') or ''} · in {', '.join(doc['samples'])} · "
           f"extraction {doc.get('extraction_version')} / prompt "
           f"{doc.get('prompt_version')} · [open the page]({doc['frame_url']})")

# -- 1. the prose, numbered, with NO events shown ---------------------------
st.markdown("#### The section, as the model saw it")
for s in doc["sentences"]:
    st.markdown(f"**{s['id']}.**&nbsp; {s['text']}", unsafe_allow_html=True)

st.markdown("#### 1 · Which sentences narrate an event?")
st.caption("Answer before revealing the extraction. A sentence that "
           "describes, comments or lists examples is not an event.")
narrating = st.multiselect(
    "Sentences that narrate something that HAPPENED",
    [s["id"] for s in doc["sentences"]],
    default=st.session_state.get(f"narr::{doc['_id']}", []),
    key=f"narr::{doc['_id']}")

covered = {i for e in doc["events"]
           for i in range(e["sentences"][0], e["sentences"][-1] + 1)}
missed = sorted(set(narrating) - covered)

reveal = st.checkbox("Reveal the extraction", key=f"rev::{doc['_id']}")
if not reveal:
    st.info("Answer the recall question above, then reveal.")
    st.stop()

if missed:
    st.warning(f"**Missed: sentence(s) {', '.join(map(str, missed))}** — you "
               f"marked them as narrating an event and none was extracted.")
else:
    st.success("Every sentence you marked is covered by an event.")

# -- 2. the events ----------------------------------------------------------
st.markdown("#### 2 · Is each event right?")
verdicts: dict[str, dict[str, str]] = {}
stored = doc.get("verdicts") or {}
for n, e in enumerate(doc["events"], 1):
    with st.container(border=True):
        head = (f"**Event {n}** · sentences {e['sentences']} · "
                f"`{e['date'] or 'undated'}`")
        if e.get("date_basis"):
            head += f" ({e['date_precision']}/{e['date_basis']})"
        st.markdown(head)
        st.markdown(f"> {e['source_text']}")
        bits = [f"**when** {e.get('date_text') or '—'}",
                f"**where** {e.get('location') or '—'} "
                f"({e.get('location_type')})",
                f"**who** {', '.join(e['actors']) or '—'}",
                f"**certainty** {e['certainty']}"]
        st.caption(" · ".join(bits))
        prior = stored.get(e["event_id"], {})
        cols = st.columns(len(review.FIELDS))
        row: dict[str, str] = {}
        for col, field in zip(cols, review.FIELDS):
            with col:
                options = review.VERDICTS[field]
                row[field] = st.radio(
                    field, options,
                    index=options.index(prior.get(field, "ok"))
                    if prior.get(field, "ok") in options else 0,
                    key=f"v::{doc['_id']}::{e['event_id']}::{field}")
        verdicts[e["event_id"]] = row

note = st.text_area("Anything worth writing down", value=doc.get("note") or "",
                    key=f"note::{doc['_id']}",
                    placeholder="what was wrong, or what the page actually says")

flagged = sum(1 for v in verdicts.values()
              for f, value in v.items() if value != "ok")
st.caption(f"{len(verdicts)} events · {flagged} field(s) flagged · "
           f"{len(missed)} missed sentence(s)")

if st.button("Save and take the next section", type="primary",
             disabled=not reviewer):
    started = st.session_state["opened_at"].pop(doc["_id"], None)
    review.save(doc["_id"], verdicts=verdicts, missed_sentences=missed,
                note=note, reviewer=reviewer,
                elapsed_s=round(time.time() - started, 1) if started else None)
    st.cache_data.clear()
    st.rerun()
if not reviewer:
    st.caption("Put your name in the Reviewer box to save — a verdict with "
               "nobody attached cannot be checked against a second pass.")
