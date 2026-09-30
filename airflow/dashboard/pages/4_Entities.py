"""
Entities — the Wikidata links and their curation (gap 09).

Two stages meet here. `kym_entities` links the words of each frame's title,
tags and About to Wikidata items, recall-first: a common noun links as
readily as a name. `kym_entity_curation` then decides which of those links
reach the graph — local rules first, an LLM judge for the rest — and keeps
the reason with every kept link (`mk:relevanceBasis`). Dropped links stay
in Mongo; nothing here is deleted, only not published.

So this page answers: how much was linked, how much of it is kept, why,
and which frequently linked items curation lets through.
"""

from __future__ import annotations

import streamlit as st

from lib import charts, components as ui, data
from lib.theme import PLOTLY_CONFIG, active_palette

st.set_page_config(page_title="Entities · KYM", page_icon="🔗", layout="wide")
pal = active_palette()

reachable, err = data.ping()
if not reachable:
    st.error(f"Cannot reach MongoDB.\n\n```\n{err}\n```")
    st.stop()

st.title("Entities")
st.caption("`kym_entities` links title, tags and About to Wikidata · "
           "`kym_entity_curation` decides which links reach the graph · "
           "owns `entities`, `entity_curation`")

ents = data.entity_state()
cur = data.curation_state()

if not ents["frames"]:
    ui.empty_state("No links yet.",
                   "Trigger `kym_entities` (it needs the Wikidata lexicon; see "
                   "`dags/modules/kg/wikidata.py`).")
    st.stop()

BASIS = {
    "title": "named by the title", "own_item": "the meme's own item",
    "platform": "a platform or community", "format": "a meme format",
    "title_agrees": "the title names it too", "tag_and_text": "a tag and the About agree",
    "tag_named": "a tag that names something", "judge": "the LLM judge",
    "deny_item": "a listed non-topic word", "deny_class": "body part, measure, maths",
    "generic_item": "a generic word in the About",
}
ROLE = {"subject": "subject · kept", "source": "source work · kept",
        "format": "format · kept", "platform": "platform · kept",
        "incidental": "incidental · dropped", "wrong_sense": "wrong sense · dropped"}

linked = cur["kept_total"] + cur["dropped_total"] + cur["pending"]
left, right = st.columns([1, 2], gap="large")
with left:
    ui.hero("Links in the graph", cur["kept_total"], pal,
            caption=(f"of {linked:,} linked mentions ({cur['kept_total'] / linked:.1%}) · "
                     f"{cur['dropped_total']:,} dropped, kept in Mongo") if linked else None)
with right:
    ui.stat_tiles([
        {"label": "Frames linked", "value": ents["frames_linked"],
         "help": f"of {ents['frames']:,} frames with at least one Wikidata link."},
        {"label": "Distinct items", "value": ents["distinct_items"],
         "help": "Different Wikidata items linked anywhere in the corpus."},
        {"label": "No link kept", "value": cur["frames_none_kept"],
         "help": "Frames with no link in the graph: every link dropped, or none "
                 "made. Title links are always kept, so these are mostly frames "
                 "whose title names nothing in Wikidata."},
        {"label": "Dead-letter", "value": cur["failures"],
         "help": "Frames the LLM judge could not answer within the schema; not "
                 "re-asked until the prompt, schema or model changes."},
    ], pal)

st.divider()

# -- linking ------------------------------------------------------------------
st.subheader("What the linker found")
st.caption("Every mention is grounded to the characters it came from. `noun_chunk` "
           "and `propn` spans are where common nouns come in (\"mug\", \"hair\") — "
           "which is what curation below is for.")
c1, c2 = st.columns(2, gap="large")
with c1:
    st.markdown("**Mentions by field**")
    st.plotly_chart(charts.magnitude_bars(ents["by_field"], pal, value_name="mentions"),
                    config=PLOTLY_CONFIG, width="stretch")
with c2:
    st.markdown("**How each span was found**")
    st.plotly_chart(charts.magnitude_bars(ents["by_method"], pal, value_name="mentions"),
                    config=PLOTLY_CONFIG, width="stretch")
ui.data_table(
    [{"breakdown": "field", "value": k, "mentions": v} for k, v in ents["by_field"].items()]
    + [{"breakdown": "method", "value": k, "mentions": v} for k, v in ents["by_method"].items()]
    + [{"breakdown": "linker version", "value": k, "mentions": v}
       for k, v in ents["linker_versions"].items()]
    + [{"breakdown": "sense list", "value": k, "mentions": v}
       for k, v in ents["senses_versions"].items()]
    + [{"breakdown": "lexicon", "value": k, "mentions": v}
       for k, v in ents["lexicon_versions"].items()],
    "Show these counts, and the linker, sense-list and lexicon versions (frames)")
if ents["last_linked_at"]:
    st.caption(f"Last linked: {ents['last_linked_at']:%Y-%m-%d %H:%M} UTC · "
               f"{ents['own_item']:,} frames joined to their own Wikidata item (P13484).")

st.divider()

# -- curation -----------------------------------------------------------------
st.subheader("What curation keeps, and why")
st.caption("Rules decide the clear cases first (titles, platforms, meme formats, "
           "agreement between title, tags and About; generic words and body parts "
           "dropped). The LLM judge reads the entry for the rest. Counts are "
           "mentions — what the graph would carry.")
c1, c2 = st.columns(2, gap="large")
with c1:
    st.markdown("**Kept — the reason**")
    if cur["kept"]:
        st.plotly_chart(charts.magnitude_bars(
            {BASIS.get(k, k): v for k, v in cur["kept"].items()}, pal, value_name="mentions"),
            config=PLOTLY_CONFIG, width="stretch")
with c2:
    st.markdown("**Dropped — the reason**")
    if cur["dropped"]:
        st.plotly_chart(charts.magnitude_bars(
            {BASIS.get(k, k): v for k, v in cur["dropped"].items()}, pal, value_name="mentions"),
            config=PLOTLY_CONFIG, width="stretch")
ui.data_table(
    [{"decision": "kept", "reason": BASIS.get(k, k), "basis": k, "mentions": v}
     for k, v in sorted(cur["kept"].items(), key=lambda kv: -kv[1])]
    + [{"decision": "dropped", "reason": BASIS.get(k, k), "basis": k, "mentions": v}
       for k, v in sorted(cur["dropped"].items(), key=lambda kv: -kv[1])]
    + ([{"decision": "waiting for the judge", "reason": "—", "basis": "pending",
         "mentions": cur["pending"]}] if cur["pending"] else []),
    "Show the decisions as a table")

st.markdown("#### The judge")
c1, c2 = st.columns([3, 2], gap="large")
with c1:
    st.markdown("**The role it gave each item it was asked about**")
    if cur["judge_roles"]:
        st.plotly_chart(charts.magnitude_bars(
            {ROLE.get(k, k): v for k, v in cur["judge_roles"].items()}, pal,
            value_name="items"), config=PLOTLY_CONFIG, width="stretch")
    st.caption("Items per frame, not mentions. Subject, source work, format and "
               "platform are kept; incidental and wrong-sense are dropped.")
with c2:
    waiting = cur["frames_waiting"]
    ui.meter("Frames judged", cur["frames_judged"], cur["frames_judged"] + waiting, pal,
             good_above=0.999, warn_above=0.0)     # progress, not a fault
    st.markdown("**The second reading**")
    ui.stat_tiles([
        {"label": "Both readings keep", "value": cur["confirm_agreed"]},
        {"label": "Second reading drops", "value": cur["confirm_overturned"]},
    ], pal)
    st.caption("An item read only from the About is kept only if the judge, "
               "asked again with a differently worded prompt, keeps it too.")
    ui.data_table([{"judge": k, "frames": v} for k, v in cur["judge_models"].items()]
                  + [{"judge": f"curation {k}", "frames": v}
                     for k, v in cur["versions"].items()],
                  "Show the judge models and curation versions")

st.divider()

# -- which items survive --------------------------------------------------------
st.subheader("The most-linked items, and what curation did with them")
st.caption("Tag and About mentions of the 20 items linked most often (title "
           "links are always kept, so they are left out). A long orange bar is a "
           "word the corpus uses constantly and curation mostly drops.")
items = data.curation_items(20)
if items:
    st.plotly_chart(
        charts.split_bars({r["item"]: {"kept": r["kept"], "dropped": r["dropped"]}
                           for r in items}, ("kept", "dropped"), pal,
                          names={"kept": "kept", "dropped": "dropped"}),
        config=PLOTLY_CONFIG, width="stretch")
    ui.data_table(items, "Show the items as a table")

st.divider()

# -- history ---------------------------------------------------------------------
st.subheader("Across runs")
rows_link = data.run_history("entities")
rows_cur = data.run_history("entity_curation")
if len(rows_link) < 2 and len(rows_cur) < 2:
    ui.empty_state("Fewer than two runs recorded for these stages — a trend needs two.",
                   "Every `kym_entities` and `kym_entity_curation` run adds one "
                   "document to `run_summaries`.")
else:
    tab_link, tab_cur = st.tabs(["Linking", "Curation"])
    with tab_link:
        series = {name: data.scalar_series(rows_link, f"corpus.{key}")
                  for name, key in [("mentions", "mentions_total"),
                                    ("frames linked", "frames_linked"),
                                    ("distinct items", "distinct_entities")]}
        series = {k: v for k, v in series.items() if v}
        if series:
            st.plotly_chart(charts.trend_lines(series, pal, value_name="count"),
                            config=PLOTLY_CONFIG, width="stretch")
    with tab_cur:
        series = {name: data.scalar_series(rows_cur, f"corpus.{key}")
                  for name, key in [("links in the graph", "entities_in_graph"),
                                    ("waiting for the judge", "entities_curation_pending"),
                                    ("frames judged", "frames_judged")]}
        series = {k: v for k, v in series.items() if v}
        if series:
            st.plotly_chart(charts.trend_lines(series, pal, value_name="count"),
                            config=PLOTLY_CONFIG, width="stretch")
