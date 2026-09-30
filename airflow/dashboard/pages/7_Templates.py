"""
Templates — imgflip meme templates per frame, and what their images show.

Two stages meet here. `kym_templates` searches imgflip with each frame's
title, merges near-identical uploads, and keeps 0 or 1-10 templates per
frame, most varied first. `kym_template_entities` reads each kept
template's image with the lab's vision model (qwen3-vl:32b) and links what
it names to Wikidata.

So this page answers: which frames found templates (and why the others did
not), how big the pool is, how far the reading has got — at what pace —
and what the model is actually seeing.
"""

from __future__ import annotations

import streamlit as st

from lib import charts, components as ui, data
from lib.theme import PLOTLY_CONFIG, active_palette

st.set_page_config(page_title="Templates · KYM", page_icon="🖼️", layout="wide")
pal = active_palette()

reachable, err = data.ping()
if not reachable:
    st.error(f"Cannot reach MongoDB.\n\n```\n{err}\n```")
    st.stop()

st.title("Templates")
st.caption("`kym_templates` searches imgflip per frame · `kym_template_entities` "
           "reads each kept template's image · owns `frame_templates`, "
           "`imgflip_templates`, `template_entities`")

tmpl = data.template_state()
if not tmpl["frames"]:
    ui.empty_state("No frames searched yet.", "Trigger `kym_templates`.")
    st.stop()
rd = data.template_reading()

STATUS_NAMES = {"selected": "templates kept", "below_threshold": "only weak matches",
                "no_results": "imgflip found nothing"}
METHOD_NAMES = {"kym_reference": "the frame's own imgflip link", "search": "found by search"}

found = tmpl["by_status"].get("selected", 0)
left, right = st.columns([1, 2], gap="large")
with left:
    ui.hero("Templates in the pool", tmpl["kept"], pal,
            caption=f"for {found:,} frames · {tmpl['links']:,} frame → template links")
with right:
    ui.stat_tiles([
        {"label": "Searched", "value": tmpl["frames"],
         "help": "Frames searched on imgflip: every `meme` entry, plus any entry "
                 "that links to imgflip or has a KYM Template section."},
        {"label": "Read", "value": rd["read"],
         "help": f"Templates the vision model has read, of {rd['readable']:,} "
                 "kept templates that have an image."},
        {"label": "Image links", "value": rd["links_in_graph"],
         "help": "Wikidata links read from template images that reach the graph: "
                 "every named entity and printed text, plus the three largest "
                 "generic regions per template."},
        {"label": "Dead-letter", "value": rd["failed"],
         "help": "Templates the model could not read within the schema. Not "
                 "retried until the prompt, schema, model or image changes."},
    ], pal)

st.divider()

# -- search -------------------------------------------------------------------------
st.subheader("Which frames found templates")
st.caption("\"imgflip found nothing\" is imgflip having no template under that "
           "title, not a failure: the search never hit a block and never needed "
           "ScrapingAnt. Frames with their own imgflip link almost always find "
           "one; other memes about half the time.")
order = ("selected", "below_threshold", "no_results")
st.plotly_chart(
    charts.split_bars(tmpl["by_priority"], order, pal, names=STATUS_NAMES),
    config=PLOTLY_CONFIG, width="stretch")
ui.data_table([{"frames": kind, **{STATUS_NAMES[s]: v.get(s, 0) for s in order},
                "total": sum(v.values())} for kind, v in tmpl["by_priority"].items()],
              "Show the search outcome as a table")

c1, c2 = st.columns([3, 2], gap="large")
with c1:
    st.markdown("**Templates kept per frame**")
    buckets = sorted(tmpl["per_frame"])
    if buckets:
        st.plotly_chart(charts.ordered_columns(
            [str(b) for b in buckets], [tmpl["per_frame"][b] for b in buckets], pal,
            value_name="frames", x_title="templates kept"),
            config=PLOTLY_CONFIG, width="stretch")
    st.caption("1 to 10 per frame, most varied first — near-identical uploads "
               f"count once ({tmpl['merged']:,} merged into another upload).")
with c2:
    st.markdown("**How a kept template was matched**")
    ui.stat_tiles([{"label": "By search", "value": tmpl["methods"].get("search", 0),
                    "help": "Frame → template links found by searching imgflip."},
                   {"label": "Own link", "value": tmpl["methods"].get("kym_reference", 0),
                    "help": "Links that are the frame's own imgflip \"Meme "
                            "Generator\" link on KYM — ground truth."}], pal)
    ui.meter("Kept templates with an image", tmpl["with_image"], tmpl["kept"], pal,
             good_above=0.98, warn_above=0.9)
    if tmpl["image_failed"]:
        st.caption(f"{tmpl['image_failed']:,} images could not be fetched.")
ui.data_table([{"templates kept": b, "frames": tmpl["per_frame"][b]}
               for b in sorted(tmpl["per_frame"])], "Show templates per frame as a table")

st.divider()

# -- reading -------------------------------------------------------------------------
st.subheader("Reading the images")
c1, c2 = st.columns([2, 3], gap="large")
with c1:
    ui.meter("Kept templates read", rd["read"], rd["readable"], pal,
             good_above=0.999, warn_above=0.0)     # progress, not a fault
    pace = (f"{rd['last_hour']:,} read in the last hour, {rd['last_6h']:,} in six hours")
    if rd["eta_hours"]:
        pace += f" · at that pace the remaining {rd['remaining']:,} take about " \
                f"{rd['eta_hours']:.0f} h"
    st.caption(pace + ". The model runs on one shared GPU host, one image at a time.")
    if rd["failure_kinds"]:
        st.markdown("**Why reading failed**")
        st.plotly_chart(charts.magnitude_bars(rd["failure_kinds"], pal,
                                              value_name="templates"),
                        config=PLOTLY_CONFIG, width="stretch")
        st.caption("\"Stuck repeating itself\": the host stopped a generation that "
                   "kept repeating. \"Answer cut off\": it ran past the length limit.")
with c2:
    st.markdown("**What the model saw**")
    if rd["regions_by_kind"]:
        st.plotly_chart(charts.magnitude_bars(rd["regions_by_kind"], pal,
                                              value_name="regions"),
                        config=PLOTLY_CONFIG, width="stretch")
    st.caption(f"{rd['regions_named']:,} of {rd['regions_total']:,} regions carry a "
               "proper name (a person, character, brand or work); the rest are "
               "generic (\"man\", \"cat\") or printed text.")

st.markdown("**Which image links reach the graph**")
SOURCE = {"named": "named regions", "text": "printed text", "generic": "generic regions"}
link_rows: dict[str, dict[str, int]] = {}
for (source, in_graph), n in rd["links"].items():
    link_rows.setdefault(SOURCE.get(source, source), {})[
        "in the graph" if in_graph else "kept in Mongo only"] = n
if link_rows:
    st.plotly_chart(charts.split_bars(link_rows, ("in the graph", "kept in Mongo only"), pal,
                                      names={"in the graph": "in the graph",
                                             "kept in Mongo only": "kept in Mongo only"}),
                    config=PLOTLY_CONFIG, width="stretch")
    ui.data_table([{"source": k, **v} for k, v in link_rows.items()],
                  "Show the image links as a table")

st.markdown("**The blind audit**")
named, confirmed = rd["blind_named"], rd["blind_confirmed"]
ui.stat_tiles([
    {"label": "Names read with context", "value": named,
     "help": "In the audit sample (1 template in 100, read twice): the names "
             "the model gave when told the template's name and frame."},
    {"label": "Also found blind", "value": confirmed,
     "help": "The same name, or the same kind of thing in an overlapping box, "
             "when the image was read with no context at all."},
    {"label": "Context-only names", "value": named - confirmed,
     "help": "Names that appeared only with context. On the pilot these were "
             "all right (context recognised Ajit Pai where the blind read said "
             "\"man\"), so context names are kept — they are read, not counted."},
], pal)

st.divider()

# -- recently read ---------------------------------------------------------------------
st.subheader("Recently read")
st.caption("The last templates the model read, with what it named and the "
           "Wikidata items linked from each region (✓ = reaches the graph). "
           "Thumbnails load from imgflip.")
recent = data.recent_templates(8)
for start in range(0, len(recent), 4):
    cols = st.columns(4, gap="medium")
    for col, t in zip(cols, recent[start:start + 4]):
        with col:
            with st.container(border=True):
                if t["thumb_url"]:
                    st.image(t["thumb_url"], width="stretch")
                title = f"[{t['name']}]({t['url']})" if t["url"] else t["name"]
                st.markdown(f"**{title}**")
                lines: list[str] = []
                seen: dict[str, int] = {}
                for r in t["regions"]:
                    what = r["name"]
                    if r["kind"] == "text" and r.get("text"):
                        words = r["text"] if len(r["text"]) <= 60 else r["text"][:57] + "…"
                        what = f"“{words}”"
                    links = f" → {', '.join(r['links'])}" if r["links"] else ""
                    line = f"{what} · {r['kind']}{links}"
                    seen[line] = seen.get(line, 0) + 1
                lines = [f"- {line}" + (f" ×{n}" if n > 1 else "") for line, n in seen.items()]
                st.markdown("\n".join(lines) or "_nothing read_")

st.divider()

# -- history -----------------------------------------------------------------------------
st.subheader("Across runs")
rows_search = data.run_history("templates")
rows_read = data.run_history("template_entities")
if len(rows_search) < 2 and len(rows_read) < 2:
    ui.empty_state("Fewer than two runs recorded for these stages — a trend needs two.",
                   "Every `kym_templates` and `kym_template_entities` run adds one "
                   "document to `run_summaries`.")
else:
    tab_search, tab_read = st.tabs(["Search", "Reading"])
    with tab_search:
        series = {name: data.scalar_series(rows_search, f"corpus.{key}")
                  for name, key in [("frames searched", "frames_searched"),
                                    ("templates kept", "templates_kept"),
                                    ("with details", "templates_with_details")]}
        series = {k: v for k, v in series.items() if v}
        if series:
            st.plotly_chart(charts.trend_lines(series, pal, value_name="count"),
                            config=PLOTLY_CONFIG, width="stretch")
    with tab_read:
        series = {name: data.scalar_series(rows_read, f"corpus.{key}")
                  for name, key in [("templates read", "templates_detected"),
                                    ("image links in the graph", "mentions_in_graph"),
                                    ("regions", "regions")]}
        series = {k: v for k, v in series.items() if v}
        if series:
            st.plotly_chart(charts.trend_lines(series, pal, value_name="count"),
                            config=PLOTLY_CONFIG, width="stretch")
        st.caption("A reading run records its summary only when every chunk "
                   "finished, so a run with a failed chunk leaves a gap here; "
                   "the live panels above include its work.")
