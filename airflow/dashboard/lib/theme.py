"""
theme.py — the dashboard's validated colour system.

Every hex here comes from the data-viz reference palette and was checked
with its validator (not by eye) before being used:

  categorical, 4 slots   light PASS · dark PASS
      light  worst adjacent CVD ΔE 9.1, normal-vision ΔE 22.9
      dark   worst adjacent CVD ΔE 8.4, normal-vision ΔE 19.8
      light carries a contrast WARN on aqua (2.74:1) and yellow (2.11:1),
      which obligates *relief*: every chart that uses those slots ships
      direct labels and a "Show data" table. That is not optional styling
      — it is the condition under which those two slots are legal.

  ordinal ramp, 5 steps  light PASS · dark PASS
      single hue, monotone lightness, adjacent ΔL ≥ 0.06, light end
      clears 2:1 against its surface.

Rules this module exists to enforce:
  * categorical hues are assigned in FIXED ORDER, never cycled — a 5th
    series folds into "Other" rather than inventing a hue;
  * magnitude (namespace sizes, missing-field counts) is ONE series in ONE
    colour (slot 1) — the bars are one quantity, not four identities, and a
    darker-where-bigger ramp on unordered categories would repeat the bar
    length in hue (the data-viz anti-pattern; changed 2026-09-30);
  * status colours (ok/failed/permanent) are reserved, never reused as a
    series, and always ship with an icon + text label.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Palette:
    mode: str
    surface: str
    page: str
    ink: str
    ink_secondary: str
    ink_muted: str
    grid: str
    axis: str
    border: str
    # categorical: fixed order, never cycled
    categorical: tuple[str, ...]
    # ordinal: light -> dark, for funnel stages
    ordinal: tuple[str, ...]
    # sequential: for magnitude bars (one hue)
    sequential: str
    sequential_soft: str
    status: dict[str, str] = field(default_factory=dict)


LIGHT = Palette(
    mode="light",
    surface="#fcfcfb",
    page="#f9f9f7",
    ink="#0b0b0b",
    ink_secondary="#52514e",
    ink_muted="#898781",
    grid="#e1e0d9",
    axis="#c3c2b7",
    border="rgba(11,11,11,0.10)",
    categorical=("#2a78d6", "#eb6834", "#1baf7a", "#eda100"),
    ordinal=("#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"),
    sequential="#2a78d6",
    sequential_soft="#cde2fb",
    status={"good": "#0ca30c", "warning": "#fab219",
            "serious": "#ec835a", "critical": "#d03b3b"},
)

DARK = Palette(
    mode="dark",
    surface="#1a1a19",
    page="#0d0d0d",
    ink="#ffffff",
    ink_secondary="#c3c2b7",
    ink_muted="#898781",
    grid="#2c2c2a",
    axis="#383835",
    border="rgba(255,255,255,0.10)",
    categorical=("#3987e5", "#d95926", "#199e70", "#c98500"),
    ordinal=("#b7d3f6", "#86b6ef", "#3987e5", "#256abf", "#184f95"),
    sequential="#3987e5",
    sequential_soft="#184f95",
    status={"good": "#0ca30c", "warning": "#fab219",
            "serious": "#ec835a", "critical": "#d03b3b"},
)

# Status is fixed across modes and always paired with these labels — colour
# never carries the meaning by itself.
STATUS_ICON = {"good": "●", "warning": "▲", "serious": "◆", "critical": "■"}

FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def active_palette() -> Palette:
    """Follow the viewer's own Streamlit theme choice (Settings →
    Appearance). Dark is a *selected* palette stepped for the dark
    surface, not an automatic inversion of the light one."""
    try:
        import streamlit as st
        if getattr(st.context.theme, "type", "light") == "dark":
            return DARK
    except Exception:
        pass
    return LIGHT


def plotly_layout(pal: Palette, height: int = 320, **overrides) -> dict:
    """Shared Plotly layout: recessive chrome, ink-coloured text, no
    background boxes. Chart text always wears text tokens — never the
    series colour."""
    layout = dict(
        height=height,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, size=13, color=pal.ink_secondary),
        hoverlabel=dict(font_family=FONT, font_size=13,
                        bgcolor=pal.surface, bordercolor=pal.axis,
                        font_color=pal.ink),
        xaxis=dict(gridcolor=pal.grid, linecolor=pal.axis, zeroline=False,
                   tickfont=dict(color=pal.ink_muted), automargin=True),
        yaxis=dict(gridcolor=pal.grid, linecolor=pal.axis, zeroline=False,
                   tickfont=dict(color=pal.ink_muted), automargin=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                    font=dict(color=pal.ink_secondary), title_text=""),
        showlegend=False,
    )
    layout.update(overrides)
    return layout


def _rgba(hex_colour: str, alpha: float) -> str:
    h = hex_colour.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def highlight_css(pal: Palette) -> str:
    """Styles for lib/highlight.py's marks: what an event extracted, shown
    in the words it came from.

    Built from the first three categorical slots (already validated above),
    as a tint behind INK text plus a 2px underline in the full hue — the
    text never wears the series colour. Each kind also has its own
    underline STYLE (solid / dashed / dotted) and a written legend, so the
    marks read without colour. Checked, not eyeballed (2026-09-30): ink on
    every tint is 15.7-16.8:1 in light mode and 11.2-13.1:1 in dark, over
    both this palette's surface and Streamlit's own page colour.
    """
    alpha = 0.22 if pal.mode == "light" else 0.30
    when, where, who = pal.categorical[:3]
    return f"""<style>
      .kym-quote {{ font-size: 1rem; line-height: 1.8; color: {pal.ink}; margin: .2rem 0 .4rem 0; }}
      .kym-legend {{ font-size: .82rem; color: {pal.ink_secondary}; margin-bottom: .3rem; }}
      .kym-aside {{ font-size: .85rem; color: {pal.ink_secondary}; }}
      mark.kym-hl {{ color: {pal.ink}; padding: .04em .18em; border-radius: 3px;
                     border-bottom-width: 2px; }}
      mark.kym-hl-when {{ background: {_rgba(when, alpha)}; border-bottom: 2px solid {when}; }}
      mark.kym-hl-where {{ background: {_rgba(where, alpha)}; border-bottom: 2px dashed {where}; }}
      mark.kym-hl-who {{ background: {_rgba(who, alpha)}; border-bottom: 2px dotted {who}; }}
    </style>"""


PLOTLY_CONFIG = {
    "displayModeBar": False,
    "scrollZoom": False,
    "responsive": True,
}
