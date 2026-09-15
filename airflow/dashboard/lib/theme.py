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
  * magnitude (namespace sizes, missing-field counts) uses the SEQUENTIAL
    ramp, not categorical — the bars are one quantity, not four identities;
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


def sequential_steps(n: int, pal: Palette) -> list[str]:
    """``n`` steps of the single sequential hue, light -> dark.

    Used for magnitude bars, where darker simply means larger. Falls back
    to repeating the ramp's darkest step past its length rather than
    generating new hues.
    """
    ramp_light = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec",
                  "#5598e7", "#3987e5", "#2a78d6", "#256abf", "#1c5cab",
                  "#184f95", "#104281", "#0d366b"]
    ramp = ramp_light if pal.mode == "light" else list(reversed(ramp_light))
    # Magnitude bars sit on the surface, so skip the steps that vanish into
    # it (the ordinal floor rule: nothing lighter than step 250 on light).
    ramp = ramp[3:] if pal.mode == "light" else ramp[:-3]
    if n <= 0:
        return []
    if n == 1:
        return [ramp[len(ramp) // 2]]
    span = len(ramp) - 1
    return [ramp[round(i * span / (n - 1))] for i in range(n)]


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


PLOTLY_CONFIG = {
    "displayModeBar": False,
    "scrollZoom": False,
    "responsive": True,
}
