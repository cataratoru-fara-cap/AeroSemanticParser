"""
components.py — the non-chart pieces.

Per the form heuristic, several things a dashboard wants are deliberately
NOT charts: a single current value is a stat tile, not a one-bar bar
chart; a ratio against a limit is a meter, not a two-slice pie; and more
than ~7 meaningful classes is a table.
"""

from __future__ import annotations

from typing import Any

import streamlit as st

from lib.theme import STATUS_ICON, Palette


def _fmt(n: float | int | None, unit: str = "") -> str:
    if n is None:
        return "—"
    if isinstance(n, float) and not n.is_integer():
        return f"{n:,.1f}{unit}"
    return f"{int(n):,}{unit}"


def hero(label: str, value: float | int, pal: Palette,
         caption: str | None = None) -> None:
    """The one number the page leads with. Sans, large, proportional
    figures (tabular-nums is for columns that must align, not for this)."""
    st.markdown(
        f"""
        <div style="margin:0 0 .25rem 0">
          <div style="font-size:.8rem;letter-spacing:.06em;text-transform:uppercase;
                      color:{pal.ink_muted}">{label}</div>
          <div style="font-size:3.2rem;line-height:1.05;font-weight:600;
                      color:{pal.ink}">{_fmt(value)}</div>
          {f'<div style="font-size:.9rem;color:{pal.ink_secondary}">{caption}</div>'
           if caption else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )


def stat_tiles(tiles: list[dict[str, Any]], pal: Palette) -> None:
    """A KPI row. Each tile: {label, value, delta?, help?}.

    ``delta`` is rendered by Streamlit's own metric, which pairs the colour
    with an arrow glyph — so the up/down meaning never rests on colour
    alone.
    """
    cols = st.columns(len(tiles))
    for col, t in zip(cols, tiles):
        with col:
            st.metric(
                label=t["label"],
                value=_fmt(t["value"]),
                delta=t.get("delta"),
                delta_color=t.get("delta_color", "normal"),
                help=t.get("help"),
                border=True,
            )


def meter(label: str, value: float, total: float, pal: Palette,
          good_above: float = 0.9, warn_above: float = 0.6) -> None:
    """One ratio against its limit, on a same-hue track.

    The status word is written out next to the bar: the colour is a
    reinforcement, never the only carrier of 'this is fine' / 'this is not'.
    """
    share = (value / total) if total else 0.0
    if share >= good_above:
        role, word = "good", "healthy"
    elif share >= warn_above:
        role, word = "warning", "partial"
    else:
        role, word = "critical", "low"
    colour = pal.status[role]

    st.markdown(
        f"""
        <div style="margin:.15rem 0 .9rem 0">
          <div style="display:flex;justify-content:space-between;align-items:baseline;
                      gap:1rem;margin-bottom:.35rem">
            <span style="color:{pal.ink_secondary};font-size:.92rem">{label}</span>
            <span style="color:{pal.ink};font-size:.92rem;
                         font-variant-numeric:tabular-nums">
              {_fmt(value)} / {_fmt(total)}
              <span style="color:{pal.ink_muted}">({share:.1%})</span>
            </span>
          </div>
          <div style="height:10px;border-radius:5px;background:{pal.grid};
                      overflow:hidden">
            <div style="height:100%;width:{min(share, 1.0) * 100:.2f}%;
                        border-radius:5px;background:{colour}"></div>
          </div>
          <div style="margin-top:.3rem;font-size:.82rem;color:{pal.ink_secondary}">
            <span style="color:{colour}">{STATUS_ICON[role]}</span> {word}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def status_row(items: list[tuple[str, str, int]], pal: Palette) -> None:
    """``[(role, label, count), ...]`` — reserved status colours, each
    shipped with its icon and its written label."""
    cols = st.columns(len(items))
    for col, (role, label, count) in zip(cols, items):
        colour = pal.status[role]
        with col:
            st.markdown(
                f"""
                <div style="padding:.6rem .8rem;border:1px solid {pal.border};
                            border-radius:8px">
                  <div style="font-size:.82rem;color:{pal.ink_secondary}">
                    <span style="color:{colour}">{STATUS_ICON[role]}</span> {label}
                  </div>
                  <div style="font-size:1.5rem;font-weight:600;color:{pal.ink};
                              font-variant-numeric:tabular-nums">{_fmt(count)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def data_table(rows: list[dict[str, Any]], caption: str = "Show data") -> None:
    """The table view behind every chart.

    Two jobs: it is the accessibility fallback for readers who cannot use
    the colour channel, and it is the *relief* the light-mode contrast WARN
    requires for the aqua and yellow categorical slots. Not decoration —
    a precondition for those colours being allowed on screen.
    """
    if not rows:
        return
    with st.expander(caption):
        st.dataframe(rows, width="stretch", hide_index=True)


def empty_state(message: str, hint: str | None = None) -> None:
    st.info(message if not hint else f"{message}\n\n{hint}")
