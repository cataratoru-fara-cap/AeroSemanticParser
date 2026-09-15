"""
charts.py — the chart builders.

Form is chosen from the data's job, not from variety:

  magnitude, low->high        horizontal bar, SEQUENTIAL ramp (one hue)
  part-to-whole by stage      horizontal stacked bar, 2 categorical slots
  pipeline stages             funnel as an ordinal-ramp bar
  change over time            line, CATEGORICAL slots, capped at 4

Rules applied throughout:
  * one y-axis, ever — two measures of different scale get two charts;
  * labels and values wear text tokens, never the series colour;
  * >= 2 series always get a legend, and <= 4 are also direct-labelled, so
    identity never rests on colour alone;
  * a 5th categorical series folds into "Other" rather than inventing a hue;
  * every chart's caller pairs it with components.data_table().
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import plotly.graph_objects as go

from lib.theme import Palette, plotly_layout, sequential_steps

MAX_BARS = 18          # beyond this the tail folds into one "(+n more)" bar
MAX_TREND_SERIES = 4   # the categorical token ceiling for this dashboard


def _fold_tail(counts: dict[str, float], limit: int
               ) -> tuple[list[str], list[float], int]:
    """Biggest first; everything past ``limit`` becomes one bar. Never
    solve 'too many categories' by adding colours."""
    items = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    if len(items) > limit:
        head, tail = items[:limit], items[limit:]
        items = head + [(f"(+{len(tail)} more)", sum(v for _, v in tail))]
        return ([k for k, _ in items], [v for _, v in items], len(tail))
    return ([k for k, _ in items], [v for _, v in items], 0)


def magnitude_bars(counts: dict[str, float], pal: Palette,
                   value_name: str = "count",
                   limit: int = MAX_BARS) -> go.Figure:
    """Horizontal bars for 'how big is each of these'.

    Sequential, not categorical: the bars are one quantity measured across
    categories, so darker simply means larger. Using four identity hues
    here would imply the categories are different *kinds* of thing.
    """
    labels, values, _ = _fold_tail(counts, limit)
    colours = sequential_steps(len(values), pal)
    # Darkest goes to the largest bar; the ramp is ordered by magnitude,
    # and _fold_tail already sorted descending.
    colours = list(reversed(colours))

    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker=dict(color=colours,
                    line=dict(color=pal.surface, width=2)),  # 2px surface gap
        text=[f"{v:,.0f}" for v in values],
        textposition="outside",
        textfont=dict(color=pal.ink_secondary, size=12),
        hovertemplate=f"%{{y}}<br>{value_name}: %{{x:,.0f}}<extra></extra>",
        cliponaxis=False,
    ))
    fig.update_layout(**plotly_layout(
        pal,
        height=max(180, 26 * len(labels) + 60),
        yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)",
                   linecolor="rgba(0,0,0,0)",
                   tickfont=dict(color=pal.ink_secondary, size=12)),
        xaxis=dict(gridcolor=pal.grid, linecolor=pal.axis, zeroline=False,
                   tickfont=dict(color=pal.ink_muted),
                   rangemode="tozero"),
        bargap=0.28,
    ))
    fig.update_xaxes(range=[0, max(values) * 1.18 if values else 1])
    return fig


def split_bars(rows: dict[str, dict[str, float]], keys: tuple[str, str],
               pal: Palette, limit: int = MAX_BARS) -> go.Figure:
    """Part-to-whole per category — two stacked segments, two categorical
    slots, with a 2px surface gap between them."""
    ordered = sorted(rows.items(),
                     key=lambda kv: sum(kv[1].values()), reverse=True)[:limit]
    labels = [k for k, _ in ordered]

    fig = go.Figure()
    for i, key in enumerate(keys):
        fig.add_bar(
            y=labels, x=[v.get(key, 0) for _, v in ordered],
            orientation="h", name=key.capitalize(),
            marker=dict(color=pal.categorical[i],
                        line=dict(color=pal.surface, width=2)),
            hovertemplate=f"%{{y}}<br>{key}: %{{x:,.0f}}<extra></extra>",
        )
    fig.update_layout(**plotly_layout(
        pal,
        height=max(200, 26 * len(labels) + 80),
        barmode="stack",
        showlegend=True,
        yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)",
                   linecolor="rgba(0,0,0,0)",
                   tickfont=dict(color=pal.ink_secondary, size=12)),
        xaxis=dict(gridcolor=pal.grid, linecolor=pal.axis, zeroline=False,
                   tickfont=dict(color=pal.ink_muted), rangemode="tozero"),
        bargap=0.28,
    ))
    return fig


def funnel(stages: list[tuple[str, float]], pal: Palette) -> go.Figure:
    """Pipeline stages as an ordinal-ramp bar chart.

    Not Plotly's funnel trace: that one scales width by value and drops
    the axis, which makes a 24k -> 4k step look like a rounding error. A
    plain bar on a shared axis keeps the drop-offs readable, and the
    ordinal ramp (validated: monotone L, adjacent ΔL >= 0.06) carries the
    stage order.
    """
    labels = [s for s, _ in stages]
    values = [v for _, v in stages]
    first = values[0] if values else 0
    pct = [(v / first * 100 if first else 0) for v in values]

    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker=dict(color=list(pal.ordinal[:len(values)]),
                    line=dict(color=pal.surface, width=2)),
        text=[f"{v:,.0f}   {p:.0f}% of discovered" for v, p in zip(values, pct)],
        textposition="outside",
        textfont=dict(color=pal.ink_secondary, size=12),
        hovertemplate="%{y}<br>%{x:,.0f}<extra></extra>",
        cliponaxis=False,
    ))
    fig.update_layout(**plotly_layout(
        pal,
        height=34 * len(labels) + 70,
        yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)",
                   linecolor="rgba(0,0,0,0)",
                   tickfont=dict(color=pal.ink, size=13)),
        xaxis=dict(gridcolor=pal.grid, linecolor=pal.axis, zeroline=False,
                   tickfont=dict(color=pal.ink_muted), rangemode="tozero"),
        bargap=0.32,
    ))
    fig.update_xaxes(range=[0, max(values) * 1.45 if values else 1])
    return fig


def trend_lines(series: dict[str, list[tuple[datetime, float]]], pal: Palette,
                value_name: str = "value") -> go.Figure:
    """Change over time, one y-axis.

    Series are capped at four — the categorical token ceiling — and the
    biggest are kept, because a fifth would mean inventing a hue. Each
    line is direct-labelled at its last point in addition to the legend,
    so identity survives both colour blindness and a greyscale print.
    """
    ranked = sorted(series.items(),
                    key=lambda kv: kv[1][-1][1] if kv[1] else 0, reverse=True)
    kept = ranked[:MAX_TREND_SERIES]

    fig = go.Figure()
    for i, (name, points) in enumerate(kept):
        if not points:
            continue
        xs = [t for t, _ in points]
        ys = [v for _, v in points]
        colour = pal.categorical[i]
        fig.add_scatter(
            x=xs, y=ys, name=name, mode="lines+markers",
            line=dict(color=colour, width=2),
            marker=dict(size=8, color=colour,
                        line=dict(color=pal.surface, width=2)),  # 2px ring
            hovertemplate=f"%{{x|%Y-%m-%d %H:%M}}<br>{name}: "
                          f"%{{y:,.0f}}<extra></extra>",
        )
        # direct label at the last point — identity without the legend
        fig.add_annotation(
            x=xs[-1], y=ys[-1], text=f"  {name}", showarrow=False,
            xanchor="left", yanchor="middle",
            font=dict(color=pal.ink_secondary, size=11),
        )

    values = [v for _, pts in kept for _, v in pts if v > 0]
    log_scale = bool(values) and max(values) / min(values) > 1000

    fig.update_layout(**plotly_layout(
        pal, height=340, showlegend=True,
        hovermode="x unified",
        xaxis=dict(gridcolor="rgba(0,0,0,0)", linecolor=pal.axis,
                   tickfont=dict(color=pal.ink_muted), showspikes=True,
                   spikemode="across", spikethickness=1,
                   spikecolor=pal.axis, spikedash="dot"),
        yaxis=dict(gridcolor=pal.grid, linecolor="rgba(0,0,0,0)",
                   tickfont=dict(color=pal.ink_muted),
                   type="log" if log_scale else "linear",
                   rangemode="tozero" if not log_scale else "normal",
                   title=dict(text=value_name,
                              font=dict(color=pal.ink_muted, size=11))),
        margin=dict(l=8, r=96, t=8, b=8),  # room for the direct labels
    ))
    return fig


def dropped_series_note(series: dict[str, Any]) -> str | None:
    """Say so when the cap hid something, rather than silently truncating."""
    extra = len(series) - MAX_TREND_SERIES
    return (f"{extra} smaller series hidden to stay within the colour budget — "
            f"see the data table for all {len(series)}.") if extra > 0 else None
