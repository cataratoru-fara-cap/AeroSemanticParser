"""
summary_plots.py — matplotlib rendering of DAG run summaries
=============================================================
Pure plotting: no Airflow imports, no Mongo. Input is the summary dict a
summarize task already returns, plus (optionally) the history list from
summary_store.load_history(). Output is PNG files on a shared volume.

Shape-agnostic on purpose. The three summarize tasks return three
different shapes (discovery: flat counts + a namespaces map; scrape and
parse: {"run": tallies, "corpus": stats with nested count maps}), and
future stages (cluster DAG, LLM extraction) will add more. Rather than
one bespoke plotter per DAG, this module walks any nested dict of
numbers and derives two kinds of figures:

  * **snapshot** charts — one horizontal bar chart per dict-of-counts it
    finds (namespaces, run tallies, missing_field_counts, ...), plus one
    for loose numeric fields at each level. Rendered from the current
    run only.
  * **trend** charts — line charts of every numeric leaf across the run
    history, grouped by top-level key ("run", "corpus", ...), one figure
    per group. Skipped until there are >= 2 historical runs. Large maps
    (e.g. ~20 namespaces) are charted as snapshots but excluded from
    trends so trend figures stay legible.

Filenames are stable (``snapshot_run.png``, ``trend_corpus.png``, ...)
and overwritten each run: the plot files are always "current state", and
the full history lives in ``run_summaries`` — anything can be
re-rendered from Mongo, so there is nothing to archive on disk.

Output directory: ``$SUMMARY_PLOTS_DIR`` (default
``/opt/airflow/data/plots``), with one subdirectory per stage. The
resolved path is logged loudly on every render — a wrong mount should be
visible, not silent.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

log = logging.getLogger("summary_plots")

ROOT_GROUP = "summary"       # bucket for loose numerics at the top level
MAX_BREAKDOWN_BARS = 30      # bars beyond this collapse into "(+n more)"
MAX_TREND_SERIES_FROM_MAP = 8   # count-maps larger than this: snapshot only
MAX_TREND_SERIES_PER_FIG = 10   # keep trend figures readable
_DPI = 120


def _plt():
    """Lazy matplotlib import (Agg backend) so importing this module never
    fails at DAG-parse time — same convention as the lazy pymongo imports
    in the stores."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "matplotlib is not installed — add it to airflow/requirements.txt "
            "and rebuild the image (docker compose build)."
        ) from exc
    return plt


def _sanitize(name: str) -> str:
    """Key/run-id -> filesystem-safe fragment."""
    return re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_") or "x"


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


# ---------------------------------------------------------------------------
# Shape analysis — one walk, two products
# ---------------------------------------------------------------------------

def analyze(summary: dict[str, Any]
            ) -> tuple[dict[str, float], dict[str, dict[str, float]]]:
    """
    Walk a summary dict and return (trend_scalars, breakdowns).

    trend_scalars : {"corpus.entries_total": 12345.0, ...} — every numeric
        leaf under its dotted path, except members of large count-maps.
    breakdowns    : {"namespaces": {...}, "run": {...}, "summary": {...}}
        — every all-numeric dict, plus the loose numeric fields found at
        each level, ready to render as one bar chart each.
    """
    trend_scalars: dict[str, float] = {}
    breakdowns: dict[str, dict[str, float]] = {}

    def walk(d: dict, prefix: str) -> None:
        loose: dict[str, float] = {}
        for k, v in d.items():
            key = str(k)
            if _is_num(v):
                loose[key] = float(v)
            elif isinstance(v, dict) and v:
                name = f"{prefix}{key}"
                if all(_is_num(x) for x in v.values()):
                    counts = {str(sk): float(sv) for sk, sv in v.items()}
                    breakdowns[name] = counts
                    if len(counts) <= MAX_TREND_SERIES_FROM_MAP:
                        for sk, sv in counts.items():
                            trend_scalars[f"{name}.{sk}"] = sv
                else:
                    walk(v, prefix=f"{name}.")
            # strings / lists / None: not chartable — skipped
        if loose:
            name = prefix.rstrip(".") or ROOT_GROUP
            breakdowns.setdefault(name, {}).update(loose)
            for lk, lv in loose.items():
                trend_scalars[f"{prefix}{lk}"] = lv

    walk(summary, "")
    return trend_scalars, breakdowns


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------

def _bar_chart(title: str, counts: dict[str, float], path: Path) -> None:
    plt = _plt()
    items = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    if len(items) > MAX_BREAKDOWN_BARS:
        head, tail = items[:MAX_BREAKDOWN_BARS], items[MAX_BREAKDOWN_BARS:]
        items = head + [(f"(+{len(tail)} more)", sum(v for _, v in tail))]

    labels = [k for k, _ in items]
    values = [v for _, v in items]
    fig, ax = plt.subplots(figsize=(9, max(2.2, 0.38 * len(items) + 1.2)))
    bars = ax.barh(range(len(items)), values)
    ax.set_yticks(range(len(items)), labels=labels, fontsize=8)
    ax.invert_yaxis()                      # biggest bar on top
    ax.set_title(title, fontsize=11)
    ax.bar_label(bars, fmt="%.0f", fontsize=8, padding=3)
    ax.margins(x=0.12)                     # room for the value labels
    fig.tight_layout()
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)


def _trend_chart(title: str,
                 series: dict[str, list[tuple[datetime, float]]],
                 path: Path) -> None:
    plt = _plt()
    if len(series) > MAX_TREND_SERIES_PER_FIG:   # keep the biggest movers
        keep = sorted(series, key=lambda k: series[k][-1][1],
                      reverse=True)[:MAX_TREND_SERIES_PER_FIG]
        series = {k: series[k] for k in keep}

    fig, ax = plt.subplots(figsize=(9, 4.5))
    for name, pts in sorted(series.items()):
        xs = [t for t, _ in pts]
        ys = [v for _, v in pts]
        ax.plot(xs, ys, marker="o", markersize=3, linewidth=1.2, label=name)

    positives = [v for pts in series.values() for _, v in pts if v > 0]
    if positives and max(positives) / min(positives) > 1000:
        ax.set_yscale("log")               # 40k corpus vs 3 failures
    ax.set_title(title, fontsize=11)
    ax.legend(fontsize=8, loc="best")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Entry point — the one call a DAG task makes
# ---------------------------------------------------------------------------

def render_all(stage: str, summary: dict[str, Any],
               history: list[dict[str, Any]] | None = None,
               out_dir: str | os.PathLike | None = None) -> list[Path]:
    """
    Render every derivable figure for one run summary. Returns the list
    of written paths.

    ``history`` is summary_store.load_history() output (oldest -> newest,
    normally already containing the current run). None / < 2 entries =>
    snapshot charts only.
    """
    base = Path(out_dir or os.getenv("SUMMARY_PLOTS_DIR",
                                     "/opt/airflow/data/plots"))
    target = base / _sanitize(stage)
    target.mkdir(parents=True, exist_ok=True)
    log.info("Rendering %s summary plots into %s", stage, target)

    written: list[Path] = []

    # -- snapshots (current run) -------------------------------------------
    _, breakdowns = analyze(summary)
    for name, counts in breakdowns.items():
        if not counts:
            continue
        path = target / f"snapshot_{_sanitize(name)}.png"
        _bar_chart(f"{stage} — {name}", counts, path)
        written.append(path)

    # -- trends (across runs) ----------------------------------------------
    rows = [r for r in (history or [])
            if r.get("created_at") and isinstance(r.get("summary"), dict)]
    if len(rows) >= 2:
        # dotted key -> [(created_at, value), ...], keys may come and go
        # across runs (schema evolution) — series simply have gaps.
        series: dict[str, list[tuple[datetime, float]]] = {}
        for row in rows:
            scalars, _ = analyze(row["summary"])
            for key, val in scalars.items():
                series.setdefault(key, []).append((row["created_at"], val))

        groups: dict[str, dict[str, list[tuple[datetime, float]]]] = {}
        for key, pts in series.items():
            group = key.split(".", 1)[0] if "." in key else ROOT_GROUP
            groups.setdefault(group, {})[key] = pts

        for group, gseries in groups.items():
            path = target / f"trend_{_sanitize(group)}.png"
            _trend_chart(f"{stage} — {group} over {len(rows)} runs",
                         gseries, path)
            written.append(path)
    else:
        log.info("Only %d historical run(s) for stage=%s — trend charts "
                 "need >= 2, skipping", len(rows), stage)

    log.info("Wrote %d plot file(s): %s", len(written),
             [p.name for p in written])
    return written