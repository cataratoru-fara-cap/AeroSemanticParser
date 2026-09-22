"""
kg/review.py — drawing and scoring the human review of the event layer
==========================================================================
Gap 08 says every `mk:Event` is a language model's reading and nobody has
checked it. This module is how that stops being true: it draws a sample a
person can actually get through, and turns their verdicts into a number
that can be published beside the graph.

No Mongo, no Airflow, no Streamlit. `review_store.py` owns the collection;
`dashboard/pages/7_Review.py` is the surface.

The review unit is a SECTION, not an event
------------------------------------------
Per-event review cannot see what was MISSED, and coverage is the thing the
event layer was built to guarantee. Reading the section once answers both
questions — are these events right, and is anything absent — for barely
more effort than judging the events alone.

Three samples, scored separately
--------------------------------
`representative` is the headline: a proportional draw across section kind
and event density, the only one whose rate describes the layer as a whole.
The other two deliberately over-sample rare, high-risk populations, so
folding them into the headline would bias it downwards and describe
nothing:

  * `relative` — dates the PIPELINE computed by counting from another
    event ("that same day"). 4% of events, so a proportional sample holds
    about nine of them, and an error here propagates down a chain.
  * `unconfirmed` — 0.9% of events, the field with the least evidence
    behind it. Small enough to review every one, which makes it a census
    rather than an estimate.

Self-contained by design
------------------------
A drawn section carries its own numbered sentences and a snapshot of its
events. Two reasons: the dashboard image deliberately has no access to the
pipeline code (see dashboard/lib/data.py), and a verdict should stay
attached to the extraction it judged — a re-parse must not silently move
the text out from under a review that has already been recorded.
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
from typing import Any, Iterable, Mapping, Sequence

SAMPLES: tuple[str, ...] = ("representative", "relative", "unconfirmed")

# What a reviewer may say about each field. "ok" first, and the default:
# a reviewer marks exceptions, never confirms 300 correct values by hand.
VERDICTS: dict[str, tuple[str, ...]] = {
    "event": ("ok", "not-an-event", "wrong-span"),
    "date": ("ok", "wrong", "missing"),
    "location": ("ok", "wrong", "missing"),
    "actors": ("ok", "wrong", "incomplete"),
    "certainty": ("ok", "wrong"),
}
FIELDS: tuple[str, ...] = tuple(VERDICTS)

# Density bands for the proportional draw. A section with ten events is a
# different reading task from one with two, and 43 of 1,528 have ten or
# more; an unstratified draw leaves them to luck.
def density_band(n: int) -> str:
    if n == 0:
        return "none"
    if n <= 2:
        return "1-2"
    if n <= 5:
        return "3-5"
    return "6+"


def _strata(docs: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], list[str]]:
    out: dict[tuple[str, str], list[str]] = {}
    for doc in docs:
        key = (doc["source_section"], density_band(len(doc["events"])))
        out.setdefault(key, []).append(doc["unit_id"])
    return out


def _allocate(strata: Mapping[tuple[str, str], list[str]], size: int) -> dict:
    """Largest-remainder allocation, so the rounding does not quietly drop
    a whole small stratum."""
    total = sum(len(v) for v in strata.values())
    if not total:
        return {}
    exact = {k: size * len(v) / total for k, v in strata.items()}
    take = {k: min(int(v), len(strata[k])) for k, v in exact.items()}
    while sum(take.values()) < min(size, total):
        k = max((k for k in strata if take[k] < len(strata[k])),
                key=lambda k: (exact[k] - take[k], -take[k], k), default=None)
        if k is None:
            break
        take[k] += 1
    return take


def draw(docs: Sequence[Mapping[str, Any]], *, seed: int,
         representative: int = 60, relative_events: int = 40) -> dict[str, list[str]]:
    """unit_id lists per sample. Deterministic for a given seed and input.

    `unconfirmed` takes every section holding one — 51 events over 42
    sections is small enough to be a census, and an estimate built on two
    hits would be worthless.
    """
    rng = random.Random(seed)
    chosen: dict[str, list[str]] = {}

    strata = _strata(docs)
    take = _allocate(strata, representative)
    picked: list[str] = []
    for key in sorted(strata):
        pool = sorted(strata[key])
        rng.shuffle(pool)
        picked.extend(pool[:take.get(key, 0)])
    chosen["representative"] = sorted(picked)

    def has(doc, predicate):
        return any(predicate(e) for e in doc["events"])

    rel_pool = sorted(d["unit_id"] for d in docs
                      if has(d, lambda e: e.get("date_basis") == "relative"))
    rng.shuffle(rel_pool)
    by_id = {d["unit_id"]: d for d in docs}
    rel, seen = [], 0
    for uid in rel_pool:
        if seen >= relative_events:
            break
        rel.append(uid)
        seen += sum(1 for e in by_id[uid]["events"]
                    if e.get("date_basis") == "relative")
    chosen["relative"] = sorted(rel)

    chosen["unconfirmed"] = sorted(
        d["unit_id"] for d in docs
        if has(d, lambda e: e.get("certainty") != "confirmed"))
    return chosen


def qualifying(sample: str, event: Mapping[str, Any]) -> bool:
    """Which of a section's events this sample is actually ABOUT. The
    reviewer judges every event they are shown; only these count towards
    the stratum's own rate."""
    if sample == "relative":
        return event.get("date_basis") == "relative"
    if sample == "unconfirmed":
        return event.get("certainty") != "confirmed"
    return True


def wilson(ok: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% interval for a proportion. Wilson, not normal-approximation:
    near 100% the normal one runs past 1.0 and reads as certainty."""
    if not n:
        return (0.0, 1.0)
    p = ok / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


def score(reviews: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Verdicts -> the number that goes next to the graph.

    Per sample: how many events were judged, the share clean on every
    field, each field on its own, and — for `representative` only —
    recall, because that is the only sample whose misses mean anything
    about the corpus.
    """
    out: dict[str, Any] = {}
    reviews = [r for r in reviews if r.get("status") == "done"]
    for sample in SAMPLES:
        rows = [r for r in reviews if sample in (r.get("samples") or ())]
        judged = [(r, e) for r in rows for e in r.get("events") or []
                  if qualifying(sample, e)
                  and (r.get("verdicts") or {}).get(e["event_id"])]
        n = len(judged)
        by_field = {}
        for field in FIELDS:
            ok = sum(1 for r, e in judged
                     if r["verdicts"][e["event_id"]].get(field, "ok") == "ok")
            lo, hi = wilson(ok, n)
            by_field[field] = {"ok": ok, "n": n,
                               "rate": round(ok / n, 4) if n else None,
                               "ci95": [round(lo, 4), round(hi, 4)]}
        clean = sum(1 for r, e in judged
                    if all(r["verdicts"][e["event_id"]].get(f, "ok") == "ok"
                           for f in FIELDS))
        lo, hi = wilson(clean, n)
        entry: dict[str, Any] = {
            "sections_reviewed": len(rows), "events_judged": n,
            "clean_on_every_field": clean,
            "rate": round(clean / n, 4) if n else None,
            "ci95": [round(lo, 4), round(hi, 4)],
            "by_field": by_field,
            "problems": _problems(judged),
        }
        if sample == "representative":
            missed = sum(len(r.get("missed_sentences") or ()) for r in rows)
            events = sum(len(r.get("events") or ()) for r in rows)
            entry["recall"] = {
                "sections": len(rows), "events_found": events,
                "sentences_with_a_missed_event": missed,
                "rate": round(events / (events + missed), 4)
                        if (events + missed) else None}
        out[sample] = entry
    out["reviewers"] = sorted({r.get("reviewer") or "?" for r in reviews})
    return out


def _problems(judged) -> dict[str, dict[str, int]]:
    tally: dict[str, dict[str, int]] = {}
    for r, e in judged:
        for field, value in (r["verdicts"][e["event_id"]] or {}).items():
            if field in VERDICTS and value != "ok":
                tally.setdefault(field, {})
                tally[field][value] = tally[field].get(value, 0) + 1
    return tally


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m modules.kg.review",
        description="Draw and score the human review of the event layer.")
    sub = parser.add_subparsers(dest="command", required=True)
    d = sub.add_parser("draw", help="draw the sample into `event_reviews`")
    d.add_argument("--seed", type=int, default=20260922)
    d.add_argument("--representative", type=int, default=60)
    d.add_argument("--relative-events", type=int, default=40)
    d.add_argument("--redraw", action="store_true",
                   help="discard PENDING rows and draw again (keeps done ones)")
    sub.add_parser("report", help="score what has been reviewed so far")

    args = parser.parse_args(argv)
    from modules import review_store          # at call time, never at import
    if args.command == "draw":
        result = review_store.draw_sample(
            seed=args.seed, representative=args.representative,
            relative_events=args.relative_events, redraw=args.redraw)
    else:
        result = review_store.report()
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":        # pragma: no cover
    sys.exit(main())
