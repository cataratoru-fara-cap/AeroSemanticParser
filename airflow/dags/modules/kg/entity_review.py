"""
entity_review.py — measuring entity curation (gap 09)
=====================================================
The bar (Gabi, 2026-09-28): of the links curation KEEPS, >= 0.85 are
relevant to the meme; of those it DROPS, <= 0.15 were relevant. Measured on
a stratified sample the reviewer reads (Gabi: Claude reads and loops, not a
person clicking through a dashboard).

Only curated links are sampled — About and tag mentions. Title links and
the frame's own item are kept by definition, and undecided ones are in
neither set. Strata, by how the link was decided:

    rule_keep    platform, format, title_agrees, tag_and_text, tag_named
    judge_keep   the judge kept it (and, for About-only links, the
                 confirming model agreed)
    judge_drop   the judge dropped it
    rule_drop    deny_item, deny_class, generic_item

``draw`` writes one line per sampled mention with everything a verdict
needs (the frame's title and tags, the words around the mention with
[[the mention]] marked, the item's label and description, the decision),
plus the corpus size of each stratum at draw time. The reader writes
``verdicts.jsonl`` — ``{"key", "relevant": "yes"|"no"|"unsure",
"right_item": "yes"|"no"}`` — and ``score`` weights each stratum by its
share of the corpus: a small stratum read closely must not count as much
as a large one.

Intervals: Wilson per stratum; for the weighted rates, Wilson at the
effective sample size of the stratified estimate (n_eff = p(1-p)/var).

Pure except for the CLI, which imports the stores at call time.
"""

from __future__ import annotations

import json
import math
import random
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from modules.kg.review import wilson

RULE_KEEP = ("platform", "format", "title_agrees", "tag_and_text", "tag_named")
RULE_DROP = ("deny_item", "deny_class", "generic_item")
NOT_SAMPLED = ("title", "own_item")
STRATA = ("rule_keep", "judge_keep", "judge_drop", "rule_drop")
KEEP_STRATA = ("rule_keep", "judge_keep")
DROP_STRATA = ("judge_drop", "rule_drop")
DEFAULT_SIZES = {"rule_keep": 40, "judge_keep": 60, "judge_drop": 60, "rule_drop": 40}


def stratum(decision: Mapping[str, Any]) -> str | None:
    """A curation decision -> its stratum, or None when it is not sampled
    (titles, the frame's own item, still undecided)."""
    basis, keep = decision.get("basis"), decision.get("keep")
    if keep is None or basis in NOT_SAMPLED or decision.get("field") == "title":
        return None
    if basis == "judge":
        return "judge_keep" if keep else "judge_drop"
    if basis in RULE_KEEP and keep:
        return "rule_keep"
    if basis in RULE_DROP and not keep:
        return "rule_drop"
    raise ValueError(f"unknown decision {basis!r}/{keep!r}")


def sample_key(frame_id: str, mention_key: str) -> str:
    return f"{frame_id}|{mention_key}"


def draw(rows: Iterable[Mapping[str, Any]], sizes: Mapping[str, int],
         seed: int) -> tuple[list[dict], dict[str, int]]:
    """``rows``: ``{frame_id, key, keep, basis, field, qid}`` per decision.
    Returns (a reproducible sample, stratified; corpus count per stratum)."""
    pools: dict[str, list[dict]] = {s: [] for s in STRATA}
    for r in rows:
        s = stratum(r)
        if s is not None:
            pools[s].append(dict(r, stratum=s))
    rng = random.Random(seed)
    out = []
    for s in STRATA:
        pool = sorted(pools[s], key=lambda r: (r["frame_id"], r["key"]))
        out.extend(rng.sample(pool, min(sizes.get(s, 0), len(pool))))
    return out, {s: len(pools[s]) for s in STRATA}


def _snippet(text: str, start: int, end: int, width: int = 160) -> str:
    from modules.kg.curation import _snippet as snip
    return snip(text, start, end, width)


def render(row: Mapping[str, Any], mention: Mapping[str, Any],
           entry: Mapping[str, Any]) -> dict:
    """What the reader sees for one sampled mention."""
    from modules.kg.curation import about_text

    tags = [t for t in entry.get("tags") or [] if isinstance(t, str)]
    if mention["field"] == "about":
        where = _snippet(about_text(entry), mention["start"], mention["end"])
    else:
        i = mention.get("tag_index")
        where = "tags: " + ", ".join(f"[[{t}]]" if n == i else t for n, t in enumerate(tags))
    return {"key": sample_key(row["frame_id"], row["key"]), "stratum": row["stratum"],
            "title": entry.get("title"), "tags": ", ".join(tags[:20]),
            "field": mention["field"], "text": mention.get("text"), "context": where,
            "qid": mention["qid"], "label": mention.get("label"),
            "description": mention.get("description"),
            "decision": "keep" if row["keep"] else "drop", "basis": row["basis"]}


def _weighted(parts: Sequence[tuple[int, int, int]]) -> dict[str, Any] | None:
    """[(corpus weight, relevant, judged)] -> the stratified rate with a
    Wilson interval at its effective sample size."""
    parts = [(w, k, n) for w, k, n in parts if n]
    total = sum(w for w, _k, _n in parts)
    if not total:
        return None
    rate = sum(w / total * k / n for w, k, n in parts)
    var = sum((w / total) ** 2 * (k / n) * (1 - k / n) / n for w, k, n in parts)
    judged = sum(n for _w, _k, n in parts)
    n_eff = rate * (1 - rate) / var if var > 0 else judged
    n_eff = max(1, min(judged, round(n_eff)))
    lo, hi = wilson(round(rate * n_eff), n_eff)
    return {"rate": round(rate, 3), "interval": [round(lo, 3), round(hi, 3)],
            "n_effective": n_eff, "judged": judged}


def score(sample: Iterable[Mapping[str, Any]], verdicts: Iterable[Mapping[str, Any]],
          corpus: Mapping[str, int]) -> dict[str, Any]:
    """Kept precision and lost rate, stratum-weighted; per-stratum rates;
    the wrong-referent rate among kept links."""
    by_key = {v["key"]: v for v in verdicts}
    per: dict[str, dict[str, int]] = {s: {"read": 0, "relevant": 0, "unsure": 0,
                                          "wrong_item": 0} for s in STRATA}
    missing = []
    for row in sample:
        v = by_key.get(row["key"])
        if v is None:
            missing.append(row["key"])
            continue
        p = per[row["stratum"]]
        if v.get("relevant") == "unsure":
            p["unsure"] += 1
            continue
        p["read"] += 1
        p["relevant"] += v.get("relevant") == "yes"
        p["wrong_item"] += v.get("right_item") == "no"
    strata = {}
    for s, p in per.items():
        lo, hi = wilson(p["relevant"], p["read"])
        strata[s] = {**p, "corpus": corpus.get(s, 0),
                     "relevant_rate": round(p["relevant"] / p["read"], 3) if p["read"] else None,
                     "interval": [round(lo, 3), round(hi, 3)]}
    kept = _weighted([(corpus.get(s, 0), per[s]["relevant"], per[s]["read"])
                      for s in KEEP_STRATA])
    lost = _weighted([(corpus.get(s, 0), per[s]["relevant"], per[s]["read"])
                      for s in DROP_STRATA])
    wrong = _weighted([(corpus.get(s, 0), per[s]["wrong_item"], per[s]["read"])
                       for s in KEEP_STRATA])
    return {"kept_precision": kept, "lost_rate": lost, "kept_wrong_item_rate": wrong,
            "meets_bar": bool(kept and lost and kept["rate"] >= 0.85
                              and lost["rate"] <= 0.15),
            "strata": strata, "missing_verdicts": len(missing)}


# ---------------------------------------------------------------------------
# CLI — the stores are imported at call time
# ---------------------------------------------------------------------------

def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


def main(argv: Sequence[str] | None = None) -> int:
    """
    python -m modules.kg.entity_review draw --seed 20260929 --out DIR
        [--sizes rule_keep=40,judge_keep=60,judge_drop=60,rule_drop=40]
        -> DIR/sample.jsonl (one mention a line) + DIR/corpus.json; write
        DIR/verdicts.jsonl after reading (see ``score``).
    python -m modules.kg.entity_review score --dir DIR
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Entity curation review.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("draw")
    d.add_argument("--seed", type=int, required=True)
    d.add_argument("--out", required=True)
    d.add_argument("--sizes", default="")
    d.add_argument("--exclude", default="", help="a sample.jsonl whose frames to leave out "
                                                  "(a holdout never re-reads a tuning frame)")
    s = sub.add_parser("score")
    s.add_argument("--dir", required=True)
    args = parser.parse_args(argv)

    if args.cmd == "score":
        out = Path(args.dir)
        got = score(_read_jsonl(out / "sample.jsonl"), _read_jsonl(out / "verdicts.jsonl"),
                    json.loads((out / "corpus.json").read_text()))
        print(json.dumps(got, indent=2))
        return 0

    from modules import entity_curation_store

    sizes = dict(DEFAULT_SIZES)
    for part in filter(None, args.sizes.split(",")):
        name, n = part.split("=")
        sizes[name] = int(n)
    skip = set()
    if args.exclude:
        skip = {r["key"].split("|", 1)[0] for r in _read_jsonl(Path(args.exclude))}
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with entity_curation_store.get_store() as st:
        rows = ({"frame_id": doc["_id"], **dec}
                for doc in st.curation.find({}, {"decisions": 1})
                if doc["_id"] not in skip
                for dec in doc.get("decisions") or [])
        sample, corpus = draw(rows, sizes, args.seed)
        ids = sorted({r["frame_id"] for r in sample})
        mentions = {doc["_id"]: doc.get("mentions") or []
                    for doc in st.entities.find({"_id": {"$in": ids}}, {"mentions": 1})}
        entries = {doc["_id"]: doc for doc in st.entries.find(
            {"_id": {"$in": ids}}, {"title": 1, "tags": 1, "sections.kind": 1,
                                    "sections.text": 1})}
    from modules.kg.curation import mention_key

    with open(out / "sample.jsonl", "w", encoding="utf-8") as fh:
        for row in sample:
            m = next(m for m in mentions[row["frame_id"]] if mention_key(m) == row["key"])
            fh.write(json.dumps(render(row, m, entries[row["frame_id"]]),
                                ensure_ascii=False) + "\n")
    (out / "corpus.json").write_text(json.dumps(corpus, indent=2))
    print(f"{len(sample)} mentions in {out / 'sample.jsonl'} (corpus {corpus}); "
          f"write verdicts.jsonl there, then `score`", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
