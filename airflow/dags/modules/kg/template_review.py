"""
template_review.py — contact sheets for reading template selections
====================================================================
The review loop (Gabi: Claude reads the output and loops on it, rather than
a person clicking through a dashboard) needs one picture per frame that
shows everything a verdict depends on:

    the frame's own KYM images        what the meme looks like
    each KEPT template                id, name, score R, why (gold/search)
      ...and the uploads merged into it   is the merge really one picture?
    suppressed as too similar         did variety cost a real template?
    the best REJECTED candidates      did the threshold cost a real template?

Pure except for Pillow and the image loader it is handed (a path on disk
for imgflip thumbnails, a fetch for KYM images), so it runs over the live
store or a pilot's in-memory one alike. Verdicts are written by the reader
into a JSONL file next to the sheets; ``score`` tallies them.
"""

from __future__ import annotations

import io
import json
import random
from pathlib import Path
from typing import Callable, Iterable, Sequence

THUMB = 120
PAD = 6
LABEL_H = 28
ROW_TITLE_H = 16


def _load(data: bytes | None, size: int = THUMB):
    from PIL import Image

    if not data:
        img = Image.new("RGB", (size, size), (230, 230, 230))
        return img
    try:
        img = Image.open(io.BytesIO(data))
        img.seek(0)
        img = img.convert("RGB")
    except Exception:
        return Image.new("RGB", (size, size), (255, 200, 200))
    img.thumbnail((size, size))
    return img


def _row(canvas, draw, y: int, title: str, cells: Sequence[tuple[bytes | None, str]],
         per_row: int) -> int:
    draw.text((PAD, y), title, fill=(0, 0, 0))
    y += ROW_TITLE_H
    if not cells:
        draw.text((PAD, y), "(none)", fill=(120, 120, 120))
        return y + ROW_TITLE_H
    for n, (data, label) in enumerate(cells):
        col, line = n % per_row, n // per_row
        x = PAD + col * (THUMB + PAD)
        yy = y + line * (THUMB + LABEL_H + PAD)
        img = _load(data)
        canvas.paste(img, (x, yy))
        for i, part in enumerate(_wrap(label, 20)[:2]):
            draw.text((x, yy + THUMB + 1 + i * 12), part, fill=(0, 0, 0))
    lines = (len(cells) + per_row - 1) // per_row
    return y + lines * (THUMB + LABEL_H + PAD)


def _wrap(text: str, width: int) -> list[str]:
    out, cur = [], ""
    for word in text.split():
        if len(cur) + len(word) + 1 > width and cur:
            out.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}".strip()
    if cur:
        out.append(cur)
    return out or [""]


def contact_sheet(frame: dict, templates: dict[int, dict],
                  thumb: Callable[[dict], bytes | None],
                  kym_image: Callable[[str], bytes | None], *, n_rejected: int = 8,
                  scored: Sequence[dict] = (), per_row: int = 8) -> bytes:
    """One frame's PNG. ``scored`` is kg/templates.score_candidates' output
    for the frame (for the rejected row); ``templates`` maps id -> doc."""
    from PIL import Image, ImageDraw

    def t_cell(tid: int, extra: str) -> tuple[bytes | None, str]:
        t = templates.get(tid) or {"_id": tid}
        return thumb(t), f"{tid} {extra} {t.get('name') or ''}"

    kym = [(kym_image(img["src"]), "KYM") for img in frame.get("frame_images") or []]
    kept = [t_cell(s["template_id"], f"#{s['mmr_rank']} R{s['R']:.2f} {s['method'][:4]}")
            for s in frame.get("selected") or []]
    merged = [t_cell(m, f"dup of {s['template_id']}")
              for s in frame.get("selected") or []
              for m in s.get("members") or [] if m != s["template_id"]][:per_row * 2]
    suppressed = [t_cell(x["template_id"], f"near {x['near']} R{x['R']:.2f}")
                  for x in frame.get("suppressed") or []][:per_row]
    rejected = [t_cell(s["template_id"], f"R{s['R']:.2f} t{s['s_text']:.2f} v{s['s_vis']:.2f}")
                for s in scored if not s["accepted"]][:n_rejected]

    rows = [("KYM images", kym), ("KEPT", kept), ("merged into a kept one", merged),
            ("suppressed as too similar", suppressed), ("best rejected", rejected)]
    height = 40
    for _title, cells in rows:
        lines = max(1, (len(cells) + per_row - 1) // per_row)
        height += ROW_TITLE_H + lines * (THUMB + LABEL_H + PAD) + 4
    width = PAD + per_row * (THUMB + PAD)
    canvas = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(canvas)
    head = (f"{frame.get('title')} | {frame.get('status')} | queries: "
            f"{'; '.join(frame.get('queries') or [])}")
    draw.text((PAD, 6), head[:150], fill=(0, 0, 0))
    draw.text((PAD, 20), frame.get("frame_url", "")[:150], fill=(90, 90, 90))
    y = 40
    for title, cells in rows:
        y = _row(canvas, draw, y, title, cells, per_row) + 4
    buf = io.BytesIO()
    canvas.save(buf, "PNG")
    return buf.getvalue()


def draw_sample(frames: Sequence[dict], strata: dict[str, Callable[[dict], bool]],
                per_stratum: dict[str, int], seed: int) -> list[dict]:
    """A fixed, reproducible sample, stratified; a frame lands in the first
    stratum it fits."""
    rng = random.Random(seed)
    placed: dict[str, list[dict]] = {name: [] for name in strata}
    for f in sorted(frames, key=lambda f: f["_id"]):
        for name, fits in strata.items():
            if fits(f):
                placed[name].append(f)
                break
    out = []
    for name, pool in placed.items():
        for f in rng.sample(pool, min(per_stratum.get(name, 0), len(pool))):
            out.append(dict(f, stratum=name))
    return out


def score(verdicts: Iterable[dict]) -> dict:
    """Tally reader verdicts: one line per kept template
    ({"frame", "template_id", "relevant": yes|no|unsure}), per kept pair
    judged duplicate ({"frame", "pair", "duplicate": true}), and per frame
    ({"frame", "missed": [ids]})."""
    kept = relevant = unsure = dup_pairs = missed = frames = 0
    for v in verdicts:
        if "relevant" in v:
            kept += 1
            relevant += v["relevant"] == "yes"
            unsure += v["relevant"] == "unsure"
        if v.get("duplicate"):
            dup_pairs += 1
        if "missed" in v:
            frames += 1
            missed += len(v["missed"])
    judged = kept - unsure
    return {"kept_judged": judged, "precision": round(relevant / judged, 3) if judged else None,
            "unsure": unsure, "duplicate_pairs": dup_pairs, "frames": frames,
            "missed_templates": missed}


def write_verdicts(path: Path, verdicts: Iterable[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for v in verdicts:
            fh.write(json.dumps(v) + "\n")


# ---------------------------------------------------------------------------
# CLI — the stores are imported at call time, never at import (kg/review.py)
# ---------------------------------------------------------------------------

STRATA_SIZES = {"gold": 15, "template_type": 20, "other_meme": 15, "other": 5,
                "nothing_selected": 5}


def _strata() -> dict[str, Callable[[dict], bool]]:
    return {
        "nothing_selected": lambda f: f.get("status") in ("no_results", "below_threshold"),
        "gold": lambda f: f.get("priority") == 1,
        "template_type": lambda f: f.get("priority") == 2,
        "other_meme": lambda f: f.get("priority") == 3,
        "other": lambda f: True,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """
    python -m modules.kg.template_review draw --seed 20260928 --out DIR
        a stratified, reproducible sample of searched frames -> one contact
        sheet per frame (PNG) + sample.jsonl; write verdicts.jsonl beside
        them (see ``score``) after reading the sheets.
    python -m modules.kg.template_review score --dir DIR
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Template selection review sheets.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("draw")
    d.add_argument("--seed", type=int, required=True)
    d.add_argument("--out", required=True)
    s = sub.add_parser("score")
    s.add_argument("--dir", required=True)
    args = parser.parse_args(argv)

    if args.cmd == "score":
        path = Path(args.dir) / "verdicts.jsonl"
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()]
        print(json.dumps(score(rows), indent=2))
        return 0

    from modules import imgflip_client, template_search, template_store
    from modules.kg import templates as kt

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    client = imgflip_client.make_client()
    with template_store.get_store() as st:
        frames = list(st.frames.find({"search_status": {"$exists": True}},
                                     {"frame_url": 1, "title": 1, "priority": 1, "queries": 1,
                                      "gold_ids": 1, "candidates": 1, "frame_images": 1,
                                      "selected": 1, "suppressed": 1, "status": 1}))
        sample = draw_sample(frames, _strata(), STRATA_SIZES, args.seed)
        ids = {c["t"] for f in sample for c in f.get("candidates") or [] if c.get("pre")}
        ids |= {m for f in sample for x in f.get("selected") or [] for m in x.get("members") or []}
        templates = st.templates_by_id(ids)

    def thumb(t: dict) -> bytes | None:
        p = t.get("thumb_path")
        return Path(p).read_bytes() if p and Path(p).exists() else None

    def kym(src: str) -> bytes | None:
        url = template_search.kym_hash_url(src)
        got = client.fetch_image(url) if url else None
        return got.content if got is not None and got.ok else None

    with open(out / "sample.jsonl", "w", encoding="utf-8") as fh:
        for n, f in enumerate(sample, 1):
            scored = kt.score_candidates(f, templates)
            png = contact_sheet(f, templates, thumb, kym, scored=scored)
            name = f"{n:02d}_{f['stratum']}_{f['_id'][:8]}.png"
            (out / name).write_bytes(png)
            fh.write(json.dumps({"sheet": name, "frame": f["_id"], "title": f.get("title"),
                                 "stratum": f["stratum"], "status": f.get("status"),
                                 "selected": [x["template_id"] for x in
                                              f.get("selected") or []]}) + "\n")
    print(f"{len(sample)} sheets in {out}; write verdicts.jsonl there, then `score`",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
