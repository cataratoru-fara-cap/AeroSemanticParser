#!/usr/bin/env python3
"""The talk's world: where every chapter and every slide sits, and the camera
between them. Writes figures/generated/world.tex.

    python3 scripts/world.py            (from presentation/; stdlib only)

The deck is one big picture, the pipeline map. Each chapter is a node of it,
and the chapter's slides live INSIDE that node, small, on a track. The talk is
a camera moving over that picture: into a node (its icon and title, and the
slides it holds), along the track from slide to slide, and out and into the
next node in one motion.

Everything the camera needs is read from the deck itself, so there is one
place to change the talk:

  chapters/registry.tex   \\gmdefchapter{id}{num}{title}{colour}{on}{x}{y}{icon}{label}
                          \\gmdefedge{from}{to}{style}{path}{label}{label options}
  deck.tex                the chapters, in order (\\input{chapters/...})
  chapters/NN-*.tex       the tour, in order:
                            \\gmoverview{caption}    the whole map
                            \\gmenter{chapter}       a chapter's node: icon, title, its slides
                            \\gmgo{id}{caption}      a slide (the frame that follows it)
                            \\gmepilogue             later slides belong to no chapter

The camera path between two stops is van Wijk and Nuij's smooth zoom-and-pan
("Smooth and efficient zooming and panning", InfoVis 2003): the optimal path
zooms out just enough to see both ends, pans, and zooms back in, as one
motion. Each move becomes a series of pages that advance by themselves.

Per page the script also decides what is visible, so a page draws only what
is on screen: chapters fade from their map icon to their contents as the
camera nears; a slide is drawn as its page of thumbs.pdf (built first by the
Makefile), and as itself when the camera arrives.
"""
from __future__ import annotations

import math
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
OUT = HERE / "figures" / "generated" / "world.tex"

PAGE_W, PAGE_H = 16.0, 9.0          # cm, the beamer 16:9 page
R = 0.62                            # a chapter node's radius on the map (cm)
ENTRY_DIAMETER = 8.6                # the node's diameter on the page when the camera enters it
S_ENTRY = ENTRY_DIAMETER / (2 * R)  # map -> entry-view scale
DOOR_ANGLE = 160                    # where Doge waits on a node's rim (degrees)
DOOR_DIST = R + 0.12
AVATAR_R = 0.15
FPS_DUR = 0.045                     # seconds per motion page (written into the deck as \gmframedur)
RHO = math.sqrt(2)                  # van Wijk's zoom/pan trade-off (d3's default): along a track
RHO_TRAVEL = 2.0                    # between chapters: pull back further, so the map is seen
FADE_FROM, FADE_TO = 1.0, 2.2       # on-screen node radius (cm): map icon -> contents


# -- reading the deck ---------------------------------------------------------------------
def strip_comments(text: str) -> str:
    return re.sub(r"(?<!\\)%.*", "", text)


def read_args(text: str, i: int, n: int) -> tuple[list[str], int]:
    """n brace-delimited arguments starting at text[i] (whitespace allowed between)."""
    args = []
    for _ in range(n):
        while i < len(text) and text[i] in " \t\n":
            i += 1
        if i >= len(text) or text[i] != "{":
            raise ValueError(f"expected an argument at {text[i:i + 40]!r}")
        depth, j = 0, i
        while True:
            if text[j] == "\\":
                j += 2
                continue
            if text[j] == "{":
                depth += 1
            elif text[j] == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        args.append(text[i + 1:j])
        i = j + 1
    return args, i


def calls(text: str, names: dict[str, int]) -> list[tuple[str, list[str]]]:
    """Every \\name{...} with names[name] arguments, in order."""
    out = []
    pat = re.compile(r"\\(" + "|".join(sorted(names, key=len, reverse=True)) + r")(?![A-Za-z@])")
    for m in pat.finditer(text):
        args, _ = read_args(text, m.end(), names[m.group(1)])
        out.append((m.group(1), args))
    return out


@dataclass
class Chapter:
    id: str
    num: int
    title: str
    x: float
    y: float
    sats: list = field(default_factory=list)


@dataclass
class Sat:
    index: int          # 1-based, the page of thumbs.pdf
    key: str
    chapter: str        # a chapter id, or "prologue" / "epilogue"
    caption: str
    x0: float = 0.0     # world rect, lower-left corner and width
    y0: float = 0.0
    w: float = 0.0
    lx: float = 0.0     # entry-view centre and width, relative to the node's centre
    ly: float = 0.0
    lw: float = 0.0

    @property
    def h(self) -> float:
        return self.w * PAGE_H / PAGE_W

    @property
    def camera(self) -> tuple[float, float, float]:
        return (self.x0 + self.w / 2, self.y0 + self.h / 2, self.w)


@dataclass
class Stop:
    key: str
    kind: str           # "sat" or "view"
    chapter: str | None
    camera: tuple[float, float, float]
    door: str | None    # the chapter whose door Doge waits at


reg = strip_comments((HERE / "chapters" / "registry.tex").read_text(encoding="utf-8"))
CHAPTERS: dict[str, Chapter] = {}
for _, a in calls(reg, {"gmdefchapter": 9}):
    CHAPTERS[a[0]] = Chapter(a[0], int(a[1]), a[2], float(a[5]), float(a[6]))
EDGES = [(a[0], a[1]) for _, a in calls(reg, {"gmdefedge": 6})]

deck = strip_comments((HERE / "deck.tex").read_text(encoding="utf-8"))
files = [m.group(1) for m in re.finditer(r"\\input\{(chapters/\d\d-[^}]+)\}", deck)]
tour: list[tuple[str, list[str], str]] = []
for f in files:
    path = HERE / (f if f.endswith(".tex") else f + ".tex")
    text = strip_comments(path.read_text(encoding="utf-8"))
    tour += [(n, a, f) for n, a in calls(text, {"gmoverview": 1, "gmenter": 1, "gmgo": 2, "gmepilogue": 0})]

sats: list[Sat] = []
stops: list[Stop] = []
group = "prologue"
overviews = 0
for name, args, f in tour:
    if name == "gmepilogue":
        group = "epilogue"
    elif name == "gmenter":
        group = args[0]
        if group not in CHAPTERS:
            sys.exit(f"{f}: \\gmenter{{{group}}} is not in chapters/registry.tex")
    elif name == "gmgo":
        s = Sat(len(sats) + 1, args[0], group, args[1].strip())
        sats.append(s)
        if group in CHAPTERS:
            CHAPTERS[group].sats.append(s)
keys = [s.key for s in sats]
dupes = {k for k in keys if keys.count(k) > 1}
if dupes:
    sys.exit(f"slide ids used twice: {sorted(dupes)}")


# -- where things sit ----------------------------------------------------------------------
# Slides inside a node, in entry-view units (cm on the page when the camera has entered the
# node; its circle has radius ENTRY_DIAMETER/2 around (0, 0)). The track is a snake: left to
# right, then right to left, so every move along it is short.
def layout(n: int) -> list[tuple[float, float, float]]:
    rows = {1: [[0.0]], 2: [[-1.75, 1.75]], 3: [[-2.55, 0.0, 2.55]], 4: [[-1.5, 1.5], [1.5, -1.5]],
            5: [[-2.45, 0.0, 2.45], [1.25, -1.25]], 6: [[-2.3, 0.0, 2.3], [2.3, 0.0, -2.3]]}
    width = {1: 4.6, 2: 3.1, 3: 2.25, 4: 2.55, 5: 2.15, 6: 1.95}
    ys = {1: [-0.75], 2: [-0.75], 3: [-0.65], 4: [0.45, -2.0], 5: [0.45, -2.05], 6: [0.4, -2.0]}
    if n not in rows:
        sys.exit(f"a chapter holds 1 to 6 slides, not {n}")
    return [(x, ys[n][r], width[n]) for r, row in enumerate(rows[n]) for x in row]


ru = ENTRY_DIAMETER / 2
for ch in CHAPTERS.values():
    for s, (lx, ly, lw) in zip(ch.sats, layout(len(ch.sats))):
        lh = lw * PAGE_H / PAGE_W
        far = max(math.hypot(abs(lx) + lw / 2, ly + sy * lh / 2) for sy in (-1, 1))
        if far > ru - 0.2:
            sys.exit(f"{ch.id}: slide {s.key} leaves its node ({far:.2f} > {ru - .2:.2f})")
        s.lx, s.ly, s.lw = lx, ly, lw
        s.w = lw / S_ENTRY
        s.x0 = ch.x + (lx - lw / 2) / S_ENTRY
        s.y0 = ch.y + (ly - lh / 2) / S_ENTRY

# the prologue to the left of the map, the epilogue to its right: the camera reads left to right
ORPHAN_W = 3.2
for grp, x_first, step in (("prologue", -7.0, 3.6), ("epilogue", 19.6, 3.6)):
    for i, s in enumerate([s for s in sats if s.chapter == grp]):
        s.w = ORPHAN_W
        s.x0 = x_first + i * step - s.w / 2
        s.y0 = PAGE_H / 2 - s.h / 2

OVERVIEW = (PAGE_W / 2, PAGE_H / 2, PAGE_W)


def entry_camera(ch: Chapter) -> tuple[float, float, float]:
    return (ch.x, ch.y, PAGE_W / S_ENTRY)


order = sorted(CHAPTERS.values(), key=lambda c: c.num)
group, door = "prologue", None
sat_by_key = {s.key: s for s in sats}
for i, (name, args, f) in enumerate(tour):
    if name == "gmepilogue":
        group = "epilogue"
    elif name == "gmoverview":
        overviews += 1
        # Doge waits at the next chapter's door, or the last one's after the tour
        nxt = next((a2[0] for n2, a2, _ in tour[i + 1:] if n2 == "gmenter"), order[-1].id)
        stops.append(Stop(f"overview-{overviews}", "view", None, OVERVIEW, nxt))
    elif name == "gmenter":
        group = door = args[0]
        stops.append(Stop(f"enter-{args[0]}", "view", args[0], entry_camera(CHAPTERS[args[0]]), door))
    elif name == "gmgo":
        s = sat_by_key[args[0]]
        stops.append(Stop(s.key, "sat", s.chapter if s.chapter in CHAPTERS else None, s.camera,
                          door if s.chapter in CHAPTERS else None))


def door_xy(ch_id: str) -> tuple[float, float]:
    ch = CHAPTERS[ch_id]
    a = math.radians(DOOR_ANGLE)
    return ch.x + DOOR_DIST * math.cos(a), ch.y + DOOR_DIST * math.sin(a)


# -- the camera ------------------------------------------------------------------------------
def zoom_path(c0, c1, RHO=RHO):
    """van Wijk & Nuij: (path length S, position at u in [0, 1])."""
    ux0, uy0, w0 = c0
    ux1, uy1, w1 = c1
    dx, dy = ux1 - ux0, uy1 - uy0
    d2 = dx * dx + dy * dy
    if d2 < 1e-12:
        big_s = math.log(w1 / w0) / RHO

        def at(u):
            return ux0 + u * dx, uy0 + u * dy, w0 * math.exp(RHO * u * big_s)
        return abs(big_s), at
    d1 = math.sqrt(d2)
    b0 = (w1 * w1 - w0 * w0 + RHO ** 4 * d2) / (2 * w0 * RHO ** 2 * d1)
    b1 = (w1 * w1 - w0 * w0 - RHO ** 4 * d2) / (2 * w1 * RHO ** 2 * d1)
    r0, r1 = -math.asinh(b0), -math.asinh(b1)
    big_s = (r1 - r0) / RHO

    def at(u):
        s = u * big_s
        u2 = w0 / (RHO ** 2 * d1) * (math.cosh(r0) * math.tanh(RHO * s + r0) - math.sinh(r0))
        return ux0 + u2 * dx, uy0 + u2 * dy, w0 * math.cosh(r0) / math.cosh(RHO * s + r0)
    return big_s, at


def ease(t: float) -> float:
    return t * t * (3 - 2 * t)


def frames_for(length: float) -> int:
    return max(12, min(26, round(11 + 4.5 * length)))


def smooth(x: float) -> float:
    x = min(1.0, max(0.0, x))
    return x * x * (3 - 2 * x)


def visible(view, x0, y0, x1, y1) -> bool:
    vx0, vy0, vx1, vy1 = view
    return x1 >= vx0 and x0 <= vx1 and y1 >= vy0 and y0 <= vy1


MACHINE = (1.72, 0.8, 12.35, 8.5)   # the machine's dashed outline (cm, map; theme's \gmFM)
CAPTION = (10.9, 7.3, 15.8, 8.8)     # the overview's caption, at its top right


def machine_visible(view) -> bool:
    """The machine's outline or the caption is on screen. Inside the box, with its
    outline off screen, it is not drawn: zoomed in, the outline is thousands of
    dashes, and a viewer pays for every one of them."""
    x0, y0, x1, y1 = MACHINE
    m = 0.25                         # the outline's rounded corners lie within this of it
    strips = ((x0 - m, y0 - m, x1 + m, y0 + m), (x0 - m, y1 - m, x1 + m, y1 + m),
              (x0 - m, y0 - m, x0 + m, y1 + m), (x1 - m, y0 - m, x1 + m, y1 + m))
    return any(visible(view, *r) for r in strips) or visible(view, *CAPTION)


def scene(cam, avatar) -> str:
    """One page: the camera (scale, centre) and what to draw, as TeX."""
    cx, cy, w = cam
    s = PAGE_W / w
    h = w * PAGE_H / PAGE_W
    pad = 0.06 * w
    view = (cx - w / 2 - pad, cy - h / 2 - pad, cx + w / 2 + pad, cy + h / 2 + pad)
    items = []
    for k, (a, b) in enumerate(EDGES, 1):
        ca, cb = CHAPTERS[a], CHAPTERS[b]
        if visible(view, min(ca.x, cb.x) - 1.2, min(ca.y, cb.y) - 1.2, max(ca.x, cb.x) + 1.2,
                   max(ca.y, cb.y) + 1.2):
            items.append(f"\\gmFE{{{k}}}")
    if machine_visible(view):
        items.append("\\gmFM")
    for ch in order:
        if not visible(view, ch.x - R - 0.9, ch.y - R - 0.5, ch.x + R + 0.9, ch.y + R + 0.2):
            continue
        alpha = smooth((R * s - FADE_FROM) / (FADE_TO - FADE_FROM))
        items.append(f"\\gmFC{{{ch.id}}}{{{alpha:.2f}}}")
        if alpha > 0:
            for st in ch.sats:
                if visible(view, st.x0, st.y0, st.x0 + st.w, st.y0 + st.h):
                    items.append(f"\\gmFS{{{st.index}}}{{{alpha:.2f}}}")
    for st in sats:
        if st.chapter not in CHAPTERS and visible(view, st.x0, st.y0, st.x0 + st.w, st.y0 + st.h):
            items.append(f"\\gmFS{{{st.index}}}{{1}}")
    if avatar is not None:
        ax, ay = avatar
        if visible(view, ax - AVATAR_R, ay - AVATAR_R, ax + AVATAR_R, ay + AVATAR_R):
            items.append(f"\\gmFD{{{ax:.4f}}}{{{ay:.4f}}}")
    return f"\\gmF{{{s:.5f}}}{{{cx:.5f}}}{{{cy:.5f}}}{{{''.join(items)}}}"


def avatar_at(stop: Stop):
    return door_xy(stop.door) if stop.door else None


out = ["% generated by scripts/world.py — do not edit",
       f"% {len(CHAPTERS)} chapters, {len(sats)} slides, {len(stops)} stops",
       f"\\gmWorldScale{{{S_ENTRY:.5f}}}{{{R}}}{{{AVATAR_R}}}{{{FPS_DUR}}}"]
for st in sats:
    out.append(f"\\gmWorldSat{{{st.index}}}{{{st.key}}}{{{st.chapter}}}{{{st.x0:.4f}}}{{{st.y0:.4f}}}"
               f"{{{st.w:.4f}}}{{{st.lx:.3f}/{st.ly:.3f}/{st.lw:.3f}}}{{{st.caption}}}")
total_pages = 0
for i, stop in enumerate(stops):
    out.append(f"\\gmWorldStop{{{stop.key}}}{{{stop.kind}}}{{{stop.chapter or ''}}}")
    if stop.kind == "view":
        out.append(f"\\gmWorldView{{{stop.key}}}{{{scene(stop.camera, avatar_at(stop))}}}")
    if i == 0:
        continue
    prev = stops[i - 1]
    travel = prev.kind == "view" or stop.kind == "view" or prev.chapter != stop.chapter
    length, at = zoom_path(prev.camera, stop.camera, RHO_TRAVEL if travel else RHO)
    n = frames_for(length)
    a0, a1 = avatar_at(prev), avatar_at(stop)
    pages = []
    for k in range(1, n):
        t = ease(k / n)
        cam = at(t)
        if a0 and a1:
            av = (a0[0] + (a1[0] - a0[0]) * t, a0[1] + (a1[1] - a0[1]) * t)
        else:
            av = a1 if t > 0.5 else a0
        pages.append(f"\\only<{k}>{{{scene(cam, av)}}}%")
    total_pages += n - 1
    out.append(f"\\gmWorldMotion{{{stop.key}}}{{{n - 1}}}{{%\n" + "\n".join(pages) + "\n}")
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(out) + "\n", encoding="utf-8")
print(f"world: {len(CHAPTERS)} chapters, {len(sats)} slides, {len(stops)} stops, "
      f"{total_pages} motion pages -> {OUT.relative_to(HERE)}")
