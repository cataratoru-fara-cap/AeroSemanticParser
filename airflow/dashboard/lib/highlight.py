"""
highlight.py — mark what an event extracted inside the words it came from.

An event stores its evidence (``source_text``, the verbatim sentences) and
the values read out of it: the date WORDS (``date_text``), the places
(``locations``) and the people or accounts (``actors``). Showing the values
in a separate line makes the reader do the matching; marking them in the
quote shows at a glance what was taken from where — and what was not.

Matching follows the event audit's own notion of "the same words"
(``kg/events._comparable``): case, quote and dash styles, whitespace runs
and KYM's ``[12]`` citation markers are ignored; nothing else is. That
keeps the highlighter from marking text the extraction could not have
used, and it maps every match back to exact offsets in the original, so
the page renders the source unchanged.

A place or an actor may be named in an EARLIER sentence of the section and
only referred to in the event's own ("Butler made ..."); those values are
not in the quote, and ``mark_event`` returns them separately so the page
can say so instead of silently showing nothing.

Pure: no Streamlit, no Mongo — tests/test_highlight.py covers it.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Any, Iterable

# The three things an event says, in the order a reader asks for them.
# The CSS classes and the legend use these names.
KINDS = ("when", "where", "who")

_QUOTES = set("'‘’‚‛\"“”„‟")
_DASHES = set("‐‑‒–—―")
_FOOTNOTE = re.compile(r"\[\d{1,3}\]")
_NO_SPACE_BEFORE = set(".,;:!?)]")


@dataclass(frozen=True)
class Span:
    start: int
    end: int
    kind: str
    value: str


def _normalise(text: str) -> tuple[str, list[int]]:
    """``text`` in comparable form, and for each character of that form
    the index of the original character it came from.

    Mirrors the audit: citation markers dropped, whitespace runs collapsed,
    no space before ``.,;:!?)]`` or beside a quote mark, case folded, every
    quote to ``"`` and every dash to ``-``."""
    out: list[str] = []
    where: list[int] = []
    i, n = 0, len(text)
    while i < n:
        m = _FOOTNOTE.match(text, i)
        if m:                                  # a citation marker is not words
            i = m.end()
            continue
        ch = text[i]
        if ch.isspace():
            if out and out[-1] != " ":
                out.append(" ")
                where.append(i)
            i += 1
            continue
        if ch in _QUOTES:
            folded = '"'
        elif ch in _DASHES:
            folded = "-"
        else:
            folded = ch.casefold()
        if (folded in _NO_SPACE_BEFORE or folded == '"') and out and out[-1] == " ":
            out.pop()
            where.pop()
        for c in folded:                       # casefold can expand ("ß" -> "ss")
            out.append(c)
            where.append(i)
        i += 1
    # a space right after a quote mark was already appended before the next
    # character arrived; drop those now
    pairs = [(c, w) for k, (c, w) in enumerate(zip(out, where))
             if not (c == " " and k > 0 and out[k - 1] == '"')]
    while pairs and pairs[0][0] == " ":
        pairs.pop(0)
    while pairs and pairs[-1][0] == " ":
        pairs.pop()
    return "".join(c for c, _ in pairs), [w for _, w in pairs]


def _comparable(value: str) -> str:
    norm, _ = _normalise(value)
    return norm.strip('"').strip()


_WORDISH = set("@_#")


def _whole(hay: str, pos: int, length: int) -> bool:
    """Whether hay[pos:pos+length] stands as its own word(s): "Randle" in
    "Randle created ..." but not inside the handle "@irvinrandle"."""
    before = hay[pos - 1] if pos > 0 else " "
    after = hay[pos + length] if pos + length < len(hay) else " "
    return (not (before.isalnum() or before in _WORDISH)
            and not (after.isalnum() or after == "_"))


def find(text: str, value: str) -> list[tuple[int, int]]:
    """Every occurrence of ``value`` in ``text`` as (start, end) offsets
    into ``text``, compared the way the event audit compares. Whole-word
    occurrences only, when there are any; otherwise every occurrence (the
    audit accepts a value inside a longer word, so the mark still shows
    where it came from)."""
    needle = _comparable(value or "")
    if not needle or not text:
        return []
    hay, where = _normalise(text)
    hits: list[int] = []
    pos = hay.find(needle)
    while pos != -1:
        hits.append(pos)
        pos = hay.find(needle, pos + len(needle))
    whole = [h for h in hits if _whole(hay, h, len(needle))]
    return [(where[h], where[h + len(needle) - 1] + 1) for h in (whole or hits)]


def event_values(ev: dict[str, Any]) -> list[tuple[str, str]]:
    """(kind, value) for everything the event extracted. Places come from
    ``locations`` (extraction >= 3.0.0) or the older single ``location``."""
    places = list(ev.get("locations") or ([ev["location"]] if ev.get("location") else []))
    out = [("when", ev["date_text"])] if ev.get("date_text") else []
    out += [("where", p) for p in places if p]
    out += [("who", a) for a in ev.get("actors") or [] if a]
    return out


def mark_event(text: str, ev: dict[str, Any]) -> tuple[list[Span], list[tuple[str, str]]]:
    """(spans to mark in ``text``, values not found in it). Overlapping
    matches keep the earlier one, then the longer; a value that loses every
    occurrence to an overlap still counts as marked (its words are)."""
    found: list[Span] = []
    missing: list[tuple[str, str]] = []
    for kind, value in event_values(ev):
        hits = find(text, value)
        if not hits:
            missing.append((kind, value))
        found.extend(Span(s, e, kind, value) for s, e in hits)
    found.sort(key=lambda s: (s.start, -(s.end - s.start), KINDS.index(s.kind)))
    kept: list[Span] = []
    for span in found:
        if kept and span.start < kept[-1].end:
            continue
        kept.append(span)
    return kept, missing


def render(text: str, spans: Iterable[Span]) -> str:
    """``text`` as HTML with each span wrapped in a <mark>; everything else
    escaped. The kind rides on the class (for the style) AND the title (so
    it is readable without seeing colour)."""
    parts: list[str] = []
    at = 0
    for span in sorted(spans, key=lambda s: s.start):
        parts.append(html.escape(text[at:span.start]))
        parts.append(f'<mark class="kym-hl kym-hl-{span.kind}" title="{span.kind}">'
                     f"{html.escape(text[span.start:span.end])}</mark>")
        at = span.end
    parts.append(html.escape(text[at:]))
    return "".join(parts).replace("\n\n", "<br><br>")


def legend() -> str:
    """The key, in the marks' own style."""
    words = {"when": "date words", "where": "places", "who": "people & accounts"}
    return " &nbsp; ".join(
        f'<mark class="kym-hl kym-hl-{k}" title="{k}">{k}</mark> {words[k]}' for k in KINDS)
