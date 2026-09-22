"""
kg/events.py — Origin/Spread narrative -> spatio-temporal event rows (LLM)
==================================================================================
One call per (frame, SECTION). The section goes to the model as numbered
sentences; what comes back is validated against
kg_config/event_extraction_schema.json and then checked, field by field,
against the text it came from.

No Mongo, no Airflow. Every model call goes through
modules/openwebui_client.py, which owns host failover and per-host API keys.

    python -m modules.kg.events extract \
        --input entries.jsonl --out data/kg/events/pilot.jsonl \
        --schema dags/kg_config/event_extraction_schema.json
    python -m modules.kg.events audit \
        --input entries.jsonl --events data/kg/events/pilot.jsonl

Extractive only — the model does not write text
------------------------------------------------
Version 2 (2026-09-18) is built around one rule: **nothing the model adds to
the source can reach the graph.** Version 1 asked the model to quote its
evidence and to summarise each event; reviewing its output showed exactly
what that invites — a summary nobody needed, a year "helpfully" inserted into
a quote, a "(TV series)" appended to a name. So:

  * The model is shown the section as NUMBERED SENTENCES and answers with
    sentence numbers. The evidence text (``source_text``) is copied by this
    module from the page, verbatim, citation markers and all. The model has
    no way to alter it.
  * There is no summary. The event IS its sentences plus when/where/who.
  * Every textual value the model does return — ``date_text``, ``location``,
    each ``actor`` — is a POINTER, not text. It is resolved against the
    section and replaced by the section's own words (``ground_value``,
    ``ground_date_text``); what is stored is always a span of the page.
    ``date_text`` is resolved inside the event's own sentences.
  * The model does not date anything. It returns the date WORDS; the
    pipeline parses them (``parse_date_phrase``), takes a missing year from
    the most recent DATE the section states before those words, and resolves a
    relative phrase ("that same day", "the following day", "three days
    later") against the nearest earlier dated event — recording which
    (``date_basis``, ``date_anchor``). Words that name no day ("shortly
    after", "the following week") leave the event undated.
  * Nothing is thrown away for being worded differently. Version 2.1 kept
    a value only if it was in the text verbatim, and a model that writes
    the year KYM's house style omits ("On June 16th, ..." -> "June 16th,
    2025") lost its date — 34 of 421 events in the 99-section pilot, and
    with them the anchor that the events after them were dated from.
    Grounding resolves the near miss to the page's words instead, so there
    is no ``discarded`` list any more: a value is either the page's or it
    is absent. ``audit()`` re-checks every stored record against its
    section, and ``extract()`` refuses to write a record that fails it.

Links, citations, photos and embedded posts are attached by POSITION, not by
the model: parser 1.6.0 records where each link sits (paragraph + offset)
and which paragraph each photo or embed follows, so an event gets the links
inside its own sentences, the references its ``[n]`` markers cite, and the
media KYM shows right after the paragraph(s) narrating it.

Coverage: nothing is capped. No truncation of the section (the longest in
the corpus is 6.6k characters, well inside the model's context), no ceiling
on events per section.

Model policy — criteria, not a name
-----------------------------------
Version 1 carried a model NAME copied from kg/semantics.py
(``mistral-small3.2:24b``), which is served only by ollama-ui, whose proxy
cut every request at 50 s on 2026-09-18 — and when it failed, the client's
same-tier fallback picked by size and ranked four REASONING models next.
The policy is now the criteria that made the working choice work:

  * never a reasoning ("thinking") model — ``EXCLUDED_CAPABILITIES``, enforced
    on the requested model and on every fallback by openwebui_client;
  * a general-purpose, non-cloud chat model on a reachable host, hosts in
    their configured priority order (ollama-ccdd first);
  * ``DEFAULT_CHAT_MODEL`` is the preferred model that satisfies them;
    ``KG_EVENTS_MODEL`` may name another, and a model that violates the
    policy is refused up front rather than silently replaced.

The lab hosts have no load balancer and serve requests FIFO. Concurrent
calls to one host do not run faster — they queue behind one another (and
behind everyone else's), so kym_events runs ONE extraction at a time.

Why not a bigger model
----------------------
Measured 2026-09-21 on the same 99 sections, same prompt, same pipeline
(date recall = of the sentences that STATE a date, how many end up carried
by a dated event):

    ministral-3:14b   427 events   94.0%   2.0 s p50   ~21 h backfill
    llama3.3:70b      406 events   95.1%   5.8 s p50   ~91 h backfill
    qwen3.8:27b       398 events   91.6%   3.0 s p50   ~34 h backfill
    mixtral:8x7b      350 events   66.9%   1.6 s p50   ~18 h backfill

llama3.3:70b wins the metric and loses the argument. Reading the 15
sentences it dates and ministral does not: "According to the post, Aquaman
was born on July 12th, 2025" (a date inside a joke), "The screenshot shows
that on June 3rd, 2014, an anonymous 4chan user claimed ..." (the case
this module refuses by design), "The post gained over 3,200 likes in a
year". It buys recall by dating reported content — and on that frame the
first of those then anchored the whole "on the same day" chain to
Aquaman's fictional birthday instead of Ozzy Osbourne's death. Four times
the host time for a worse graph.

qwen3.8:27b looked better on actors (86% of events against 73%) until the
same frame was read side by side: it fills them with "people", "users" and
"they" (hence _UNNAMED_ACTORS). gpt-oss:120b, the largest general model on
the lab, returns an EMPTY message.content on every call even with
think=False — its reasoning goes somewhere else — which is the excluded
capability earning its place in the policy.

The lesson: a count is not a verdict. Every one of these was decided by
reading one frame, not by the table.

Drawn from EventKG, not copied from it
--------------------------------------
SEM's what/when/where/who and EventKG's rule that every statement stays
traceable to its source are taken; its per-statement named graphs and its
cross-source event registry are not. See kg_config/MODEL.md.

Two things this module deliberately does differently from kg/semantics.py
------------------------------------------------------------------------
1. **It APPENDS JSONL; it does not rewrite the artifact after every item.**
   semantics.py rewrites its whole file per item — right at 119 items,
   about a terabyte of writes at ~36,500.
2. **It does not refuse to mix models on resume.** Each (frame, section) is
   an independent record; every doc records its own ``model``/``digest`` and
   the store surfaces the mix, so a heterogeneous corpus is visible rather
   than prevented.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import logging
import os
import re
import sys
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from modules.openwebui_client import (
    LLMConfig,
    ModelRequest,
    ModelUnavailableError,
    OpenWebUIClient,
    resolve_candidates,
)

log = logging.getLogger("kg.events")

__all__ = [
    "PROMPT_VERSION", "EXTRACTION_VERSION", "SOURCE_SECTIONS",
    "EXTRACT_PURPOSE", "DEFAULT_CHAT_MODEL", "EXCLUDED_CAPABILITIES",
    "SYSTEM_PROMPT", "USER_TMPL", "SECTION_FRAMING",
    "load_schema", "request_format", "item_checker", "make_validator",
    "ground_value", "ground_date_text", "resolve_dates",
    "split_sentences", "strip_footnotes", "event_id",
    "frame_key", "unit_id", "section_unit", "numbered_text", "model_request",
    "choose_model", "audit", "extract", "append_jsonl", "iter_jsonl",
    "completed_keys", "main",
]

# Bump when the system prompt or the user template changes: the store
# re-queues every unit whose stored prompt_version differs.
PROMPT_VERSION = "5"

# Bump when THIS MODULE's contract changes — the output shape, the
# validator's rules, the sentence splitter, the event_id recipe, how media
# are attached. Same effect: every unit is re-queued.
#   2.6.0  when NOTHING dated precedes the event's date words, a year
#          the section names exactly once supplies them.
#   2.5.0  a DECADE ("the 2010s") no longer dates an event to its first
#          year, and mk:dateAnchoredTo names the anchor event itself
#          rather than whatever event started at the same sentence.
#   2.4.1  a section the GRAMMAR cannot express (non-Latin text stops
#          Ollama's constrained sampler mid-string) is retried once with
#          no grammar rather than dead-lettered.
#   2.4.0  a citation marker ending a paragraph is no longer split off
#          as a sentence of its own: it rendered an EMPTY numbered line
#          for the model and took the citation away from the sentence
#          that cites it, in 19% of the corpus's sections.
#   2.3.0  a missing year comes from the most recent DATE stated before
#          the event's own date words, not from the most recent year
#          TOKEN anywhere before the event (which read years out of
#          quoted captions and festival names), and a year the phrase
#          itself states always wins.
#   2.2.0  the sentence splitter no longer reads "on X." as an initial;
#          values are GROUNDED, not merely checked: a value the model
#          worded differently is resolved to the span of the page it names
#          (ground_value / ground_date_text) instead of being discarded,
#          and the `discarded` list is gone. An event whose sentence
#          numbers are outside the section is now a rejected REPLY, not a
#          silently dropped row.
#   2.1.1  a relative phrase is also read from the event's own sentences
#          when the model quoted no date words.
#   2.1.0  the DATE is computed by the pipeline, not the model: it returns
#          only the date WORDS (verified to be in its sentences) and the
#          pipeline parses them, takes a missing year from an earlier
#          sentence, and resolves relative phrases ("that same day", "the
#          following day") against the nearest earlier dated event —
#          recorded as date_basis "stated" / "relative" with the anchor.
#   2.0.0  extractive-only: sentence numbers instead of quotes, no summary,
#          every returned string verified against the section, media by
#          position, no caps. (1.x asked for quotes and summaries.)
EXTRACTION_VERSION = "2.6.0"

SOURCE_SECTIONS: tuple[str, ...] = ("origin", "spread")

EXTRACT_PURPOSE = "kg.events.extract"

# The preferred model satisfying the policy (see the module docstring):
# non-thinking, general, served by ollama-ccdd. On the 20-unit pilot it
# answered at 15 s p50 / 43 s p95 — with version 1's much longer output.
DEFAULT_CHAT_MODEL = "ministral-3:14b"
EXCLUDED_CAPABILITIES: frozenset[str] = frozenset({"thinking"})

# Values models reach for instead of JSON null.
_NULLISH = {"", "null", "none", "n/a", "na", "unknown", "undated", "-", "?"}

# KYM's inline citation markers, "[12]". They stay in the evidence text we
# store (they are part of the page, and they are how an event's citations
# are found), but are hidden from the model and ignored when comparing.
_FOOTNOTE_MARKER = re.compile(r"\s*\[\d{1,3}\]\s*")
_MARKER = re.compile(r"\[(\d{1,3})\]")
_SPACE_BEFORE_PUNCT = re.compile(r"\s+([.,;:!?)\]])")

# Typography, folded for comparison only: every quote character to one,
# every dash to "-", no spacing beside a quote mark. Two strings that differ
# only in these are the same words in the same order.
_QUOTES = str.maketrans({c: '"' for c in "'‘’‚‛"
                                         '"“”„‟'})
_DASHES = str.maketrans({c: "-" for c in "‐‑‒–—―"})
_SPACE_AROUND_QUOTE = re.compile(r'\s*"\s*')

_MONTHS = ("january", "february", "march", "april", "may", "june", "july",
           "august", "september", "october", "november", "december")


SYSTEM_PROMPT = (
    "You extract EVENTS from one section of a knowyourmeme.com (KYM) "
    "entry, for a knowledge graph of internet memes. The section is "
    "given as numbered sentences. An event is something that HAPPENED "
    "at a time, in a place, or to someone — a first upload, a post, a "
    "video, a ban, a lawsuit, a death, a trend crossing to another "
    "platform."
    "\n"
    "\n"
    "Work through the sentences in order and return one event for each "
    "happening. A sentence that gives a date almost always narrates "
    "one: do not skip it because it is worded as an illustration (\"For "
    "instance, on January 3rd, 2025, TikToker @x posted ...\"). Return "
    "nothing for a sentence that only describes the meme, comments on "
    "it, or lists examples with no date and nobody acting."
    "\n"
    "\n"
    "For each event, return:"
    "\n"
    "- sentences: the numbers of the sentences that narrate it, "
    "consecutive."
    "\n"
    "- date_text: the words in those sentences that say WHEN it "
    "happened, copied exactly — \"May 4th, 2013\", \"early 2013\", \"Between "
    "May 28th and June 6th, 2025\", \"that same day\", \"the following day\" "
    "— or null if the sentences say nothing about when. Copy the words "
    "only: do NOT work out a date, a year or a day yourself. If the "
    "sentence gives a day but no year, write just the day as it stands "
    "(\"June 16th\"); the pipeline takes the year from earlier in the "
    "section."
    "\n"
    "- location: where it happened — a platform (\"TikTok\", "
    "\"/r/roblox\"), a place (\"Longyearbyen, Norway\"), or both — copied "
    "exactly as the section writes it, or null."
    "\n"
    "- location_type: \"platform\", \"geo\", \"both\", or \"unknown\" when "
    "location is null."
    "\n"
    "- actors: the people, accounts, communities or organizations that "
    "TOOK PART, copied exactly as written (\"@blockboy_192\", "
    "\"u/Shibetoshi\", \"/r/dogecoin\", \"Atsuko Sato\"); [] if none. Each "
    "one must NAME somebody — a handle, a person, a channel, a "
    "subreddit. \"users\", \"people\", \"fans\", \"viewers\", \"they\", \"a "
    "TikToker\" name nobody: leave them out, and return [] if that "
    "empties the list. Only who acted, not everyone the sentence "
    "happens to mention."
    "\n"
    "- certainty: whether the SECTION is sure THIS HAPPENING happened."
    "\n"
    "  \"confirmed\" — the section states it plainly. This is the normal "
    "case."
    "\n"
    "  \"unconfirmed\" — the section hedges the happening itself: "
    "\"reportedly posted\", \"is said to have originated\", \"allegedly "
    "filmed\", \"purportedly the first\"."
    "\n"
    "  \"disputed\" — the section says people disagree about whether it "
    "happened."
    "\n"
    "  \"debunked\" — the section says it turned out to be false, staged "
    "or a hoax."
    "\n"
    "  Someone posting a claim is a CONFIRMED event: in \"TikToker @x "
    "posted a video claiming Y\", the posting happened — it is Y that is "
    "a claim. Judge the verb of the event, not what was said inside it. "
    "Never upgrade a hedge to a fact."
    "\n"
    "\n"
    "COPY, NEVER ADD. Every text value you return is matched back "
    "against the section and stored in the SECTION'S own words. Do not "
    "add, complete, expand, explain or translate anything: no years "
    "that are not written, no qualifiers, no parentheses, no "
    "descriptions. A value with nothing behind it in the section is not "
    "stored at all."
    "\n"
    "\n"
    "DATES: return the WORDS only. A relative expression (\"that same "
    "day\", \"the following day\", \"three days later\") is a perfectly good "
    "date_text — keep it exactly as written; it is resolved afterwards "
    "against the events before it. Never write a date that the "
    "sentences do not spell out."
    "\n"
    "\n"
    "MERGE RULE: if two happenings are known only by their relative "
    "order and neither carries a date, return ONE event covering both "
    "sentences."
    "\n"
    "\n"
    "Respond ONLY with JSON: {\"events\": [...]}. Two example events:"
    "\n"
    "{\"sentences\": [1], \"date_text\": \"February 23rd, 2010\", \"location\": "
    "\"Tumblr\", \"location_type\": \"platform\", \"actors\": [\"Atsuko Sato\"], "
    "\"certainty\": \"confirmed\"}"
    "\n"
    "{\"sentences\": [4], \"date_text\": \"that same day\", \"location\": null, "
    "\"location_type\": \"unknown\", \"actors\": [], \"certainty\": "
    "\"unconfirmed\"}"
)

SECTION_FRAMING: dict[str, str] = {
    "origin": ("This is the ORIGIN section: where the meme came from — its "
               "first appearance, who made or posted it, the source work it "
               "was taken from, and when."),
    "spread": ("This is the SPREAD section: how the meme propagated — which "
               "platforms and communities picked it up, in what order, which "
               "posts or videos were milestones, and when each happened."),
}

USER_TMPL = ('Entry: "{title}" ({category}).\n'
             'Section: {section} (heading: "{heading}")\n'
             '{framing}\n\n'
             'Sentences:\n{numbered}')


# ------------------------------------------------------------------ time ----

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------- text ------

def _norm_ws(value: Any) -> str:
    return " ".join(str(value or "").split())


def strip_footnotes(text: str) -> str:
    """Drop KYM's inline ``[12]`` citation markers, and the space a marker
    before punctuation would leave behind ("by Sato [22]." -> "by Sato.")."""
    stripped = _FOOTNOTE_MARKER.sub(" ", str(text or ""))
    return _SPACE_BEFORE_PUNCT.sub(r"\1", stripped).strip()


def _comparable(value: Any) -> str:
    """The form every "is it in the text?" check compares in.

    Deliberately narrow: whitespace, citation markers and typography — all
    differences in how the same words are RENDERED. Anything more forgiving
    (dropping punctuation, prefixes, fuzzy ratios) would start accepting
    text the page does not contain, which is the one thing it must not do.
    """
    folded = _norm_ws(strip_footnotes(value)).casefold()
    folded = folded.translate(_QUOTES).translate(_DASHES)
    return _SPACE_AROUND_QUOTE.sub('"', folded).strip('"').strip()


# Sentence ends: a terminator, optional closing quotes/brackets, optional
# citation markers (which belong to the sentence they follow), then space
# and something that can start a sentence — but never a citation marker.
# Without that last exclusion a paragraph ending "... first appeared. [2]"
# split into the sentence and a second "sentence" holding only "[2]",
# which renders to an EMPTY numbered line for the model and takes the
# citation away from the sentence that actually cites it.
_SENTENCE_END = re.compile(
    r"[.!?]+[\"'”’)\]]*(?:\s*\[\d{1,3}\])*"
    r"(?=\s+(?!\[\d{1,3}\])[\"'“‘(\[@#/A-Z0-9])")
_ABBREVIATIONS = frozenset({
    "mr", "mrs", "ms", "dr", "prof", "st", "jr", "sr", "vs", "etc", "inc",
    "ltd", "co", "corp", "no", "vol", "ep", "fig", "approx", "dept", "est",
    "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "sept", "oct",
    "nov", "dec", "u.s", "u.k", "e.g", "i.e", "a.m", "p.m", "d.c",
})
# "J. K. Rowling", "George R. R. Martin": another initial follows.
_INITIAL_NEXT = re.compile(r"\s*[A-Z]\.")


def split_sentences(paragraph: str) -> list[tuple[int, int]]:
    """(start, end) character spans of the sentences in one paragraph.

    Conservative in ONE direction only. A split too many costs nothing —
    every span is still a verbatim slice, so the model can just point at
    both halves. A split MISSED costs the whole section: the model reads
    the prose, numbers the sentences it sees, and points past the end of
    ours, which make_validator rejects. So a period only fails to end a
    sentence when it is an abbreviation or a name's initial (_is_initial),
    and "X" — the platform, at the end of a third of KYM's sentences — is
    not treated as one.
    """
    spans: list[tuple[int, int]] = []
    start = 0
    for m in _SENTENCE_END.finditer(paragraph):
        before = paragraph[start:m.start()].split()
        token = before[-1].lower().rstrip(".") if before else ""
        if token in _ABBREVIATIONS:
            continue
        if len(token) == 1 and token.isalpha() and _is_initial(
                paragraph, before, m.end()):
            continue
        end = m.end()
        if paragraph[start:end].strip():
            spans.append(_trim(paragraph, start, end))
        start = end
    if paragraph[start:].strip():
        spans.append(_trim(paragraph, start, len(paragraph)))
    return spans


def _is_initial(paragraph: str, before: list[str], after: int) -> bool:
    """Is the single letter that just ended in a period a name's initial?

    Only when another initial follows ("J. K. Rowling") or a name precedes
    it ("George R. R. Martin"). Treating every single letter as one merged
    "... went viral on X. For example, ..." into a single sentence — and
    then the model, reading the same prose, numbered it its own way and
    pointed at a sentence this module does not have. That cost a whole
    section of the 99-section pilot (2.2.0).
    """
    if _INITIAL_NEXT.match(paragraph, after):
        return True
    return len(before) > 1 and before[-2][:1].isupper()


def _trim(text: str, start: int, end: int) -> tuple[int, int]:
    while start < end and text[start].isspace():
        start += 1
    while end > start and text[end - 1].isspace():
        end -= 1
    return start, end


# ----------------------------------------------------------------- ids ------

def frame_key(frame_url: str) -> str:
    """sha1 of the frame URL. MUST equal modules/mongo_base.url_doc_id —
    event docs join to `entries` on it; tests pin the two together."""
    return hashlib.sha1(frame_url.encode("utf-8")).hexdigest()


def unit_id(frame_url: str, section: str) -> str:
    return f"{frame_key(frame_url)}:{section}"


def event_id(frame_url: str, section: str, sentences: Sequence[int],
             date: str | None, date_precision: str) -> str:
    """``<frame12>-<content10>``: the evidence (which sentences) and when.

    Deterministic at temperature 0, so re-extracting unchanged text mints
    the same IRI. Two readings of the same sentences at the same date are one
    event; the same sentences read as two dates are two. The hyphen keeps
    the CSV column from ever looking numeric to pandas inside morph-kgc.
    """
    content = "\x00".join([frame_url, section,
                           ",".join(str(i) for i in sorted(set(sentences))),
                           date or "", date_precision or "none"])
    return (f"{frame_key(frame_url)[:12]}-"
            f"{hashlib.sha1(content.encode('utf-8')).hexdigest()[:10]}")


# --------------------------------------------------------------- schema ----

def load_schema(path: str) -> tuple[dict[str, Any], str]:
    """(event schema, sha256[:16] of the file). Read at CALL time — the sha is
    a staleness stamp, so editing the schema re-queues every unit."""
    with open(path, "rb") as fh:
        raw = fh.read()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()[:16]


def _model_item(item_schema: dict[str, Any]) -> dict[str, Any]:
    """The event schema minus everything marked ``x-derived`` — what the
    MODEL fills in, and nothing it cannot be trusted with."""
    item = copy.deepcopy(item_schema)
    for key in ("$schema", "title", "description"):
        item.pop(key, None)
    derived = {k for k, v in item.get("properties", {}).items()
               if isinstance(v, dict) and v.get("x-derived")}
    for key in derived:
        item["properties"].pop(key)
    item["required"] = [r for r in item.get("required", []) if r not in derived]
    item["additionalProperties"] = False
    return item


def request_format(item_schema: dict[str, Any]) -> dict[str, Any]:
    """The grammar handed to Ollama as ``format=``. No ``maxItems``: nothing
    is capped (and Ollama would not enforce it anyway)."""
    return {"type": "object", "additionalProperties": False,
            "required": ["events"],
            "properties": {"events": {"type": "array",
                                      "items": _model_item(item_schema)}}}


def item_checker(item_schema: dict[str, Any]) -> Draft202012Validator:
    """One compiled validator per run, from the same model-facing schema."""
    return Draft202012Validator(_model_item(item_schema))


# ------------------------------------------------------------- the units ----

def section_unit(entry: dict, section: str) -> dict | None:
    """One extraction unit from a parsed `entries` doc, or None.

    The WHOLE section, never truncated. Note the trap this reads around:
    entry["origin"] is the infobox line ("TikTok"), NOT the Origin section —
    that is entry["sections"][i] with kind "origin" (see kg/build.py).
    """
    url = entry.get("url")
    if not url:
        return None
    paragraphs: list[str] = []
    links: list[dict] = []
    images: list[dict] = []
    embeds: list[dict] = []
    heading = ""
    for s in entry.get("sections") or []:
        if s.get("kind") != section:
            continue
        heading = heading or (s.get("heading") or "")
        base = len(paragraphs)          # a page may split a section in two
        paragraphs.extend(s.get("text") or [])
        for link in s.get("links") or []:
            if link.get("paragraph") is not None:
                links.append({**link, "paragraph": base + link["paragraph"]})
        for image in s.get("images") or []:
            images.append({**image, "after_paragraph":
                           _shift(image.get("after_paragraph"), base)})
        for embed in s.get("embeds") or []:
            embeds.append({**embed, "after_paragraph":
                           _shift(embed.get("after_paragraph"), base)})
    if not any(p.strip() for p in paragraphs):
        return None

    sentences: list[dict] = []
    for pi, para in enumerate(paragraphs):
        for start, end in split_sentences(para):
            sentences.append({"id": len(sentences) + 1, "paragraph": pi,
                              "start": start, "end": end})

    markers = {int(n) for para in paragraphs for n in _MARKER.findall(para)}
    citations = {str(r["index"]): r["url"]
                 for r in entry.get("external_references") or []
                 if r.get("url") and r.get("index") is not None
                 and int(r["index"]) in markers}

    full = "\n\n".join(paragraphs)
    # The staleness stamp covers everything the unit is built from: the text
    # (what the model reads) AND the media positions (what gets attached),
    # so a re-parse that changes either re-queues this unit and no other.
    fingerprint = json.dumps([paragraphs, links, images, embeds, citations],
                             sort_keys=True, ensure_ascii=False, default=str)
    return {
        "unit_id": unit_id(url, section),
        "entry_id": frame_key(url),
        "frame_url": url,
        "source_section": section,
        "heading": heading or section.capitalize(),
        "title": entry.get("title") or "",
        "category": entry.get("category") or "unknown",
        "parser_version": entry.get("parser_version"),
        "paragraphs": paragraphs,
        "sentences": sentences,
        "links": links,
        "images": images,
        "embeds": embeds,
        "citations": citations,
        "source_sha256": hashlib.sha256(fingerprint.encode("utf-8")).hexdigest(),
        "source_chars": len(full),
    }


def _shift(after: int | None, base: int) -> int | None:
    return None if after is None else base + after


def _sentence_text(unit: dict, s: dict) -> str:
    return unit["paragraphs"][s["paragraph"]][s["start"]:s["end"]]


def numbered_text(unit: dict) -> str:
    """What the model reads: one numbered sentence per line, a blank line
    between paragraphs, citation markers hidden."""
    lines, last = [], None
    for s in unit["sentences"]:
        if last is not None and s["paragraph"] != last:
            lines.append("")
        lines.append(f"{s['id']}: {strip_footnotes(_sentence_text(unit, s))}")
        last = s["paragraph"]
    return "\n".join(lines)


def _span(unit: dict, ids: Sequence[int]) -> tuple[list[dict], str]:
    """The sentences from the first to the last of ``ids``, and their text
    copied VERBATIM from the page — within a paragraph the exact slice,
    across paragraphs joined the way m4s:origin/m4s:spread join them."""
    lo, hi = min(ids), max(ids)
    covered = [s for s in unit["sentences"] if lo <= s["id"] <= hi]
    parts: list[str] = []
    for pi in sorted({s["paragraph"] for s in covered}):
        in_p = [s for s in covered if s["paragraph"] == pi]
        parts.append(unit["paragraphs"][pi][in_p[0]["start"]:in_p[-1]["end"]])
    return covered, "\n\n".join(parts)


def _context(unit: dict, covered: list[dict]) -> str:
    """The section up to the end of the event — where a year may be taken
    from when the date words give none. Never anything AFTER the event."""
    last = covered[-1]
    before = unit["paragraphs"][:last["paragraph"]] + [
        unit["paragraphs"][last["paragraph"]][:last["end"]]]
    return _comparable("\n\n".join(before))


# ------------------------------------------------------------ validation ----

_FENCE_OPEN = re.compile(r"^```[A-Za-z]*\s*")
_FENCE_CLOSE = re.compile(r"\s*```$")


def _loads(content: str) -> Any:
    """json.loads, tolerating the ``` fence a model puts around JSON when
    it is NOT being held to a grammar — which is how the no-grammar retry
    below gets its reply back. Nothing else is relaxed: the object still
    has to be the schema's shape, and every value still has to ground."""
    text = (content or "").strip()
    if text.startswith("```"):
        text = _FENCE_CLOSE.sub("", _FENCE_OPEN.sub("", text)).strip()
    return json.loads(text)


def _nullish(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return None if text.lower() in _NULLISH else text


_MONTH_RE = "|".join(_MONTHS) + "|" + "|".join(m[:3] for m in _MONTHS) + "|sept"
# "February 23rd, 2010" / "Feb 23 2010" / "May 7th" (year from context)
_DAY_DATE = re.compile(rf"\b(?P<month>{_MONTH_RE})\.?\s+(?P<day>\d{{1,2}})(?:st|nd|rd|th)?"
                       rf"(?:\s*,)?(?:\s*(?P<year>(?:19|20)\d{{2}}))?\b", re.I)
# "the 23rd of February, 2010"
_DAY_DATE_OF = re.compile(rf"\b(?P<day>\d{{1,2}})(?:st|nd|rd|th)?\s+of\s+(?P<month>{_MONTH_RE})\.?"
                          rf"(?:\s*,)?(?:\s*(?P<year>(?:19|20)\d{{2}}))?\b", re.I)
_MONTH_DATE = re.compile(rf"\b(?P<month>{_MONTH_RE})\.?\s*,?\s*(?P<year>(?:19|20)\d{{2}})\b", re.I)
# A DECADE is not a year. "the 2010s", "the mid-2000s" and "the late
# 1990s" were all read as the decade's first year, which puts an
# mk:eventStart of 2010-01-01 on a page that said "the first half of the
# 2010s". date_precision has no "decade", and inventing one year out of
# ten is exactly what this module refuses to do, so a decade names no
# date. "2016's election" still does: the "s" there follows an apostrophe.
_YEAR = re.compile(r"(?<!\d)(?P<year>(?:19|20)\d{2})(?!\d)(?!s\b)")

# Relative expressions the pipeline can resolve ARITHMETICALLY against the
# nearest earlier dated event. Deliberately narrow: only phrases whose day
# offset is unambiguous. "shortly after", "the following week" and "later
# that year" are NOT here — they do not name a day, so the event stays
# undated rather than being given a date nobody wrote.
_SAME_DAY = re.compile(
    r"\b(?:(?:that|the) same (?:day|evening|morning|afternoon|night)"
    r"|(?:later|earlier) (?:that|the same) day"
    r"|(?:on )?that day"
    r"|that (?:evening|morning|afternoon|night)"
    r"|(?:a few |several |some )?(?:minutes|hours) later"
    r"|an hour later|a minute later|moments later)\b", re.I)
_NEXT_DAY = re.compile(r"\b(?:the (?:next|following) day|a day later|the day after)\b", re.I)
_N_DAYS = re.compile(r"\b(?P<n>\d{1,2}|two|three|four|five|six|seven|eight|nine|ten)\s+days?\s+later\b", re.I)
_WORD_NUMBERS = {"two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
                 "seven": 7, "eight": 8, "nine": 9, "ten": 10}


def _month_number(name: str) -> int:
    name = name.lower().rstrip(".")
    for i, month in enumerate(_MONTHS, 1):
        if month.startswith(name[:3]):
            return i
    return 0


def parse_date_phrase(phrase: str, year_hint: int | None = None
                      ) -> tuple[str | None, str]:
    """The date WORDS -> (normalized date, precision). Pure text parsing.

    2.1.0 moved this out of the model. It used to hand back both the words
    and its own normalization, and the two could disagree — on the first v2
    pilot it read "On June 4th, 2014" as 2014-06-03. Now it points at the
    words and this parses them, so that disagreement cannot exist.

    ``year_hint`` is the year stated most recently BEFORE the event; it is
    used only when the phrase itself names no year.
    """
    text = _norm_ws(strip_footnotes(phrase))
    if not text:
        return None, "none"
    for pattern in (_DAY_DATE, _DAY_DATE_OF):
        m = pattern.search(text)
        if m:
            month, day = _month_number(m.group("month")), int(m.group("day"))
            stated = m.group("year")
            if not stated:
                # A year the PHRASE states beats the hint even when it sits
                # past the day the pattern matched: "Around June 7th or June
                # 8th, 2026" attaches 2026 to the SECOND day, and the first
                # was taking its year from elsewhere on the page entirely.
                inline = _YEAR.search(text)
                stated = inline.group("year") if inline else None
            year = int(stated) if stated else year_hint
            if month and year and 1 <= day <= 31:
                try:
                    return (datetime(year, month, day).strftime("%Y-%m-%d"), "day")
                except ValueError:
                    return None, "none"          # e.g. February 31st
    m = _MONTH_DATE.search(text)
    if m and _month_number(m.group("month")):
        return f"{int(m.group('year')):04d}-{_month_number(m.group('month')):02d}", "month"
    m = _YEAR.search(text)
    if m:
        return m.group("year"), "year"
    # A month with no year at all: datable only if the section stated one.
    m = re.search(rf"\b(?P<month>{_MONTH_RE})\b", text, re.I)
    if m and year_hint and _month_number(m.group("month")):
        return f"{year_hint:04d}-{_month_number(m.group('month')):02d}", "month"
    return None, "none"


def relative_offset(phrase: str) -> int | None:
    """Days to add to the previous dated event, or None if not relative."""
    text = _norm_ws(strip_footnotes(phrase))
    if _SAME_DAY.search(text):
        return 0
    if _NEXT_DAY.search(text):
        return 1
    m = _N_DAYS.search(text)
    if m:
        n = m.group("n").lower()
        return int(n) if n.isdigit() else _WORD_NUMBERS.get(n)
    return None


def _shift_days(date: str, precision: str, days: int) -> str | None:
    if days == 0:
        return date
    if precision != "day":
        return None      # "the next day" after "May 2013" names no day
    return (datetime.strptime(date, "%Y-%m-%d")
            + timedelta(days=days)).strftime("%Y-%m-%d")


def _year_before(unit: dict, covered: list[dict],
                 phrase: str | None = None) -> int | None:
    """The year a date phrase without one may take: the year of the most
    recent DATE the section states before those very words.

    Two things this is careful about, both of them real pilot bugs:

    * **A date, not a number.** Taking the last year token picked up years
      out of quoted captions ("Reject modern memes, return to 2010"),
      band-name years, festival names ("Stagecoach 2025") and editorial
      asides ("it didn't establish the year 2026 as a start date") — 10 of
      339 dated events in the 99-section pilot, all wrong. A year attached
      to a month is a date the page is telling the time with.
    * **Before the words, not before the event.** The context stops where
      the event's own date phrase begins, so a date quoted LATER in the
      same sentences ("We will return to January 1st, 2016") cannot supply
      the year, while one earlier in them still can.

    A bare year is still used when the section states no month-bearing
    date before the event at all — and when nothing at all precedes them,
    a year the SECTION names exactly once. KYM often states it after the
    first event rather than before it ("On April 13th, YouTuber ... . On
    May 2nd, 2019, ..."), which left 30 of 5,612 sampled events undated on
    a day the page gives. Exactly once: a section naming two years says
    nothing about which one an undated April belongs to.
    """
    first = covered[0]
    head = "\n\n".join(unit["paragraphs"][:first["paragraph"]]
                       + [unit["paragraphs"][first["paragraph"]][:first["start"]]])
    if phrase:
        _covered, span_text = _span(unit, [s["id"] for s in covered])
        at = span_text.find(phrase)
        if at > 0:
            head = f"{head}\n\n{span_text[:at]}"
    spans = _date_spans(_comparable(head))
    for _start, _end, (year, month, _day) in reversed(spans):
        if year and month:
            return year
    for _start, _end, (year, _month, _day) in reversed(spans):
        if year:
            return year
    whole = {year for _s, _e, (year, month, _d)
             in _date_spans(_comparable("\n\n".join(unit["paragraphs"])))
             if year and month}
    return whole.pop() if len(whole) == 1 else None


# ------------------------------------------------------------- grounding ----
#
# What the model returns is a POINTER at the page, never text of its own.
# Grounding resolves it: the value it wrote is matched against the section
# and replaced by the span of the section it names. Two consequences worth
# stating, because 2.1 got them wrong in opposite directions:
#
#   * a value worded differently is NOT lost. KYM states a year once and
#     then omits it ("On June 16th, TikToker ..."), and the model writes it
#     back in. 2.1 required the phrase verbatim, so 34 of 421 pilot events
#     lost their date — and every "that same day" after one of them was
#     then anchored to the wrong event, silently.
#   * a value with nothing behind it is still not stored. Grounding needs
#     the HEAD of what the model wrote (its last word, asides removed) to
#     be on the page: "Atsuko Sato (blogger)" grounds to how the page names
#     her, "Kabosu's vet" grounds to nothing at all and is dropped, even
#     though "Kabosu" is on the page.

_WORD = re.compile(r"[^\W_]+", re.UNICODE)
_ASIDE = re.compile(r"\s*\([^)]*\)")
_POSSESSIVE = re.compile(r"[\u2019']s\b")

# Words a grounded span may contain that the model did not write: they only
# ever join the words around them, so including one cannot change who or
# what the span names.
_JOINERS = frozenset({"the", "a", "an", "of", "on", "in", "at", "to", "and"})

# An actor has to NAME somebody. These are on the page and ground perfectly
# well, and name nobody: as mk:eventActor literals they are noise a SPARQL
# query cannot join on. qwen3.8:27b returned "people", "users" and "they"
# on the first two retroslop events, which is how this list started.
_UNNAMED_ACTORS = frozenset({
    "user", "users", "person", "people", "fan", "fans", "viewer", "viewers",
    "commenter", "commenters", "poster", "posters", "other", "others",
    "they", "them", "he", "she", "it", "someone", "somebody", "anyone",
    "everyone", "many", "some", "most", "several", "various",
    "redditor", "redditors", "tiktoker", "tiktokers", "youtuber",
    "youtubers", "streamer", "streamers", "netizens", "the internet",
    "internet users", "social media users", "the public", "the community",
    "members", "fans of the meme", "meme creators", "creators",
})


def _token_spans(text: str) -> list[tuple[str, int, int, bool]]:
    """(folded word, start, end, is_citation_marker) for every word.

    A ``[12]`` marker tokenizes as its number, so it is flagged: inside a
    span it neither counts as a match nor blocks one, which is how
    strip_footnotes treats it everywhere else.
    """
    markers = [m.span() for m in _MARKER.finditer(text)]
    out: list[tuple[str, int, int, bool]] = []
    for m in _WORD.finditer(text):
        start, end = m.span()
        marker = any(a <= start and end <= b for a, b in markers)
        out.append((m.group().casefold(), start, end, marker))
    return out


def _same_word(a: str, b: str) -> bool:
    """Equal, or one the other's stem: "tiktok"/"tiktoker", "reddit"/
    "redditor", "meme"/"memes". Four characters minimum, so "may" does not
    reach "maybe" and "x" only ever matches "x"."""
    if a == b:
        return True
    short, long = (a, b) if len(a) < len(b) else (b, a)
    return len(short) >= 4 and long.startswith(short)


def ground_value(value: Any, hay: str) -> str | None:
    """The page's own words for what ``value`` names, or None.

    Verbatim first — a value already in the text is kept exactly as the
    model wrote it, which is how every value that passed 2.1 still passes.
    Otherwise the span of ``hay`` carrying the most of the model's words
    wins; it must begin and end on one of them, may contain nothing else
    but citation markers and joining words, and must include the head of
    what the model wrote.
    """
    text = _norm_ws(value)
    if not text:
        return None
    if _comparable(text) and _comparable(text) in _comparable(hay):
        return text

    wanted = _POSSESSIVE.sub("", _ASIDE.sub(" ", text))
    words = [w for w, _s, _e, _m in _token_spans(wanted)] or \
            [w for w, _s, _e, _m in _token_spans(text)]
    if not words:
        return None
    head = words[-1]
    tokens = _token_spans(hay)

    def wanted_here(word: str) -> bool:
        return any(_same_word(word, w) for w in words)

    best: tuple[tuple[int, int], str] | None = None
    for i, (word, start, _end, marker) in enumerate(tokens):
        if marker or word in _JOINERS or not wanted_here(word):
            continue
        run: list[int] = []
        for j in range(i, len(tokens)):
            other = tokens[j]
            if other[3]:                       # a citation marker, passed over
                continue
            if wanted_here(other[0]) or other[0] in _JOINERS:
                run.append(j)
                continue
            break
        while run and (tokens[run[-1]][0] in _JOINERS
                       or not wanted_here(tokens[run[-1]][0])):
            run.pop()
        if not run or not any(_same_word(tokens[j][0], head) for j in run):
            continue
        matched = sum(1 for j in run if wanted_here(tokens[j][0]))
        key = (matched, -start)
        if best is None or key > best[0]:
            # Handle and subreddit sigils sit outside the word: "/r/ x",
            # "@name". They belong to the name the page wrote.
            at = start
            while at and hay[at - 1] in "@#/":
                at -= 1
            best = (key, _norm_ws(strip_footnotes(hay[at:tokens[run[-1]][2]])))
    return best[1] if best else None


def _without_markers(text: str) -> tuple[str, list[int]]:
    """``text`` with its ``[12]`` markers removed, plus the original index of
    every character left — so a span found in the clean text can be cut out
    of the real one, markers and all."""
    kept: list[str] = []
    index: list[int] = []
    pos = 0
    for m in _MARKER.finditer(text):
        for i in range(pos, m.start()):
            kept.append(text[i])
            index.append(i)
        pos = m.end()
    for i in range(pos, len(text)):
        kept.append(text[i])
        index.append(i)
    return "".join(kept), index


def _date_components(text: str) -> tuple[int | None, int, int | None] | tuple[int | None, None, None]:
    """(year, month, day) that ``text`` states, any of them None."""
    text = _norm_ws(strip_footnotes(text))
    for pattern in (_DAY_DATE, _DAY_DATE_OF):
        m = pattern.search(text)
        if m and _month_number(m.group("month")):
            return (int(m.group("year")) if m.group("year") else None,
                    _month_number(m.group("month")), int(m.group("day")))
    m = _MONTH_DATE.search(text)
    if m and _month_number(m.group("month")):
        return int(m.group("year")), _month_number(m.group("month")), None
    m = _YEAR.search(text)
    if m:
        return int(m.group("year")), None, None
    m = re.search(rf"\b(?P<month>{_MONTH_RE})\b", text, re.I)
    if m and _month_number(m.group("month")):
        return None, _month_number(m.group("month")), None
    return None, None, None


def _date_spans(text: str) -> list[tuple[int, int, tuple]]:
    """Every date expression the TEXT itself states, as (start, end, parts)
    spans of ``text`` — the only candidates a date may be grounded to."""
    clean, index = _without_markers(text)
    taken: list[tuple[int, int]] = []
    found: list[tuple[int, int, tuple]] = []

    def free(a: int, b: int) -> bool:
        return not any(a < y and x < b for x, y in taken)

    for pattern in (_DAY_DATE, _DAY_DATE_OF, _MONTH_DATE,
                    re.compile(rf"\b(?P<month>{_MONTH_RE})\b", re.I), _YEAR):
        for m in pattern.finditer(clean):
            a, b = m.span()
            if not free(a, b):
                continue
            parts = _date_components(clean[a:b])
            if parts == (None, None, None):
                continue
            taken.append((a, b))
            found.append((index[a], index[b - 1] + 1, parts))
    found.sort()
    return found


def _date_agrees(candidate: tuple, wanted: tuple) -> bool:
    """Does a date the PAGE states say what the model's words said?

    Every component the model gave must be the same, at the same precision
    — no sharpening a month into a day, no shifting a day. The one thing
    the page may leave out is the YEAR, because KYM states it once and then
    stops: that year comes from earlier in the section instead.
    """
    c_year, c_month, c_day = candidate
    w_year, w_month, w_day = wanted
    if (w_day is None) != (c_day is None):
        return False
    if w_day is not None and (w_day != c_day or w_month != c_month):
        return False
    if (w_month is None) != (c_month is None):
        return False
    if w_month is not None and w_month != c_month:
        return False
    if w_year is not None and c_year is not None and w_year != c_year:
        return False
    return True


def _relative_span(text: str) -> str | None:
    """The page's own words for a relative date ("that same day"), or None."""
    clean, index = _without_markers(text)
    for pattern in (_SAME_DAY, _NEXT_DAY, _N_DAYS):
        m = pattern.search(clean)
        if m:
            return text[index[m.start()]:index[m.end() - 1] + 1]
    return None


def ground_date_text(phrase: Any, source_text: str) -> str | None:
    """The page's own words for this event's date, or None.

    Three ways in, in order: the phrase as written if the sentences carry
    it; the date the sentences state that says what the phrase said (this
    is what recovers "June 16th, 2025" -> "June 16th"); the sentences' own
    relative wording. Never a date the sentences do not state — a phrase
    that names June 2nd where the sentence says June 3rd grounds to
    nothing, and the event stays undated.
    """
    phrase = _norm_ws(phrase)
    if not phrase:
        return None
    if _comparable(phrase) and _comparable(phrase) in _comparable(source_text):
        return phrase
    wanted = _date_components(phrase)
    if wanted != (None, None, None):
        for start, end, parts in _date_spans(source_text):
            if _date_agrees(parts, wanted):
                return source_text[start:end]
        return None
    if relative_offset(phrase) is not None:
        return _relative_span(source_text)
    return ground_value(phrase, source_text)


def _check_event(raw: dict, checker: Draft202012Validator, unit: dict) -> dict:
    """One event, in the page's own words.

    Every value is GROUNDED (see above), so what comes out is a span of the
    section or nothing. Raising ValueError rejects the whole REPLY, which
    openwebui_client retries and then dead-letters: the one thing that
    cannot be resolved is an event pointing at no sentence of the section,
    and a section whose events have no evidence is a visible failure to
    re-run, never a row quietly dropped.

    The DATE is not here: the model returns only the date words, and
    resolve_dates() computes the date from them afterwards (2.1.0).
    """
    row = {k: raw[k] for k in checker.schema["properties"] if k in raw}
    for key in ("date_text", "location"):
        if key in row:
            row[key] = _nullish(row[key])
    row.setdefault("actors", [])
    row.setdefault("location_type", "unknown")
    for key in ("date_text", "location"):
        row.setdefault(key, None)
    try:
        checker.validate(row)
    except ValidationError as exc:
        raise ValueError(f"not the schema's shape: {exc.message}") from None

    n = len(unit["sentences"])
    ids = sorted({int(i) for i in row["sentences"] if 1 <= int(i) <= n})
    if not ids:
        raise ValueError(f"event points at sentences {row['sentences']}, and "
                         f"the section has {n}")
    covered, source_text = _span(unit, ids)
    section = "\n\n".join(unit["paragraphs"])

    date_text = ground_date_text(row.get("date_text"), source_text)
    location = ground_value(row.get("location"), section)
    location_type = (row.get("location_type") or "unknown") if location else "unknown"

    actors: list[str] = []
    seen: set[str] = set()
    for actor in row.get("actors") or []:
        grounded = ground_value(actor, section)
        key = _comparable(grounded) if grounded else ""
        if key and key not in seen and key not in _UNNAMED_ACTORS:
            seen.add(key)
            actors.append(grounded)

    return {
        "sentences": ids,
        "source_text": source_text,
        "date": None, "date_precision": "none", "date_basis": None,
        "date_text": date_text,
        "location": location,
        "location_type": location_type,
        "actors": actors,
        "certainty": row["certainty"],
    }


def resolve_dates(rows: list[dict], unit: dict) -> None:
    """Date every event, in document order, from the text alone.

    Two ways, both the pipeline's arithmetic and never the model's:

    * **stated** — the event's date words parse to a date. A year the words
      do not give is taken from the most recent year stated EARLIER in the
      section (never from later on the page).
    * **relative** — the words are "that same day", "the following day",
      "three days later"...: the date is that offset from the nearest
      earlier dated event, and ``date_anchor`` names it. Chains are
      allowed, because a chain is how the page itself reads.

    Anything else stays undated with its words kept: "shortly after" and
    "the following week" name no day, and inventing one is exactly what
    this module exists to prevent. So does a relative phrase with no dated
    event before it, and one measured from a date that names no day.
    """
    ordered = sorted(rows, key=lambda r: (r["sentences"][0], r["sentences"][-1]))
    anchor: dict | None = None
    for row in ordered:
        phrase = row.get("date_text") or ""
        covered = [s for s in unit["sentences"] if s["id"] in row["sentences"]]
        date, precision = parse_date_phrase(
            phrase, _year_before(unit, covered, phrase))
        if date:
            row.update(date=date, date_precision=precision, date_basis="stated")
            anchor = row
            continue
        # A relative phrase is unambiguous wherever it sits, so if the
        # model gave no usable date words, the event's OWN sentences are
        # read for one. (An absolute date is NOT taken this way: a sentence
        # often carries a date belonging to what it reports — "the
        # screenshot shows that on June 3rd, 2014 ..." — and picking it
        # would date the event by guesswork.)
        offset = relative_offset(phrase) if phrase else None
        if offset is None:
            offset = relative_offset(row["source_text"])
            if offset is not None:
                # The sentences carry the relative wording the model did not
                # quote; date_text is the page's, as everywhere else.
                row["date_text"] = _relative_span(row["source_text"]) or phrase or None
        if offset is None or anchor is None:
            continue
        shifted = _shift_days(anchor["date"], anchor["date_precision"], offset)
        if shifted is None:
            continue
        row.update(date=shifted, date_precision=anchor["date_precision"],
                   date_basis="relative", date_anchor=anchor)
        anchor = row
    # date_anchor carries the anchor ROW itself while ids do not exist yet;
    # make_validator swaps it for that row's event_id. Not its first
    # sentence number: 74 of 5,612 events in a 1,528-section sample share a
    # first sentence with another event, and an undated one between anchor
    # and anchored would have pointed mk:dateAnchoredTo at the wrong event.


def _attach(row: dict, unit: dict) -> dict:
    """Links, citations, photos and embeds for one event — by position only.

    * links: every hyperlink whose anchor starts inside one of the event's
      sentences (parser 1.6.0 records paragraph + offset per link);
    * citations: the reference each ``[n]`` marker in those sentences points
      to (the page's own external_references list);
    * images / embeds: what KYM shows right after a paragraph the event is
      narrated in (media before the first paragraph go with the first).
    A paragraph narrating two events shows its media with both: position
    cannot tell them apart, and inventing a finer split would be guessing.
    """
    covered, _ = _span(unit, row["sentences"])
    paragraphs = {s["paragraph"] for s in covered}
    links: list[dict] = []
    seen: set[tuple[str, str]] = set()

    def add(url: str, text: str, kind: str) -> None:
        if url and (kind, url) not in seen:
            seen.add((kind, url))
            links.append({"url": url, "text": text, "kind": kind})

    for link in unit["links"]:
        off, pi = link.get("offset"), link.get("paragraph")
        if off is None or pi not in paragraphs:
            continue
        if any(s["paragraph"] == pi and s["start"] <= off < s["end"] for s in covered):
            add(link["url"], link.get("text") or "", "link")
    for s in covered:
        for n in _MARKER.findall(_sentence_text(unit, s)):
            url = unit["citations"].get(n)
            if url:
                add(url, f"[{n}]", "citation")

    def shown_with(item: dict) -> bool:
        after = item.get("after_paragraph")
        return after in paragraphs or (after == -1 and 0 in paragraphs)

    images, embeds, seen_media = [], [], set()
    for image in unit["images"]:
        if shown_with(image) and image["src"] not in seen_media:
            seen_media.add(image["src"])
            images.append({k: image.get(k) for k in ("src", "alt", "caption")
                           if image.get(k)})
    for embed in unit["embeds"]:
        if shown_with(embed) and embed["url"] not in seen_media:
            seen_media.add(embed["url"])
            embeds.append({"url": embed["url"], "platform": embed.get("platform")})
    return {**row, "links": links, "images": images, "embeds": embeds}


def make_validator(checker: Draft202012Validator,
                   unit: dict) -> Callable[[str], list[dict]]:
    """The ``validate=`` callable for one unit's chat call.

    Raising is the contract openwebui_client.chat expects: retried, then
    returned as error_kind="invalid" DATA, and the section dead-lettered.
    It raises for a broken REPLY — not JSON, no ``events`` list, an event
    pointing at sentences the section does not have — and for nothing else.
    What the model returned is otherwise grounded, not judged: every value
    comes back in the page's words or not at all (2.2.0), so there is
    nothing here that drops an event for being worded badly.

    ``{"events": []}`` is valid: a section that only describes has none.
    """

    def validate(content: str) -> list[dict]:
        payload = _loads(content)                               # ValueError
        if not isinstance(payload, dict):
            raise TypeError("top level is not an object")
        rows = payload["events"]                                # KeyError
        if not isinstance(rows, list):
            raise TypeError("events is not a list")
        kept: list[dict] = []
        for raw in rows:
            if not isinstance(raw, dict):
                raise TypeError("an event is not an object")
            kept.append(_check_event(raw, checker, unit))

        # Dates come last: they are the pipeline's arithmetic over the
        # whole section, and a relative one needs the events before it.
        resolve_dates(kept, unit)
        out: list[dict] = []
        by_id: dict[str, dict] = {}
        ids_by_row: dict[int, str] = {}
        for original in sorted(kept, key=lambda r: (r["sentences"][0], r["sentences"][-1])):
            row = _attach(original, unit)
            row["frame_url"] = unit["frame_url"]
            row["source_section"] = unit["source_section"]
            row["event_id"] = event_id(unit["frame_url"], unit["source_section"],
                                       row["sentences"], row["date"],
                                       row["date_precision"])
            anchor_row = original.get("date_anchor")
            if anchor_row is not None:
                # Identity, not sentence number — see resolve_dates.
                row["date_anchor"] = ids_by_row.get(id(anchor_row))
            ids_by_row[id(original)] = row["event_id"]
            first = by_id.get(row["event_id"])
            if first is not None:
                # The same sentences read at the same date twice IS one
                # event, so the two readings are merged rather than one of
                # them thrown away: whatever the second saw that the first
                # did not is kept.
                _merge_event(first, row)
                continue
            by_id[row["event_id"]] = row
            out.append(row)
        return out

    return validate


def _merge_event(into: dict, other: dict) -> None:
    """Fold a second reading of the same event into the first."""
    for key in ("date_text", "location", "location_type"):
        if not into.get(key) or into.get(key) == "unknown":
            if other.get(key):
                into[key] = other[key]
    seen = {_comparable(a) for a in into["actors"]}
    into["actors"].extend(a for a in other["actors"]
                          if _comparable(a) not in seen
                          and not seen.add(_comparable(a)))


def audit(record: dict, unit: dict) -> list[str]:
    """Every way a stored record could contain something not on the page.

    Independent of the validator on purpose: extract() runs it on each
    record before writing and refuses on any violation, so a validator bug
    cannot leak an addition into the store. Also the `audit` CLI command,
    for re-checking an artifact against the current corpus.
    """
    problems: list[str] = []
    section = "\n\n".join(unit["paragraphs"])
    section_hay = _comparable(section)
    known_links = ({(l["url"], "link") for l in unit["links"]}
                   | {(u, "citation") for u in unit["citations"].values()})
    known_images = {i["src"] for i in unit["images"]}
    known_embeds = {e["url"] for e in unit["embeds"]}
    n = len(unit["sentences"])
    for i, ev in enumerate(record.get("events") or []):
        where = f"event {i}"
        ids = ev.get("sentences") or []
        if not ids or any(not 1 <= s <= n for s in ids):
            problems.append(f"{where}: sentences {ids} out of range 1..{n}")
            continue
        covered, expected = _span(unit, ids)
        if ev.get("source_text") != expected:
            problems.append(f"{where}: source_text is not the verbatim span")
        for part in (ev.get("source_text") or "").split("\n\n"):
            if part not in section:
                problems.append(f"{where}: source_text is not in the section")
        hay = _comparable(expected)
        if ev.get("date_text") and _comparable(ev["date_text"]) not in hay:
            problems.append(f"{where}: date_text {ev['date_text']!r} not in its sentences")
        if ev.get("location") and _comparable(ev["location"]) not in section_hay:
            problems.append(f"{where}: location {ev['location']!r} not in the section")
        for actor in ev.get("actors") or []:
            if _comparable(actor) not in section_hay:
                problems.append(f"{where}: actor {actor!r} not in the section")
        if ev.get("date"):
            # Recompute it from the words: a stored date must be exactly what
            # the pipeline derives, so neither the model nor a bug can put a
            # date on the page that the page does not carry.
            basis = ev.get("date_basis")
            phrase = ev.get("date_text") or ""
            if basis == "stated":
                date, precision = parse_date_phrase(
                    phrase, _year_before(unit, covered, phrase))
                if (date, precision) != (ev["date"], ev.get("date_precision")):
                    problems.append(f"{where}: date {ev['date']!r} is not what "
                                    f"{phrase!r} parses to ({date!r})")
            elif basis == "relative":
                # The same two places resolve_dates reads: the date words,
                # then the event's own sentences (2.1.1).
                if (relative_offset(phrase) is None
                        and relative_offset(ev.get("source_text") or "") is None):
                    problems.append(f"{where}: date {ev['date']!r} is marked "
                                    f"relative but neither {phrase!r} nor its "
                                    f"sentences hold a relative phrase")
                if not ev.get("date_anchor"):
                    problems.append(f"{where}: relative date with no anchor")
            else:
                problems.append(f"{where}: date {ev['date']!r} with no basis")
        for link in ev.get("links") or []:
            if (link.get("url"), link.get("kind")) not in known_links:
                problems.append(f"{where}: link {link.get('url')!r} is not on the page")
        for image in ev.get("images") or []:
            if image.get("src") not in known_images:
                problems.append(f"{where}: image {image.get('src')!r} is not on the page")
        for embed in ev.get("embeds") or []:
            if embed.get("url") not in known_embeds:
                problems.append(f"{where}: embed {embed.get('url')!r} is not on the page")
    return problems


# ---------------------------------------------------------------- model ----

def model_request(env: Mapping[str, str] | None = None) -> ModelRequest:
    """The policy as a request: KG_EVENTS_* from the environment, the
    preferred default, and — always — no reasoning models."""
    base = ModelRequest.from_env("KG_EVENTS", default_model=DEFAULT_CHAT_MODEL,
                                 env=env)
    return replace(base, exclude_capabilities=EXCLUDED_CAPABILITIES)


def choose_model(client: OpenWebUIClient, request: ModelRequest):
    """The model the policy will use right now — refused up front if none.

    A KG_EVENTS_MODEL that names a reasoning model is an error, not
    something to route around: it would otherwise be silently replaced by
    a same-tier fallback the operator never asked for.
    """
    inventory = client.inventory()
    named = [m for models in inventory.values() for m in models
             if request.model and m.name == request.model]
    if any(m.capabilities & request.exclude_capabilities for m in named):
        raise ModelUnavailableError(
            f"{request.model} is a {'/'.join(sorted(request.exclude_capabilities))} "
            f"model; kym_events never uses one (see kg/events.py, model policy)")
    candidates = resolve_candidates(inventory, request, client.cfg.host_order,
                                    allow_cloud=client.cfg.allow_cloud)
    if not candidates:
        raise ModelUnavailableError(
            f"no reachable host serves a model the event policy allows ({request})")
    first = candidates[0]
    if request.model and first.name != request.model:
        log.warning("%s is unavailable; the policy falls back to %s on %s",
                    request.model, first.name, first.host)
    return first


# ------------------------------------------------------------- the artifact -

def append_jsonl(path: str, record: dict) -> None:
    """One line, flushed. Durable per unit at O(1) cost."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        fh.flush()


def iter_jsonl(path: str) -> Iterator[dict]:
    """Every parseable line; a torn final line is skipped with a warning."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except ValueError:
                log.warning("%s:%d is not valid JSON; skipped", path, n)


def completed_keys(path: str) -> set[tuple[str, str]]:
    """(frame_url, source_section) already in the artifact — the CLI's
    resume index. The DAG re-filters against Mongo instead."""
    return {(r.get("frame_url"), r.get("source_section")) for r in iter_jsonl(path)}


# --------------------------------------------------------------- extract ----

def extract(client: OpenWebUIClient, units: Iterable[dict], out_path: str,
            request: ModelRequest, *, schema_path: str, resume: bool = True,
            on_record: Callable[[dict], None] | None = None,
            progress: Callable[[str], None] = print) -> dict[str, Any]:
    """Run one batch of units, appending a line per unit to ``out_path``.

    Failures are DATA in the summary, not exceptions. ``on_record`` is
    called with each record right after its line is on disk — how the DAG
    gets a unit into Mongo the moment it is durable.
    """
    units = list(units)
    item_schema, schema_sha = load_schema(schema_path)
    fmt = request_format(item_schema)
    checker = item_checker(item_schema)

    done = completed_keys(out_path) if resume else set()
    todo = [u for u in units
            if (u["frame_url"], u["source_section"]) not in done]
    if len(todo) < len(units):
        progress(f"Resuming: {len(units) - len(todo)} already in {out_path}, "
                 f"{len(todo)} to go")
    if todo:
        chosen = choose_model(client, request)
        progress(f"Model: {chosen.name} on {chosen.host} (policy: no "
                 f"{', '.join(sorted(request.exclude_capabilities)) or 'excluded'} "
                 f"models)")

    records: list[dict] = []
    failed: list[dict] = []
    for i, unit in enumerate(todo, 1):
        section = unit["source_section"]
        started = time.monotonic()
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_TMPL.format(
                title=unit["title"], category=unit["category"],
                section=section, heading=unit["heading"],
                framing=SECTION_FRAMING[section], numbered=numbered_text(unit))}]
        result = client.chat(
            messages,
            request, purpose=EXTRACT_PURPOSE, format=fmt,
            # temperature 0: event_id is content-addressed, so a rerun must
            # reproduce it. think=False: belt and braces over the policy.
            options={"temperature": 0}, think=False,
            validate=make_validator(checker, unit))
        if not result.ok and result.error_kind == "invalid":
            # Ollama's grammar-constrained sampler dies mid-string on
            # non-Latin text: on a Russian entry it stopped dead after
            # emitting two characters of "СтоЛичный Она-Нас", every time,
            # at any num_predict, with format=schema AND format=json —
            # and returned complete JSON with no format at all. 1.5% of
            # sections carry a non-Latin script, so losing them is not
            # random loss, it is losing the international memes.
            # The grammar is belt; jsonschema in _check_event is braces,
            # and it still runs. So: one retry with no grammar.
            result = client.chat(messages, request, purpose=EXTRACT_PURPOSE,
                                 options={"temperature": 0}, think=False,
                                 validate=make_validator(checker, unit))
            if result.ok:
                log.info("%s needed the no-grammar retry", unit["unit_id"])
        elapsed = round(time.monotonic() - started, 2)
        if not result.ok:
            failed.append({"unit_id": unit["unit_id"], "entry_id": unit["entry_id"],
                           "frame_url": unit["frame_url"], "source_section": section,
                           "source_sha256": unit["source_sha256"],
                           "error_kind": result.error_kind, "error": result.error})
            progress(f"  !! [{i}/{len(todo)}] {unit['frame_url']} {section}: "
                     f"{result.error_kind}: {result.error}")
            continue

        record = {
            "unit_id": unit["unit_id"], "entry_id": unit["entry_id"],
            "frame_url": unit["frame_url"], "source_section": section,
            "heading": unit["heading"], "events": list(result.parsed),
            "event_count": len(result.parsed),
            "sentence_count": len(unit["sentences"]),
            "source_sha256": unit["source_sha256"],
            "source_chars": unit["source_chars"],
            "parser_version": unit.get("parser_version"),
            "prompt_version": PROMPT_VERSION,
            "extraction_version": EXTRACTION_VERSION,
            "schema_sha": schema_sha,
            "model": result.model, "digest": result.digest, "host": result.host,
            "extracted_at": _now(), "elapsed_s": elapsed,
            "attempts": result.attempts,
        }
        problems = audit(record, unit)
        if problems:
            # A validator bug, not model behaviour — never write it.
            raise AssertionError(f"{unit['unit_id']} failed its audit: {problems}")
        append_jsonl(out_path, record)      # durable BEFORE anything else
        if on_record is not None:
            on_record(record)
        records.append(record)
        progress(f"  [{i}/{len(todo)}] {unit['frame_url']} {section}: "
                 f"{record['event_count']} events ({elapsed}s)")

    models = sorted({r["model"] for r in records if r.get("model")})
    dated = sum(1 for r in records for e in r["events"] if e.get("date"))
    summary = {
        "units": len(units), "attempted": len(todo),
        "extracted": len(records), "skipped_done": len(units) - len(todo),
        "events": sum(r["event_count"] for r in records),
        "zero_event_units": sum(1 for r in records if not r["events"]),
        "dated_events": dated,
        "failed": failed, "failed_count": len(failed),
        "models": models, "schema_sha": schema_sha,
        "prompt_version": PROMPT_VERSION,
        "extraction_version": EXTRACTION_VERSION,
        "latency_s": sorted(r["elapsed_s"] for r in records),
        "out_path": out_path,
    }
    progress(f"Done: {len(records)}/{len(todo)} units, {summary['events']} events, "
             f"{dated} dated, {len(failed)} failed "
             f"({', '.join(models) or 'no model'})")
    return summary


# ------------------------------------------------------------------- CLI ----

def _units_from(path: str, sections: Sequence[str]) -> list[dict]:
    units = []
    for entry in iter_jsonl(path):
        for section in sections:
            unit = section_unit(entry, section)
            if unit:
                units.append(unit)
    return units


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m modules.kg.events",
        description="Extract spatio-temporal events from KYM narrative sections.")
    sub = parser.add_subparsers(dest="command", required=True)

    ex = sub.add_parser("extract", help="entries JSONL -> events JSONL")
    ex.add_argument("--input", required=True, help="JSONL of `entries` docs")
    ex.add_argument("--out", required=True, help="events JSONL (appended)")
    ex.add_argument("--schema", required=True,
                    help="kg_config/event_extraction_schema.json")
    ex.add_argument("--sections", default=",".join(SOURCE_SECTIONS))
    ex.add_argument("--limit", type=int, default=0)
    ex.add_argument("--no-resume", action="store_true")

    au = sub.add_parser("audit", help="re-check an events JSONL against its entries")
    au.add_argument("--input", required=True, help="JSONL of `entries` docs")
    au.add_argument("--events", required=True, help="events JSONL to audit")
    au.add_argument("--sections", default=",".join(SOURCE_SECTIONS))

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sections = [s.strip() for s in args.sections.split(",") if s.strip()]
    for section in sections:
        if section not in SECTION_FRAMING:
            parser.error(f"unknown section {section!r}")
    units = _units_from(args.input, sections)

    if args.command == "audit":
        by_id = {u["unit_id"]: u for u in units}
        records = {r["unit_id"]: r for r in iter_jsonl(args.events)}
        problems = {uid: audit(r, by_id[uid]) for uid, r in records.items()
                    if uid in by_id}
        bad = {k: v for k, v in problems.items() if v}
        print(json.dumps({"records": len(records), "audited": len(problems),
                          "not_on_this_input": len(records) - len(problems),
                          "with_violations": len(bad), "violations": bad}, indent=2))
        return 1 if bad else 0

    if args.limit:
        units = units[:args.limit]
    print(f"{len(units)} units from {args.input}")
    # Configuration is read here, where it is used — never at import time.
    client = OpenWebUIClient(LLMConfig.from_env())
    summary = extract(client, units, args.out, model_request(),
                      schema_path=args.schema, resume=not args.no_resume)
    print(json.dumps({k: v for k, v in summary.items()
                      if k not in ("failed", "latency_s")}, indent=2))
    return 1 if summary["failed_count"] and not summary["extracted"] else 0


if __name__ == "__main__":       # pragma: no cover
    sys.exit(main())
