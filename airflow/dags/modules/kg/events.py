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
  * Every textual value the model does return — ``date_text``, ``locations``,
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
PROMPT_VERSION = "8"

# Bump when THIS MODULE's contract changes — the output shape, the
# validator's rules, the sentence splitter, the event_id recipe, how media
# are attached. Same effect: every unit is re-queued.
#   3.2.0  third pass: a sentence that only reports a post's reception
#          ("The post received 2,000 reactions in three days") joins that
#          post's event, and an event made only of such sentences is
#          folded into it — 127 of 1,370 sentences had been left out of
#          every event; "face" is no longer a stem of "Facebook"; a poster
#          named twice ("IDF", "IDF YouTube channel") is one actor, and
#          "the person who uploaded the clip" is nobody's name; a sentence
#          opening "On <date>," dates its event when the model gave no
#          date words; the verb "may" is not May, "Spider-Woman 2099" not
#          a year. With prompt 8 (a sentence carrying its own time words
#          is almost always a happening). Then, from reading all 127 again:
#          the splitter no longer cuts inside a quotation, after a title
#          ("RAdm.", "a.k.a.", "Bros.") or before "singer K. Michelle";
#          an actor or place named only AFTER its event joins it from the
#          very next sentence or is dropped; names are grounded verbatim
#          from the event's own and earlier sentences before any near
#          match; "liked 124 times", "1,300 smiles" and a post's own quoted
#          words are its reception; "That month, ..." dates its event;
#          role-only names, crowds, "... channel" and quote marks are
#          cleaned from actors, all-lowercase common nouns from places.
#          From a 60-entry holdout never looked at before: "until <date>,
#          when X posted" dates X's post; "over the following month" is a
#          stretch of time, not the next month; "May, 1st, 2019"; "George
#          R.R. Martin"; Vine loops and a duration ("In two months") in a
#          reception line; "iFunnyer" -> iFunny; an @handle is an account
#          (an actor) unless only quoted; "Viner"/"Instagrammer" titles.
#   3.1.0  second pass over the same 127 sections:
#          * two bounds of 3.0.0's were too broad: a bare "after <date>,
#            when X posted" dates X's post, and "did not ... until <date>"
#            is a start — only an attributive date ("after Cody Ko's April
#            30th video") and an un-negated "until" are bounds;
#          * the same date words read twice from one sentence are one time,
#            not a relative step from each other;
#          * "that date"; values grounded in the event's OWN sentences
#            first; a place or actor the page introduces with "a"/"an" and
#            a common noun names nobody; a source "according to" is not an
#            actor; the poster's own page/channel/account is an actor, not
#            a place.
#   3.0.0  from reviewing 127 sections by hand (2026-09-24):
#          * the page is ONE timeline: a missing year, month or relative
#            anchor comes from the events dated before this one — Origin's
#            too, for Spread — not from the nearest date string. Spread
#            whose year sat only in Origin had lost every date (9 in one
#            section), and a referenced "April 2011" had re-dated an
#            April 2013 event;
#          * "as of", "prior to", "by", "after X's ... video" are bounds,
#            not the event's date, and never anchors;
#          * joined dates are dated at the precision that CONTAINS them
#            ("Between 2009 and 2013" is undated, not 2009);
#          * relative shifts in months and years, "meanwhile", bare
#            ordinals ("on the 21st"), and "<Month> of <Year>";
#          * `location` becomes `locations`, a LIST: platform and venue;
#          * actors lose role words and unnamed ones; a venue listed as an
#            actor moves to locations.
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
EXTRACTION_VERSION = "3.2.0"

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
    "in the world at a time: a post, an upload, a video, a release, an "
    "airing, a ban, a lawsuit, a death, a trend spreading to a "
    "platform, a hashtag trending, coverage by a news site."
    "\n"
    "\n"
    "WHAT IS AN EVENT. Work through the sentences in order and return "
    "one event for each happening. A spread development narrated as "
    "happening IS an event: \"parodies became much more common\", \"the "
    "hashtag began trending\", \"the trend spread to TikTok\". A dated "
    "sentence worded as an illustration (\"For instance, on January 3rd, "
    "2025, TikToker @x posted ...\") is an event too. So is a sentence "
    "that carries its own time words, even in the middle of another "
    "story, even with a view count after it: \"That October, a forum "
    "thread claimed ...\", \"The next day, a TikToker stitched the "
    "video ...\", \"A fan wiki was also started that day\", \"Discord "
    "added the feature in March of 2019\". "
    "Read every sentence with a date or a time word twice before "
    "leaving it out."
    "\n"
    "Ask of every sentence: did someone DO something here, at some "
    "time? If it only describes, counts or explains, it is not an "
    "event. These are NOT events, and must not be returned:"
    "\n"
    "- what happens INSIDE a video, image, comic, episode, song or "
    "story (\"In the video, the man says ...\", \"The comic depicts ...\"); "
    "the posting, release or airing of it is the event;"
    "\n"
    "- events inside fiction, lore or a creepypasta's own mythology "
    "(\"According to the mythology, The Rake was documented in 1691\");"
    "\n"
    "- descriptions of a work, where footage came from, or who someone "
    "is (\"The screen capture is taken from a video by ...\", \"the women "
    "in the photo are ...\");"
    "\n"
    "- counts of views, likes, followers, results or posts, with or "
    "without a date (\"the video has 2.9 million views as of June 2017\", "
    "\"It has nearly 1,700 followers as of December 2nd\", \"there are "
    "over 2 million images tagged #murica\");"
    "\n"
    "- how something is commonly used (\"'Murica is often used on "
    "Twitter as commentary\");"
    "\n"
    "- commentary, and lists of examples with no date and nobody "
    "acting."
    "\n"
    "\n"
    "ONE EVENT, ALL ITS SENTENCES. A sentence that only reports how a "
    "post was received (\"The post received over 2,000 likes in three "
    "days\"), or continues its quote or description, belongs to the SAME "
    "event: put its number in that event's sentences, never make it an "
    "event of its own. When one sentence names a happening and the next "
    "one dates it, return ONE event covering both. But never merge two "
    "sentences that each state their own date, or their own poster, "
    "into one event — and never merge a rumor or claim with a dated "
    "fact it is about."
    "\n"
    "\n"
    "For each event, return:"
    "\n"
    "- sentences: the numbers of the sentences that narrate it, "
    "consecutive."
    "\n"
    "- date_text: the words in those sentences that say WHEN this "
    "happening took place, copied exactly — \"May 4th, 2013\", \"early "
    "2013\", \"sometime in 2007\", \"throughout 2023\", \"In 2008\", \"2002's\", "
    "\"that August\", \"that same day\", \"the following month\" — or null if "
    "the sentences say nothing about when. Quote vague words too; do "
    "not skip them. Copy the words only: do NOT work out a date, a year "
    "or a day yourself. If a sentence gives a day but no year, write "
    "just the day as it stands (\"June 16th\"). The words must date THIS "
    "happening: in \"After Cody Ko's April 30th video, TikToker @x "
    "clipped it\", April 30th dates the video, not the clip, so "
    "date_text is null."
    "\n"
    "- locations: every place where it happened, most general first, "
    "each copied exactly as the section writes it: the platform AND the "
    "venue on it where it was posted — a group, subreddit, board, "
    "server or forum, somewhere OTHERS post too — or a geographic "
    "place. The poster's own page, channel or account is not a "
    "location: it is an actor. \"posted ... in the Star Wars Sithposting "
    "shitposting group\" on Facebook -> [\"Facebook\", \"Star Wars "
    "Sithposting shitposting group\"]; \"submitted to /r/OutOfTheLoop\" -> "
    "[\"/r/OutOfTheLoop\"]; \"at the Fox Theater in Atlanta, Georgia\" -> "
    "[\"Fox Theater in Atlanta, Georgia\"]. Only places the section NAMES "
    "for this happening — not where an earlier post was. A TV show, "
    "film, game or other work is not a place. [] if none."
    "\n"
    "- location_type: \"platform\", \"geo\", \"both\", or \"unknown\" when "
    "locations is empty."
    "\n"
    "- actors: who PERFORMED the happening, by name, copied as written: "
    "\"@blockboy_192\", \"u/Shibetoshi\", \"Atsuko Sato\", \"Duolingo\". Write "
    "the name without its role: \"Caiden Butler\", not \"Facebook user "
    "Caiden Butler\". A page, channel or account that posted something "
    "is an actor: \"the Facebook page King K Rool posted a video\" -> "
    "actors [\"King K Rool\"], locations [\"Facebook\"]. NOT actors: the "
    "work being released (a film, book, game, show); fictional "
    "characters; a source being cited (\"According to Pixiv "
    "Encyclopedia\"); the subject of a photo; someone who was replied to "
    "or reacted to; a person whose earlier post others built on (\"After "
    "@x's usage, others ...\" -> the others acted); a venue (a group or "
    "subreddit is a location); anyone unnamed (\"an anonymous user\", \"a "
    "Reuters photojournalist\", \"users\", \"they\"). [] if none."
    "\n"
    "- certainty: whether the SECTION is sure THIS HAPPENING happened."
    "\n"
    "  \"confirmed\" — stated plainly. The normal case. \"earliest known\", "
    "\"as early as\", \"sometime in\", \"one of the first\" describe the "
    "evidence or the date, not doubt: still confirmed."
    "\n"
    "  \"unconfirmed\" — the section hedges the happening itself: "
    "\"reportedly posted\", \"is said to have originated\", \"allegedly "
    "filmed\", \"it appears to have been first posted\", \"is believed to "
    "have begun\"."
    "\n"
    "  \"disputed\" — the section says people disagree about whether it "
    "happened."
    "\n"
    "  \"debunked\" — the section says it turned out to be false, staged "
    "or a hoax."
    "\n"
    "  Judge the verb of the event, not what surrounds it. A hedge on "
    "whether it was the FIRST or the ORIGINAL (\"may have been the "
    "first\", \"it is unclear if this is the original\"), on intent "
    "(\"possibly accidentally\"), on authorship, or on a claim made "
    "INSIDE the post (\"@x posted a video claiming Y\", \"a post "
    "purportedly debunking the claim\") does not make the posting "
    "unconfirmed. Never upgrade a hedge to a fact."
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
    "day\", \"the following day\", \"three days later\", \"that month\", "
    "\"later that year\") is a perfectly good date_text — keep it exactly "
    "as written; it is resolved afterwards against the events before "
    "it."
    "\n"
    "\n"
    "Respond ONLY with JSON: {\"events\": [...]}. Three example events:"
    "\n"
    "{\"sentences\": [1, 2], \"date_text\": \"February 23rd, 2010\", "
    "\"locations\": [\"Tumblr\"], \"location_type\": \"platform\", \"actors\": "
    "[\"Atsuko Sato\"], \"certainty\": \"confirmed\"}"
    "\n"
    "{\"sentences\": [4], \"date_text\": \"that day\", \"locations\": "
    "[\"Facebook\", \"Star Wars Sithposting shitposting group\"], "
    "\"location_type\": \"platform\", \"actors\": [\"elliott.boydstringer\"], "
    "\"certainty\": \"confirmed\"}"
    "\n"
    "{\"sentences\": [7], \"date_text\": \"sometime in 2007\", \"locations\": "
    "[], \"location_type\": \"unknown\", \"actors\": [], \"certainty\": "
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
    # titles and ranks, "a.k.a." and "Bros." (2026-09-24 review: "RAdm.
    # Daniel Hagari", "Wojak a.k.a. That Feel Guy", "Super Smash Bros.
    # Ultimate" were each cut in two)
    "adm", "radm", "vadm", "gen", "lt", "col", "capt", "cpt", "sgt", "maj",
    "cmdr", "gov", "sen", "rep", "rev", "hon", "pres", "a.k.a", "aka",
    "bros", "feat", "ft", "mt",
})
# A lowercase word before a single capital letter makes it a name's
# initial ("singer K. Michelle") — unless the word is one that puts a
# PLATFORM there ("went viral on X. For example"), or the letter is X.
_BEFORE_A_PLACE = frozenset({"on", "to", "in", "at", "via", "from", "of", "and",
                             "or", "with", "by", "for", "as", "into", "onto", "than"})
# A quotation longer than this is not trusted to be one: an unclosed quote
# (a KYM typo) must never swallow the rest of a paragraph.
_MAX_QUOTE = 800
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
    quotes = _quotations(paragraph)
    for m in _SENTENCE_END.finditer(paragraph):
        # Inside a quotation that goes on after this point, a period ends
        # a sentence of the QUOTE, not of the page: "posted, "If I have to
        # see it one more time lol. I honestly think ..."" (2026-09-24).
        if any(a < m.start() and m.end() <= b for a, b in quotes):
            continue
        before = paragraph[start:m.start()].split()
        token = before[-1].lower().rstrip(".") if before else ""
        if token in _ABBREVIATIONS:
            continue
        if (len(token) == 1 and token.isalpha()
                or re.fullmatch(r"(?:[a-z]\.)+[a-z]", token)) and _is_initial(
                paragraph, before, m.end()):
            continue            # "J. K. Rowling", "George R.R. Martin"
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
    if len(before) > 1 and before[-2][:1].isupper():
        return True
    letter = before[-1].rstrip(".")
    nxt = paragraph[after:].lstrip()[:1]
    return (len(before) > 1 and letter.isupper() and letter not in "XI"
            and before[-2].islower() and before[-2] not in _BEFORE_A_PLACE
            and nxt.isupper())


def _quotations(paragraph: str) -> list[tuple[int, int]]:
    """(open, close) index pairs of the double-quoted passages.

    Straight quotes pair up in order only when the paragraph has an even
    number of them; an odd count means one is unclosed, and then none is
    trusted. Curly quotes pair by direction. Longer than _MAX_QUOTE is
    not a quotation either.
    """
    out: list[tuple[int, int]] = []
    straight = [i for i, c in enumerate(paragraph) if c == '"']
    if len(straight) % 2 == 0:
        out += list(zip(straight[::2], straight[1::2]))
    opened = None
    for i, c in enumerate(paragraph):
        if c == "“":
            opened = i
        elif c == "”" and opened is not None:
            out.append((opened, i))
            opened = None
    return [(a, b) for a, b in out if b - a <= _MAX_QUOTE]


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


def unit_id_for_entry(entry_id: str, section: str) -> str:
    """The same id, from an entry id already in hand."""
    return f"{entry_id}:{section}"


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
    # so a re-parse that changes either re-queues this unit. Spread is also
    # DATED against Origin (resolve_dates' ``prior``), so for Spread it
    # covers Origin's text too: an edited Origin re-queues both.
    upstream = [p for s in entry.get("sections") or [] if s.get("kind") == "origin"
                for p in (s.get("text") or [])] if section == "spread" else []
    fingerprint = json.dumps([paragraphs, links, images, embeds, citations, upstream],
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
_DAY_DATE = re.compile(rf"\b(?P<month>{_MONTH_RE})\.?,?\s+(?P<day>\d{{1,2}})(?:st|nd|rd|th)?"
                       rf"(?:\s*,)?(?:\s*(?P<year>(?:19|20)\d{{2}}))?\b", re.I)
# "the 23rd of February, 2010"
_DAY_DATE_OF = re.compile(rf"\b(?P<day>\d{{1,2}})(?:st|nd|rd|th)?\s+of\s+(?P<month>{_MONTH_RE})\.?"
                          rf"(?:\s*,)?(?:\s*(?P<year>(?:19|20)\d{{2}}))?\b", re.I)
# "May 2013", "May, 2013" and — since 3.0.0 — "May of 2013": "mid-May of
# 2018" and "January of 2014" were read as bare YEARS, a month lost.
_MONTH_DATE = re.compile(rf"\b(?P<month>{_MONTH_RE})\.?\s*,?\s*(?:of\s+)?"
                         rf"(?P<year>(?:19|20)\d{{2}})\b", re.I)
_MONTH_ONLY = re.compile(rf"\b(?P<month>{_MONTH_RE})\b\.?", re.I)
# "it may have been posted in 2013" is no month: a bare "may" followed by a
# word that is not a preposition or conjunction is the verb (dont-judge-
# challenge, 2026-09-24 review). Checked wherever a BARE month is read.
_MODAL_MAY = re.compile(
    r"may\s+(?!(?:of|and|or|to|through|thru|until|till|when|after|before"
    r"|in|on|at|as|with|while|through)\b)[a-z]", re.I)


def _is_modal(text: str, at: int) -> bool:
    return bool(_MODAL_MAY.match(text, at))
# A DECADE is not a year. "the 2010s", "the mid-2000s" and "the late
# 1990s" were all read as the decade's first year, which puts an
# mk:eventStart of 2010-01-01 on a page that said "the first half of the
# 2010s". date_precision has no "decade", and inventing one year out of
# ten is exactly what this module refuses to do, so a decade names no
# date. "2016's election" still does: the "s" there follows an apostrophe.
_YEAR = re.compile(r"(?<!\d)(?P<year>19\d{2}|20[0-3]\d)(?!\d)(?!s\b)")
# A day with no month: "on the 21st", "the 24th that month". Its month
# comes from the timeline, like a missing year does.
_ORDINAL = re.compile(r"(?<![\w-])(?P<day>\d{1,2})(?:st|nd|rd|th)\b", re.I)
# Words that join two dates into a RANGE or an either/or.
_RANGE_JOIN = re.compile(r"\b(?:and|or|to|through|thru|until|till)\b|[–—-]|&", re.I)

# A date the event is measured AGAINST, not the date it happened: "as of
# December 2nd" (a statistic), "prior to August 2019", "by March 26th"
# (bounds), and "after Cody Ko's April 30th video" (the date of something
# ELSE). 2.x dated all of these to the named day — and then used the
# "as of" ones as anchors, so a "the following day" after one landed a
# week late.
_BOUND_TIGHT = re.compile(
    r"\b(?:as\s+of|prior\s+to|before|until|till|up\s+(?:to|until)|by|"
    r"no\s+later\s+than|ahead\s+of)\s+(?:the\s+)?(?:(?:early|mid|late)[-\s]+)?$",
    re.I)
_BOUND_LOOSE = re.compile(
    r"\b(?:after|following)\s+(?:[\w'’.@&-]+\s+){1,3}$", re.I)
# "did not start ... until August 2016" STARTED in August 2016: a negated
# "until" is a start, not an end.
_NEGATED = re.compile(r"\b(?:not|never|n[’']t|no\s+one|nobody)\b", re.I)

# Relative expressions resolved ARITHMETICALLY against the nearest earlier
# dated event. Each names a unit and a signed amount; the result is never
# more precise than the anchor or the unit, so "the following month" after
# "May 4th, 2013" is June 2013, not a June day nobody wrote. "shortly
# after", "the following week" and "in the following days" name no unit
# the graph has, and stay undated.
_WORD_NUMBERS = {"a": 1, "an": 1, "one": 1, "two": 2, "three": 3, "four": 4,
                 "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
                 "ten": 10, "eleven": 11, "twelve": 12}
_REL_RULES: tuple[tuple[re.Pattern, str, int], ...] = tuple(
    (re.compile(p, re.I), unit, n) for p, unit, n in (
        (r"\b(?:(?:that|the)\s+(?:very\s+)?same\s+(?:day|evening|morning|afternoon|night)"
         r"|(?:later|earlier)\s+(?:on\s+)?(?:that|the\s+same)\s+day"
         r"|(?:on\s+)?that\s+(?:very\s+)?day"
         r"|(?:from\s+|on\s+)?(?:that|the\s+same|that\s+same)\s+date"
         r"|that\s+(?:evening|morning|afternoon|night)"
         r"|(?:a\s+few\s+|several\s+|some\s+)?(?:minutes|hours)\s+later"
         r"|an\s+hour\s+later|a\s+minute\s+later|moments\s+later"
         r"|meanwhile|at\s+the\s+same\s+time|simultaneously)\b", "day", 0),
        (r"\b(?:the\s+(?:next|following)\s+day|a\s+day\s+later|the\s+day\s+after)\b",
         "day", 1),
        (r"\b(?:the\s+(?:previous|preceding)\s+day|a\s+day\s+(?:earlier|before)"
         r"|the\s+day\s+before)\b", "day", -1),
        (r"\b(?:(?:later|earlier)\s+)?(?:that|the)\s+same\s+month\b"
         r"|\b(?:later|earlier)\s+that\s+month\b|\bthat\s+month\b", "month", 0),
        (r"\b(?:the\s+(?:next|following)\s+month|the\s+month\s+after)\b", "month", 1),
        (r"\b(?:the\s+(?:previous|preceding)\s+month|the\s+month\s+before)\b",
         "month", -1),
        (r"\b(?:(?:later|earlier)\s+)?(?:that|the)\s+same\s+year\b"
         r"|\b(?:later|earlier)\s+that\s+year\b|\bthat\s+year\b", "year", 0),
        (r"\b(?:the\s+(?:next|following)\s+year|the\s+year\s+after)\b", "year", 1),
        (r"\b(?:the\s+(?:previous|preceding)\s+year|the\s+year\s+before)\b", "year", -1),
    ))
_REL_COUNT = re.compile(
    r"\b(?P<n>\d{1,2}|a|an|one|two|three|four|five|six|seven|eight|nine|ten"
    r"|eleven|twelve)\s+(?P<unit>day|week|month|year)s?\s+"
    r"(?P<dir>later|after(?:wards?)?|earlier|before|prior)\b", re.I)
_PRECISION_RANK = {"day": 3, "month": 2, "year": 1, "none": 0}


def _month_number(name: str) -> int:
    name = name.lower().rstrip(".")
    for i, month in enumerate(_MONTHS, 1):
        if month.startswith(name[:3]):
            return i
    return 0


def _points(text: str) -> list[tuple[int, int, int | None, int | None, int | None]]:
    """(start, end, year, month, day) for every date point in ``text``, in
    order, most specific pattern first; years and months a point leaves out
    are filled from the other points of the SAME phrase ("April 13th and
    14th, 2018" -> the 14th is April 2018 too)."""
    taken: list[tuple[int, int]] = []
    found: list[list] = []

    def free(a: int, b: int) -> bool:
        return not any(a < y and x < b for x, y in taken)

    def add(a, b, y, m, d):
        taken.append((a, b))
        found.append([a, b, y, m, d])

    for pattern in (_DAY_DATE, _DAY_DATE_OF):
        for m in pattern.finditer(text):
            month = _month_number(m.group("month"))
            if month and free(*m.span()):
                add(*m.span(), int(m.group("year")) if m.group("year") else None,
                    month, int(m.group("day")))
    for m in _MONTH_DATE.finditer(text):
        if _month_number(m.group("month")) and free(*m.span()):
            add(*m.span(), int(m.group("year")), _month_number(m.group("month")), None)
    for m in _MONTH_ONLY.finditer(text):
        if (_month_number(m.group("month")) and free(*m.span())
                and not _is_modal(text, m.start())):
            add(*m.span(), None, _month_number(m.group("month")), None)
    for m in _YEAR.finditer(text):
        if free(*m.span()):
            add(*m.span(), int(m.group("year")), None, None)
    for m in _ORDINAL.finditer(text):
        if free(*m.span()):
            add(*m.span(), None, None, int(m.group("day")))
    found.sort()
    # Fill within the phrase: a year from the nearest point that states one
    # (later first — "September and October 2020"), a month from the
    # point before (a bare "14th" after "April 13th").
    for i, p in enumerate(found):
        if p[2] is None and (p[3] is not None or p[4] is not None):
            later = [q[2] for q in found[i + 1:] if q[2] is not None]
            earlier = [q[2] for q in found[:i] if q[2] is not None]
            p[2] = (later or earlier or [None])[0]
        if p[3] is None and p[4] is not None:
            earlier = [q[3] for q in found[:i] if q[3] is not None]
            p[3] = earlier[-1] if earlier else None
    # A bare year that only lends itself to another point ("April 13th and
    # 14th, 2018") is that point's year, not a date of its own.
    dated = {q[2] for q in found if q[3] is not None}
    found = [q for q in found if q[3] is not None or q[4] is not None
             or q[2] not in dated]
    return [tuple(p) for p in found]


def _joined(text: str, points) -> bool:
    """Are the points of a phrase one range or either/or, rather than two
    dates that merely share a sentence?"""
    for (_a, b, *_), (c, _d, *_) in zip(points, points[1:]):
        if not _RANGE_JOIN.search(text[b:c]):
            return False
    return True


def parse_date_phrase(phrase: str, year_hint: int | None = None,
                      month_hint: int | None = None) -> tuple[str | None, str]:
    """The date WORDS -> (normalized date, precision). Pure text parsing.

    ``year_hint`` / ``month_hint`` come from the page's timeline (see
    _timeline_hints) and fill only what the phrase itself leaves out.

    Several points joined by and/or/to ("Between May 28th and June 6th,
    2025", "April 13th and 14th", "June 7th or June 8th") are dated at the
    precision that CONTAINS them all — a year, a month — or not at all when
    they span years. 2.x dated them to their first point, which claimed a
    day the page never gave.
    """
    text = _norm_ws(strip_footnotes(phrase))
    if not text:
        return None, "none"
    points = _points(text)
    if not points:
        return None, "none"
    if len(points) > 1 and not _joined(text, points):
        points = points[:1]
    resolved = []
    for _a, _b, y, m, d in points:
        if d is not None and m is None:
            m = month_hint
        if y is None and (m is not None or d is not None):
            y = year_hint
        if y is None:
            return None, "none"
        resolved.append((y, m, d))
    years = {y for y, _m, _d in resolved}
    if len(years) != 1:
        return None, "none"
    year = years.pop()
    months = {m for _y, m, _d in resolved}
    if len(months) != 1 or None in months:
        return f"{year:04d}", "year"
    month = months.pop()
    days = {d for _y, _m, d in resolved}
    if len(days) != 1 or None in days:
        return f"{year:04d}-{month:02d}", "month"
    try:
        return datetime(year, month, days.pop()).strftime("%Y-%m-%d"), "day"
    except ValueError:
        return None, "none"          # February 31st


def date_is_bound(phrase: str, source_text: str) -> bool:
    """Is this date what the event is measured AGAINST ("as of", "prior
    to", "after X's ... video") rather than when it happened? Checked in the
    words themselves and in the event's text just before them."""
    text = _norm_ws(strip_footnotes(phrase))
    points = _points(text)
    head = text[:points[0][0]] if points else text
    source = _norm_ws(strip_footnotes(source_text))
    at = _comparable(source).find(_comparable(text)) if text else -1
    before = source[:at] if at > 0 else ""
    after = source[at + len(text):] if at >= 0 else ""
    for context in (head, f"{before}{head}"):
        tight = _BOUND_TIGHT.search(context)
        # "did not ... until August 2016" STARTED then; and "remained
        # unknown until January 5th, 2017, when @x posted" is WHEN @x posted
        # (how-to-break-your-thumb-ligament, 2026-09-25 holdout).
        if tight and re.match(r"(?:until|till|up\s+until)", tight.group(0), re.I) and (
                _NEGATED.search(context[max(0, tight.start() - 80):tight.start()])
                or re.match(r"\s*,?\s*when\b", after, re.I)):
            continue
        if tight or _BOUND_LOOSE.search(context):
            return True
    return False


def relative_shift(phrase: str) -> tuple[str, int] | None:
    """(unit, signed amount) a relative phrase moves from the anchor, or
    None. Units: day / month / year; a week is seven days."""
    text = _norm_ws(strip_footnotes(phrase))
    m = _REL_COUNT.search(text)
    if m:
        raw = m.group("n").lower()
        n = int(raw) if raw.isdigit() else _WORD_NUMBERS.get(raw)
        if n is not None:
            sign = -1 if m.group("dir").lower() in ("earlier", "before", "prior") else 1
            unit = m.group("unit").lower()
            if unit == "week":
                return "day", sign * 7 * n
            return unit, sign * n
    for pattern, unit, n in _REL_RULES:
        if pattern.search(text):
            return unit, n
    return None


# "OkCron continued posting updates over the following month" (no-poop-
# july, 2026-09-25 holdout): a stretch of time after the anchor, not the
# calendar month after it. Read in the words and just before them.
_DURATION_LEAD = re.compile(r"\b(?:over|throughout|during|within|for)\s+$", re.I)


def _is_duration(phrase: str, source_text: str) -> bool:
    text = _norm_ws(strip_footnotes(phrase))
    m = re.search(r"\bthe\s+(?:next|following|coming)\b", text, re.I)
    if m and _DURATION_LEAD.search(text[:m.start()]):
        return True
    source = _norm_ws(strip_footnotes(source_text))
    at = source.lower().find(text.lower()) if text else -1
    return at > 0 and bool(_DURATION_LEAD.search(source[max(0, at - 15):at]))


def relative_offset(phrase: str) -> int | None:
    """Days a phrase moves from the anchor, for phrases measured in days
    (kept for callers that only ever dealt in days)."""
    shift = relative_shift(phrase)
    return shift[1] if shift and shift[0] == "day" else None


def shift_date(date: str, precision: str, unit: str, n: int
               ) -> tuple[str, str] | None:
    """Move an anchor date; the result is never finer than either."""
    if not date or precision == "none":
        return None
    if unit == "day":
        if n == 0:
            return date, precision
        if precision != "day":
            return None      # "the next day" after "May 2013" names no day
        return ((datetime.strptime(date, "%Y-%m-%d") + timedelta(days=n))
                .strftime("%Y-%m-%d"), "day")
    year = int(date[:4])
    if unit == "month":
        if precision not in ("day", "month"):
            return None      # "the following month" after "2013" names none
        index = year * 12 + int(date[5:7]) - 1 + n
        return f"{index // 12:04d}-{index % 12 + 1:02d}", "month"
    if unit == "year":
        return f"{year + n:04d}", "year"
    return None


def _date_parts(date: str | None, precision: str) -> tuple[int | None, int | None]:
    if not date or precision == "none":
        return None, None
    return int(date[:4]), (int(date[5:7]) if precision in ("day", "month") else None)


def _timeline_hints(unit: dict, rows: Sequence[dict], index: int,
                    prior: Sequence[dict] = ()) -> tuple[int | None, int | None]:
    """(year, month) a date phrase may borrow — from the page's TIMELINE.

    The timeline is what the page has dated so far, in reading order: the
    events of the Origin section (``prior``, when this is Spread), then this
    section's own events up to this one, with the month-bearing dates of
    sentences that narrate no event in between. Three things this is careful
    about, each a bug the 2026-09-24 review found:

    * **Across sections.** Spread continues Origin's story; "On November
      5th" there takes Origin's 2017. 2.x resolved each section alone, and a
      Spread whose year was stated only in Origin lost every date it had —
      one section all nine.
    * **Events, not every date on the page.** "the tweet from April 2011"
      inside an April 2013 event does not move the story to 2011; only an
      event's own date does. 2.x dated the next "On April 15th" to 2011.
    * **The event's own words first.** "Back in 2018, on July 3rd ..." takes
      2018 from its own sentence before any timeline.
    """
    row = rows[index]
    covered = [s for s in unit["sentences"] if s["id"] in row["sentences"]]
    phrase = row.get("date_text") or ""
    _c, span_text = _span(unit, row["sentences"])
    at = _comparable(span_text).find(_comparable(phrase)) if phrase else -1
    if at > 0:
        own = _points(_comparable(span_text)[:at])
        for _a, _b, y, m, _d in reversed(own):
            if y and m:
                return y, m
        for _a, _b, y, _m, _d in reversed(own):
            if y:
                return y, None

    first = row["sentences"][0]
    markers: list[tuple[tuple[int, int], int, int | None]] = []
    for n, event in enumerate(prior):
        y, m = _date_parts(event.get("date"), event.get("date_precision", "none"))
        if y:
            markers.append(((-1, n), y, m))
    spans = set()
    for other in rows:
        spans.update(range(other["sentences"][0], other["sentences"][-1] + 1))
    for s in unit["sentences"]:
        if s["id"] < first and s["id"] not in spans:
            for _a, _b, y, m, _d in _points(_comparable(_sentence_text(unit, s))):
                if y and m:
                    markers.append(((s["id"], -1), y, m))
    for n, other in enumerate(rows[:index]):
        y, m = _date_parts(other.get("date"), other.get("date_precision", "none"))
        if y:
            markers.append(((other["sentences"][0], n), y, m))
    markers.sort()
    if markers:
        year = markers[-1][1]
        month = next((m for _p, y, m in reversed(markers) if m and y == year), None)
        return year, month
    whole = {y for _a, _b, y, m, _d in
             _points(_comparable("\n\n".join(unit["paragraphs"]))) if y and m}
    return (whole.pop() if len(whole) == 1 else None), None


def _anchor_before(rows: Sequence[dict], index: int, prior: Sequence[dict]):
    """The nearest DATED event before ``rows[index]`` — this section's, or
    Origin's last when there is none here yet."""
    for other in reversed(rows[:index]):
        if other.get("date"):
            return other
    for event in reversed(prior):
        if event.get("date"):
            return event
    return None


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
    # ...and most of the longer word: "face" is not a stem of "facebook"
    # (muvvafukka's location came back as "face", 2026-09-24 review).
    return (len(short) >= 4 and long.startswith(short)
            and len(short) >= 0.6 * len(long))


def ground_value(value: Any, hay: str, align: bool = True) -> str | None:
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
    if not align:
        return None

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
    m = next((m for m in _MONTH_ONLY.finditer(text)
              if not _is_modal(text, m.start())), None)
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
            if not free(a, b) or _is_modal(clean, a):
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


def _relative_span(text: str, days_only: bool = False) -> str | None:
    """The page's own words for a relative date ("that same day"), or None.
    ``days_only`` restricts it to shifts measured in days — the only kind
    read from an event's sentences when the model quoted no date words."""
    clean, index = _without_markers(text)
    patterns = [_REL_COUNT] + [p for p, unit, _n in _REL_RULES
                               if not days_only or unit == "day"]
    for pattern in patterns:
        m = pattern.search(clean)
        if m and (not days_only or pattern is not _REL_COUNT
                  or m.group("unit").lower() in ("day", "week")):
            return text[index[m.start()]:index[m.end() - 1] + 1]
    return None


def _narrow(phrase: str) -> str:
    """Just the date, when the model quoted half a sentence around it:
    "November 18th, iFunny user Magmapanda77 posted a video" is stored as
    "November 18th". A few words around the date stay ("Later on",
    "as early as", "Between"); a clause does not."""
    points = _points(phrase)
    if not points:
        return phrase
    start, end = points[0][0], points[-1][1]
    outside = phrase[:start].split() + phrase[end:].split()
    if len(outside) <= 3:
        return phrase
    lead = re.search(r"(?:\b(?:early|mid|late|around|about|between|from|circa)"
                     r"[-\s]+)+$", phrase[:start], re.I)
    return phrase[lead.start() if lead else start:end].strip(" ,")


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
        return _narrow(phrase)
    wanted = _date_components(phrase)
    if wanted != (None, None, None):
        for start, end, parts in _date_spans(source_text):
            if _date_agrees(parts, wanted):
                return source_text[start:end]
        return None
    if relative_shift(phrase) is not None:
        return _relative_span(source_text)
    return ground_value(phrase, source_text)


# A role in front of a name is the page saying who someone IS, not part of
# the name: "Facebook user Caiden Butler", "the YouTube channel for HUGEL",
# "film critic Roger Ebert". Kept, one person is a different mk:eventActor
# literal on every page that introduces them differently.
_ROLES = ("user|account|page|channel|profile|member|poster|handle|redditor|"
          "tiktoker|youtuber|streamer|vtuber|rapper|singer|actor|actress|"
          "artist|illustrator|animator|cosplayer|journalist|writer|blogger|"
          "critic|comedian|musician|producer|director|host|creator|editor|"
          "reporter|influencer|podcaster|viner|instagrammer|ifunnyer")
_ACTOR_ROLE = re.compile(
    rf"^(?:the\s+)?(?:[\w.!/'’&-]+\s+){{0,3}}?(?:{_ROLES})s?"
    rf"(?:\s+and\s+(?:[\w.-]+\s+)?(?:{_ROLES}))?\s+"
    rf"(?:for\s+|called\s+|named\s+)?(?=\S)", re.I)
# An actor or a place introduced with an indefinite ("a 4chan user", "an
# anonymous Soyjak.party user", "a personal blog") names nobody in
# particular — nothing a query could ever join on.
_UNNAMED_LEAD = re.compile(
    r"^(?:a|an|another|some|several|many|various|numerous|multiple|other|"
    r"others|anonymous|unknown|unidentified)\b", re.I)
# ...and so does a DESCRIPTION of someone: "the person who uploaded the
# clip" is a role the page gives, not a name (chris-turns-blue, 2026-09-24).
_DESCRIBED = re.compile(r"^(?:the\s+)?[a-z][\w-]*(?:\s+[a-z][\w-]*){0,2}"
                        r"\s+(?:who|that|whose)\b")
# The place a post was made IN is a location, not a participant: "the Star
# Wars Sithposting shitposting group" is where elliott.boydstringer posted,
# not someone who posted (Gabi's review, 2026-09-24).
_VENUE_NOUN = re.compile(
    r"\b(?:group|subreddit|sub|server|forum|board|imageboard|thread|"
    r"chatroom|group\s?chat)s?$", re.I)
_UNNAMED_PLACES = frozenset({
    "online", "internet", "the internet", "the web", "social media",
    "the site", "the website", "the platform", "the app", "the group",
    "the subreddit", "the page", "the forum", "the board", "the thread",
    "the channel", "the server", "various platforms", "other platforms",
    "several platforms", "multiple platforms", "other sites", "various sites",
    "other social media", "other social media platforms", "the same platform",
    # bare kinds of place: a page with its name dropped
    "website", "site", "blog", "forum", "platform", "app", "page", "group",
    "subreddit", "server", "board", "channel", "thread", "profile", "account",
})
# Words that introduce a page, channel or account BY NAME: what follows is
# the poster ("the Facebook page King K Rool posted"), not a place.
_ACCOUNT_NOUN = re.compile(r"\b(?:page|channel|account|profile|handle|user)$", re.I)
_LOWER_BRANDS = frozenset({"tumblr", "reddit", "twitter", "youtube", "tiktok",
                           "instagram", "facebook", "imgur", "ifunny", "twitch",
                           "discord", "snapchat", "pinterest", "vine", "myspace",
                           "9gag"})
_INDEFINITE = frozenset({"a", "an", "another", "some", "several", "many",
                         "various", "numerous", "multiple"})


# "the ApeThrowbacks channel", "the Couples Court YouTube channel": the
# name is what comes before (atlorgy, ms-jacksons, 2026-09-24 review).
# Lowercase only: "Ghost X Channel" is a channel's NAME.
_ACCOUNT_SUFFIX = re.compile(
    r"\s+(?:(?:YouTube|Twitch|Facebook|Instagram|TikTok|Twitter|Tumblr|Reddit"
    r"|Discord)\s+)?(?:channel|page|account|profile)$")


def _clean_actor(value: str) -> str:
    stripped = _ACTOR_ROLE.sub("", value, count=1).strip()
    stripped = _ACCOUNT_SUFFIX.sub("", stripped or value).strip()
    return stripped or value


def _unquote(value: str) -> str:
    """'"Uzuki's ganbarimasu"' -> Uzuki's ganbarimasu: the quote marks
    around a name are the page's punctuation, not the name."""
    while len(value) > 2 and value[0] in "\"“'‘" and value[-1] in "\"”'’":
        value = value[1:-1].strip()
    # ...and one mark the name was cut from: 'Instagrammer "fawaz_Alfahad'
    if sum(value.count(q) for q in "\"“”") == 1:
        value = value.strip("\"“” ")
    return value


# A role with no name after it ("Yahoo Answers user posed the question")
# and a crowd ("Tumblr users", "4chan's moderators", "Anonymous members")
# name nobody a query could join on (bedroom-eyes, 4chumblr, 2026-09-24).
# The role word in lowercase: "Chance the Rapper" is a name.
_ROLE_ONLY = re.compile(rf"^(?i:the\s+)?(?:[\w.!/'’&-]+\s+){{0,3}}(?:{_ROLES})$")
_CROWD = re.compile(
    r"^(?:[\w.!/'’&-]+\s+){0,3}(?:users|members|fans|people|posters|commenters"
    r"|viewers|moderators|mods|admins|redditors|tweeters|anons|netizens|followers"
    r"|subscribers|players|gamers|critics|outlets|accounts|pages|channels)$")


def _names_nobody(value: str) -> bool:
    key = _comparable(value)
    return (not key or key in _UNNAMED_ACTORS or key in _UNNAMED_PLACES
            or bool(_UNNAMED_LEAD.match(value)) or bool(_DESCRIBED.match(value))
            or bool(_ROLE_ONLY.match(value)) or bool(_CROWD.match(value)))


def _split_places(value: str) -> list[str]:
    """"Monorail and Funny Junk", "Facebook and Twitter" -> each place. Only
    short parts are split, so a venue whose own name has an "and" in it
    survives whole."""
    joined = re.split(r"\s+and\s+|\s*&\s*", value)
    if len(joined) == 1:
        return [value]       # "Longyearbyen, Norway" is one place, not two
    parts = [p.strip(" ,") for part in joined for p in part.split(",")]
    parts = [p for p in parts if p]
    if len(parts) > 1 and all(len(p.split()) <= 4 for p in parts):
        return parts
    return [value]


def _words_before(value: str, hay: str) -> list[str]:
    """The word just before each place ``value`` occurs in ``hay``."""
    comp, needle = _comparable(hay), _comparable(value)
    out, at = [], comp.find(needle)
    while at >= 0 and needle:
        words = comp[:at].split()
        out.append(words[-1] if words else "")
        at = comp.find(needle, at + 1)
    return out


def _introduced_indefinitely(value: str, hay: str) -> bool:
    """"an anime forum about the show", "a Reuters photojournalist": the
    page names no particular one. The model tends to drop the article, so
    it is looked for on the page, in front of every occurrence."""
    # Only a COMMON noun is unnamed this way: "an Imgur compilation" still
    # names Imgur, while "an anime forum" names no forum.
    head = (value.split() or [""])[-1]
    if not head[:1].islower():
        return False
    before = _words_before(value, hay)
    return bool(before) and all(w in _INDEFINITE for w in before)


def _grounded_list(values, hay: str, *, clean=None, own: str = "",
                   before: str = "") -> list[str]:
    """Each value grounded, first match wins:

    1. verbatim in the event's OWN sentences;
    2. verbatim in the section up to the event's end (``before``) — the
       page referring back: "in the group" is the group named two
       sentences up, "Butler made ..." is the Caiden Butler named above;
    3. by alignment in its own sentences — "Adam Warski" -> "Warski";
    4. verbatim anywhere in the section — _settle_borrowed then decides
       whether a value named only LATER belongs to this event at all.

    Aligning against the whole section once resolved a subreddit to a
    Tumblr blog's name two sentences away, so alignment is own-only; and
    aligning before looking back turned "Caiden Butler" into "Butler" and
    the group into "the group" (2026-09-24 review).
    """
    out, seen = [], set()
    for value in values or []:
        grounded = (ground_value(value, own, align=False)
                    or (ground_value(value, before, align=False) if before else None)
                    or ground_value(value, own)
                    or ground_value(value, hay, align=False)
                    if own else ground_value(value, hay))
        if not grounded:
            continue
        grounded = _unquote(grounded)
        if clean:
            grounded = _unquote(clean(grounded))
        key = _comparable(grounded)
        if (key and key not in seen and not _names_nobody(grounded)
                and not _introduced_indefinitely(grounded, hay)):
            seen.add(key)
            out.append(grounded)
    return out


# "On September 13th, user elie posted a video ..." — a sentence that OPENS
# with its date is dated by it, whatever else it goes on to say. The model
# returned no date words for four such events in the 2026-09-24 review
# sample; the date is read only from the event's FIRST sentence, only when
# it leads it, so a date deeper in ("a video from 2010", "after the game
# was announced in 2020") is never taken for the event's.
_LEAD_IN = re.compile(r"(?:(?:for\s+(?:example|instance)|then|later|also|finally"
                      r"|afterwards?|meanwhile|however|additionally),?\s+)?on\s*", re.I)


_LEAD_WORDS = re.compile(r"\s*(?:(?:for\s+(?:example|instance)|then|also|and|but),?\s+)?", re.I)


def _leading_date(sentence: str) -> str | None:
    """...and "That month, the Moran appeared ..." (get-a-brain-morans): a
    relative date the sentence opens with, in any unit."""
    rel = _relative_span(sentence)
    if rel:
        rest = sentence[_LEAD_WORDS.match(sentence).end():]
        if rest.lower().startswith(rel.lower()) and rest[len(rel):len(rel) + 1] in (",", " "):
            return rel
    spans = _date_spans(sentence)
    if not spans:
        return None
    a, b, _parts = spans[0]
    if (_LEAD_IN.fullmatch(strip_footnotes(sentence[:a]).lstrip())
            and sentence[b:b + 1] == ","):
        return sentence[a:b]
    return None


def _only_quoted(value: str, text: str) -> bool:
    """Named only inside quoted words: "... has no place on @YouTube" in
    PETA's tweet addresses YouTube; it does not say YouTube did anything."""
    spans = [m.span() for m in _QUOTED.finditer(text)]
    hits = [m.start() for m in re.finditer(re.escape(value), text)]
    return bool(hits) and all(any(a <= h < b for a, b in spans) for h in hits)


def _platform_in(place: str) -> str:
    key = _comparable(place)
    for brand in _LOWER_BRANDS:
        if re.fullmatch(re.escape(brand) + r"(?:m?e?rs?|ors?)", key) and key != brand:
            return place[:len(brand)]
    return place


def _contains_run(longer: list[str], shorter: list[str]) -> bool:
    n = len(shorter)
    return any(longer[i:i + n] == shorter for i in range(len(longer) - n + 1))


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
    if "date_text" in row:
        row["date_text"] = _nullish(row["date_text"])
    for key in ("actors", "locations"):
        value = row.get(key)
        if isinstance(value, str):           # a model that sent one string
            value = [value]
        row[key] = [v for v in (value or []) if _nullish(v)]
    row.setdefault("location_type", "unknown")
    row.setdefault("date_text", None)
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
    if date_text is None:
        # The model gave no words, or words that are not in the event's
        # sentences — "September 9th, 2005" for "That month, the Moran
        # appeared ..." (the sentence before's date): the page's own
        # opening words win over both.
        date_text = _leading_date(_sentence_text(unit, covered[0]))
    places = [part for value in row["locations"] for part in _split_places(value)]
    last = covered[-1]
    before = "\n\n".join(unit["paragraphs"][:last["paragraph"]]
                         + [unit["paragraphs"][last["paragraph"]][:last["end"]]])
    actors = _grounded_list(row["actors"], section, clean=_clean_actor,
                            own=source_text, before=before)
    # A source being CITED is not a participant: "According to Pixiv
    # Encyclopedia, the first ..." (ahegao, 2026-09-24 review).
    own_comp = _comparable(source_text)
    actors = [a for a in actors if f"according to {_comparable(a)}" not in own_comp]
    # A venue listed as a participant is moved to where it belongs.
    venues = [a for a in actors if _VENUE_NOUN.search(a)]
    actors = [a for a in actors if a not in venues]
    locations = _grounded_list(places + venues, section, own=source_text,
                               before=before)
    # ...and the poster's own page, channel or account listed as a place is
    # moved the other way: "the Facebook page King K Rool posted" names who
    # posted. Anything that is already an actor is not also a place.
    # "iFunnyer Choctaw posted": the place is the iFunny in "iFunnyer" —
    # still the page's own letters (holdout, 2026-09-25).
    locations = [_platform_in(l) for l in locations]
    # ...and a place is no place when it is all lowercase common words
    # ("personal blog", the-rake): a named place has a capital, a digit or
    # a sigil somewhere, the platforms their own brand spelling.
    locations = [l for l in locations
                 if not re.fullmatch(r"[a-z][a-z\s-]*", l) or _comparable(l) in _LOWER_BRANDS]
    # Whoever the sentence says DID it is an actor, whatever the model
    # filed it as: "Relentlessly Optimistic posted the image", "TMZ
    # published footage" (the model put some outlets under actors and
    # others under places; 2026-09-24 review).
    doers = [l for l in locations
             if l[:1].isupper() or l[:1].isdigit() or l[:1] == "@"]
    doers = [l for l in doers
             if _comparable(l) not in _LOWER_BRANDS | {"x"}
             and re.search(r"(?<!\w)" + re.escape(_comparable(l)) + r"(?:['’]s)?\s+"
                           r"(?:then\s+|also\s+|later\s+)?(?:posted|published|uploaded"
                           r"|tweeted|shared|released|reposted|covered"
                           r"|reported|featured|wrote|announced)\b", own_comp)]
    posters = [l for l in locations
               if l in doers
               or ((l.startswith("@") or re.match(r"u/\w", l))
                   and not _only_quoted(l, source_text))
               or _ACCOUNT_NOUN.search(l)
               or any(_ACCOUNT_NOUN.search(w) for w in _words_before(l, source_text))
               or (_comparable(l) not in _LOWER_BRANDS | {"x"}
                   and re.search(re.escape(_comparable(l)) + r"\s+(?:(?:youtube|twitch"
                                 r"|facebook|instagram|tiktok|twitter|tumblr|reddit"
                                 r"|discord)\s+)?(?:channel|page|account|profile)\b",
                                 own_comp))]
    actor_keys = {_comparable(a) for a in actors}
    for poster in posters:
        name = _clean_actor(poster)
        if _comparable(name) not in actor_keys:
            actors.append(name)
        actor_keys |= {_comparable(name), _comparable(poster)}
    locations = [l for l in locations
                 if l not in posters and _comparable(l) not in actor_keys]
    # ...nor is the actor's own page named around the actor: "the personal
    # blog of Something Awful user Brian Somerville" (the-rake, 2026-09-25).
    locations = [l for l in locations
                 if not any(re.search(r"(?<!\w)" + re.escape(k) + r"(?!\w)", _comparable(l))
                            for k in actor_keys if len(k) > 2)]
    # A part of a name listed beside the whole: "Cody Ko", "Cody", "Ko".
    words = {a: _comparable(a).split() for a in actors}
    actors = [a for a in actors
              if not any(len(words[b]) > len(words[a]) and _contains_run(words[b], words[a])
                         for b in actors)]
    # One poster named twice: "IDF" and "IDF YouTube channel" (there-is-a-
    # list, 2026-09-24 review). The name is kept, its channel dropped.
    actors = [a for a in actors
              if not (_ACCOUNT_NOUN.search(_comparable(a)) and any(
                  _comparable(a).startswith(k + " ")
                  and len(_comparable(a)[len(k):].split()) <= 3
                  for k in actor_keys if k != _comparable(a)))]
    location_type = (row.get("location_type") or "unknown") if locations else "unknown"
    if locations and location_type == "unknown":
        location_type = "platform"

    return {
        "sentences": ids,
        "source_text": source_text,
        "date": None, "date_precision": "none", "date_basis": None,
        "date_text": date_text,
        "locations": locations,
        "location_type": location_type,
        "actors": actors,
        "certainty": row["certainty"],
    }


# A sentence that only reports how a post was RECEIVED — "The post received
# more than 2,000 reactions, 410 comments and 500 shares in three days" —
# narrates no happening of its own: it is part of the event of the post it
# reports on. The prompt says so, and the model still left 127 of 1,370
# sentences of the 2026-09-24 review sample out of every event, and made
# another few events of their own (the reviewer's "missed sentence 3").
# So the pipeline puts them where they belong, and only when nothing on
# the way could be a happening the model missed: no posting verb, no named
# poster, no date (an "as of" bound is not one), no relative date.
_RECEPTION = re.compile(
    r"\d[\d,.]*\+?\s*(?:million|thousand|[km])?\s+(?:up\s*|down\s*)?"
    r"(?:views|likes|dislikes|shares|retweets|reposts|quote[\s-]tweets|reactions"
    r"|votes|points|notes|comments|replies|favorites|favourites|faves|bookmarks"
    r"|reblogs|plays|listens|streams|subscribers|followers|members|karma"
    r"|downloads|hits|impressions|interactions|smiles|saves|loops|revines)\b"
    r"|\b(?:watched|viewed|liked|shared|retweeted|reblogged|favorited|upvoted"
    r"|played|streamed|downloaded)\s+(?:over\s+|more\s+than\s+|nearly\s+"
    r"|almost\s+|about\s+|roughly\s+|approximately\s+|around\s+)?"
    r"\d[\d,.]*\s*(?:million|thousand|[km])?\s+times\b", re.I)
# ...and one that only gives the words of the post just narrated: "They
# wrote, "Churches should pay property tax periodt."" (periodt, she-took-
# the-fucking-kids: left out, or made an event of its own).
_SPEECH = re.compile(
    r"^(?:(?:in|on)\s+(?:the|their|his|her|its)\s+(?:post|tweet|video|caption"
    r"|comment|reply|thread|story)\s*,?\s*)?(?:they|he|she|it|the\s+(?:user"
    r"|poster|post|tweet|caption|video|comment|reply|thread|account|page|artist"
    r"|creator))\b[^\"“]{0,40}?\b(?:wrote|writes|said|says|captioned|added"
    r"|asked|stated|explained|replied|commented|joked|noted)\b", re.I)
_QUOTED = re.compile(r'["“][^"”]*(?:["”]|$)')
_HAPPENING = re.compile(
    r"\b(?:posted|uploaded|re-?uploaded|reposted|re-?shared|shared|tweeted"
    r"|retweeted|created|made|published|released|launched|submitted|aired"
    r"|premiered|began|started|appeared|founded|established|drew|edited"
    r"|remixed|dubbed)\b|@\w|(?<![\w/])u/\w", re.I)
# "One such post by YouTuber Nathan Zed ... has gained 22,800 retweets" is
# another post, not the last one's reception. Only a WORK by someone: a
# passive "struck by the Hinox" in a description is not a new post.
_BY_NAME = re.compile(
    r"\b(?i:posts?|videos?|tweets?|comics?|images?|clips?|edits?|versions?"
    r"|threads?|drawings?|gifs?|remix(?:es)?|(?:re)?uploads?|photos?|pictures?"
    r"|one|examples?|submissions?)\b[^.;]{0,20}?\bby\s+"
    r"(?:(?:[a-z]+\s+){0,2}[A-Z@]"          # "by YouTuber Nathan Zed"
    r"|(?!(?:the|a|an|its|his|her|their|this|that|these|those|some|many|other"
    r"|others|users?|people|fans)\b)[a-z0-9_.]+\b)")  # "by crybabygrande"


def _narrates_nothing_new(text: str) -> bool:
    """No happening, poster or date of its own in this sentence — quoted
    words and the reception counts themselves aside ("shared 27 times")."""
    text = _RECEPTION.sub(" ", _QUOTED.sub(" ", text))
    if _HAPPENING.search(text) or _BY_NAME.search(text) or _relative_span(text):
        return False
    clean, _index = _without_markers(text)
    return all(date_is_bound(clean[a:b], clean) for a, b, _p in _date_spans(clean))


# A sentence ABOUT the work the event before posted — "The post features
# host Matthew O'Dowd discussing ...", "In the video, the sound effect ...
# accompanies a clip" — describes; the model made events of them in 8 of
# the 127 sections it was prompted not to (2026-09-24 review). Only when
# it narrates nothing new and says nothing about the work spreading.
_ABOUT_THE_WORK = re.compile(
    r"^(?:(?:in|at)\s+(?:the|this|that|its|his|her|their)\s+(?:[\w-]+\s+){0,2}?"
    r"(?:video|clip|episode|film|movie|scene|comic|strip|image|picture|photo|post"
    r"|tweet|game|song|stream|trailer|footage|recording)\b"
    r"|(?:the|this|that|its|his|her|their)\s+(?:[\w-]+\s+){0,3}?(?:video|clip"
    r"|episode|film|movie|scene|comic|strip|image|picture|photo|photograph"
    r"|screenshot|screen\s+capture|post|tweet|game|song|audio|recording|footage"
    r"|gif|caption|copypasta|story|thread|artwork|drawing|template)\b)", re.I)
_SPREADING = re.compile(
    r"\b(?:viral|spread\w*|trend\w*|popular\w*|became|become|grew|grow\w*"
    r"|began|begun|start\w*|inspir\w+|went|resurfac\w+|circulat\w+|meme[ds]?"
    r"|exploitable|parod\w+|remix\w*)\b", re.I)


# ...and "the picture ..." must say what it SHOWS: "The picture was very
# well-received, and variations flowed forth" is its spread.
_DEPICTS = re.compile(
    r"\b(?:features?|featuring|depict(?:s|ed|ing)?|show(?:s|n|ing)?|contain(?:s|ed|ing)?"
    r"|consist(?:s|ed|ing)?|includ(?:es|ed|ing)|had|has|reads?|says?|sings?|plays?"
    r"|accompan\w+|(?:is|was|are|were)\s+(?:a|an|the)\b)", re.I)


def _describes_the_work(text: str) -> bool:
    m = _ABOUT_THE_WORK.match(text)
    return (bool(m) and (m.group(0).lower().startswith(("in ", "at ")) or bool(_DEPICTS.search(text)))
            and not _SPREADING.search(text) and _narrates_nothing_new(text))


def _reception_only(text: str) -> bool:
    """Only how the post before was received, or only its own words."""
    reports = _RECEPTION.search(text) or (_SPEECH.search(text) and _QUOTED.search(text))
    return bool(reports) and _narrates_nothing_new(text)


def _same_day_only(text: str) -> bool:
    """No time of its own beyond "that day" / "the same day"."""
    clean, _index = _without_markers(text)
    if any(not date_is_bound(clean[a:b], clean) for a, b, _p in _date_spans(clean)):
        return False
    phrase = _relative_span(text)
    return phrase is None or relative_shift(phrase) == ("day", 0)


def _settle_borrowed(rows: list[dict], unit: dict) -> None:
    """An actor or place the page first names AFTER the event's sentences.

    Named earlier, it is the page referring back ("the group" -> the group
    named above) and stays. Named only later, the model took it from
    another sentence, and there are two cases (2026-09-24 review):

    * the very next sentence, in no event and adding no time of its own,
      is where the model read it — "The claim was refuted online that day
      by many. For example, that day, X user @zoo_bear made a post ..." —
      and that sentence is part of this event: the span grows to it, so
      the event's quoted evidence contains its own actor;
    * otherwise it belongs to a later happening — WhiteCrowWolf's May 16th
      post placed in the subreddit of the May 17th one — and is dropped.
    """
    texts = {s["id"]: _comparable(_sentence_text(unit, s)) for s in unit["sentences"]}
    covered = {i for r in rows for i in range(r["sentences"][0], r["sentences"][-1] + 1)}

    def named(value: str) -> re.Pattern:
        # "YouTuber x uploaded", "TikToker @y", "Redditor z", "tweeted":
        # the page names the platform in the word for its user or its post.
        key = _comparable(value)
        alt = re.escape(key) + r"(?:e?rs?|ors?)?"
        if key in ("twitter", "x", "x (twitter)", "twitter / x", "x / twitter"):
            alt += r"|tweet(?:s|ed|ing)?|retweet(?:s|ed|ing)?"
        return re.compile(rf"(?<!\w)(?:{alt})(?!\w)")
    for row in rows:
        for key in ("actors", "locations"):
            kept = []
            for value in row[key]:
                pat = named(value)
                where = [i for i in sorted(texts) if pat.search(texts[i])]
                last = row["sentences"][-1]
                if not where or where[0] <= last:
                    kept.append(value)
                    continue
                nxt = last + 1
                if (where[0] == nxt and nxt not in covered
                        and _same_day_only(_sentence_text(unit, unit["sentences"][nxt - 1]))):
                    row["sentences"] = sorted(set(row["sentences"]) | {nxt})
                    _c, row["source_text"] = _span(unit, row["sentences"])
                    covered.add(nxt)
                    kept.append(value)
            row[key] = kept
        if not row["locations"]:
            row["location_type"] = "unknown"


_DURATION = re.compile(
    r"^(?:in|within|over|for|during|throughout)\s+(?:the\s+)?(?:(?:first|next"
    r"|following|past|last)\s+)?(?:\S+\s+)?(?:hours?|days?|weeks?|months?|years?"
    r"|decades?)$", re.I)


def _absorb_reception(rows: list[dict], unit: dict) -> list[dict]:
    """Fold each reception-only sentence into the event just before it.

    An event made of nothing but reception sentences, with no date words,
    place or actor, is folded the same way; one with no event before it
    to join stays as it was, so nothing the model returned is lost. The
    sentences between the event and the reception sentence join it too,
    since each narrates nothing new (the comic's description, a quote).
    Returns the rows that remain, in their original order.
    """
    texts = {s["id"]: strip_footnotes(_sentence_text(unit, s))
             for s in unit["sentences"]}

    def stat_only(row: dict) -> bool:
        # "In two months, the Vine received over 26,000 loops" came back
        # with "In two months" as its date words: a duration, not a date.
        timeless = not row["date_text"] or bool(_DURATION.match(row["date_text"]))
        return (timeless and not row["locations"] and not row["actors"]
                and all(_reception_only(texts[i]) or _describes_the_work(texts[i])
                        for i in range(row["sentences"][0], row["sentences"][-1] + 1)))

    folded = [r for r in rows if stat_only(r)]
    kept = [r for r in rows if not stat_only(r)]
    # what a folded event narrated joins the event before it, like reception
    joins = {i for r in folded for i in range(r["sentences"][0], r["sentences"][-1] + 1)}
    covered: dict[int, list[dict]] = {}
    for row in kept:
        for i in range(row["sentences"][0], row["sentences"][-1] + 1):
            covered.setdefault(i, []).append(row)

    for sid in sorted(texts):
        if sid in covered or not (_reception_only(texts[sid]) or sid in joins):
            continue
        j = sid - 1
        while j >= 1 and j not in covered and _narrates_nothing_new(texts[j]):
            j -= 1
        ending = [r for r in covered.get(j, []) if r["sentences"][-1] == j]
        if not ending:
            continue
        # A sentence two events end on belongs to the one that STARTS
        # there: "E1 [1, 2], E2 [2]" — the reception of s2 is E2's post's.
        start = max(r["sentences"][0] for r in ending)
        for row in ending:
            if row["sentences"][0] != start:
                continue
            row["sentences"] = sorted(set(row["sentences"]) | set(range(j + 1, sid + 1)))
            _covered, row["source_text"] = _span(unit, row["sentences"])
        for i in range(j + 1, sid + 1):
            covered[i] = [r for r in ending if r["sentences"][0] == start]

    # A stat-only event nothing absorbed stays an event of its own.
    kept.extend(r for r in folded
                if any(i not in covered for i in
                       range(r["sentences"][0], r["sentences"][-1] + 1)))
    order = {id(r): n for n, r in enumerate(rows)}
    return sorted(kept, key=lambda r: order[id(r)])


def resolve_dates(rows: list[dict], unit: dict,
                  prior: Sequence[dict] = ()) -> None:
    """Date every event, in reading order, from the text and the timeline.

    Three outcomes, all the pipeline's arithmetic and never the model's:

    * **stated** — the date words parse to a date; a year or month they
      leave out comes from the page's timeline (_timeline_hints), which for
      Spread begins with Origin's events (``prior``).
    * **relative** — "that same day", "the following month", "two weeks
      later": that shift from the nearest earlier DATED event, which for the
      opening of Spread is Origin's last; ``date_anchor`` names it.
    * **bound** — "as of December 2nd", "prior to August 2019", "after Cody
      Ko's April 30th video": the date is what the event is measured
      against. Undated — and so never an anchor for the events after it.

    Anything else stays undated with its words kept: "shortly after" and
    "the following week" name no unit the graph has, and inventing one is
    exactly what this module exists to prevent.
    """
    ordered = sorted(rows, key=lambda r: (r["sentences"][0], r["sentences"][-1]))
    for i, row in enumerate(ordered):
        phrase = row.get("date_text") or ""
        if phrase and date_is_bound(phrase, row["source_text"]):
            continue
        date, precision = parse_date_phrase(
            phrase, *_timeline_hints(unit, ordered, i, prior))
        if date:
            row.update(date=date, date_precision=precision, date_basis="stated")
            continue
        # A relative phrase measured in DAYS is unambiguous wherever it
        # sits, so when the model gave no usable date words the event's own
        # sentences are read for one. Months and years are not read this
        # way ("the meme of that year"), and an absolute date never is: a
        # sentence often carries a date belonging to what it reports.
        shift = relative_shift(phrase) if phrase else None
        if shift is None:
            found = _relative_span(row["source_text"], days_only=True)
            if found:
                shift = relative_shift(found)
                row["date_text"] = phrase = found
        if shift and _is_duration(phrase, row["source_text"]):
            shift = None
        # The same date words read twice from one sentence name one time.
        # Resolving the second against the first chained "the next day" into
        # a day later still (hallway-swimming, 2026-09-24 review). Checked
        # on the EFFECTIVE words, wherever they came from.
        twin = next((o for o in ordered[:i] if o.get("date") and phrase
                     and o["sentences"][0] == row["sentences"][0]
                     and _comparable(o.get("date_text") or "") == _comparable(phrase)),
                    None) if shift else None
        if twin is not None:
            row.update(date=twin["date"], date_precision=twin["date_precision"],
                       date_basis=twin["date_basis"])
            if twin.get("date_anchor") is not None:
                row["date_anchor"] = twin["date_anchor"]
            continue
        anchor = _anchor_before(ordered, i, prior) if shift else None
        if anchor is None:
            continue
        moved = shift_date(anchor["date"], anchor["date_precision"], *shift)
        if moved is None:
            continue
        row.update(date=moved[0], date_precision=moved[1], date_basis="relative",
                   date_anchor=anchor)
    # date_anchor carries the anchor itself while ids do not exist yet — a
    # row of this section, or an Origin event that already has its id —
    # and make_validator swaps it for an event_id. Not a sentence number:
    # 74 of 5,612 sampled events share a first sentence with another.


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

        _settle_borrowed(kept, unit)
        kept = _absorb_reception(kept, unit)
        # Dates come last: they are the pipeline's arithmetic over the
        # whole section, and a relative one needs the events before it.
        resolve_dates(kept, unit, unit.get("prior") or ())
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
                # Identity, not sentence number — see resolve_dates. An
                # Origin event already carries its id.
                row["date_anchor"] = (ids_by_row.get(id(anchor_row))
                                      or anchor_row.get("event_id"))
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
    for key in ("date_text", "location_type"):
        if not into.get(key) or into.get(key) == "unknown":
            if other.get(key):
                into[key] = other[key]
    for key in ("actors", "locations"):
        seen = {_comparable(a) for a in into[key]}
        into[key].extend(a for a in other[key]
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
    prior = unit.get("prior") or ()
    events = record.get("events") or []
    ordered = sorted((e for e in events if e.get("sentences")),
                     key=lambda e: (e["sentences"][0], e["sentences"][-1]))
    by_id = {e.get("event_id"): e for e in events}
    by_id.update({e.get("event_id"): e for e in prior})
    for i, ev in enumerate(events):
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
        for place in ev.get("locations") or []:
            if _comparable(place) not in section_hay:
                problems.append(f"{where}: location {place!r} not in the section")
            elif _names_nobody(place):
                problems.append(f"{where}: location {place!r} names no place")
        for actor in ev.get("actors") or []:
            if _comparable(actor) not in section_hay:
                problems.append(f"{where}: actor {actor!r} not in the section")
            elif _names_nobody(actor):
                problems.append(f"{where}: actor {actor!r} names nobody")
        if ev.get("date"):
            problems.extend(f"{where}: {p}" for p in
                            _audit_date(ev, ordered, by_id, prior, unit))
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


def _audit_date(ev: dict, ordered: list[dict], by_id: dict,
                prior: Sequence[dict], unit: dict) -> list[str]:
    """Recompute a stored date exactly as resolve_dates does — through the
    same timeline — so neither the model nor a bug can put a date on the
    page that the page does not carry."""
    phrase = ev.get("date_text") or ""
    basis = ev.get("date_basis")
    got = (ev["date"], ev.get("date_precision"))
    if phrase and date_is_bound(phrase, ev.get("source_text") or ""):
        return [f"date {ev['date']!r} comes from a bound ({phrase!r}), "
                f"not from when it happened"]
    if basis == "stated":
        index = next(i for i, e in enumerate(ordered) if e is ev)
        want = parse_date_phrase(phrase, *_timeline_hints(unit, ordered, index, prior))
        if want != got:
            return [f"date {got} is not what {phrase!r} resolves to ({want})"]
        return []
    if basis == "relative":
        anchor = by_id.get(ev.get("date_anchor"))
        if anchor is None or not anchor.get("date"):
            return [f"relative date {ev['date']!r} with no dated anchor "
                    f"({ev.get('date_anchor')!r})"]
        shift = relative_shift(phrase) if phrase else None
        if shift is None:
            found = _relative_span(ev.get("source_text") or "", days_only=True)
            shift = relative_shift(found) if found else None
        if shift is None:
            return [f"date {ev['date']!r} is marked relative but neither "
                    f"{phrase!r} nor its sentences hold a relative phrase"]
        want = shift_date(anchor["date"], anchor["date_precision"], *shift)
        if want != got:
            return [f"relative date {got} is not {shift} from its anchor "
                    f"{anchor['date']!r} ({want})"]
        return []
    return [f"date {ev['date']!r} with no basis"]


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
            prior_lookup: Callable[[str], Sequence[dict]] | None = None,
            progress: Callable[[str], None] = print) -> dict[str, Any]:
    """Run one batch of units, appending a line per unit to ``out_path``.

    Failures are DATA in the summary, not exceptions. ``on_record`` is
    called with each record right after its line is on disk — how the DAG
    gets a unit into Mongo the moment it is durable.

    An entry's Origin is always extracted before its Spread, and Spread is
    dated against Origin's events (resolve_dates' ``prior``): the ones just
    extracted in this batch, else whatever ``prior_lookup(entry_id)``
    returns — the stored Origin, when only Spread needed redoing.
    """
    order = {name: i for i, name in enumerate(SOURCE_SECTIONS)}
    units = sorted(units, key=lambda u: (u["entry_id"],
                                         order.get(u["source_section"], 99)))
    origin_events: dict[str, list[dict]] = {}
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
        if section == "spread":
            prior = origin_events.get(unit["entry_id"])
            if prior is None and prior_lookup is not None:
                prior = list(prior_lookup(unit["entry_id"]) or [])
            unit = {**unit, "prior": prior or []}
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
        if section == "origin":
            origin_events[unit["entry_id"]] = record["events"]
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
        # A Spread record was dated against its Origin's events: audit it
        # against the same ones, from the same artifact.
        for uid, unit in by_id.items():
            if unit["source_section"] == "spread":
                origin = records.get(unit_id(unit["frame_url"], "origin"))
                unit["prior"] = (origin or {}).get("events") or []
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
