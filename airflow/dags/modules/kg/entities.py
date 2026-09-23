"""
kg/entities.py — title, tags and About -> Wikidata entities (NLP + a local lexicon)
======================================================================================
One unit per FRAME. Named entities (and the common-noun concepts a page
talks about) are recognised with spaCy in the title and the About section,
each tag is looked up whole, and every candidate span is linked to a
Wikidata item from the lexicon kg/wikidata.py builds out of a downloaded
dump. No Mongo, no Airflow, no network.

    python -m modules.kg.entities link \
        --input entries.jsonl --lexicon data/wikidata/lexicon.sqlite \
        --out data/kg/entities/sample.jsonl
    python -m modules.kg.entities audit \
        --input entries.jsonl --entities data/kg/entities/sample.jsonl

What IMKG did, and what this does instead
-----------------------------------------
IMKG sent the About text and the tags to DBpedia Spotlight (confidence
0.5), mapped the DBpedia resources it returned to Wikidata QIDs, and
emitted ``m4s:fromAbout`` / ``m4s:fromTags`` from the frame to each QID
(github.com/riccardotommasini/imkg, kym/mappings/
kym.media.frames.textual.enrichment.yaml). The frame itself it joined to
Wikidata on KYM's own identifier. MemeAtlas keeps both predicates as IMKG
spells them, adds ``mk:fromTitle`` (IMKG did not link titles), and replaces
the remote annotator with two local steps:

  1. **Recognition (NLP).** spaCy (``en_core_web_sm`` by default) finds
     candidate spans: its named entities, its noun chunks and every suffix
     of one ("multi-colored pool noodles", "pool noodles", "noodles"), and
     runs of proper nouns. A title is also tried whole; a tag is only ever
     tried whole — tags are lower-cased keyword phrases, where NER has
     nothing to read.
  2. **Linking.** Each span is looked up in the lexicon (its own words, then
     without a possessive, then with its head noun lemmatised or its plural
     folded), longest span first; a span that links claims its characters,
     so "Shiba Inu dog" cannot also yield "Shiba". Candidates are ranked on
     five features, and the winner's score adds a sixth, ``clarity`` — how
     far it leads the runner-up (``score``). A span whose winner scores
     under ``MIN_LINK_SCORE`` is not linked.

Two questions, not one: WHICH item a span names (the ranking: popularity,
context, label match, type, KYM) and WHETHER to believe it (the ranking's
winner, plus clarity). Without clarity, "4chan" — the only item with that
label, 50 Wikipedias, nothing in the About to corroborate it — scored
under a threshold that "dog" cleared on popularity alone. A unique exact
match is strong evidence even for an obscure item; a close race between
two senses is weak evidence even for famous ones.

The certain link comes first: when the lexicon has an item whose KYM slug
(P13484) is this frame's URL, that item is the frame's title entity, at
score 1.0 — and wins every span of the page that could name it.

Recognition is deliberately recall-oriented
-------------------------------------------
Spotlight annotates common nouns too, and so does this: "hair", "mug",
"store" are real Wikidata items, and the About text of a meme names them.
Most of them say nothing about what the meme MEANS. Deciding which do is a
separate, later step (gap 09: curating the entity layer), and this module
is built to make that step cheap: every link carries the features it was
scored on, its method, whether the span was a proper noun, and how many
candidates it beat and by what margin — so a curation pass can re-rank or
cut without re-running NLP.

NER labels are a HINT, never a filter
-------------------------------------
On the first real About text probed, en_core_web_sm called "4chan" a
CARDINAL, "Reddit" a GPE, "Shiba Inus" an ORG and "Pool Noodle Hat Guy" a
PERSON. A type check that could veto a candidate would have thrown away
three correct links out of four. So the NER label only ever ADDS score, when
it agrees with the candidate's Wikidata class (P31, walked up P279), and a
disagreement costs nothing.

Grounded, like the event layer
------------------------------
Every mention stores the exact characters it was read from — ``field``,
``start``, ``end`` (and ``tag_index`` for a tag) — and ``text`` is always
that slice of the page, never a normalised form. ``audit()`` re-checks each
record against its unit and ``link_units()`` refuses to emit one that fails.

Deterministic
-------------
Same text, same lexicon, same spaCy model: same links. That is what makes
the staleness stamps (``source_sha256``, ``LINKER_VERSION``, the lexicon's
version, the model's name and version) sufficient: a frame is re-linked
exactly when one of them moves.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Iterator, Sequence

from modules.kg import tag_normalize
from modules.kg.wikidata import Candidate, Lexicon, norm, prior

log = logging.getLogger("kg.entities")

__all__ = [
    "LINKER_VERSION", "SOURCE_FIELDS", "METHODS", "DEFAULT_SPACY_MODEL",
    "MIN_LINK_SCORE", "WEIGHTS", "W_CLARITY", "NER_FAMILIES", "frame_key", "frame_unit",
    "field_text", "load_nlp", "nlp_stamp", "model_stamp", "Linker", "audit",
    "link_units", "append_jsonl", "iter_jsonl", "main",
]

# Bump when THIS MODULE's contract changes — recognition, lookup keys, the
# scoring, the threshold, the record's shape. The store re-links every frame
# whose stored linker_version differs.
#   1.1.0  MIN_LINK_SCORE 0.50 -> 0.45, re-read on the full lexicon.
LINKER_VERSION = "1.1.0"

# Where a mention was read from. Order is page order and the graph's.
SOURCE_FIELDS: tuple[str, ...] = ("title", "tag", "about")

# How a span was found. "kym_id" is the certain link (P13484 == this page).
METHODS: tuple[str, ...] = ("kym_id", "title", "tag", "ner", "propn", "noun_chunk")

DEFAULT_SPACY_MODEL = "en_core_web_sm"

# The winner must score at least this to be linked. Read on 403 random
# frames against the full lexicon (dump 20260914), 25 hand-judged links per
# band — right item for the phrase in its sentence, relevance aside:
#   0.40-0.45  12/25   0.45-0.50  18/25   0.50-0.55  18/25
#   0.55-0.60  22/25   0.60-0.70  23/25   0.70+      23/25
# 0.45-0.50 was as good as 0.50-0.55, so the line sits at 0.45: ~40% more
# links than at 0.50 for ~82% estimated precision overall instead of ~86%.
# (0.50 was set first on a 5 GB partial lexicon that lacked many right
# senses, where 0.45-0.50 was right only ~40% of the time.) The wrong ones
# are mostly patterns no threshold removes — a common word's other sense,
# a nationality read as its language, a fragment of a longer name — gap 09.
MIN_LINK_SCORE = 0.45

# Ranking weights (sum 1.0). The certain link (P13484) bypasses them.
#   prior    popularity: log Wikipedia sitelinks (kg/wikidata.prior)
#   context  the candidate's label + description share content words with
#            this frame's title, tags and About (not counting the span's own)
#   exact    matched the item's LABEL rather than an alias, and with the
#            same initial capital ("Pool" the film vs "pool" the basin,
#            "memes" meets "meme"); tags and titles carry no case signal —
#            tags are lower-cased, titles Title-Cased — so for them the
#            capital always agrees
#   type     the NER label agrees with the item's class (a hint only)
#   kym      the item is itself a KYM entry (it has a KYM slug or number)
WEIGHTS: dict[str, float] = {"prior": 0.30, "context": 0.30, "exact": 0.20,
                             "type": 0.10, "kym": 0.10}
# The winner's final score is (1 - W_CLARITY) x its ranking score +
# W_CLARITY x clarity, where clarity = its lead over the runner-up,
# saturating at CLARITY_SATURATION (a lone candidate has full clarity).
W_CLARITY = 0.15
CLARITY_SATURATION = 0.25
_CONTEXT_SATURATION = 3      # shared content words for full context credit

# NER label -> Wikidata classes whose P279 closure counts as agreeing.
NER_FAMILIES: dict[str, frozenset[int]] = {
    "PERSON": frozenset({5, 215627, 95074}),        # human, person, fictional character
    "NORP": frozenset({41710, 9174, 7278, 231002}),  # ethnic group, religion, party, nationality
    "ORG": frozenset({43229}),                        # organization
    "GPE": frozenset({2221906, 56061}),               # geographic location, admin. territory
    "LOC": frozenset({2221906, 618123}),              # geographic location, geographic feature
    "FAC": frozenset({13226383, 41176}),              # facility, building
    "PRODUCT": frozenset({2424752, 7397, 28877}),     # product, software, goods
    "EVENT": frozenset({1190554, 1656682}),           # occurrence, event
    "WORK_OF_ART": frozenset({17537576, 386724}),     # creative work, work
    "LAW": frozenset({7748}),
    "LANGUAGE": frozenset({34770}),
}

# Entity labels that are never a thing to link: dates belong to the event
# layer, amounts to nothing. CARDINAL/ORDINAL are kept only when one TOKEN
# mixes digits and letters — en_core_web_sm tags "4chan" as a CARDINAL, and
# "approximately 100" mixes them only across tokens.
_SKIP_NER = frozenset({"DATE", "TIME", "PERCENT", "MONEY", "QUANTITY"})
_NUMERIC_NER = frozenset({"CARDINAL", "ORDINAL"})
# Tokens a span may not start with (after trimming): "of Japan", "and dogs".
_BAD_START_POS = frozenset({"ADP", "CCONJ", "SCONJ", "PART", "PUNCT", "DET",
                            "PRON", "AUX", "VERB"})
_LEAD_TRIM_POS = frozenset({"DET", "PRON", "PUNCT", "CCONJ", "ADP", "PART"})
_LEAD_TRIM_TAGS = frozenset({"PRP$", "WDT", "WP$", "POS"})
_PRIORITY = {"title": 0, "ner": 1, "propn": 2, "noun_chunk": 3}

_WORD = re.compile(r"[^\W\d_][\w'’-]*", re.UNICODE)
_POSSESSIVE = re.compile(r"['’]s?$")
_QID = re.compile(r"^Q[1-9][0-9]*$")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ------------------------------------------------------------- the units ----

def frame_key(frame_url: str) -> str:
    """sha1 of the frame URL. MUST equal modules/mongo_base.url_doc_id — an
    entities doc's _id IS its entry's _id; tests pin the two together."""
    return hashlib.sha1(frame_url.encode("utf-8")).hexdigest()


def _about_text(entry: dict) -> str:
    """The About section exactly as kg/build.py renders ``m4s:about`` —
    every About section's paragraphs, joined by a blank line — so a
    mention's offsets index the literal the graph carries."""
    return "\n\n".join(p for s in entry.get("sections") or []
                       if s.get("kind") == "about"
                       for p in (s.get("text") or []) if p)


def frame_unit(entry: dict) -> dict | None:
    """One linking unit from a parsed `entries` doc, or None (no url)."""
    url = entry.get("url")
    if not url:
        return None
    title = (entry.get("title") or "").strip()
    tags = list(dict.fromkeys(t.strip() for t in entry.get("tags") or []
                              if isinstance(t, str) and t.strip()))
    about = _about_text(entry)
    fingerprint = json.dumps([url, title, tags, about], ensure_ascii=False)
    return {
        "unit_id": frame_key(url),
        "entry_id": frame_key(url),
        "frame_url": url,
        "title": title,
        "tags": tags,
        "about": about,
        "parser_version": entry.get("parser_version"),
        "source_sha256": hashlib.sha256(fingerprint.encode("utf-8")).hexdigest(),
        "source_chars": len(title) + sum(len(t) for t in tags) + len(about),
    }


def field_text(unit: dict, field: str, tag_index: int | None = None) -> str:
    """The text a mention's offsets index into."""
    if field == "title":
        return unit["title"]
    if field == "about":
        return unit["about"]
    if field == "tag" and tag_index is not None and 0 <= tag_index < len(unit["tags"]):
        return unit["tags"][tag_index]
    return ""


# -------------------------------------------------------------------- NLP ---

def load_nlp(model: str | None = None):
    """spaCy, loaded where it is used — never at import (the DAG processor
    imports this module to parse the DAG file). Every component of the
    small English pipeline is needed: the tagger and lemmatizer for the
    lookup keys, the parser for noun chunks, NER for named entities."""
    import spacy
    return spacy.load(model or os.getenv("KG_ENTITIES_SPACY_MODEL")
                      or DEFAULT_SPACY_MODEL)


def nlp_stamp(nlp) -> str:
    """``en_core_web_sm@3.8.0`` — the model is part of the contract."""
    meta = nlp.meta
    return f"{meta.get('lang')}_{meta.get('name')}@{meta.get('version')}"


def model_stamp(model: str | None = None) -> str:
    """The same stamp WITHOUT loading the model (the DAG's select step):
    a spaCy model is a pip package whose version is its own."""
    from importlib.metadata import PackageNotFoundError, version
    name = model or os.getenv("KG_ENTITIES_SPACY_MODEL") or DEFAULT_SPACY_MODEL
    try:
        return f"{name}@{version(name)}"
    except PackageNotFoundError:
        raise RuntimeError(
            f"spaCy model {name!r} is not installed in this image — it is "
            f"pinned in requirements.txt; rebuild (docker compose build)") from None


# ----------------------------------------------------------------- spans ----

@dataclass(frozen=True)
class _Span:
    start: int                  # char offsets in the field's text
    end: int
    method: str
    ner_label: str | None
    proper: bool
    lemma_key: str | None       # the span with its head noun lemmatised


def _trim(tokens: list) -> list:
    while tokens and (tokens[0].pos_ in _LEAD_TRIM_POS
                      or tokens[0].tag_ in _LEAD_TRIM_TAGS or tokens[0].is_space):
        tokens = tokens[1:]
    while tokens and (tokens[-1].tag_ == "POS" or tokens[-1].is_punct
                      or tokens[-1].is_space):
        tokens = tokens[:-1]
    return tokens


def _span(tokens: list, method: str, ner_label: str | None = None) -> _Span | None:
    if not tokens:
        return None
    start = tokens[0].idx
    end = tokens[-1].idx + len(tokens[-1].text)
    lemma_key = None
    head = tokens[-1]
    if head.pos_ in ("NOUN", "PROPN") and head.lemma_ and head.lemma_ != head.text:
        words = [t.text_with_ws for t in tokens[:-1]] + [head.lemma_]
        lemma_key = norm("".join(words))
    return _Span(start=start, end=end, method=method, ner_label=ner_label,
                 proper=any(t.pos_ == "PROPN" for t in tokens),
                 lemma_key=lemma_key)


def candidate_spans(doc) -> list[_Span]:
    """Every span of a parsed text worth looking up, before any lookup.

    Nothing overlapping a date, time or amount is a candidate: "November"
    is a proper noun to the tagger and an item to Wikidata, but inside
    "November 2019" it is part of a date — the event layer's business.
    """
    spans: list[_Span] = []
    blocked: list[tuple[int, int]] = []
    for ent in doc.ents:
        if ent.label_ in _SKIP_NER or (ent.label_ in _NUMERIC_NER and not any(
                any(c.isalpha() for c in t.text) and any(c.isdigit() for c in t.text)
                for t in ent)):
            blocked.append((ent.start_char, ent.end_char))
            continue
        s = _span(_trim(list(ent)), "ner", ent.label_)
        if s:
            spans.append(s)
    for chunk in doc.noun_chunks:
        tokens = _trim(list(chunk))
        if not tokens or chunk.root.pos_ == "PRON":
            continue
        for i in range(len(tokens)):
            sub = tokens[i:]
            if sub[0].pos_ in _BAD_START_POS or sub[0].is_punct:
                continue
            s = _span(sub, "noun_chunk")
            if s:
                spans.append(s)
    run: list = []
    for tok in list(doc) + [None]:
        if tok is not None and tok.pos_ == "PROPN":
            run.append(tok)
            continue
        if run:
            s = _span(_trim(run), "propn")
            if s:
                spans.append(s)
        run = []
    # One entry per (start, end): the most informative method wins —
    # an NER span keeps its label even when a noun chunk covers it exactly.
    best: dict[tuple[int, int], _Span] = {}
    for s in spans:
        if any(s.start < b and a < s.end for a, b in blocked):
            continue
        key = (s.start, s.end)
        if key not in best or _PRIORITY[s.method] < _PRIORITY[best[key].method]:
            best[key] = s
    return list(best.values())


def _keys(text: str, lemma_key: str | None) -> list[str]:
    """Lookup keys for a span, most literal first: as written, without a
    possessive, with its head noun lemmatised, plural-folded (spaCy does
    not lemmatise proper-noun plurals: "Shiba Inus" stays "Inus"), and
    without a leading "the"."""
    keys = [norm(text)]
    bare = _POSSESSIVE.sub("", text.rstrip())
    if bare != text:
        keys.append(norm(bare))
    if lemma_key:
        keys.append(lemma_key)
    keys.append(tag_normalize.fold(keys[-1]))
    low = keys[0]
    if low.startswith("the "):
        keys.append(low[4:])
    return [k for k in dict.fromkeys(keys) if k]


# ---------------------------------------------------------------- linking ---

def _stem(word: str) -> str:
    """Plural folding for context overlap — tag_normalize's rule, applied to
    one word so "memes" in a description meets "meme" in a tag."""
    return tag_normalize.fold(word)


class Linker:
    """Recognise and link entities for frame units against one lexicon and
    one spaCy pipeline. Construct once per mapped task."""

    def __init__(self, lexicon: Lexicon, nlp, *, min_score: float = MIN_LINK_SCORE):
        from spacy.lang.en.stop_words import STOP_WORDS
        self.lexicon = lexicon
        self.nlp = nlp
        self.min_score = min_score
        self.stop = frozenset(STOP_WORDS)
        self.stamps = {"linker_version": LINKER_VERSION,
                       "lexicon_version": lexicon.version,
                       "nlp_model": nlp_stamp(nlp)}
        self._families: dict[int, frozenset[int]] = {}
        self._self_qid: int | None = None     # the frame being linked's own item

    # -- features -----------------------------------------------------------

    def words(self, text: str) -> set[str]:
        return {_stem(w.lower().strip("'’-")) for w in _WORD.findall(text or "")
                if len(w) >= 3 and w.lower() not in self.stop}

    def _type_agrees(self, cand: Candidate, ner_label: str | None) -> bool:
        wanted = NER_FAMILIES.get(ner_label or "")
        if not wanted:
            return False
        fam = self._families.get(cand.qid)
        if fam is None:
            fam = self._families[cand.qid] = self.lexicon.families(cand.types)
        return bool(fam & wanted)

    def score(self, text: str, cands: Sequence[Candidate], *, ner_label: str | None,
              context: set[str], self_qid: int | None,
              case_known: bool = True) -> list[tuple[float, Candidate, dict, float]]:
        """Every candidate as (score, candidate, features, ranking score),
        best first.

        Candidates are RANKED on the weighted features; only the winner's
        score then folds in its clarity (see W_CLARITY) — every candidate
        keeps its ranking score as the tuple's last element, which is what
        a lead is measured in. Ties go to the more linked item, then the
        lower QID: deterministic.
        """
        own = self.words(text)
        out = []
        for c in cands:
            if self_qid is not None and c.qid == self_qid:
                feats = {"prior": round(prior(c.sitelinks), 3), "context": 1.0,
                         "exact": 1.0, "type": 1.0, "kym": 1.0, "clarity": 1.0}
                out.append((1.0, c, feats, 1.0))
                continue
            shared = (self.words(f"{c.label} {c.description or ''}") - own) & context
            same = (not case_known
                    or text[:1].isupper() == c.surface[:1].isupper())
            feats = {
                "prior": prior(c.sitelinks),
                "context": min(1.0, len(shared) / _CONTEXT_SATURATION),
                "exact": (0.6 if c.is_label else 0.3) + (0.4 if same else 0.0),
                "type": 1.0 if self._type_agrees(c, ner_label) else 0.0,
                "kym": 1.0 if c.kym else 0.0,
            }
            s = round(sum(WEIGHTS[k] * v for k, v in feats.items()), 3)
            out.append((s, c, {k: round(v, 3) for k, v in feats.items()}, s))
        out.sort(key=lambda r: (-r[0], -r[1].sitelinks, r[1].qid))
        if out and out[0][0] < 1.0:
            best, c, feats, rank = out[0]
            lead = rank - out[1][3] if len(out) > 1 else rank
            clarity = round(min(1.0, max(0.0, lead) / CLARITY_SATURATION), 3)
            final = round((1 - W_CLARITY) * best + W_CLARITY * clarity, 3)
            out[0] = (final, c, {**feats, "clarity": clarity}, rank)
        return out

    # -- one field ------------------------------------------------------------

    def _resolve(self, text: str, keys: Sequence[str], *, ner_label: str | None,
                 context: set[str], self_qid: int | None, case_known: bool = True):
        """(scored candidates, the key that matched) for the first key that
        names anything in the lexicon; ([], None) when none does."""
        for key in keys:
            cands = self.lexicon.candidates(key)
            if cands:
                return self.score(text, cands, ner_label=ner_label,
                                  context=context, self_qid=self_qid,
                                  case_known=case_known), key
        return [], None

    def _mention(self, field: str, text: str, start: int, end: int, method: str,
                 scored: list, *, ner_label: str | None, proper: bool,
                 tag_index: int | None = None) -> dict:
        s, c, feats, rank = scored[0]
        # The winner's lead in RANKING score — uncapped, unlike clarity.
        margin = rank - scored[1][3] if len(scored) > 1 else rank
        m = {"field": field, "text": text, "start": start, "end": end,
             "qid": c.id, "label": c.label, "description": c.description,
             "score": s, "method": method,
             "ner_label": ner_label, "proper": proper, "matched": c.surface,
             "candidates": len(scored), "margin": round(margin, 3),
             "features": feats}
        if tag_index is not None:
            m["tag_index"] = tag_index
        return {k: v for k, v in m.items() if v is not None}

    def _link_text(self, field: str, text: str, doc, *, context: set[str],
                   whole: bool, mentions: list, nil: list, stats: dict,
                   case_known: bool = True) -> None:
        occupied: list[tuple[int, int]] = []

        def free(a: int, b: int) -> bool:
            return all(b <= x or a >= y for x, y in occupied)

        spans = candidate_spans(doc) if doc is not None else []
        if whole and text:
            spans.append(_Span(0, len(text), "title", None,
                               proper=any(ch.isupper() for ch in text), lemma_key=None))
        # Longest first; at equal length the more informative method, then
        # page order. A span that links claims its characters.
        spans.sort(key=lambda s: (-(s.end - s.start), _PRIORITY[s.method], s.start))
        unlinked_ner: list[_Span] = []
        for sp in spans:
            if not free(sp.start, sp.end):
                continue
            surface = text[sp.start:sp.end]
            scored, _key = self._resolve(surface, _keys(surface, sp.lemma_key),
                                         ner_label=sp.ner_label, context=context,
                                         self_qid=self._self_qid,
                                         case_known=case_known)
            if not scored:
                if sp.method == "ner":
                    unlinked_ner.append(sp)
                continue
            if scored[0][0] < self.min_score:
                stats["rejected"] += 1
                continue
            occupied.append((sp.start, sp.end))
            mentions.append(self._mention(field, surface, sp.start, sp.end, sp.method,
                                          scored, ner_label=sp.ner_label,
                                          proper=sp.proper))
        for sp in unlinked_ner:
            if free(sp.start, sp.end):
                nil.append({"field": field, "text": text[sp.start:sp.end],
                            "start": sp.start, "end": sp.end, "ner_label": sp.ner_label})

    # -- one frame ------------------------------------------------------------

    def link(self, unit: dict, *, title_doc=None, about_doc=None) -> dict:
        """One unit -> one record. Pass pre-parsed docs to batch spaCy
        (``link_units`` does); otherwise the texts are parsed here."""
        started = time.monotonic()
        if title_doc is None and unit["title"]:
            title_doc = self.nlp(unit["title"])
        if about_doc is None and unit["about"]:
            about_doc = self.nlp(unit["about"])
        context = self.words(" ".join([unit["title"], " ".join(unit["tags"]),
                                       unit["about"]]))
        self_item = self.lexicon.by_kym(unit["frame_url"])
        self._self_qid = self_item.qid if self_item else None
        mentions: list[dict] = []
        nil: list[dict] = []
        stats = {"rejected": 0}

        # Title: the certain link claims it whole; otherwise it is parsed
        # like any text, and also tried whole.
        if unit["title"] and self_item is not None:
            mentions.append({
                "field": "title", "text": unit["title"], "start": 0,
                "end": len(unit["title"]), "qid": self_item.id,
                "label": self_item.label, "description": self_item.description,
                "score": 1.0, "method": "kym_id", "proper": True,
                "matched": self_item.label, "candidates": 1, "margin": 1.0,
                "features": {"prior": round(prior(self_item.sitelinks), 3),
                             "context": 1.0, "exact": 1.0, "type": 1.0, "kym": 1.0,
                             "clarity": 1.0}})
            mentions[-1] = {k: v for k, v in mentions[-1].items() if v is not None}
        elif unit["title"]:
            self._link_text("title", unit["title"], title_doc, context=context,
                            whole=True, mentions=mentions, nil=nil, stats=stats,
                            case_known=False)

        # Tags: whole, then plural-folded. Case is gone, so "exact" can only
        # ever credit a label match, never its capitalisation.
        for i, tag in enumerate(unit["tags"]):
            keys = [k for k in dict.fromkeys([norm(tag), tag_normalize.fold(norm(tag))]) if k]
            scored, _key = self._resolve(tag, keys, ner_label=None,
                                         context=context - self.words(tag),
                                         self_qid=self._self_qid, case_known=False)
            if not scored:
                continue
            if scored[0][0] < self.min_score:
                stats["rejected"] += 1
                continue
            mentions.append(self._mention("tag", tag, 0, len(tag), "tag", scored,
                                          ner_label=None, proper=False, tag_index=i))

        if unit["about"]:
            self._link_text("about", unit["about"], about_doc, context=context,
                            whole=False, mentions=mentions, nil=nil, stats=stats)

        rank = {f: i for i, f in enumerate(SOURCE_FIELDS)}
        mentions.sort(key=lambda m: (rank[m["field"]], m.get("tag_index", -1), m["start"]))
        nil.sort(key=lambda m: (rank[m["field"]], m["start"]))
        return {
            "unit_id": unit["unit_id"], "entry_id": unit["entry_id"],
            "frame_url": unit["frame_url"],
            "mentions": mentions, "mention_count": len(mentions),
            "entity_count": len({m["qid"] for m in mentions}),
            "nil": nil, "rejected_count": stats["rejected"],
            "self_qid": self_item.id if self_item else None,
            "source_sha256": unit["source_sha256"],
            "source_chars": unit["source_chars"],
            "parser_version": unit.get("parser_version"),
            **self.stamps,
            "linked_at": _now(),
            "elapsed_s": round(time.monotonic() - started, 4),
        }


# ------------------------------------------------------------------ audit ---

def audit(record: dict, unit: dict, *, min_score: float = MIN_LINK_SCORE) -> list[str]:
    """Everything a stored record must hold against the unit it came from.
    An empty list means the record is safe to write."""
    problems: list[str] = []
    seen: dict[tuple, list[tuple[int, int]]] = {}
    for i, m in enumerate(record.get("mentions") or []):
        where = f"mention {i}"
        field = m.get("field")
        if field not in SOURCE_FIELDS:
            problems.append(f"{where}: unknown field {field!r}")
            continue
        if m.get("method") not in METHODS:
            problems.append(f"{where}: unknown method {m.get('method')!r}")
        tag_index = m.get("tag_index")
        if field == "tag" and tag_index is None:
            problems.append(f"{where}: a tag mention without tag_index")
        hay = field_text(unit, field, tag_index)
        start, end = m.get("start"), m.get("end")
        if not (isinstance(start, int) and isinstance(end, int) and 0 <= start < end <= len(hay)):
            problems.append(f"{where}: offsets {start}..{end} outside the {field}")
        elif hay[start:end] != m.get("text"):
            problems.append(f"{where}: text {m.get('text')!r} is not the page's "
                            f"{hay[start:end]!r}")
        if not _QID.match(str(m.get("qid") or "")):
            problems.append(f"{where}: {m.get('qid')!r} is not a QID")
        score = m.get("score")
        if not isinstance(score, (int, float)) or not 0.0 <= score <= 1.0:
            problems.append(f"{where}: score {score!r} outside [0, 1]")
        elif score < min_score:
            problems.append(f"{where}: score {score} under the threshold {min_score}")
        spans = seen.setdefault((field, tag_index), [])
        if isinstance(start, int) and isinstance(end, int):
            if any(not (end <= a or start >= b) for a, b in spans):
                problems.append(f"{where}: overlaps another mention in the {field}")
            spans.append((start, end))
    mentions = record.get("mentions") or []
    if record.get("mention_count") != len(mentions):
        problems.append("mention_count does not match mentions")
    if record.get("entity_count") != len({m.get("qid") for m in mentions}):
        problems.append("entity_count does not match the distinct QIDs")
    if record.get("source_sha256") != unit.get("source_sha256"):
        problems.append("source_sha256 is not the unit's")
    for key in ("linker_version", "lexicon_version", "nlp_model"):
        if not record.get(key):
            problems.append(f"{key} missing")
    return problems


# --------------------------------------------------------------- the batch --

def link_units(linker: Linker, units: Iterable[dict], *,
               on_record: Callable[[dict], None] | None = None,
               batch_size: int = 64) -> dict[str, Any]:
    """Link a batch of units, spaCy-parsing their texts in batches.

    ``on_record`` receives each audited record — how the DAG gets it into
    Mongo. A record that fails its audit is a linker bug, not data: it is
    raised, never written (the event layer's rule).
    """
    units = list(units)
    totals = {"units": 0, "mentions": 0, "nil": 0, "rejected": 0,
              "frames_with_links": 0, "self_links": 0}
    by_field: dict[str, int] = {f: 0 for f in SOURCE_FIELDS}
    by_method: dict[str, int] = {m: 0 for m in METHODS}
    for i in range(0, len(units), batch_size):
        batch = units[i:i + batch_size]
        titles = list(linker.nlp.pipe([u["title"] for u in batch]))
        abouts = list(linker.nlp.pipe([u["about"] for u in batch]))
        for unit, tdoc, adoc in zip(batch, titles, abouts):
            record = linker.link(unit, title_doc=tdoc if unit["title"] else None,
                                 about_doc=adoc if unit["about"] else None)
            problems = audit(record, unit, min_score=linker.min_score)
            if problems:
                raise AssertionError(f"{unit['unit_id']} failed its audit: {problems}")
            if on_record is not None:
                on_record(record)
            totals["units"] += 1
            totals["mentions"] += record["mention_count"]
            totals["nil"] += len(record["nil"])
            totals["rejected"] += record["rejected_count"]
            totals["frames_with_links"] += bool(record["mentions"])
            totals["self_links"] += bool(record["self_qid"])
            for m in record["mentions"]:
                by_field[m["field"]] += 1
                by_method[m["method"]] += 1
    return {**totals, "by_field": by_field, "by_method": by_method,
            **linker.stamps}


# ------------------------------------------------------------ the artifact --

def append_jsonl(path: str, record: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def iter_jsonl(path: str) -> Iterator[dict]:
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


# ------------------------------------------------------------------- CLI ----

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m modules.kg.entities",
        description="Link KYM titles, tags and About text to Wikidata.")
    sub = parser.add_subparsers(dest="command", required=True)

    ln = sub.add_parser("link", help="entries JSONL -> entities JSONL")
    ln.add_argument("--input", required=True, help="JSONL of `entries` docs")
    ln.add_argument("--lexicon", required=True, help="kg/wikidata.py lexicon")
    ln.add_argument("--out", required=True, help="entities JSONL (appended)")
    ln.add_argument("--model", default=None, help=f"spaCy model ({DEFAULT_SPACY_MODEL})")
    ln.add_argument("--limit", type=int, default=0)

    au = sub.add_parser("audit", help="re-check an entities JSONL against its entries")
    au.add_argument("--input", required=True)
    au.add_argument("--entities", required=True)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    units = [u for u in (frame_unit(e) for e in iter_jsonl(args.input)) if u]

    if args.command == "audit":
        by_id = {u["unit_id"]: u for u in units}
        records = {r["unit_id"]: r for r in iter_jsonl(args.entities)}
        problems = {uid: audit(r, by_id[uid]) for uid, r in records.items()
                    if uid in by_id}
        bad = {k: v for k, v in problems.items() if v}
        print(json.dumps({"records": len(records), "audited": len(problems),
                          "with_violations": len(bad), "violations": bad}, indent=2))
        return 1 if bad else 0

    if args.limit:
        units = units[:args.limit]
    with Lexicon(args.lexicon) as lexicon:
        linker = Linker(lexicon, load_nlp(args.model))
        summary = link_units(linker, units,
                             on_record=lambda r: append_jsonl(args.out, r))
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":       # pragma: no cover
    sys.exit(main())
