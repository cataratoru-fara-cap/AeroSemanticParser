"""
curation.py — which linked entities matter to the meme (gap 09)
=================================================================
The entity layer (kg/entities.py) links everything it recognises: 282,914
links over 23,882 frames, 59% of the About links common nouns ("mug",
"face", "popularity"). This module decides, per link, whether it belongs
in the knowledge graph. Nothing is deleted: every decision, with its
reason, is stored (modules/entity_curation_store.py) and only the KEPT
links reach kg/build.py.

Pure: the lexicon and the model client are handed in; no Mongo, no Airflow.

Relevant (Gabi, 2026-09-28) = what the meme is about or shows (subject,
people, characters); the works and franchises it comes from; the kind of
meme it is (image macro, copypasta, reaction image ...); the platforms and
communities where it started or spread. Not relevant: incidental detail of
a described picture, words used in passing.

Two steps
---------
1. **Rules** (``apply_rules``), over what is stored, settle the clear cases
   (kg_config/entity_curation.yaml has the lists and the order):

      title field, or the frame's own item (P13484)      keep   title / own_item
      a deny item (popularity, "the term", Wikipedia)    drop   deny_item
      a platform (by class, or listed)                   keep   platform
      a meme format (image macro, copypasta ...)         keep   format
      also linked from the title                         keep   title_agrees
      linked from both a tag and the About               keep   tag_and_text
      About only: a deny class (body parts, measures)    drop   deny_class
      a whole tag naming something (a capitalised label) keep   tag_named
      About only: a generic word ("image", "man")        drop   generic_item
      anything else                                      the judge

   Titles are always kept: a title is the frame's name. Tags are the
   site editors' keywords: in the bake-off, a whole tag whose item's label
   is capitalised (Wikidata writes common nouns in lower case: "hat",
   "conspiracy theory"; names capitalised: "Drake", "A Goofy Movie") was
   relevant 34 times in 39 — and both judges dropped a third to a half of
   those, not knowing the tag from the About. So the rule keeps them.
2. **The judge** (``judge_frame``): one LLM call per frame over its
   undecided items, EXTRACTIVE — it sees each item's label, description and
   the words it was read from, and can only answer keep/drop for each
   numbered item; it can add nothing. Its verdicts are stored per item, so
   editing the rules never re-asks it.

Until the judge has read a frame, only rule-kept links reach the graph
(Gabi): a partial run shrinks the graph rather than leaving noise in it.

The bake-off (2026-09-29) — how the judge was chosen
-----------------------------------------------------
Round 1: 50 random frames with undecided items, 280 items, every one read
and labelled by hand (Claude, not a person). Targets, Gabi's: of the kept,
>= 0.85 relevant; of the dropped, <= 0.15 relevant.

    prompt / setup                          kept prec.   lost    p50/frame
    v1 keep/drop, mistral-small3.2:24b        0.72       0.21     1.6 s
    v1 keep/drop, ministral-3:14b             0.63       0.14     0.6 s
    v1 keep/drop, qwen3:8b                    0.61       0.18     0.3 s
    v2 + the sentence around each mention     0.73-0.79  0.15-0.25
    v3 a ROLE per item                        0.79-0.80  0.16-0.25
    v3 + tag_named rule, mistral-small        0.88       0.13     1.4 s
       confirming About-only keeps

The first review of the live run (200 links read; kg/entity_review.py)
did not bear that out: of the judge's drops 0.27 were relevant, not 0.13 —
round 1 had chosen the setup on its own items. Round 2 used 353 labelled
items (round 1's plus the review's judge items) and the corpus projection
of rules + judge: every single model and pair stayed below the bar
(ministral alone kept 0.83 / lost 0.16; with mistral confirming, lost
0.18). What moved it was the RULES — curation 1.1.0 keeps meme formats
and drops generic About words by list (entity_curation.yaml) — and asking
ministral twice, with two prompts, rather than asking a second model:

    curation 1.1.0 rules + ...              corpus kept  corpus lost
    ministral v3 alone                        0.855        0.135
    ministral v4 alone                        0.853        0.124
    ministral v4, mistral-small confirming    0.878        0.162
    ministral v4, ministral v3 confirming     0.875        0.126   (THIS)

(llama3.3:70b and qwen3.6:35b crashed ollama-ccdd with CUDA errors and
were not tried further.) The review loop's holdout, drawn after the full
run, is the measurement; these are projections from labels the setup was
chosen on.
"""

from __future__ import annotations

import copy
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

CURATION_VERSION = "1.1.0"
JUDGE_PROMPT_VERSION = "4"          # SYSTEM_PROMPT and CONFIRM_PROMPT together
DEFAULT_JUDGE_MODEL = "ministral-3:14b"          # the bake-off (module docstring)
DEFAULT_CONFIRM_MODEL = "ministral-3:14b"    # the same model, the other prompt
JUDGE_PURPOSE = "kg.entities.curate"
CONFIRM_PURPOSE = "kg.entities.curate.confirm"   # its own purpose: its own pin
CURATED_FIELDS = ("about", "tag")
ABOUT_CHARS = 4000
MAX_JUDGE_ITEMS = 40

# The judge classifies each item; the first four are relevant (Gabi's four
# kinds). Asking for the kind, not a bare keep/drop, makes the model commit
# to WHY (the bake-off in the module docstring).
KEEP_ROLES = ("subject", "source", "format", "platform")
DROP_ROLES = ("incidental", "wrong_sense")
KEEP_BASES = ("title", "own_item", "platform", "format", "title_agrees", "tag_and_text",
              "tag_named", "judge")
DROP_BASES = ("deny_item", "deny_class", "generic_item", "judge")
PENDING = "pending"


# ---------------------------------------------------------------------------
# The curated lists
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Lists:
    platform_classes: dict[int, str]
    platform_items: dict[int, str]
    deny_classes: dict[int, str]
    deny_items: dict[int, str]
    version: str
    format_items: dict[int, str] = None      # type: ignore[assignment]
    generic_items: dict[int, str] = None     # type: ignore[assignment]

    def __post_init__(self):
        for name in ("format_items", "generic_items"):
            if getattr(self, name) is None:
                object.__setattr__(self, name, {})


def _qid(value: Any) -> int:
    s = str(value).strip()
    if not (s[:1] == "Q" and s[1:].isdigit()):
        raise ValueError(f"not a QID: {value!r}")
    return int(s[1:])


def load_lists(path: str) -> Lists:
    """kg_config/entity_curation.yaml. Refuses a qid listed twice — a class
    that is both a platform and a deny class is a mistake, not a precedence."""
    import yaml

    with open(path, "rb") as fh:
        raw = fh.read()
    doc = yaml.safe_load(raw) or {}
    tables: dict[str, dict[int, str]] = {}
    seen: dict[int, str] = {}
    for name in ("platform_classes", "platform_items", "deny_classes", "deny_items",
                 "format_items", "generic_items"):
        table: dict[int, str] = {}
        for row in doc.get(name) or []:
            q = _qid(row["qid"])
            if q in seen:
                raise ValueError(f"Q{q} is on both {seen[q]} and {name}")
            seen[q] = name
            table[q] = str(row.get("label") or f"Q{q}")
        tables[name] = table
    return Lists(**tables, version=hashlib.sha256(raw).hexdigest()[:16])


# ---------------------------------------------------------------------------
# Mention identity
# ---------------------------------------------------------------------------

def mention_key(m: Mapping[str, Any]) -> str:
    """A mention's identity within its frame: where it was read and what it
    was linked to. Stable across re-links of unchanged text."""
    tag = m.get("tag_index")
    return f"{m['field']}|{-1 if tag is None else int(tag)}|{m['start']}|{m['end']}|{m['qid']}"


def mentions_sha(mentions: Iterable[Mapping[str, Any]]) -> str:
    """What a curation was computed over — a re-link that changes any
    mention makes the curation stale."""
    keys = sorted(mention_key(m) for m in mentions)
    return hashlib.sha256("\n".join(keys).encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------

class ClassIndex:
    """An item's classes, two ways.

    ``closure``: the P279 closure of its P31 types, plus its own P279
    closure (t-shirt has no P31, only P279 shirt). Used for PLATFORMS,
    where it was checked against real platforms and news sites.

    ``direct``: its P31 types and its direct P279 parents only. Used for
    DENY classes, because the closure is not safe to deny by: Wikidata's
    upper ontology is tangled enough that the closure of "mathematical
    concept" held image macro, catchphrase, copypasta and GIF (25,987
    mentions on 2026-09-29), and that of "measure" cryptocurrency, Bitcoin
    and NFTs — the very things memes are about.
    """

    def __init__(self, lexicon):
        self.lexicon = lexicon
        self._closure: dict[int, frozenset[int]] = {}
        self._direct: dict[int, frozenset[int]] = {}

    def _types(self, qid: int) -> tuple[int, ...]:
        item = self.lexicon.entity(qid)
        return tuple(item.types) if item is not None else ()

    def closure(self, qid: int) -> frozenset[int]:
        got = self._closure.get(qid)
        if got is None:
            got = frozenset(self.lexicon.families(self._types(qid))
                            | self.lexicon.ancestors(qid))
            self._closure[qid] = got
        return got

    def direct(self, qid: int) -> frozenset[int]:
        got = self._direct.get(qid)
        if got is None:
            got = frozenset(self._types(qid)) | self.lexicon.parents(qid)
            self._direct[qid] = got
        return got


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

def apply_rules(record: Mapping[str, Any], lists: Lists, classes) -> list[dict]:
    """One ``entities`` doc -> a decision per mention, in mention order:
    ``{key, qid, field, keep: True|False|None, basis}``. ``keep`` None is
    "for the judge" (basis ``pending``). ``classes`` is a ClassIndex (or
    anything with ``closure(qid)`` and ``direct(qid)``)."""
    mentions = list(record.get("mentions") or [])
    by_field: dict[str, set[str]] = {}
    for m in mentions:
        by_field.setdefault(m["field"], set()).add(m["qid"])
    title_q = by_field.get("title", set())
    tag_q = by_field.get("tag", set())
    about_q = by_field.get("about", set())
    self_qid = record.get("self_qid")
    out: list[dict] = []
    for m in mentions:
        qid = m["qid"]
        q = int(qid[1:])
        keep: bool | None
        if m["field"] == "title":
            keep, basis = True, "title"
        elif m.get("method") == "kym_id" or qid == self_qid:
            keep, basis = True, "own_item"
        elif q in lists.deny_items:
            keep, basis = False, "deny_item"
        elif q in lists.platform_items or classes.closure(q) & lists.platform_classes.keys():
            keep, basis = True, "platform"
        elif q in lists.format_items:
            keep, basis = True, "format"
        elif qid in title_q:
            keep, basis = True, "title_agrees"
        elif qid in tag_q and qid in about_q:
            keep, basis = True, "tag_and_text"
        elif m["field"] == "about" and classes.direct(q) & lists.deny_classes.keys():
            keep, basis = False, "deny_class"
        elif m["field"] == "tag" and m.get("method") == "tag" and is_named(m.get("label")):
            keep, basis = True, "tag_named"
        elif m["field"] == "about" and q in lists.generic_items:
            keep, basis = False, "generic_item"
        else:
            keep, basis = None, PENDING
        out.append({"key": mention_key(m), "qid": qid, "field": m["field"],
                    "keep": keep, "basis": basis})
    return out


def is_named(label: str | None) -> bool:
    """A name, not a common noun, by Wikidata's own labelling convention:
    common nouns are lower case ("hat"), names capitalised ("Drake")."""
    return bool(label) and label[:1].isupper()


def judge_items(record: Mapping[str, Any], decisions: Sequence[dict],
                about: str | None = None) -> list[dict]:
    """The frame's undecided ITEMS (one per QID), with what the judge needs:
    label, description, the words they were read from and where — and, for
    an About mention when ``about`` is given, the words around it (small
    models judge "incidental or central?" far better with the sentence in
    front of them than with the whole About two thousand characters up)."""
    pending = {d["qid"] for d in decisions if d["keep"] is None}
    items: dict[str, dict] = {}
    for m in record.get("mentions") or []:
        if m["qid"] not in pending:
            continue
        it = items.setdefault(m["qid"], {"qid": m["qid"], "label": m.get("label"),
                                         "description": m.get("description"),
                                         "texts": [], "fields": []})
        if m.get("text") and m["text"] not in it["texts"]:
            it["texts"].append(m["text"])
        if m["field"] not in it["fields"]:
            it["fields"].append(m["field"])
        if about and m["field"] == "about" and "snippet" not in it:
            it["snippet"] = _snippet(about, m["start"], m["end"])
    return list(items.values())


def _snippet(text: str, start: int, end: int, width: int = 90) -> str:
    a, b = max(0, start - width), min(len(text), end + width)
    body = text[a:start] + "[[" + text[start:end] + "]]" + text[end:b]
    return ("…" if a else "") + " ".join(body.split()) + ("…" if b < len(text) else "")


def resolve(decisions: Sequence[dict], verdicts: Mapping[str, bool]) -> list[dict]:
    """Fill the pending decisions from the judge's per-item verdicts; those
    it has not answered stay pending (and out of the graph)."""
    out = []
    for d in decisions:
        if d["keep"] is None and d["qid"] in verdicts:
            d = dict(d, keep=bool(verdicts[d["qid"]]), basis="judge")
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# The judge
# ---------------------------------------------------------------------------

# The judge's prompt (4). Written from the first review: it names the people
# an entry involves, topic tags and what a subject is known for as relevant,
# and media and role words used in passing as not.
SYSTEM_PROMPT = """You curate a knowledge graph of internet memes built from Know Your Meme (KYM) entries.

An entity linker read an entry and linked words in it to Wikidata items. Give each numbered item its ROLE in this entry:

- subject: what the meme is about or shows: its topic; a specific person, character, animal or object in it; a body part or object the meme is about; the emotion or reaction it expresses. Also any specific person the entry names as appearing in it, making it, popularizing it, or portrayed in it (e.g. the actor playing the character in the scene, the streamer who made a word popular). For an entry about a person, group or channel: what they are and what they are known for ("comedian", "parody" for a parody animator).
- source: a specific work, franchise, game, show, film, song, band, company, product or event the meme comes from or refers to (e.g. The Matrix, Pawn Stars, Call of Duty, the 2016 Summer Olympics).
- format: the kind of meme or media it is, when the entry says so (image macro, copypasta, snowclone, reaction image, exploitable, phrasal template, parody, remix, viral video, fan art, webcomic, song cover).
- platform: a website, app, platform or online community where it started or spread.
- incidental: true, but not what the meme is: a general word for media or content used in passing ("image", "video", "screenshot", "social media", "website", "character", "episode", "television program", "feature film"); a role or group mentioned in passing ("YouTuber", "TikToker", "rapper", "man", "woman", "husband", "people"); an abstract notion (belief, culture, popularity, criticism, emotion, time); a year, decade, city, country or nationality the meme is not about; a source that merely reports on it; a detail of a described picture that is not its point; the generic words "meme", "internet meme" or "Internet", unless the entry is about memes or the Internet as such.
- wrong_sense: the item's description is not what the entry means by those words (e.g. a film called "Work" for the common word "work"; "sheets" of a calendar linked to bed sheets; a city for a person's name).

Tags were chosen by the site's editors to say what the entry is about. A tag naming the meme's topic, field or genre is relevant, as subject or source: e.g. "cryptocurrency" on an NFT meme, "dubstep" on a dubstep remix meme, "soccer" on a football player's meme, "fisheye lens" on a wide-angle close-up meme. Drop a tag only when it is off-topic, generic filler, or a wrong sense.

Answer with JSON only: {"items": [{"n": 1, "role": "subject"}, {"n": 2, "role": "incidental"}, ...]}, every number exactly once."""

# The confirming prompt: the judge's previous prompt (3). Asking the same
# model the same question in other words and keeping an About-only item
# only when both answers keep it removes most of what one reading keeps
# by chance (the bake-off in the module docstring).
CONFIRM_PROMPT = """You curate a knowledge graph of internet memes built from Know Your Meme (KYM) entries.

An entity linker read an entry and linked words in it to Wikidata items. Many links are incidental. Give each numbered item its ROLE in this entry:

- subject: what the meme is about or shows: its topic; a specific person, character, animal or object in it; the emotion or reaction it expresses; or, for an entry about a person, group or channel, what they are known for (e.g. Neo in a Matrix quote meme; stairs in a meme about falling down stairs; "comedian" for an entry about a comedian).
- source: a specific work, franchise, game, show, film, song, band, company, product or event the meme comes from or refers to (e.g. The Matrix, Pawn Stars, Call of Duty, the 2016 Summer Olympics).
- format: the kind of meme or media it is, when the entry says so (image macro, copypasta, snowclone, reaction image, exploitable, phrasal template, parody, remix, viral video, fan art, webcomic).
- platform: a website, app, platform or online community where it started or spread.
- incidental: true, but not what the meme is: a general category or role used in passing ("feature film", "episode", "television program", "short story", "YouTuber", "man"); an abstract notion (belief, culture, popularity, criticism, time); a year, decade, city, country or nationality the meme is not about; a source that merely reports on it (a news site quoted); a detail of a described picture that is not its point; the generic words "meme", "internet meme" or "Internet", unless the entry is about memes or the Internet as such.
- wrong_sense: the item's description is not what the entry means by those words (e.g. a film called "Work" for the common word "work"; a city for a person's name).

Tags were chosen by the site's editors to say what the entry is about. A tag naming a specific person, character, work, franchise, band, company, product or event is almost always subject or source, even when the About does not mention it, unless its description is a different thing than the tag means.

Answer with JSON only: {"items": [{"n": 1, "role": "subject"}, {"n": 2, "role": "incidental"}, ...]}, every number exactly once."""

USER_TMPL = """ENTRY: {title}
Tags: {tags}
Origin: {origin}

About:
{about}

ITEMS:
{items}"""


def load_schema(path: str) -> tuple[dict[str, Any], str]:
    with open(path, "rb") as fh:
        raw = fh.read()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()[:16]


def request_format(schema: dict[str, Any], n_items: int) -> dict[str, Any]:
    """The grammar, with ``n`` narrowed to this call's numbers and the list
    to exactly that many rows — without the bound a model can keep writing
    rows past the last number until the token cap (3 frames in 50)."""
    fmt = copy.deepcopy(schema)
    for key in ("$schema", "title", "description"):
        fmt.pop(key, None)
    rows = fmt["properties"]["items"]
    rows["minItems"] = rows["maxItems"] = n_items
    rows["items"]["properties"]["n"] = {
        "type": "integer", "enum": list(range(1, n_items + 1))}
    return fmt


def model_request(env: Mapping[str, str] | None = None):
    from modules.openwebui_client import ModelRequest
    return ModelRequest.from_env("KG_CURATION", default_model=DEFAULT_JUDGE_MODEL, env=env)


def render_items(items: Sequence[dict]) -> str:
    lines = []
    for n, it in enumerate(items, 1):
        where = " and ".join({"about": "the About", "tag": "a tag"}.get(f, f)
                             for f in it["fields"])
        said = ", ".join(f'"{t}"' for t in it["texts"][:3])
        desc = f" — {it['description']}" if it.get("description") else ""
        line = f"{n}. {it['label']}{desc} (read from {said} in {where})"
        if it.get("snippet"):
            line += f"\n   context: {it['snippet']}"
        lines.append(line)
    return "\n".join(lines)


def make_validator(schema: dict[str, Any], n_items: int) -> Callable[[str], dict[int, str]]:
    """The reply -> {n: role}; every number exactly once, or ValueError."""
    import jsonschema

    checker = jsonschema.Draft202012Validator(schema)

    def validate(content: str) -> dict[int, str]:
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError(f"not JSON: {exc}") from None
        errors = sorted(checker.iter_errors(data), key=lambda e: list(e.path))
        if errors:
            raise ValueError(f"schema: {errors[0].message} at {list(errors[0].path)}")
        got: dict[int, str] = {}
        for row in data["items"]:
            n = int(row["n"])
            if not 1 <= n <= n_items:
                raise ValueError(f"item {n} is not one of 1..{n_items}")
            if n in got:
                raise ValueError(f"item {n} answered twice")
            got[n] = row["role"]
        missing = set(range(1, n_items + 1)) - set(got)
        if missing:
            raise ValueError(f"items {sorted(missing)} not answered")
        return got

    return validate


def about_text(entry: Mapping[str, Any]) -> str:
    """The About exactly as the linker read it (mention offsets index it)."""
    from modules.kg.entities import _about_text

    return _about_text(entry)


def frame_context(entry: Mapping[str, Any]) -> dict[str, str]:
    """What the judge reads about the frame, from its ``entries`` doc."""
    about = about_text(entry)
    if len(about) > ABOUT_CHARS:
        about = about[:ABOUT_CHARS].rsplit(" ", 1)[0] + " …"
    return {"title": entry.get("title") or "",
            "tags": ", ".join((entry.get("tags") or [])[:30]) or "(none)",
            "origin": entry.get("origin") or "(not stated)",
            "about": about or "(no About section)"}


def _ask(client, request, purpose: str, context: Mapping[str, str], items: Sequence[dict],
         schema: dict[str, Any], system: str | None = None) -> dict[str, Any]:
    """One model over ``items`` (in batches of at most MAX_JUDGE_ITEMS) ->
    ``{ok, roles {qid: role}, model, digest, host, attempts, error,
    error_kind}``; stops at the first failed batch."""
    roles: dict[str, str] = {}
    meta: dict[str, Any] = {"model": None, "digest": None, "host": None, "attempts": 0}
    for i in range(0, len(items), MAX_JUDGE_ITEMS):
        batch = list(items[i:i + MAX_JUDGE_ITEMS])
        messages = [{"role": "system", "content": system or SYSTEM_PROMPT},
                    {"role": "user", "content": USER_TMPL.format(
                        **context, items=render_items(batch))}]
        result = client.chat(messages, request, purpose=purpose,
                             format=request_format(schema, len(batch)),
                             options={"temperature": 0,
                                      # an answer is ~12 tokens an item,
                                      # more when pretty-printed (64 + 24 a
                                      # item cut 3 frames in 50); a runaway
                                      # (ministral once wrote 15k
                                      # characters) is cut and retried
                                      "num_predict": 256 + 32 * len(batch)},
                             think=False,
                             validate=make_validator(schema, len(batch)))
        meta["attempts"] += result.attempts or 0
        if not result.ok:
            return {"ok": False, "roles": roles, **meta,
                    "error": result.error, "error_kind": result.error_kind}
        meta.update(model=result.model, digest=result.digest, host=result.host)
        for n, role in result.parsed.items():
            roles[batch[n - 1]["qid"]] = role
    return {"ok": True, "roles": roles, **meta, "error": None, "error_kind": None}


def needs_confirming(item: Mapping[str, Any]) -> bool:
    """Only an item read from the About alone is confirmed: that is where
    one judge keeps what is merely mentioned (a city, "photograph", a film
    named like a common word). Tags are the editors' keywords."""
    return set(item["fields"]) <= {"about"}


def judge_frame(client, request, context: Mapping[str, str], items: Sequence[dict], *,
                schema: dict[str, Any], confirm=None) -> dict[str, Any]:
    """Ask the judge about one frame's undecided items and, when ``confirm``
    (a second ModelRequest) is given, ask it — with CONFIRM_PROMPT — about
    the About-only items the judge kept: those stay kept only if it keeps
    them too.

    Returns ``{ok, verdicts {qid: keep}, roles {qid: role}, confirm {roles,
    model, digest, host} | None, model, digest, host, attempts, error,
    error_kind, elapsed_s}`` — a failure (of either model) is data, as in
    kg/events.extract."""
    started = time.monotonic()
    first = _ask(client, request, JUDGE_PURPOSE, context, items, schema)
    roles = first.pop("roles")
    verdicts = {q: r in KEEP_ROLES for q, r in roles.items()}
    out: dict[str, Any] = {"verdicts": verdicts, "roles": roles, "confirm": None, **first}
    if first["ok"] and confirm is not None:
        doubted = [it for it in items if verdicts.get(it["qid"]) and needs_confirming(it)]
        if doubted:
            second = _ask(client, confirm, CONFIRM_PURPOSE, context, doubted, schema,
                          system=CONFIRM_PROMPT)
            out["attempts"] += second["attempts"]
            out["confirm"] = {k: second[k] for k in ("roles", "model", "digest", "host")}
            if second["ok"]:
                for q, r in second["roles"].items():
                    verdicts[q] = verdicts[q] and r in KEEP_ROLES
            else:
                out.update(ok=False, error=f"confirm: {second['error']}",
                           error_kind=second["error_kind"])
    out["elapsed_s"] = round(time.monotonic() - started, 2)
    return out


def confirm_request(env: Mapping[str, str] | None = None):
    from modules.openwebui_client import ModelRequest
    return ModelRequest.from_env("KG_CURATION_CONFIRM", default_model=DEFAULT_CONFIRM_MODEL,
                                 env=env)


def judge_stamps(request, schema_sha: str, confirm=None) -> dict[str, str]:
    """What a judge verdict was given under; any of them moving re-asks."""
    return {"judge_prompt_version": JUDGE_PROMPT_VERSION, "judge_schema_sha": schema_sha,
            "judge_model": request.model or "",
            "judge_confirm_model": (confirm.model or "") if confirm is not None else ""}


def rule_stamps(lists: Lists, lexicon_version: str) -> dict[str, str]:
    return {"curation_version": CURATION_VERSION, "curation_lists_version": lists.version,
            "lexicon_version": lexicon_version}
