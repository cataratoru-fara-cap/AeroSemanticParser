"""
template_entities.py — what a meme template shows, linked to Wikidata
======================================================================
The third model-reading module in kg/ (after semantics.py and events.py).
It reaches the network only through the OpenWebUIClient it is handed, so
importing it reads no configuration and contacts nothing. Persistence is
modules/template_entity_store.py; orchestration dags/kym_template_entities_dag.py.

What it does
------------
IMKG linked a frame's IMAGE to Wikidata with Google Vision labels and web
entities (``m4s:fromImage``). MemeAtlas does the same for imgflip TEMPLATES,
with the lab's vision model instead of a paid API (Gabi, 2026-09-28: the
"segmentall" step is a vision model that names what it sees and boxes it —
no segmentation masks):

1. **Detect** (``detect``): one ``qwen3-vl:32b`` call per template image.
   The model returns every distinct thing it sees — people and characters
   by name when it recognises them, animals, objects, logos, artworks, and
   text printed in the image — each with a box on a 0-1000 grid. Output is
   constrained by kg_config/template_entity_schema.json (the ``format=``
   grammar) and checked again here.
2. **Ground** (``ground``): boxes must be real rectangles of a sensible
   size, names must not be placeholders ("meme", "caption"), text regions
   must carry their text. Anything else is dropped and counted — the model
   never gets to store something the pipeline did not check.
3. **Link** (``link_detection``): each region's name goes to the same
   local Wikidata lexicon the frame layer uses (kg/entities.Linker
   .link_label), with the frame's own text as context and the items the
   frame already links as the preferred senses. Printed text is read with
   the frame pipeline's recognition (named entities and proper nouns).

What reaches the graph (Gabi): every NAMED entity and every link read from
printed text, plus the ``GENERIC_IN_GRAPH`` largest generic ones ("man",
"cat") per template. Everything is kept in Mongo for curation (gap 09).

Context, and the blind audit
----------------------------
The model is told the template's name and the frame's title and opening
About text: it names "Drake" more reliably when told the meme is about
Drake. The risk is the reverse — naming Drake BECAUSE it was told. So a
deterministic 1% of templates (``in_blind_sample``) is also read with no
context at all, and ``blind_agreement`` counts the named entities only the
context run produced. It is a measurement, not a switch: on the 200-template
pilot 3 of 23 names were context-only (13%) and all 3 were RIGHT (context
gave "Ajit Pai" where the blind read said "man", and "Principal Skinner"
where it said "Kirk Van Houten"), so Gabi kept the context names
(2026-09-29) rather than the planned switch to agreed names above 10%.
Read the context-only names when the rate moves; the summary records it.

Box coordinates
---------------
The schema asks for a 0-1000 grid. Qwen2.5-VL answered in pixels of the
image it was sent, Qwen3-VL on a 0-1000 grid; ``BOX_GRID`` records the
convention, checked on drawn overlays before the full run (phase-2 spike).
Stored boxes are fractions of the image (0..1), whatever the model used.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import logging
import re
import time
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Sequence

from modules.openwebui_client import ModelRequest

log = logging.getLogger(__name__)

EXTRACTOR_VERSION = "1.1.0"
PROMPT_VERSION = "2"
DEFAULT_VLM_MODEL = "qwen3-vl:32b"
DETECT_PURPOSE = "kg.templates.detect"

# Downscaled before sending: detail beyond this does not change what the
# model names, and a smaller image keeps a call under Open WebUI's 50 s cut.
MAX_IMAGE_SIDE = 768
# The most the grammar allows: 12 regions of an 80-character name and a
# 300-character text, pretty-printed, ~1,800 tokens. 700 cut 2 templates in
# 30 on the 2026-09-29 spike, 1400 cut 2 blind reads of long text in 200.
MAX_OUTPUT_TOKENS = 2000
MAX_REGIONS = 12
# A box under this share of the image is a speck, not an entity.
MIN_BOX_AREA = 0.003
# The grid the model's boxes are on; see "Box coordinates".
BOX_GRID = 1000

KINDS = ("person", "character", "animal", "object", "text", "logo", "artwork", "other")
# The region kind as a hint to the linker's NER-type agreement (a hint only:
# kg/entities never lets a type veto a link).
NER_HINT = {"person": "PERSON", "character": "PERSON", "logo": "ORG",
            "artwork": "WORK_OF_ART"}
PLACEHOLDER_NAMES = frozenset({
    "meme", "template", "caption", "image", "picture", "photo", "photograph",
    "text box", "textbox", "blank", "panel", "background", "frame", "border",
    "space", "white space", "empty space", "blank space", "unknown", "none"})
# Names that say "there is text here" rather than being the text: a text
# region named anything else, with no "text", carries its words in the name
# (qwen3-vl:32b did that for "Panik", "Kalm" — spike, 2026-09-29).
TEXT_WORDS = frozenset({
    "text", "caption", "captions", "words", "writing", "label", "sign", "title",
    "subtitle", "subtitles", "watermark", "handwriting", "lettering"})
GENERIC_IN_GRAPH = 3
DUPLICATE_IOU = 0.8

# The blind audit: templates whose id is divisible by this are also read
# with no context. Deterministic, so a re-run audits the same templates.
# 1 in 100 (was 1 in 10 for the pilot): over the ~50-65k-template pool that
# is still ~500-650 audited, and it saves ~9% of the calls (2026-09-29).
BLIND_SAMPLE_MOD = 100
# A rejected reading (a runaway list) is retried once at this temperature:
# at temperature 0 the same request only gives the same answer again.
RETRY_TEMPERATURE = 0.4
BLIND_AGREE_IOU = 0.5

SYSTEM_PROMPT = """You catalogue what a blank meme template image shows, for a knowledge graph.

List every distinct thing visible in the image:
- people and fictional characters: give their NAME only if you recognise them for certain (e.g. "Drake", "Kermit the Frog", "SpongeBob SquarePants"); otherwise describe them plainly ("man", "woman", "girl") and set named to false;
- animals, notable objects, logos and brands, artworks;
- any text printed in the image: kind "text", name "text", with the exact words in "text".

"named" is true ONLY for the proper name of one specific person, character, brand, work or place. A kind of thing is never named: "cat", "caracal", "door", "towel", "stuffed toy" are named false.

For each, give a bounding box [x0, y0, x1, y1] on a 0-1000 grid of the whole image (0,0 is the top-left corner, 1000,1000 the bottom-right).

Do not list empty caption space, the background, the image border, or the meme as a whole. At most 12 items, largest first. Answer with JSON only."""

USER_TMPL = """Template name: {name}
Also called: {alt_names}
It is used for the meme "{frame_title}". What the meme is: {about}

List what this image shows."""

BLIND_USER_TMPL = "List what this image shows."


# ---------------------------------------------------------------------------
# Schema and request
# ---------------------------------------------------------------------------

def load_schema(path: str) -> tuple[dict[str, Any], str]:
    """(schema, sha256[:16] of the file). The sha is a staleness stamp."""
    with open(path, "rb") as fh:
        raw = fh.read()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()[:16]


def request_format(schema: dict[str, Any]) -> dict[str, Any]:
    """The grammar for Ollama's ``format=``: the schema without its prose,
    and at most MAX_REGIONS entities — ground() keeps no more, and an
    unbounded list lets the model write until the token cap."""
    fmt = copy.deepcopy(schema)
    for key in ("$schema", "title", "description"):
        fmt.pop(key, None)
    fmt["properties"]["entities"]["maxItems"] = MAX_REGIONS
    return fmt


def model_request(env: Mapping[str, str] | None = None) -> ModelRequest:
    """KG_TEMPLATES_VLM_MODEL / _TIER / _SPECIALIZATION, defaulting to
    qwen3-vl:32b. Unlike kym_events, a model that CAN think is allowed —
    qwen3-vl:32b reports the capability — so thinking is switched off per
    call (``think=False``) instead of excluding the model."""
    base = ModelRequest.from_env("KG_TEMPLATES_VLM", default_model=DEFAULT_VLM_MODEL,
                                 env=env)
    # No fallback to another model: the same weights on any host serve (so
    # the work can be spread over hosts), a different vision model never.
    return replace(base, capabilities=base.capabilities | {"vision"}, allow_fallback=False)


# ---------------------------------------------------------------------------
# Image and context
# ---------------------------------------------------------------------------

def prepare_image(data: bytes) -> tuple[str, int, int]:
    """(base64 JPEG no larger than MAX_IMAGE_SIDE, width, height)."""
    from modules.kg import visual

    img = visual.decode(data)
    img.thumbnail((MAX_IMAGE_SIDE, MAX_IMAGE_SIDE))
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=90)
    return base64.b64encode(buf.getvalue()).decode("ascii"), img.width, img.height


def template_context(template: dict, frames: Sequence[dict]) -> dict[str, str]:
    """What the model is told: the template's names and the frame it serves
    best (highest selection score), with the opening of its About."""
    best = max(frames, key=lambda f: f.get("R", 0.0), default={})
    about = " ".join((best.get("about") or "").split())
    return {"name": template.get("name") or "",
            "alt_names": ", ".join((template.get("alt_names") or [])[:8]) or "(none)",
            "frame_title": best.get("title") or "",
            "about": about[:600] or "(not described)"}


def context_sha(ctx: Mapping[str, str]) -> str:
    return hashlib.sha256(json.dumps(dict(ctx), sort_keys=True).encode()).hexdigest()[:16]


def messages_for(image_b64: str, ctx: Mapping[str, str] | None) -> list[dict]:
    user = BLIND_USER_TMPL if ctx is None else USER_TMPL.format(**ctx)
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user, "images": [image_b64]}]


def in_blind_sample(template_id: int) -> bool:
    return int(template_id) % BLIND_SAMPLE_MOD == 0


# ---------------------------------------------------------------------------
# Validation and grounding
# ---------------------------------------------------------------------------

def normalise_box(box: Sequence[int | float], grid: int = BOX_GRID) -> list[float] | None:
    """A model box -> [x0, y0, x1, y1] as fractions of the image, or None
    when it is not a real rectangle inside the image."""
    if len(box) != 4:
        return None
    x0, y0, x1, y1 = (float(v) / grid for v in box)
    if not (0.0 <= x0 < x1 <= 1.0 and 0.0 <= y0 < y1 <= 1.0):
        return None
    return [round(x0, 4), round(y0, 4), round(x1, 4), round(y1, 4)]


def area(box: Sequence[float]) -> float:
    return max(0.0, box[2] - box[0]) * max(0.0, box[3] - box[1])


def iou(a: Sequence[float], b: Sequence[float]) -> float:
    ix = max(0.0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    union = area(a) + area(b) - inter
    return inter / union if union > 0 else 0.0


def _printed_words(e: Mapping[str, Any], name: str) -> str:
    """A text region's words: its ``text``, else its name when the name is
    not just a word for text."""
    words = " ".join(str(e.get("text") or "").split())
    if words.lower() in TEXT_WORDS:          # "text": a word for text, not text
        words = ""
    if not words and name.lower() not in TEXT_WORDS:
        words = name
    return words


def ground(entities: Iterable[Mapping[str, Any]]) -> tuple[list[dict], list[dict]]:
    """(regions kept, regions dropped with the reason).

    Kept regions: ``name``, ``kind``, ``named``, ``box`` (fractions),
    ``area``, ``text`` (text regions only), ``confidence``, ``index`` (the
    model's own order). Two regions with the same name that overlap by
    more than DUPLICATE_IOU are one region.
    """
    kept: list[dict] = []
    dropped: list[dict] = []
    for i, e in enumerate(entities):
        name = " ".join(str(e.get("name") or "").split())
        kind = e.get("kind")
        why = None
        box = normalise_box(e.get("box") or [])
        if not name:
            why = "empty name"
        elif name.lower() in PLACEHOLDER_NAMES:
            why = "placeholder name"
        elif kind not in KINDS:
            why = f"unknown kind {kind!r}"
        elif box is None:
            why = "not a box inside the image"
        elif area(box) < MIN_BOX_AREA:
            why = "box too small"
        elif kind == "text" and not _printed_words(e, name):
            why = "text region without text"
        if why:
            dropped.append({"index": i, "name": name, "reason": why})
            continue
        # named only when the name is a name: a model's "named" on "door" or
        # "caracal" (spike, 2026-09-29) is not taken at its word
        named = bool(e.get("named")) and any(ch.isupper() for ch in name)
        region = {"index": i, "name": name, "kind": kind, "named": named,
                  "box": box, "area": round(area(box), 4),
                  "confidence": e.get("confidence") or "low"}
        if kind == "text":
            region["text"] = _printed_words(e, name)
            region["named"] = False
        if any(r["name"].lower() == name.lower() and iou(r["box"], box) > DUPLICATE_IOU
               for r in kept):
            dropped.append({"index": i, "name": name, "reason": "duplicate region"})
            continue
        kept.append(region)
        if len(kept) >= MAX_REGIONS:
            break
    return kept, dropped


def make_validator(schema: dict[str, Any]) -> Callable[[str], dict]:
    """The ``validate=`` for one chat call: JSON, then the schema, then
    grounding. Returns {"regions", "dropped"}; raises ValueError on output
    that is not the schema's shape (the client retries it)."""
    import jsonschema

    checker = jsonschema.Draft202012Validator(schema)

    def validate(content: str) -> dict:
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError(f"not JSON: {exc}") from None
        errors = sorted(checker.iter_errors(data), key=lambda e: list(e.path))
        if errors:
            raise ValueError(f"schema: {errors[0].message} at {list(errors[0].path)}")
        regions, dropped = ground(data["entities"])
        return {"regions": regions, "dropped": dropped}

    return validate


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def detect(client, request: ModelRequest, unit: dict, image: bytes, *,
           schema: dict[str, Any], schema_sha: str) -> dict:
    """One template -> its detection record (``ok`` False on failure, with
    the error — failures are data, as in kg/events.extract).

    ``unit``: {template_id, image_sha256, image_source, context, frames}.
    The blind re-read runs for templates in the audit sample.
    """
    started = time.monotonic()
    image_b64, width, height = prepare_image(image)
    validate = make_validator(schema)
    fmt = request_format(schema)

    def call(ctx):
        return client.chat(messages_for(image_b64, ctx), request, purpose=DETECT_PURPOSE,
                           format=fmt, options={"temperature": 0,
                                                "num_predict": MAX_OUTPUT_TOKENS},
                           think=False, validate=validate,
                           retry_temperature=RETRY_TEMPERATURE)

    result = call(unit["context"])
    record: dict[str, Any] = {
        "template_id": unit["template_id"], "image_sha256": unit["image_sha256"],
        "image_source": unit.get("image_source"), "image_size": [width, height],
        "context_sha": context_sha(unit["context"]),
        "extractor_version": EXTRACTOR_VERSION, "prompt_version": PROMPT_VERSION,
        "schema_sha": schema_sha, "requested_model": request.model,
        "detected_at": _now(),
    }
    if not result.ok:
        return {**record, "ok": False, "error_kind": result.error_kind,
                "error": result.error, "attempts": result.attempts,
                "elapsed_s": round(time.monotonic() - started, 2)}
    record.update(ok=True, regions=result.parsed["regions"],
                  dropped=result.parsed["dropped"], model=result.model,
                  digest=result.digest, host=result.host, attempts=result.attempts)
    if in_blind_sample(unit["template_id"]):
        blind = call(None)
        record["blind"] = ({"regions": blind.parsed["regions"], "model": blind.model}
                           if blind.ok else {"error": blind.error,
                                             "error_kind": blind.error_kind})
    record["elapsed_s"] = round(time.monotonic() - started, 2)
    problems = audit_detection(record)
    if problems:
        # A grounding bug, not model behaviour: never store it.
        raise AssertionError(f"template {unit['template_id']} failed its audit: {problems}")
    return record


def audit_detection(record: dict) -> list[str]:
    problems: list[str] = []
    for key in ("extractor_version", "prompt_version", "schema_sha", "image_sha256",
                "model", "digest"):
        if not record.get(key):
            problems.append(f"missing {key}")
    for r in record.get("regions") or []:
        if r["kind"] not in KINDS:
            problems.append(f"region {r['index']}: kind {r['kind']!r}")
        b = r["box"]
        if not (0 <= b[0] < b[2] <= 1 and 0 <= b[1] < b[3] <= 1):
            problems.append(f"region {r['index']}: box {b}")
        if r["name"].lower() in PLACEHOLDER_NAMES:
            problems.append(f"region {r['index']}: placeholder {r['name']!r}")
        if r["kind"] == "text" and not r.get("text"):
            problems.append(f"region {r['index']}: text region without text")
    if len(record.get("regions") or []) > MAX_REGIONS:
        problems.append("too many regions")
    return problems


def blind_agreement(regions: Sequence[dict], blind: Sequence[dict]) -> dict[str, int]:
    """How many of the context run's NAMED regions the blind run also found:
    the same name (whatever kind each run gave it — George Washington as
    "person" in one and "artwork" in the other is one finding), or the same
    kind in an overlapping box (BLIND_AGREE_IOU)."""
    named = [r for r in regions if r["named"]]
    confirmed = 0
    for r in named:
        if any(b["name"].lower() == r["name"].lower()
               or (b["kind"] == r["kind"] and iou(b["box"], r["box"]) >= BLIND_AGREE_IOU)
               for b in blind):
            confirmed += 1
    return {"named": len(named), "confirmed": confirmed,
            "context_only": len(named) - confirmed}


# ---------------------------------------------------------------------------
# Linking
# ---------------------------------------------------------------------------

LINK_METHODS = ("vlm_named", "vlm_generic", "frame_agree", "title", "ner", "propn")
# How a template's regions are turned into links; a change re-links every
# template without re-reading it (template_entity_store.LINK_STAMP_KEYS).
# 1.1.0: printed text links only names (text_link_ok).
TEMPLATE_LINK_VERSION = "1.1.0"
TEXT_LINK_MIN_CHARS = 4


def text_link_ok(mention: Mapping[str, Any], printed: str) -> bool:
    """Whether a link read from PRINTED TEXT is worth keeping. On the
    200-template pilot (2026-09-29) half of them were wrong: single
    characters ("H" -> hydrogen, "3"), caption words in capitals read as
    names by the NLP model ("NEAT" -> Near-Earth Asteroid Tracking, "SIZE"
    -> cardinality), common words linked whole ("RIGHT" -> right-wing,
    "HELLO," -> greeting). Kept: a NAME (the item's label is capitalised,
    Wikidata's convention), from a span of at least TEXT_LINK_MIN_CHARS
    letters or digits, and not a named-entity/proper-noun guess inside
    all-capital text, where every word looks like a name. 12 of 14 kept
    were right, against 19 of 38 before."""
    label = mention.get("label") or ""
    if not label[:1].isupper():
        return False
    if len(re.sub(r"[^0-9A-Za-z]", "", mention.get("text") or "")) < TEXT_LINK_MIN_CHARS:
        return False
    shouting = printed.upper() == printed and any(ch.isalpha() for ch in printed)
    return not (shouting and mention.get("method") in ("ner", "propn"))


def link_detection(linker, detection: dict, *, context_text: str,
                   prefer: Iterable[int] = (), link_context_sha: str = "") -> dict:
    """A detection record -> its links record.

    ``context_text``: the template's names and the frames' titles and
    About text, whose words disambiguate. ``prefer``: the QIDs (as ints)
    the frames already link. Each mention carries its region's index, kind,
    named flag, box and confidence, and ``in_graph``.
    """
    started = time.monotonic()
    context = linker.words(context_text)
    prefer = set(prefer)
    mentions: list[dict] = []
    nil: list[dict] = []
    rejected = 0
    for r in detection.get("regions") or []:
        region = {k: r[k] for k in ("index", "kind", "named", "box", "area", "confidence")}
        if r["kind"] == "text":
            for m in linker.link_spans(r["text"], field="image_text", context=context):
                if text_link_ok(m, r["text"]):
                    mentions.append({**m, "region": region, "source": "text"})
                else:
                    rejected += 1
            continue
        method = "vlm_named" if r["named"] else "vlm_generic"
        m, outcome = linker.link_label(r["name"], field="image", context=context,
                                       ner_label=NER_HINT.get(r["kind"]), prefer=prefer,
                                       method=method)
        if m is not None:
            mentions.append({**m, "region": region, "source": "named" if r["named"]
                             else "generic"})
        elif outcome == "nil":
            nil.append({"index": r["index"], "name": r["name"], "kind": r["kind"],
                        "named": r["named"]})
        else:
            rejected += 1
    mark_in_graph(mentions)
    return {"template_id": detection["template_id"], "mentions": mentions,
            "mention_count": len(mentions),
            "in_graph_count": sum(m["in_graph"] for m in mentions),
            "nil": nil, "rejected_count": rejected,
            "detection_sha": detection_sha(detection), "link_context_sha": link_context_sha,
            **linker.stamps, "template_link_version": TEMPLATE_LINK_VERSION,
            "linked_at": _now(),
            "elapsed_s": round(time.monotonic() - started, 4)}


def mark_in_graph(mentions: list[dict]) -> None:
    """Gabi: every named entity and every link read from printed text, plus
    the GENERIC_IN_GRAPH largest generic ones. One item linked from several
    regions counts once, at its largest region."""
    for m in mentions:
        m["in_graph"] = m["source"] in ("named", "text")
    generic = sorted((m for m in mentions if m["source"] == "generic"),
                     key=lambda m: (-m["region"]["area"], m["region"]["index"]))
    seen: set[str] = {m["qid"] for m in mentions if m["in_graph"]}
    kept = 0
    for m in generic:
        if kept >= GENERIC_IN_GRAPH:
            break
        if m["qid"] in seen:
            continue
        m["in_graph"] = True
        seen.add(m["qid"])
        kept += 1


def detection_sha(detection: dict) -> str:
    """What a detection SAYS — the linking staleness key."""
    body = [(r["index"], r["name"], r["kind"], r["named"], r.get("text"))
            for r in detection.get("regions") or []]
    return hashlib.sha256(json.dumps(body).encode()).hexdigest()[:16]
