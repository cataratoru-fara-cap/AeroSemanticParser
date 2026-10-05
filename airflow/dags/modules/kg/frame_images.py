"""
kg/frame_images.py — what a KYM entry's own image shows, linked to Wikidata
============================================================================
The frame counterpart of kg/template_entities.py, and the step that makes
IMKG's ``m4s:fromImage`` mean here what it meant there. IMKG ran Google
Vision on each Know Your Meme entry's image and linked what it found to the
MEDIA FRAME; its paper's first use case (Tommasini, Ilievski &
Wijesiriwardene, ESWC 2023, Table 4) asks for the frames whose image shows
SpongeBob with exactly that edge. Until 7.1.0 MemeAtlas read only imgflip
templates, so the query found nothing (Gabi, 2026-10-05: read each
frame's image).

The image is the entry's ``og:image`` — KYM's entry icon
(``i.kym-cdn.com/entries/icons/...``), the picture at the top of the page.
Every one of the 24,291 frames in 7.0.0 has one, and none is the NSFW cover
of gap 12 (that one replaces SECTION images).

Reused from kg/template_entities.py as they are: the output grammar
(kg_config/template_entity_schema.json), grounding, the audit, the image
preparation and the linking — so a frame region and a template region
mean the same thing and reach the graph by the same rule (every named
entity and printed name, plus the GENERIC_IN_GRAPH largest generic ones).
Only the prompt and the context differ: an entry image is a photo, a
screenshot, a comic, a meme example or a logo, not a blank template.

Kept OUT of kg/template_entities.py on purpose: its version constants are
staleness stamps, and touching them would re-read all 26,871 templates.

Pure, like its sibling: the network only through the client it is handed.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any, Iterable, Mapping

from modules.kg import template_entities as te
from modules.openwebui_client import ModelRequest

READER_VERSION = "1.0.0"
PROMPT_VERSION = "1"
DETECT_PURPOSE = "kg.frames.detect"
# 1 in 100 frames is also read with no context, like templates (te.BLIND_SAMPLE_MOD).
BLIND_SAMPLE_MOD = te.BLIND_SAMPLE_MOD

SYSTEM_PROMPT = """You catalogue what the main image of a Know Your Meme entry shows, for a knowledge graph. The image may be a photo, a screenshot, a video still, a drawing, a comic, an example of the meme, or a logo.

List every distinct thing visible in the image:
- people and fictional characters: give their NAME only if you recognise them for certain (e.g. "Donald Trump", "Kermit the Frog", "SpongeBob SquarePants"); otherwise describe them plainly ("man", "woman", "girl") and set named to false;
- animals, notable objects, logos and brands, artworks;
- any text printed in the image: kind "text", name "text", with the exact words in "text".

"named" is true ONLY for the proper name of one specific person, character, brand, work or place. A kind of thing is never named: "cat", "dog", "car", "phone", "stuffed toy" are named false.

For each, give a bounding box [x0, y0, x1, y1] on a 0-1000 grid of the whole image (0,0 is the top-left corner, 1000,1000 the bottom-right).

Do not list the background, the image border, watermarks of the website, or the image as a whole. At most 12 items, largest first. Answer with JSON only."""

USER_TMPL = """Entry: {title}
Kind of entry: {category}
What the entry is about: {about}

List what this image shows."""

BLIND_USER_TMPL = te.BLIND_USER_TMPL


def frame_context(entry: Mapping[str, Any]) -> dict[str, str]:
    """What the model is told: the entry's title, its category, and the
    opening of its About — the same help a template gets from its frame."""
    about = next((s for s in entry.get("sections") or [] if s.get("kind") == "about"), {})
    text = " ".join(" ".join(about.get("text") or []).split())
    return {"title": str(entry.get("title") or ""),
            "category": str(entry.get("category") or "entry"),
            "about": text[:600] or "(not described)"}


def messages_for(image_b64: str, ctx: Mapping[str, str] | None) -> list[dict]:
    user = BLIND_USER_TMPL if ctx is None else USER_TMPL.format(**ctx)
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user, "images": [image_b64]}]


def in_blind_sample(frame_url: str) -> bool:
    """Deterministic: the same frames are audited on every run."""
    return int(hashlib.sha1(frame_url.encode("utf-8")).hexdigest(), 16) % BLIND_SAMPLE_MOD == 0


def model_request(env: Mapping[str, str] | None = None) -> ModelRequest:
    """The template reader's model and settings (KG_TEMPLATES_VLM_*): one
    vision model for every image in the graph."""
    return te.model_request(env)


def detect(client, request: ModelRequest, unit: dict, image: bytes, *,
           schema: dict[str, Any], schema_sha: str) -> dict:
    """One frame -> its detection record (``ok`` False on failure, with the
    error; failures are data). ``unit``: {frame_url, image_url, context}."""
    from modules.kg.visual import ImageDecodeError   # numpy: loaded only to read

    started = time.monotonic()
    stamps = {
        "frame_url": unit["frame_url"], "image_url": unit["image_url"],
        "image_sha256": hashlib.sha256(image).hexdigest(),
        "context_sha": te.context_sha(unit["context"]),
        "reader_version": READER_VERSION, "prompt_version": PROMPT_VERSION,
        "schema_sha": schema_sha, "requested_model": request.model,
    }
    try:
        image_b64, width, height = te.prepare_image(image)
    except ImageDecodeError as exc:
        return {**stamps, "detected_at": te._now(), "ok": False, "error_kind": "image",
                "error": str(exc), "attempts": 0,
                "elapsed_s": round(time.monotonic() - started, 2)}
    validate = te.make_validator(schema)
    fmt = te.request_format(schema)

    def call(ctx):
        return client.chat(messages_for(image_b64, ctx), request, purpose=DETECT_PURPOSE,
                           format=fmt, options={"temperature": 0,
                                                "num_predict": te.MAX_OUTPUT_TOKENS},
                           think=False, validate=validate,
                           retry_temperature=te.RETRY_TEMPERATURE)

    result = call(unit["context"])
    record: dict[str, Any] = {**stamps, "image_size": [width, height],
                              "detected_at": te._now()}
    if not result.ok:
        return {**record, "ok": False, "error_kind": result.error_kind,
                "error": result.error, "attempts": result.attempts,
                "elapsed_s": round(time.monotonic() - started, 2)}
    record.update(ok=True, regions=result.parsed["regions"],
                  dropped=result.parsed["dropped"], model=result.model,
                  digest=result.digest, host=result.host, attempts=result.attempts)
    if in_blind_sample(unit["frame_url"]):
        blind = call(None)
        record["blind"] = ({"regions": blind.parsed["regions"], "model": blind.model}
                           if blind.ok else {"error": blind.error,
                                             "error_kind": blind.error_kind})
    record["elapsed_s"] = round(time.monotonic() - started, 2)
    problems = audit_detection(record)
    if problems:
        raise AssertionError(f"frame {unit['frame_url']} failed its audit: {problems}")
    return record


def audit_detection(record: dict) -> list[str]:
    """The template audit, with the frame's own stamps."""
    problems = te.audit_detection({**record, "extractor_version": record.get("reader_version")})
    if not record.get("frame_url"):
        problems.append("missing frame_url")
    return problems


def link_detection(linker, detection: dict, *, context_text: str,
                   prefer: Iterable[int] = (), link_context_sha: str = "") -> dict:
    """A frame's detection -> its links record, by the template rule
    (te.link_detection), keyed by the frame."""
    record = te.link_detection(linker, {**detection, "template_id": None},
                               context_text=context_text, prefer=prefer,
                               link_context_sha=link_context_sha)
    record.pop("template_id", None)
    record["frame_url"] = detection["frame_url"]
    return record


detection_sha = te.detection_sha
blind_agreement = te.blind_agreement
