"""
kym_models.py — Pydantic v2 models for the KYM *scrape* layer.

Contract: everything here is deterministically liftable from raw HTML. No
summaries, no judgement calls. Interpreted fields (a one-line "about", a
"search interest" summary, curated "notable example" titles) are produced by
the downstream extraction layer and live in a separate model — keep the two
schemas apart so validation never asks the scraper for something it can't
honestly produce.

Design notes tied to the 28,799-entry dump:
  * `sections` is an ORDERED list, not a heading-keyed dict. Keying by heading
    text produced ~3000 one-off keys (embed JS, whole paragraphs, misspellings).
  * `entry_type` is many-valued and stored as /types/<slug> URLs -> normalise.
  * `status`/`category` coerce unknowns to 'unknown' instead of raising.
  * `year` arrives as a string-or-null -> coerce, never require.
  * Only url/title/category/status are required; ~46% of entries are sparse
    stubs (deadpool/submission/non-Meme) with no content sections.

Fail-loud posture (per project convention): sub-models use extra='forbid', so
an unexpected scraped key surfaces as a validation error rather than silently
vanishing.

Scope: confirmed memes only. Two levels of "required", kept separate on purpose:
  1. Model-required fields (url, title, category, status, origin) — the
     fields structurally guaranteed on a confirmed-meme page (essentially
     100% present across 5,574 confirmed memes). Missing one means a
     malformed scrape; raising is correct.
  2. Corpus-completeness gate (`corpus_ready`) — the documentation bar
     (year, entry_type, region, tags, about/origin/spread sections). This
     FLAGS and reports missing fields instead of raising, so a
     thin-but-valid entry is kept and inspectable rather than discarded
     with its scrape work. `tags` moved here from model-required: at 99.7%
     coverage it looked safe to hard-require, but the remaining 0.3% are
     real confirmed memes that were being rejected outright rather than
     flagged — exactly the failure mode this two-tier split exists to
     avoid. Measured on a 100-page live sample (2026-07): year 99%,
     entry_type 100%, about/origin/spread sections 100%, region 94%, tags
     ~99.7% — all enabled by default as of that measurement.
"""

from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from urllib.parse import urlparse

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
)

_KYM_HOSTS = {"knowyourmeme.com", "www.knowyourmeme.com"}
_TYPE_SLUG_RE = re.compile(r"/types/([a-z0-9-]+)")
# Heuristic: a "heading" longer than this, or containing a trends embed call,
# is DOM garbage the old scraper mistook for a section title.
_MAX_HEADING_LEN = 120
_EMBED_NOISE_RE = re.compile(r"renderexplorewidget|trends\.embed", re.IGNORECASE)


class Status(str, Enum):
    confirmed = "confirmed"
    submission = "submission"
    deadpool = "deadpool"
    researching = "researching"
    unlisted = "unlisted"
    unknown = "unknown"


class Category(str, Enum):
    meme = "meme"
    subculture = "subculture"
    culture = "culture"
    event = "event"
    person = "person"
    site = "site"
    unknown = "unknown"


class SectionKind(str, Enum):
    about = "about"
    origin = "origin"
    spread = "spread"
    search_interest = "search_interest"
    notable_examples = "notable_examples"
    various_examples = "various_examples"
    related_memes = "related_memes"
    external_references = "external_references"
    template = "template"
    other = "other"


class Link(BaseModel):
    model_config = ConfigDict(extra="forbid")
    text: str
    url: HttpUrl


class Image(BaseModel):
    model_config = ConfigDict(extra="forbid")
    src: HttpUrl
    alt: str | None = None
    caption: str | None = None


class Reference(BaseModel):
    model_config = ConfigDict(extra="forbid")
    index: int | None = None
    text: str | None = None
    url: HttpUrl


class NamedReference(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    url: HttpUrl


class Section(BaseModel):
    model_config = ConfigDict(extra="forbid")
    heading: str
    kind: SectionKind
    level: int = Field(..., ge=2, le=3)
    text: list[str] = Field(default_factory=list)
    links: list[Link] = Field(default_factory=list)
    images: list[Image] = Field(default_factory=list)

    @field_validator("heading", mode="before")
    @classmethod
    def _clean_heading(cls, v: str) -> str:
        v = (v or "").strip()
        if _EMBED_NOISE_RE.search(v):
            return "other"
        return v[:_MAX_HEADING_LEN]

    @field_validator("text", mode="before")
    @classmethod
    def _strip_embed_paragraphs(cls, v: list[str]) -> list[str]:
        if not v:
            return []
        return [p for p in v if p and not _EMBED_NOISE_RE.search(p)]


class KYMEntryScrape(BaseModel):
    """Deterministic scrape-layer record for one KYM entry."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    schema_version: str = "1.0.0"

    # --- identity (the only required fields) ---
    url: HttpUrl
    title: str
    category: Category = Category.unknown
    status: Status = Status.unknown

    # --- details sidebar ---
    entry_type: list[str] = Field(default_factory=list)
    year: int | None = Field(
        default=None,
        description="Loosened from an earlier ge=1500: non-meme categories "
                    "(culture/event/person) can have a genuinely pre-1500 "
                    "origin year (e.g. a painting, a historical event used "
                    "as the meme's origin point), and that constraint was "
                    "rejecting the whole page — not just the field — since "
                    "Field bounds apply to any non-None value.")
    origin: str = Field(..., min_length=1)  # required: 100% on confirmed memes
    region: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)  # gated, not required — see CorpusPolicy

    # --- flags / media ---
    badges: list[str] = Field(default_factory=list)
    template_image_url: HttpUrl | None = None
    og_image: HttpUrl | None = None

    # --- relations ---
    parent: HttpUrl | None = None

    # --- references ---
    additional_references: list[NamedReference] = Field(default_factory=list)
    external_references: list[Reference] = Field(default_factory=list)

    # --- narrative body ---
    sections: list[Section] = Field(default_factory=list)

    # --- provenance ---
    meta: dict[str, str] = Field(default_factory=dict)
    kym_last_updated: int | None = None
    kym_added: int | None = None
    scraped_at: datetime | None = None

    # ---- validators / normalizers ----

    @field_validator("url")
    @classmethod
    def _url_is_kym(cls, v: HttpUrl) -> HttpUrl:
        if urlparse(str(v)).netloc not in _KYM_HOSTS:
            raise ValueError(f"entry url is not on knowyourmeme.com: {v}")
        return v

    @field_validator("category", mode="before")
    @classmethod
    def _norm_category(cls, v):
        if v is None:
            return Category.unknown
        s = str(v).strip().lower()
        return s if s in Category._value2member_map_ else Category.unknown

    @field_validator("status", mode="before")
    @classmethod
    def _norm_status(cls, v):
        if v is None:
            return Status.unknown
        s = str(v).strip().lower()
        return s if s in Status._value2member_map_ else Status.unknown

    @field_validator("year", mode="before")
    @classmethod
    def _coerce_year(cls, v):
        # Dump stores year as "2006" or null; tolerate ints, digit strings,
        # and strings with stray text. Anything non-numeric -> None.
        if v is None or v == "":
            return None
        if isinstance(v, int):
            return v
        m = re.search(r"\d{4}", str(v))
        return int(m.group()) if m else None

    @field_validator("entry_type", mode="before")
    @classmethod
    def _slugify_types(cls, v):
        # details.type is a list of /types/<slug> URLs in the dump; keep slugs.
        # Also accept already-clean slugs so the model is re-parseable.
        if not v:
            return []
        out: list[str] = []
        for item in v:
            s = str(item)
            m = _TYPE_SLUG_RE.search(s)
            out.append(m.group(1) if m else s.strip().lower().replace(" ", "-"))
        return out

    @field_validator("tags", "region", "aliases", "badges", mode="before")
    @classmethod
    def _dedupe_keep_order(cls, v):
        if not v:
            return []
        seen: set[str] = set()
        out: list[str] = []
        for item in v:
            s = str(item).strip()
            if s and s.lower() not in seen:
                seen.add(s.lower())
                out.append(s)
        return out

    @field_validator("additional_references", mode="before")
    @classmethod
    def _map_or_list_refs(cls, v):
        # Dump stores this as {name: url}; also accept the list form.
        if isinstance(v, dict):
            return [{"name": k, "url": u} for k, u in v.items()]
        return v or []


class CorpusPolicy(BaseModel):
    """The documentation bar for admitting a scraped meme into the corpus.

    Tune this instead of editing schema `required`. Every knob here is a field
    that IS sometimes legitimately absent on a confirmed meme, so enforcing it
    is a corpus-quality choice, not a well-formedness one.
    """
    model_config = ConfigDict(extra="forbid")
    require_year: bool = True
    require_entry_type: bool = True
    require_region: bool = True  # 94% coverage measured on a 100-page live sample
    require_tags: bool = True  # 99.7% coverage, but real confirmed memes lack it
    require_sections: tuple[str, ...] = ("about", "origin", "spread")


DEFAULT_CORPUS_POLICY = CorpusPolicy()

# Bump whenever DEFAULT_CORPUS_POLICY's field defaults change (e.g. the
# require_region flip below). parse_store stamps this onto every entries
# doc so a later policy change can be distinguished from a stale grade
# without needing to know Python's own change history.
CORPUS_POLICY_VERSION = "2026-07-16-tags-gated-not-required"


def corpus_ready(
    entry: KYMEntryScrape, policy: CorpusPolicy = DEFAULT_CORPUS_POLICY
) -> tuple[bool, list[str]]:
    """(is_ready, missing_fields). Never raises — flags, so the record is kept.

    Route entries where is_ready is False to a quarantine/review collection
    instead of the corpus; nothing is lost and the reasons are explicit.
    """
    missing: list[str] = []
    if policy.require_year and entry.year is None:
        missing.append("year")
    if policy.require_entry_type and not entry.entry_type:
        missing.append("entry_type")
    if policy.require_region and not entry.region:
        missing.append("region")
    if policy.require_tags and not entry.tags:
        missing.append("tags")
    have = {s.kind for s in entry.sections}  # use_enum_values -> plain strings
    missing.extend(f"section:{k}" for k in policy.require_sections if k not in have)
    return (not missing, missing)


# --- convenience: pull one canonical section's text out of the ordered list.
def section_text(entry: KYMEntryScrape, kind: SectionKind | str) -> list[str]:
    """First matching section's paragraphs, or [] — for feeding the extractor."""
    k = kind.value if isinstance(kind, SectionKind) else kind
    for s in entry.sections:
        if s.kind == k:
            return s.text
    return []


if __name__ == "__main__":
    # Smoke test with a record shaped like the raw dump (pre-normalisation).
    sample = {
        "url": "https://knowyourmeme.com/memes/this-is-relevant-to-my-interests",
        "title": "This is Relevant To My Interests",
        "category": "Meme",
        "status": "confirmed",
        "entry_type": ["https://knowyourmeme.com/types/catchphrase",
                       "https://knowyourmeme.com/types/image-macro"],
        "year": "2006",
        "origin": "I Can Has Cheezburger",
        "tags": ["image macros", "comment", "comment", "approval"],
        "additional_references": {
            "Encyclopedia Dramatica": "https://encyclopediadramatica.wiki/index.php/Roflcopter"
        },
        "sections": [
            {"heading": "About", "kind": "about", "level": 2,
             "text": ["An expression used to convey approval and enthusiasm."],
             "links": [{"text": "image macros",
                        "url": "https://knowyourmeme.com/memes/image-macros"}]},
            {"heading": "Search Interest", "kind": "search_interest", "level": 2,
             "text": ["Trends.embed.renderExploreWidget(...junk...)", "Real note."]},
        ],
        "kym_last_updated": 1547002898,
        "kym_added": 1229112761,
    }
    entry = KYMEntryScrape.model_validate(sample)
    assert entry.entry_type == ["catchphrase", "image-macro"], entry.entry_type
    assert entry.year == 2006
    assert entry.tags == ["image macros", "comment", "approval"]
    assert entry.additional_references[0].name == "Encyclopedia Dramatica"
    assert section_text(entry, SectionKind.search_interest) == ["Real note."]

    # Corpus gate: this sample has about+search_interest but no origin/spread.
    ready, missing = corpus_ready(entry)
    assert ready is False
    assert missing == ["section:origin", "section:spread"], missing
    print("OK — sample validated and normalised.")
    print("corpus_ready:", ready, "| missing:", missing)