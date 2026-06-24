# ─── Schema ──────────────────────────────────────────────────────────────────
import json

from pydantic import BaseModel, field_validator, model_validator


class MemeRecord(BaseModel):
    """
    Validated meme record.  All fields except url are optional to handle
    incomplete KYM pages gracefully.  Validators normalize LLM output quirks
    (e.g. "Confirmed" -> "confirmed", year as string "2019" -> int 2019).
    """
    url: str
    name: str | None
    status: str | None
    type: str | None
    year: int | None
    origin: str | None
    tags: str | None
    about: str | None
    spread: str | None
    views: int | None

    @field_validator("url")
    @classmethod
    def must_be_kym_url(cls, v):
        if "knowyourmeme.com" not in v:
            raise ValueError(f"unexpected domain in url: {v!r}")
        return v.strip()

    @field_validator("status")
    @classmethod
    def normalize_status(cls, v):
        if v is None:
            return v
        v = str(v).lower().strip()
        return v if v in {"confirmed", "submission", "deadpool"} else None

    @field_validator("year", mode="before")
    @classmethod
    def coerce_year(cls, v):
        if v is None:
            return None
        try:
            v = int(str(v).strip()[:4])
        except (ValueError, TypeError):
            return None
        return v if 1990 <= v <= 2030 else None

    @field_validator("views", mode="before")
    @classmethod
    def coerce_views(cls, v):
        if v is None:
            return None
        try:
            # Handle "1.2M" style strings from LLM output
            s = str(v).strip().upper().replace(",", "")
            if s.endswith("M"):
                return int(float(s[:-1]) * 1_000_000)
            if s.endswith("K"):
                return int(float(s[:-1]) * 1_000)
            return int(float(s))
        except (ValueError, TypeError):
            return None

    @field_validator("tags", mode="before")
    @classmethod
    def normalize_tags(cls, v):
        if not v:
            return []
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return [str(t).strip() for t in parsed if t]
            except json.JSONDecodeError:
                pass
            return [t.strip() for t in v.split(",") if t.strip()]
        return [str(t).strip() for t in v if t]

    @field_validator("name", "about", "origin", "spread", mode="before")
    @classmethod
    def clean_string(cls, v):
        if v is None:
            return None
        s = str(v).strip()
        return s if s and s.lower() not in {"null", "none", "n/a", ""} else None

    @model_validator(mode="after")
    def has_minimum_content(self):
        """A record with neither name nor about is almost certainly a scrape failure."""
        if not self.name and not self.about:
            raise ValueError(
                "record has neither name nor about — likely a page fetch failure or LLM error"
            )
        return self
