"""
imgflip_parse.py — imgflip search and template pages -> plain dicts
====================================================================
Pure parsing: HTML in, dicts out. No HTTP (imgflip_client.py), no Mongo
(template_store.py), no Airflow. The archived pages live in
``imgflip_pages``, so a parser fix re-derives every frame's candidates
without a single re-fetch: bump PARSER_VERSION and re-run with
``reparse_only``.

What imgflip serves (verified 2026-09-28)
-----------------------------------------
``/memesearch?q=<q>&page=<n>`` is rendered on the server — the only script
on it inserts ads — and lists TEMPLATES only, never user-made memes: 40 a
page, still and animated mixed, in a relevance-like order (the featured
exact match first; alternate names match too; deep pages are recent,
little-used uploads). Pages past 250 answer 500.

    #mt-boxes-wrap .mt-box            one result
      h3.mt-title > a                 name, and the template's /meme/ URL
      .mt-img-wrap img                thumbnail //i.imgflip.com/4/<key>.jpg
                                      (/2/ for animated templates)
      .mt-animated-label              present on animated templates
      a.mt-caption                    /memegenerator/... or /gif-maker/...
    .pager a.pager-next               a further page exists
    #mt-boxes-wrap h2 "No results for that query."

The template id is the image KEY in base 36: ``int("1ur9b0", 36)`` is
112126428, Distracted Boyfriend. Featured templates' URLs carry no id
(``/meme/Distracted-Boyfriend``), so the key is the one place every result
states it.

``/memetemplate/<id>`` redirects to the template's canonical page, which
states ``Template ID: N``, ``Format: jpg``, ``Dimensions: WxH px``, its
alternate names ("also called: ...") and, sometimes, a description.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

PARSER_VERSION = "1.0.0"

SITE = "https://imgflip.com"
CDN = "https://i.imgflip.com"
PAGE_SIZE = 40
# imgflip answers 500 past this page, for every query.
LAST_PAGE = 250

IMGFLIP_HOSTS = frozenset({"imgflip.com", "www.imgflip.com"})

# An image on imgflip's CDN: an optional size directory (/2/, /4/), then the
# base-36 key and the file type.
_IMAGE_KEY = re.compile(
    r"(?:^|//|\b)i\.imgflip\.com/(?:(\d+)/)?([0-9a-z]+)\.(jpg|jpeg|png|gif|mp4|webp)\b",
    re.I)

# /meme/<id>/<slug>, /meme/<Slug>, /memegenerator/..., /memetemplate/<id>,
# /gif-maker/<id>/<slug>. A lone all-digit segment is an id (memetemplate/<id>
# is how imgflip itself links a template it has no slug for).
_TEMPLATE_PATH = re.compile(
    r"^/(meme|memegenerator|memetemplate|gif-maker)"
    r"(?:/(\d+))?(?:/([^/?#]+))?/?$")
_INSTANCE_PATH = re.compile(r"^/(i|gif)/([0-9a-z]+)/?$")

_TEMPLATE_ID_TEXT = re.compile(r"Template ID:\s*(\d+)")
_FORMAT_TEXT = re.compile(r"Format:\s*([A-Za-z0-9]+)")
_DIMENSIONS_TEXT = re.compile(r"Dimensions:\s*(\d+)\s*x\s*(\d+)")
_ALSO_CALLED = re.compile(r"^\s*(?:also called|aka)\s*:\s*", re.I)
_TITLE_SUFFIX = re.compile(r"\s+(?:Meme|GIF|Animated)?\s*Template\s*$", re.I)


class ImgflipParseError(ValueError):
    """The page is not the kind of page it was fetched as — a changed
    layout or a block page, never a normal "no results"."""


# ---------------------------------------------------------------------------
# Ids, keys and URLs
# ---------------------------------------------------------------------------

def template_id_from_key(key: str) -> int:
    """The template id an image key encodes (base 36)."""
    return int(key, 36)


def key_from_template_id(template_id: int) -> str:
    """The inverse of template_id_from_key: base 36, lower case."""
    if template_id < 0:
        raise ValueError(f"negative template id: {template_id}")
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = ""
    n = int(template_id)
    while True:
        n, r = divmod(n, 36)
        out = digits[r] + out
        if n == 0:
            return out


def image_key(src: str | None) -> tuple[str, str] | None:
    """(key, file type) of an imgflip CDN image URL, or None."""
    if not src:
        return None
    m = _IMAGE_KEY.search(src)
    if not m:
        return None
    return m.group(2).lower(), m.group(3).lower()


def blank_url(key: str, file_type: str) -> str:
    """The full-size blank template. The extension must be the template's
    own format: a wrong one is a 404, not a conversion."""
    return f"{CDN}/{key}.{file_type.lower()}"


def thumb_urls(key: str, animated: bool = False) -> list[str]:
    """The listing thumbnail(s) to try, for a template not met in a search
    listing (whose own ``thumb_url`` is authoritative). Still templates have
    a 250 px /4/ thumbnail, usually jpg, sometimes png; animated ones have
    only the smaller /2/ still — /4/ is a 404 for them (checked
    2026-09-28)."""
    if animated:
        return [f"{CDN}/2/{key}.jpg"]
    return [f"{CDN}/4/{key}.jpg", f"{CDN}/4/{key}.png"]


def template_page_url(template_id: int) -> str:
    """The id-only template page; imgflip redirects it to the canonical one."""
    return f"{SITE}/memetemplate/{int(template_id)}"


def classify_imgflip_url(url: str) -> dict[str, Any]:
    """What an imgflip link points at.

    ``kind`` is one of ``meme`` / ``memegenerator`` / ``memetemplate`` /
    ``gif-maker`` (a template, by id and/or slug), ``instance`` (a meme
    someone made, ``/i/<key>`` or ``/gif/<key>``), ``other`` (another imgflip
    page), or ``not_imgflip``.
    """
    out: dict[str, Any] = {"kind": "not_imgflip", "template_id": None,
                           "slug": None, "instance_key": None}
    try:
        parsed = urlparse(url.strip())
    except ValueError:
        return out
    if (parsed.hostname or "").lower() not in IMGFLIP_HOSTS:
        return out
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    m = _TEMPLATE_PATH.match(path)
    if m:
        out["kind"] = m.group(1)
        out["template_id"] = int(m.group(2)) if m.group(2) else None
        out["slug"] = m.group(3) or None
        if out["template_id"] is None and out["slug"] is None:
            out["kind"] = "other"          # the bare /memegenerator page
        return out
    m = _INSTANCE_PATH.match(path)
    if m:
        out["kind"] = "instance"
        out["instance_key"] = m.group(2).lower()
        return out
    out["kind"] = "other"
    return out


def _clean(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def parse_search(html: str) -> dict[str, Any]:
    """One search page -> ``{results, has_next, no_results, skipped,
    id_mismatches}``.

    Each result: ``rank`` (0-based on THIS page), ``name``, ``url`` (the
    template's /meme/ page), ``template_id``, ``key``, ``featured`` (its
    URL carries no id — imgflip's curated templates), ``animated``,
    ``thumb_url``, ``caption_url``.

    A box without a name link or a readable image key is counted in
    ``skipped``, not guessed at. ``id_mismatches`` counts boxes whose URL
    id and image key disagree (the URL's wins); non-zero means imgflip
    changed how it names images, and the key rule above needs a look.
    """
    soup = BeautifulSoup(html, "lxml")
    wrap = soup.select_one("#mt-boxes-wrap")
    if wrap is None:
        raise ImgflipParseError("no #mt-boxes-wrap — not a meme-search page")
    no_results = any("no results" in _clean(h.get_text()).lower()
                     for h in wrap.select("h2"))
    results: list[dict[str, Any]] = []
    skipped = mismatches = 0
    for box in wrap.select(".mt-box"):
        link = box.select_one("h3.mt-title a[href]")
        img = box.select_one(".mt-img-wrap img")
        keyed = image_key(img.get("src") if img else None)
        if link is None or keyed is None:
            skipped += 1
            continue
        key, _ext = keyed
        url = urljoin(SITE, link["href"])
        path = classify_imgflip_url(url)
        key_id = template_id_from_key(key)
        template_id = path["template_id"] or key_id
        if path["template_id"] is not None and path["template_id"] != key_id:
            mismatches += 1
        caption = box.select_one("a.mt-caption[href]")
        results.append({
            "rank": len(results),
            "name": _clean(link.get_text(" ")),
            "url": url,
            "template_id": template_id,
            "key": key,
            "featured": path["template_id"] is None,
            "animated": box.select_one(".mt-animated-label") is not None,
            "thumb_url": urljoin("https:", img["src"]),
            "caption_url": urljoin(SITE, caption["href"]) if caption else None,
        })
    has_next = soup.select_one(".pager a.pager-next[href]") is not None
    return {"results": results, "has_next": has_next,
            "no_results": no_results and not results,
            "skipped": skipped, "id_mismatches": mismatches}


def parse_template_page(html: str) -> dict[str, Any]:
    """A /memetemplate/ page -> the template's details.

    ``template_id``, ``name``, ``alt_names``, ``description``,
    ``file_type``, ``width``, ``height``, ``key`` (from the page's own
    image or video). Raises ImgflipParseError when the page states no
    template id: the one field every other is keyed on.
    """
    soup = BeautifulSoup(html, "lxml")
    title = soup.select_one("#mtm-title")
    if title is None:
        raise ImgflipParseError("no #mtm-title — not a template page")
    text = soup.get_text("\n")
    m = _TEMPLATE_ID_TEXT.search(text)
    if not m:
        raise ImgflipParseError("template page states no 'Template ID'")
    template_id = int(m.group(1))

    alt_names: list[str] = []
    subtitle = soup.select_one("#mtm-subtitle")
    if subtitle is not None:
        raw = _ALSO_CALLED.sub("", _clean(subtitle.get_text(" ")))
        seen: set[str] = set()
        for name in (_clean(p) for p in raw.split(",")):
            if name and name.lower() not in seen:
                seen.add(name.lower())
                alt_names.append(name)

    description_el = soup.select_one("#mtm-description")
    description = _clean(description_el.get_text(" ")) if description_el else ""

    fmt = _FORMAT_TEXT.search(text)
    dims = _DIMENSIONS_TEXT.search(text)

    media = soup.select_one("#mtm-img") or soup.select_one("#mtm-video source")
    keyed = image_key(media.get("src") if media else None)
    key = keyed[0] if keyed else None
    if key is None or template_id_from_key(key) != template_id:
        # Featured templates show /s/meme/<Slug>.jpg, which has no key; the
        # id is authoritative either way.
        key = key_from_template_id(template_id)

    return {
        "template_id": template_id,
        "key": key,
        "name": _TITLE_SUFFIX.sub("", _clean(title.get_text(" "))),
        "alt_names": alt_names,
        "description": description or None,
        "file_type": fmt.group(1).lower() if fmt else None,
        "width": int(dims.group(1)) if dims else None,
        "height": int(dims.group(2)) if dims else None,
    }
