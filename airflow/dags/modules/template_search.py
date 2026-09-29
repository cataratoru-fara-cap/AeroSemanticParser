"""
template_search.py — glue between imgflip_client, template_store and kg/templates
==================================================================================
kg/templates.py decides; imgflip_client.py fetches; template_store.py keeps.
This module wires them for the templates DAG, so the DAG file stays
orchestration only and each wiring decision is testable with a fake client
and a mongomock store:

  * ``SearchIO``: the page cache (a search page is fetched once per query
    and page, and shared by every frame that asks), the thumbnail cache (a
    template is downloaded and hashed once, whichever frame met it), the
    KYM image hashing, and gold-link resolution.
  * ``run_assignment``: the global selection — score every searched frame,
    cluster every accepted template across frames, select per frame, write
    what changed.
  * ``fetch_details``: the /memetemplate page and full-size image of each
    template some frame keeps.

Files on disk (TEMPLATE_DATA_DIR, default /opt/airflow/data/templates):
    thumbs/<key>.<ext>   every hashed thumbnail — kept, so a new hash or a
                         new Pillow re-hashes without re-downloading
    blank/<id>.<ext>     the full-size blank of every kept template
"""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from modules import imgflip_client as ic
from modules import imgflip_parse as ip
from modules.kg import templates as kt
from modules.kg import visual

log = logging.getLogger("template_search")

DEFAULT_DATA_DIR = "/opt/airflow/data/templates"

# KYM's og:image is the "original" upload (often 200-500 KB); its
# "newsfeed" rendition is the same picture resized, not cropped, at ~10 KB
# (pHash distance 0-1 on three samples, 2026-09-28) — all a 32x32 hash needs.
_KYM_ORIGINAL = "/entries/icons/original/"
_KYM_SMALL = "/entries/icons/newsfeed/"
# KYM's cover for NSFW entries: the same picture on 88 frames, never the meme.
_KYM_PLACEHOLDERS = ("/assets/image-covers/",)


def data_dir() -> Path:
    return Path(os.getenv("TEMPLATE_DATA_DIR", DEFAULT_DATA_DIR))


def kym_hash_url(src: str) -> str | None:
    """The URL to hash for a KYM image, or None for a placeholder."""
    if any(p in src for p in _KYM_PLACEHOLDERS):
        return None
    return src.replace(_KYM_ORIGINAL, _KYM_SMALL)


def _ext(url: str, default: str = "jpg") -> str:
    tail = urlparse(url).path.rsplit(".", 1)
    return tail[1].lower() if len(tail) == 2 and len(tail[1]) <= 4 else default


# ---------------------------------------------------------------------------
# SearchIO
# ---------------------------------------------------------------------------

class StoreIO:
    """Builds kg/templates.SearchIO over one client and one store."""

    def __init__(self, client: ic.ImgflipClient, store, root: Path, *,
                 max_page_age_days: float, reparse_only: bool = False):
        self.client = client
        self.store = store
        self.root = root
        self.max_page_age_days = max_page_age_days
        self.reparse_only = reparse_only
        self.tally = {"pages_cached": 0, "pages_fetched": 0, "page_failures": 0,
                      "thumbs_cached": 0, "thumbs_rehashed": 0, "thumbs_fetched": 0,
                      "thumb_failures": 0, "kym_images": 0, "kym_image_failures": 0,
                      "gold_resolved": 0, "gold_unresolved": 0}

    def search_io(self) -> kt.SearchIO:
        return kt.SearchIO(search_page=self.search_page, thumb_hashes=self.thumb_hashes,
                           image_hashes=self.image_hashes, resolve_gold=self.resolve_gold)

    # -- pages --------------------------------------------------------------

    def _page(self, url: str, *, kind: str, markers: tuple[str, ...],
              end_of_results_on_500: bool = False, query: str | None = None,
              page: int | None = None) -> dict:
        cached = self.store.get_page(url, self.max_page_age_days)
        if cached is not None:
            self.tally["pages_cached"] += 1
            return {"ok": True, "html": cached["html"],
                    "final_url": cached.get("final_url") or url}
        if self.reparse_only:
            return {"ok": False, "error_kind": "not_archived",
                    "error": "reparse_only and the page is not archived"}
        res = self.client.fetch_page(url, markers=markers,
                                     end_of_results_on_500=end_of_results_on_500)
        self.store.save_page(res.as_doc(), kind=kind, query=query, page=page)
        self.tally["pages_fetched"] += 1
        if not res.ok and res.error_kind != "end_of_results":
            self.tally["page_failures"] += 1
        return {"ok": res.ok, "html": res.html, "final_url": res.final_url,
                "error_kind": res.error_kind, "error": res.error}

    def search_page(self, query: str, page: int) -> dict:
        return self._page(ic.search_url(query, page), kind="search",
                          markers=ic.SEARCH_MARKERS, end_of_results_on_500=page > 1,
                          query=query, page=page)

    # -- images -------------------------------------------------------------

    def thumb_hashes(self, template: dict) -> dict | None:
        tid = int(template["template_id"])
        doc = self.store.templates_by_id([tid], ("hashes", "visual_version",
                                                 "thumb_path")).get(tid) or {}
        if doc.get("hashes") and doc.get("visual_version") == visual.VISUAL_VERSION:
            self.tally["thumbs_cached"] += 1
            return doc["hashes"]
        path = Path(doc["thumb_path"]) if doc.get("thumb_path") else None
        if path is not None and path.exists():
            hashed = self._hash_file(tid, path)
            if hashed:
                self.tally["thumbs_rehashed"] += 1
                return hashed
        urls = list(dict.fromkeys(
            ([template["thumb_url"]] if template.get("thumb_url") else [])
            + ip.thumb_urls(template["key"], bool(template.get("animated")))))
        error = "no thumbnail URL"
        for url in urls:
            got = self.client.fetch_image(url)
            if not got.ok:
                error = f"{got.error_kind}: {got.error}"
                continue
            path = self.root / "thumbs" / f"{template['key']}.{_ext(url)}"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(got.content)
            hashed = self._hash_file(tid, path)
            if hashed:
                self.tally["thumbs_fetched"] += 1
                return hashed
            error = "undecodable image"
        self.tally["thumb_failures"] += 1
        self.store.save_thumb_error(tid, error)
        return None

    def _hash_file(self, tid: int, path: Path) -> dict | None:
        try:
            h = visual.hashes(path.read_bytes()).as_doc()
        except visual.ImageDecodeError as exc:
            log.warning("template %s: cannot decode %s (%s)", tid, path, exc)
            return None
        self.store.save_hashes(tid, dict(h, visual_version=visual.VISUAL_VERSION,
                                         path=str(path)))
        return h

    def image_hashes(self, src: str) -> dict:
        url = kym_hash_url(src)
        if url is None:
            return {"error": "placeholder"}
        self.tally["kym_images"] += 1
        got = self.client.fetch_image(url)
        if not got.ok and url != src:
            got = self.client.fetch_image(src)          # no small rendition
        if not got.ok:
            self.tally["kym_image_failures"] += 1
            return {"error": f"{got.error_kind}: {got.error}"}
        try:
            return visual.hashes(got.content).as_doc()
        except visual.ImageDecodeError as exc:
            self.tally["kym_image_failures"] += 1
            return {"error": f"undecodable: {exc}"}

    # -- gold links ---------------------------------------------------------

    def resolve_gold(self, link: dict) -> dict | None:
        """A KYM "Meme Generator" link -> the template it names, via its
        /memetemplate page (which also gives the details). Links to a
        user-made meme (/i/...) are not templates and are left out."""
        if link["kind"] == "instance":
            self.tally["gold_unresolved"] += 1
            return None
        if link.get("template_id"):
            url = ip.template_page_url(link["template_id"])
        elif link.get("slug"):
            url = f"{ip.SITE}/memetemplate/{link['slug']}"
        else:
            self.tally["gold_unresolved"] += 1
            return None
        got = self._page(url, kind="template", markers=ic.TEMPLATE_MARKERS)
        if not got.get("ok"):
            self.tally["gold_unresolved"] += 1
            return None
        try:
            details = ip.parse_template_page(got["html"])
        except ip.ImgflipParseError as exc:
            log.warning("gold %s: %s", link["url"], exc)
            self.tally["gold_unresolved"] += 1
            return None
        self.tally["gold_resolved"] += 1
        template = template_from_details(details, got.get("final_url") or url)
        self.store.save_details(template["template_id"], detail_fields(template))
        return template


def template_from_details(details: dict, final_url: str) -> dict:
    """A /memetemplate page's details as a template record. The canonical
    page URL says whether imgflip features it (no id in the path) and
    names its /meme/ page (IMKG's template URL)."""
    page = ip.classify_imgflip_url(final_url)
    featured = page["template_id"] is None and bool(page["slug"])
    path = urlparse(final_url).path.replace("/memetemplate/", "/meme/", 1)
    animated = (details.get("file_type") or "") in ("mp4", "gif")
    return {**details, "featured": featured, "animated": animated,
            "url": f"{ip.SITE}{path}", "page_url": final_url,
            "thumb_url": ip.thumb_urls(details["key"], animated)[0]}


def detail_fields(template: dict) -> dict:
    return {k: template.get(k) for k in ("key", "name", "alt_names", "description",
                                         "file_type", "width", "height", "page_url",
                                         "featured", "animated", "url")}


# ---------------------------------------------------------------------------
# Per-chunk search
# ---------------------------------------------------------------------------

def search_units(units: Iterable[dict], io: StoreIO, stamps: dict[str, str],
                 params: kt.RelevanceParams = kt.DEFAULT_PARAMS) -> dict[str, int]:
    """Search each unit, persisting as it goes (a crash loses one frame)."""
    tally = {"frames": 0, "searched": 0, "no_results": 0, "failed": 0,
             "candidates": 0, "gold": 0}
    for unit in units:
        record = kt.search_frame(unit, io.search_io(), params)
        io.store.upsert_templates(record["templates"])
        io.store.save_search(record, stamps)
        tally["frames"] += 1
        tally[record["search_status"]] += 1
        tally["candidates"] += len(record["candidates"])
        tally["gold"] += len(record["gold_ids"])
    return {**tally, **io.tally, "html_requests": io.client.requests["html"],
            "image_requests": io.client.requests["image"],
            "scrapingant_credits": io.client.credits_used}


# ---------------------------------------------------------------------------
# The global selection
# ---------------------------------------------------------------------------

def run_assignment(store, run_id: str,
                   params: kt.RelevanceParams = kt.DEFAULT_PARAMS) -> dict[str, Any]:
    """Score every searched frame, cluster every accepted template across
    all of them, select per frame, and write what changed.

    Global on purpose: a picture's representative must be the same upload
    for every frame that selects it, and that depends on every frame's
    candidates (a gold link on one frame makes its upload the
    representative for all).
    """
    stamps = kt.stamps()
    store.start_assignment(run_id, stamps)
    records = list(store.iter_search_records())
    wanted = {c["t"] for r in records for c in r.get("candidates") or [] if c.get("pre")}
    wanted |= {t for r in records for t in r.get("gold_ids") or []}
    templates = store.templates_by_id(wanted, kt_fields())

    scored_by_frame: dict[str, list[dict]] = {}
    accepted: set[int] = set()
    gold_all: set[int] = set()
    for r in records:
        scored = kt.score_candidates(r, templates, params)
        scored_by_frame[r["_id"]] = scored
        accepted.update(s["template_id"] for s in scored if s["accepted"])
        gold_all.update(r.get("gold_ids") or [])
    leader_of = kt.cluster(accepted, templates, gold_all)

    selections = []
    counts = {"frames": len(records), "accepted_templates": len(accepted),
              "representatives": len(set(leader_of.values())), "selected_links": 0}
    for r in records:
        scored = scored_by_frame[r["_id"]]
        chosen = kt.select(scored, leader_of, templates)
        out = kt.outcome(r, scored, chosen)
        counts["selected_links"] += len(chosen["selected"])
        counts[out["status"]] = counts.get(out["status"], 0) + 1
        selections.append({
            "unit_id": r["_id"], "selected": chosen["selected"],
            "suppressed": chosen["suppressed"], "accepted_groups": chosen["accepted_groups"],
            **out, "previous_sha": r.get("selection_sha"),
            "stamps_moved": any(r.get(k) != stamps[k]
                                for k in ("relevance_version", "dedup_version",
                                          "selection_version", "visual_version"))})
    counts.update(store.save_selections(selections, stamps=stamps, run_id=run_id))
    store.set_leaders(leader_of, kt.DEDUP_VERSION)
    store.complete_assignment(run_id, counts)
    return counts


def kt_fields() -> tuple[str, ...]:
    return ("name", "alt_names", "featured", "animated", "hashes")


# ---------------------------------------------------------------------------
# Details of kept templates
# ---------------------------------------------------------------------------

def fetch_details(template_ids: Iterable[int], io: StoreIO) -> dict[str, int]:
    """/memetemplate page + the full-size blank, per kept template.

    The blank is what entity extraction reads. For an animated template the
    blank is an mp4, so the /2/ still is kept instead (Gabi: entities of an
    animated template come from its still)."""
    tally = {"templates": 0, "details": 0, "blanks": 0, "failures": 0}
    for tid in template_ids:
        tally["templates"] += 1
        got = io._page(ip.template_page_url(tid), kind="template",
                       markers=ic.TEMPLATE_MARKERS)
        if not got.get("ok"):
            io.store.save_detail_error(tid, got.get("error") or "fetch failed",
                                       got.get("error_kind"))
            tally["failures"] += 1
            continue
        try:
            details = ip.parse_template_page(got["html"])
        except ip.ImgflipParseError as exc:
            io.store.save_detail_error(tid, str(exc), "parse")
            tally["failures"] += 1
            continue
        template = template_from_details(details, got.get("final_url") or "")
        fields = detail_fields(template)
        # The search listing's own URL and flags stay authoritative.
        for k in ("url", "featured", "animated"):
            fields.pop(k, None)
        tally["details"] += 1
        blank = (ip.thumb_urls(template["key"], True)[0] if template["animated"]
                 else ip.blank_url(template["key"], template.get("file_type") or "jpg"))
        img = io.client.fetch_image(blank)
        if img.ok:
            path = io.root / "blank" / f"{tid}.{_ext(blank)}"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(img.content)
            fields.update(blank_url=blank, blank_path=str(path),
                          blank_sha256=hashlib.sha256(img.content).hexdigest())
            tally["blanks"] += 1
        else:
            fields.update(blank_url=blank, blank_error=f"{img.error_kind}: {img.error}")
        io.store.save_details(tid, fields)
    return tally
