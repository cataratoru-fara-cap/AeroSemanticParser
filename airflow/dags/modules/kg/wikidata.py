"""
kg/wikidata.py — the Wikidata JSON dump -> a local entity lexicon (SQLite)
===========================================================================
Pure: no Mongo, no Airflow, no network. Streams a dump that is already on
disk into one SQLite file that kg/entities.py looks names up in:

    python -m modules.kg.wikidata build \
        --dump data/wikidata/wikidata-20260914-all.json.gz \
        --out  data/wikidata/lexicon.sqlite
    python -m modules.kg.wikidata info   --lexicon data/wikidata/lexicon.sqlite
    python -m modules.kg.wikidata lookup --lexicon data/wikidata/lexicon.sqlite "Shiba Inu"

Getting the dump (~156 GB as of 2026-09; 8.5 h at the ~5 MB/s
dumps.wikimedia.org gave this host). Download a DATED dump, never
``latest-all``: that name is repointed to each new weekly dump, so a
``curl -C -`` resume against it appends the tail of a different file —
same name, different bytes, and nothing fails until the checksum
(2026-09-23: the 0914 download had finished, the "resume" spliced 99 MB of
the 0921 dump onto it). Pick a date under /wikidatawiki/entities/, then:

    D=20260914; F=wikidata-$D-all.json.gz
    curl -C - -o data/wikidata/$F \
        https://dumps.wikimedia.org/wikidatawiki/entities/$D/$F
    curl -s https://dumps.wikimedia.org/wikidatawiki/entities/$D/wikidata-$D-md5sums.txt \
        | grep " $F$" | (cd data/wikidata && md5sum -c -)

Keep the dated file name: it goes into the lexicon's ``meta.version``, so
the lexicon says which dump it came from.

Why a local copy of Wikidata, and not an API
--------------------------------------------
IMKG linked its About text and tags with the public DBpedia Spotlight API
(confidence 0.5) and mapped the DBpedia resources to Wikidata afterwards
(github.com/riccardotommasini/imkg, kym/tags/spotlight.py and
kym/KYM.Enrichment.ipynb); its Wikidata enrichment itself ran over
DOWNLOADED dumps, through KGTK. A local lexicon gives the same answer every
time for the same dump (the staleness stamp is the lexicon's version, not
the day an API was asked), costs no quota, cannot go down mid-run, and
links straight to Wikidata rather than through a second knowledge base
whose Wikidata mapping is incomplete. The price is one long download and
one long build per dump — paid once, not per entry.

What is kept
------------
The dump is ~115M entities; the lexicon keeps the ones a KYM page could
plausibly be talking about, and only what linking needs:

  * items (Q-ids) only — no properties, no lexemes;
  * with an English or ``mul`` label. ``mul`` is Wikidata's "same in every
    language" label (2024), and a third of the items at the head of the dump
    use it — mostly names, which is exactly what NER finds;
  * with at least ``min_sitelinks`` Wikipedia articles (default 1) — OR a
    Know Your Meme slug (P13484) or numeric ID (P6760), whatever its
    sitelinks. That second clause is the one that matters most here: a meme
    or an internet personality is often on Wikidata and KYM without being
    on any Wikipedia;
  * never an item that is an instance of a Wikimedia-internal class
    (``EXCLUDED_CLASSES``: disambiguation pages, categories, lists,
    templates, scholarly articles, ...). "Doge" must not link to the
    disambiguation page that lists every Doge.

For each kept item: its label and description (en, else mul), its English
and mul aliases, its Wikipedia sitelink count (the popularity prior), its
enwiki title, its P31 (instance of) classes, and its KYM identifiers.

KYM identifiers: two properties, and only one of them joins
-----------------------------------------------------------
IMKG joined memes to Wikidata on P6760, "Know Your Meme numeric ID"
(KGTK Wikidata Enrichment.ipynb). Its values are KYM's internal entry
numbers ("13066" is The Office), which KYM redirects from /memes/<n> but
never shows on the page — and the parser does not extract them, so P6760
cannot be joined to a frame today. P13484, "Know Your Meme slug"
("diet-coke-and-mentos", "reddit"), is the LAST SEGMENT of the page URL
(memes/sites/reddit -> "reddit"), which every frame has. So frames join on
P13484 (``Lexicon.by_kym``), and P6760 is kept alongside it
(``Lexicon.by_kym_id``) for the day the parser records the number.

P279 (subclass of) is kept for EVERY item that has one, kept or not, as a
separate table: a type check walks P31 -> P279* up to a coarse family
("human", "organization", ...), and a class the filter dropped would
otherwise break the chain for every item below it.

Memory and time
---------------
One entity per line (the dump is one JSON array, one item per line), read
through ``pigz -dc`` when it is installed (it is on this host) and gzip
otherwise. Two cheap byte-level checks drop most lines before any JSON is
parsed: a line that is not an item, and a line with ``"sitelinks":{}``
and no KYM identifier or P279. The rest are parsed in a worker pool with a BOUNDED
number of batches in flight — ``Pool.imap`` would read ahead without limit
and hold the dump in memory. Rows go into ``<out>.tmp`` with journaling
off, indexes are built once at the end, and the file is renamed into place:
a crashed build leaves the previous lexicon untouched.

The version
-----------
``meta.version`` is a sha of (builder version, filters, dump file name and
size, newest ``modified`` in the dump). kg/entities.py stamps it on every
link it makes, and modules/entity_store.py re-links every frame whose stamp
differs — so a new dump re-links the corpus, and the same dump never does.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import unicodedata
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Callable, Iterable, Iterator, Sequence

log = logging.getLogger("kg.wikidata")

__all__ = [
    "LEXICON_BUILDER_VERSION", "LABEL_LANGUAGES", "EXCLUDED_CLASSES",
    "KYM_ID_PROPERTY", "KYM_SLUG_PROPERTY", "WD_ENTITY", "MAX_CANDIDATES",
    "norm", "qid_str", "qid_int", "kym_slug", "parse_entity", "iter_dump_lines",
    "build_lexicon", "Candidate", "Lexicon", "prior", "main",
]

# Bump when what a lexicon CONTAINS changes for the same dump — the
# filters, the normaliser, the schema. It is part of meta.version, so every
# frame is re-linked against the new build.
LEXICON_BUILDER_VERSION = "1.0.0"

# The label languages a KYM page's English text can match.
LABEL_LANGUAGES: tuple[str, ...] = ("en", "mul")

# The canonical entity IRI — what Wikidata's own RDF and query service
# use, NOT the https://www.wikidata.org/wiki/Q… page URL IMKG emitted (see
# kg_config/MODEL.md, "Deliberate differences").
WD_ENTITY = "http://www.wikidata.org/entity/"

KYM_ID_PROPERTY = "P6760"       # numeric; not on the page (see above)
KYM_SLUG_PROPERTY = "P13484"    # the URL's last segment; what frames join on

# Items that are an instance of one of these are Wikimedia's own
# bookkeeping, or (Q13442814) the ~40M scholarly articles that make up a
# third of Wikidata and are never what a meme page means by a word.
EXCLUDED_CLASSES: frozenset[int] = frozenset({
    4167410,    # Wikimedia disambiguation page
    4167836,    # Wikimedia category
    13406463,   # Wikimedia list article
    11266439,   # Wikimedia template
    15184295,   # Wikimedia module
    14204246,   # Wikimedia project page
    17362920,   # Wikimedia duplicated page
    21528878,   # Wikimedia redirect page
    17633526,   # Wikinews article
    13442814,   # scholarly article
})

# Sitelinks that are not Wikipedias. Every other ``<lang>wiki`` key is one,
# and the number of Wikipedias an item has is the standard popularity prior.
_NON_WIKIPEDIA = frozenset({
    "commonswiki", "specieswiki", "metawiki", "mediawikiwiki",
    "wikidatawiki", "sourceswiki", "outreachwiki", "wikimaniawiki",
    "incubatorwiki", "foundationwiki", "wikifunctionswiki", "abstractwiki",
})

# How many candidates a lookup returns, most-linked first. "John Smith"
# names hundreds of items; past the first few dozen none of them can win
# on a KYM page, and every one costs a context comparison.
MAX_CANDIDATES = 25

_BATCH_LINES = 2000
_INSERT_BATCH = 5000


# ------------------------------------------------------------ normalising ----

# Typography folded for lookup: every quote to ', every dash to a space
# (so "Etch-a-Sketch" and "Etch A Sketch" meet), NFKC, casefold.
_QUOTES = str.maketrans({c: "'" for c in "‘’‚‛`´"})
_DQUOTES = str.maketrans({c: '"' for c in "“”„‟"})
_DASHES = re.compile(r"[-‐‑‒–—―_]+")
_SPACE = re.compile(r"\s+")
_EDGE = " \t\n\"'.,;:!?()[]{}<>*#"


def norm(text: Any) -> str:
    """The lookup key. Applied identically to every label and alias when
    the lexicon is built and to every mention when it is looked up — the
    one function both sides share, so they cannot disagree about spelling."""
    t = unicodedata.normalize("NFKC", str(text or ""))
    t = t.translate(_QUOTES).translate(_DQUOTES)
    t = _DASHES.sub(" ", t).casefold()
    return _SPACE.sub(" ", t).strip(_EDGE).strip()


def qid_int(qid: str) -> int:
    return int(str(qid).lstrip("Qq"))


def qid_str(qid: int) -> str:
    return f"Q{int(qid)}"


def kym_slug(value: str) -> str:
    """A KYM page as P13484 spells it: the last segment of its URL path
    ("https://knowyourmeme.com/memes/sites/reddit" -> "reddit"). A bare slug
    comes back unchanged, so a frame URL and a P13484 value compare equal."""
    v = str(value or "").strip().split("?", 1)[0].split("#", 1)[0]
    return v.rstrip("/").rsplit("/", 1)[-1].lower()


# ---------------------------------------------------------------- parsing ----

def _claim_items(claims: dict, prop: str) -> list[int]:
    out: list[int] = []
    for c in claims.get(prop) or ():
        if c.get("rank") == "deprecated":
            continue
        value = ((c.get("mainsnak") or {}).get("datavalue") or {}).get("value")
        if isinstance(value, dict) and value.get("numeric-id") is not None:
            out.append(int(value["numeric-id"]))
    return out


def _claim_strings(claims: dict, prop: str) -> list[str]:
    out: list[str] = []
    for c in claims.get(prop) or ():
        if c.get("rank") == "deprecated":
            continue
        value = ((c.get("mainsnak") or {}).get("datavalue") or {}).get("value")
        if isinstance(value, str) and value.strip():
            out.append(value.strip())
    return out


def _first(values: dict, languages: Sequence[str]) -> str | None:
    for lang in languages:
        v = (values.get(lang) or {}).get("value")
        if v:
            return v
    return None


def parse_entity(line: bytes | str, *, min_sitelinks: int = 1) -> dict | None:
    """One dump line -> the row(s) the lexicon keeps, or None.

    An item the filter drops but whose P279 edges the type walk still needs
    comes back as ``{"qid", "subclass", "modified"}`` only; a kept item
    also carries its label, aliases and the rest.
    """
    if isinstance(line, str):
        line = line.encode("utf-8")
    line = line.strip()
    if line.endswith(b","):
        line = line[:-1]
    if not line.startswith(b"{") or b'"type":"item"' not in line[:40]:
        return None
    no_sitelinks = b'"sitelinks":{}' in line
    has_kym = b'"P13484"' in line or b'"P6760"' in line
    has_p279 = b'"P279"' in line
    if no_sitelinks and not has_kym and not has_p279 and min_sitelinks > 0:
        return None
    try:
        e = json.loads(line)
    except ValueError:
        return None
    qid = e.get("id") or ""
    if not qid.startswith("Q"):
        return None
    q = qid_int(qid)
    claims = e.get("claims") or {}
    out: dict[str, Any] = {"qid": q,
                           "subclass": [(q, p) for p in _claim_items(claims, "P279")],
                           "modified": e.get("modified")}

    labels = e.get("labels") or {}
    label = _first(labels, LABEL_LANGUAGES)
    slugs = ([kym_slug(v) for v in _claim_strings(claims, KYM_SLUG_PROPERTY)]
             if has_kym else [])
    numbers = _claim_strings(claims, KYM_ID_PROPERTY) if has_kym else []
    sitelinks = e.get("sitelinks") or {}
    wikipedias = sum(1 for k in sitelinks
                     if k.endswith("wiki") and k not in _NON_WIKIPEDIA)
    types = _claim_items(claims, "P31")
    keep = (label is not None
            and (wikipedias >= min_sitelinks or slugs or numbers)
            and not (EXCLUDED_CLASSES & set(types)))
    if not keep:
        return out if out["subclass"] else None

    aliases: dict[str, tuple[str, int]] = {}
    for lang in LABEL_LANGUAGES:
        v = (labels.get(lang) or {}).get("value")
        if v and norm(v):
            aliases.setdefault(norm(v), (v, 1))
    for lang in LABEL_LANGUAGES:
        for a in (e.get("aliases") or {}).get(lang) or ():
            v = a.get("value")
            if v and norm(v):
                aliases.setdefault(norm(v), (v, 0))
    out.update({
        "label": label,
        "description": _first(e.get("descriptions") or {}, LABEL_LANGUAGES),
        "sitelinks": wikipedias,
        "enwiki": (sitelinks.get("enwiki") or {}).get("title"),
        "types": types,
        # (key, kind): what a frame (slug) or a future parser field
        # (number) joins on.
        "kym": [(k, "slug") for k in dict.fromkeys(slugs) if k]
               + [(k, "id") for k in dict.fromkeys(numbers)],
        "aliases": [(key, surface, is_label)
                    for key, (surface, is_label) in aliases.items()],
    })
    return out


def _parse_batch(args: tuple[list[bytes], int]) -> tuple[list[dict], int, str | None]:
    lines, min_sitelinks = args
    rows = []
    newest = None
    for line in lines:
        row = parse_entity(line, min_sitelinks=min_sitelinks)
        if row is None:
            continue
        rows.append(row)
        m = row.get("modified")
        if m and (newest is None or m > newest):
            newest = m
    return rows, len(lines), newest


def iter_dump_lines(path: str, limit: int = 0) -> Iterator[bytes]:
    """Raw lines of a .json.gz / .json.bz2 / .json dump, decompressed by
    the fastest tool available. ``limit`` counts lines (for samples)."""
    proc = None
    if path.endswith(".gz") and shutil.which("pigz"):
        proc = subprocess.Popen(["pigz", "-dc", path], stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, bufsize=1 << 20)
        fh = proc.stdout
    elif path.endswith(".bz2") and (shutil.which("lbzip2") or shutil.which("pbzip2")):
        tool = shutil.which("lbzip2") or shutil.which("pbzip2")
        proc = subprocess.Popen([tool, "-dc", path], stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, bufsize=1 << 20)
        fh = proc.stdout
    elif path.endswith(".gz"):
        fh = gzip.open(path, "rb")
    elif path.endswith(".bz2"):
        import bz2
        fh = bz2.open(path, "rb")
    else:
        fh = open(path, "rb")
    n = 0
    try:
        for line in fh:
            yield line
            n += 1
            if limit and n >= limit:
                break
    except (EOFError, OSError) as exc:
        # A truncated file (a partial download, a head sample) ends in a
        # torn gzip member. Everything read up to there is good data.
        log.warning("%s ends early after %d lines (%s); keeping what was read",
                    path, n, exc)
    finally:
        fh.close()
        if proc is not None:
            proc.kill()
            proc.wait()


# ---------------------------------------------------------------- building ---

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE entity (
    qid INTEGER PRIMARY KEY, label TEXT NOT NULL, description TEXT,
    sitelinks INTEGER NOT NULL, enwiki TEXT, types TEXT NOT NULL,
    kym TEXT);
CREATE TABLE alias (key TEXT NOT NULL, qid INTEGER NOT NULL,
                    surface TEXT NOT NULL, is_label INTEGER NOT NULL);
CREATE TABLE subclass (qid INTEGER NOT NULL, parent INTEGER NOT NULL);
CREATE TABLE kym (key TEXT NOT NULL, kind TEXT NOT NULL, qid INTEGER NOT NULL);
"""
_INDEXES = """
CREATE INDEX alias_key ON alias (key);
CREATE INDEX subclass_qid ON subclass (qid);
CREATE INDEX kym_key ON kym (key, kind);
"""


def _batches(lines: Iterable[bytes], size: int) -> Iterator[list[bytes]]:
    batch: list[bytes] = []
    for line in lines:
        batch.append(line)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _bounded_map(fn: Callable, items: Iterable, workers: int) -> Iterator:
    """``fn`` over ``items`` in a process pool, at most 3 x workers in
    flight — unlike Pool.imap, whose feeder thread drains the input as fast
    as it can read it (here: a 1.5 TB decompressed stream)."""
    if workers <= 0:
        for item in items:
            yield fn(item)
        return
    import multiprocessing as mp
    ctx = mp.get_context("fork" if hasattr(os, "fork") else "spawn")
    with ctx.Pool(workers) as pool:
        pending: deque = deque()
        for item in items:
            pending.append(pool.apply_async(fn, (item,)))
            while len(pending) >= workers * 3:
                yield pending.popleft().get()
        while pending:
            yield pending.popleft().get()


def build_lexicon(dump_path: str, out_path: str, *, min_sitelinks: int = 1,
                  workers: int | None = None, limit_lines: int = 0,
                  progress: Callable[[str], None] = print) -> dict[str, Any]:
    """Stream ``dump_path`` into a fresh lexicon at ``out_path``.

    ``workers=0`` parses in-process (tests, small samples); the default is
    one per CPU but one, leaving a core for decompression and SQLite.
    """
    if workers is None:
        workers = max(1, (os.cpu_count() or 2) - 1)
    tmp = out_path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    db = sqlite3.connect(tmp)
    db.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;"
                     "PRAGMA temp_store=FILE; PRAGMA cache_size=-262144;")
    db.executescript(_SCHEMA)

    started = time.monotonic()
    stats = {"lines": 0, "entities": 0, "aliases": 0, "subclass_edges": 0,
             "kym_ids": 0}
    newest: str | None = None
    ent_rows: list[tuple] = []
    alias_rows: list[tuple] = []
    sub_rows: list[tuple] = []
    kym_rows: list[tuple] = []

    def flush() -> None:
        db.executemany("INSERT OR REPLACE INTO entity VALUES (?,?,?,?,?,?,?)", ent_rows)
        db.executemany("INSERT INTO alias VALUES (?,?,?,?)", alias_rows)
        db.executemany("INSERT INTO subclass VALUES (?,?)", sub_rows)
        db.executemany("INSERT INTO kym VALUES (?,?,?)", kym_rows)
        db.commit()
        for rows in (ent_rows, alias_rows, sub_rows, kym_rows):
            rows.clear()

    batches = ((b, min_sitelinks) for b in _batches(
        iter_dump_lines(dump_path, limit=limit_lines), _BATCH_LINES))
    last_report = started
    for rows, n_lines, batch_newest in _bounded_map(_parse_batch, batches, workers):
        stats["lines"] += n_lines
        if batch_newest and (newest is None or batch_newest > newest):
            newest = batch_newest
        for row in rows:
            sub_rows.extend(row["subclass"])
            stats["subclass_edges"] += len(row["subclass"])
            if "label" not in row:
                continue
            q = row["qid"]
            ent_rows.append((q, row["label"], row["description"], row["sitelinks"],
                             row["enwiki"], " ".join(str(t) for t in row["types"]),
                             row["kym"][0][0] if row["kym"] else None))
            alias_rows.extend((key, q, surface, is_label)
                              for key, surface, is_label in row["aliases"])
            kym_rows.extend((key, kind, q) for key, kind in row["kym"])
            stats["entities"] += 1
            stats["aliases"] += len(row["aliases"])
            stats["kym_ids"] += len(row["kym"])
        if len(ent_rows) + len(sub_rows) >= _INSERT_BATCH:
            flush()
        now = time.monotonic()
        if now - last_report >= 60:
            last_report = now
            progress(f"  {stats['lines']:,} lines, {stats['entities']:,} entities "
                     f"kept ({stats['lines'] / (now - started):,.0f} lines/s)")
    flush()

    progress("Indexing ...")
    db.executescript(_INDEXES)
    db.execute("ANALYZE")
    stat = os.stat(dump_path)
    filters = {"min_sitelinks": min_sitelinks, "languages": list(LABEL_LANGUAGES),
               "excluded_classes": sorted(EXCLUDED_CLASSES)}
    identity = {"builder": LEXICON_BUILDER_VERSION, "filters": filters,
                "dump": os.path.basename(dump_path), "dump_bytes": stat.st_size,
                "dump_newest_modified": newest, "limit_lines": limit_lines}
    version = hashlib.sha256(
        json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    meta = {
        "version": version, "builder_version": LEXICON_BUILDER_VERSION,
        "dump": os.path.basename(dump_path), "dump_bytes": str(stat.st_size),
        "dump_newest_modified": newest or "", "filters": json.dumps(filters),
        "limit_lines": str(limit_lines),
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **{k: str(v) for k, v in stats.items()},
    }
    db.executemany("INSERT INTO meta VALUES (?,?)", sorted(meta.items()))
    db.commit()
    db.close()
    os.replace(tmp, out_path)
    elapsed = round(time.monotonic() - started, 1)
    progress(f"Done in {elapsed}s: {stats['entities']:,} entities, "
             f"{stats['aliases']:,} aliases, {stats['subclass_edges']:,} P279 "
             f"edges, {stats['kym_ids']:,} KYM ids -> {out_path} ({version})")
    return {**stats, "version": version, "elapsed_s": elapsed,
            "out_path": out_path, "dump_newest_modified": newest}


# ----------------------------------------------------------------- reading ---

@dataclass(frozen=True)
class Candidate:
    """One item a surface form may name, with what disambiguation needs."""
    qid: int
    label: str
    description: str | None
    sitelinks: int
    types: tuple[int, ...]
    kym: str | None          # its KYM slug (or number): the item is itself a KYM entry
    surface: str             # the label/alias that matched, as Wikidata spells it
    is_label: bool           # matched the label, not an alias

    @property
    def id(self) -> str:
        return qid_str(self.qid)


def _candidate(row: Sequence, surface: str | None = None,
               is_label: bool = True) -> Candidate:
    return Candidate(qid=row[0], label=row[1], description=row[2],
                     sitelinks=row[3],
                     types=tuple(int(t) for t in row[4].split() if t),
                     kym=row[5], surface=surface or row[1], is_label=is_label)


class Lexicon:
    """Read-only lookups against a built lexicon. Cheap to open; one per
    mapped task. Lookups are cached for the life of the object."""

    def __init__(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"no Wikidata lexicon at {path} — build one with "
                f"`python -m modules.kg.wikidata build` (see kg/wikidata.py)")
        self.path = path
        self._db = sqlite3.connect(f"file:{path}?mode=ro", uri=True,
                                   check_same_thread=False)
        self.meta = dict(self._db.execute("SELECT key, value FROM meta"))
        self.version = self.meta["version"]
        self._ancestors: dict[int, frozenset[int]] = {}
        self.candidates = lru_cache(maxsize=200_000)(self._candidates)

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "Lexicon":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def _candidates(self, key: str, limit: int = MAX_CANDIDATES) -> tuple[Candidate, ...]:
        if not key:
            return ()
        rows = self._db.execute(
            "SELECT e.qid, e.label, e.description, e.sitelinks, e.types, e.kym, "
            "a.surface, a.is_label FROM alias a JOIN entity e ON e.qid = a.qid "
            "WHERE a.key = ? ORDER BY e.sitelinks DESC, e.qid LIMIT ?",
            (key, limit)).fetchall()
        return tuple(_candidate(r, surface=r[6], is_label=bool(r[7])) for r in rows)

    def entity(self, qid: int | str) -> Candidate | None:
        q = qid_int(qid) if isinstance(qid, str) else qid
        r = self._db.execute(
            "SELECT qid, label, description, sitelinks, types, kym FROM entity "
            "WHERE qid = ?", (q,)).fetchone()
        return _candidate(r) if r else None

    def by_kym(self, frame_url: str) -> Candidate | None:
        """The item whose KYM slug (P13484) is this page — the meme itself."""
        return self._by_kym_key(kym_slug(frame_url), "slug")

    def by_kym_id(self, number: str | int) -> Candidate | None:
        """The item whose KYM numeric ID (P6760) is ``number`` — unused until
        the parser records the number (see "KYM identifiers" above)."""
        return self._by_kym_key(str(number).strip(), "id")

    def _by_kym_key(self, key: str, kind: str) -> Candidate | None:
        if not key:
            return None
        rows = self._db.execute("SELECT qid FROM kym WHERE key = ? AND kind = ? "
                                "ORDER BY qid LIMIT 2", (key, kind)).fetchall()
        # Two items claiming one KYM page is a Wikidata data error; picking
        # either would be a guess, and this link is meant to be the certain one.
        return self.entity(rows[0][0]) if len(rows) == 1 else None

    def ancestors(self, cls: int, max_depth: int = 8) -> frozenset[int]:
        """``cls`` and every class above it by P279, to ``max_depth`` hops.
        Memoised per class, so a whole run's type checks cost one walk per
        distinct class rather than one per mention."""
        cached = self._ancestors.get(cls)
        if cached is not None:
            return cached
        seen = {cls}
        frontier = [cls]
        for _ in range(max_depth):
            if not frontier:
                break
            marks = ",".join("?" * len(frontier))
            parents = [p for (p,) in self._db.execute(
                f"SELECT parent FROM subclass WHERE qid IN ({marks})", frontier)]
            frontier = [p for p in dict.fromkeys(parents) if p not in seen]
            seen.update(frontier)
        result = frozenset(seen)
        self._ancestors[cls] = result
        return result

    def families(self, types: Iterable[int]) -> frozenset[int]:
        out: set[int] = set()
        for t in types:
            out |= self.ancestors(t)
        return frozenset(out)


def prior(sitelinks: int, saturation: int = 150) -> float:
    """Popularity in [0, 1]: log Wikipedia count, saturating at
    ``saturation`` Wikipedias (a country, a household name)."""
    return min(1.0, math.log1p(max(0, sitelinks)) / math.log1p(saturation))


# ------------------------------------------------------------------- CLI ----

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m modules.kg.wikidata",
        description="Build and query the local Wikidata entity lexicon.")
    sub = parser.add_subparsers(dest="command", required=True)

    b = sub.add_parser("build", help="Wikidata JSON dump -> lexicon.sqlite")
    b.add_argument("--dump", required=True, help="wikidata-YYYYMMDD-all.json.gz (or .bz2)")
    b.add_argument("--out", required=True, help="lexicon .sqlite to (re)write")
    b.add_argument("--min-sitelinks", type=int, default=1)
    b.add_argument("--workers", type=int, default=None)
    b.add_argument("--limit-lines", type=int, default=0,
                   help="stop after N dump lines (samples only)")

    i = sub.add_parser("info", help="what a lexicon was built from")
    i.add_argument("--lexicon", required=True)

    lk = sub.add_parser("lookup", help="candidates for a surface form")
    lk.add_argument("--lexicon", required=True)
    lk.add_argument("text")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.command == "build":
        summary = build_lexicon(args.dump, args.out, min_sitelinks=args.min_sitelinks,
                                workers=args.workers, limit_lines=args.limit_lines)
        print(json.dumps(summary, indent=2))
        return 0
    with Lexicon(args.lexicon) as lex:
        if args.command == "info":
            print(json.dumps(lex.meta, indent=2))
            return 0
        key = norm(args.text)
        rows = [{"qid": c.id, "label": c.label, "description": c.description,
                 "sitelinks": c.sitelinks, "matched": c.surface,
                 "is_label": c.is_label, "kym": c.kym}
                for c in lex.candidates(key)]
        print(json.dumps({"key": key, "candidates": rows}, indent=2,
                         ensure_ascii=False))
        return 0


if __name__ == "__main__":       # pragma: no cover
    sys.exit(main())
