"""
kg/wikidata_statements.py — the Wikidata statements of the linked items
=======================================================================
Pure: no Mongo, no Airflow, no network. Reads the same dated dump the
lexicon was built from (kg/wikidata.py) and returns, for a set of items,
every statement IMKG would have imported.

Why (KG 7.1.0)
--------------
IMKG enriched its graph with Wikidata statements — 505k edges, 805
relations ("WD subset", Table 2 of the ESWC 2023 paper) — and three of the
paper's four use cases read them: the most meme-able PEOPLE
(``(person)-[:P31]->(:Q5)``), memes based on FILMS
(``(t)-[:P31]->(:Q11424)``), and the sex or gender of the people in the
graph (``(person)-[:P21]->(gender)``). Up to 7.0.0 MemeAtlas linked items
and imported none of their statements, so none of those queries ran
(Gabi, 2026-10-05: import everything, like IMKG).

What IMKG imported (``KGTK Wikidata Enrichment.ipynb``, its repository),
from KGTK's ``claims.wikibase-item`` file:
  2a. every statement to or from a meme's OWN item (joined on P6760);
  2b. for every other item, the statements whose subject AND value were
      both already nodes of its graph.

What is imported here
---------------------
Every TRUTHY, ITEM-VALUED statement OF each linked item: per property the
preferred-rank statements if there are any, else the normal-rank ones —
Wikidata's own definition of a "truthy" statement, the one its ``wdt:``
predicates carry — never a deprecated one, and only a value that is
another item. The values become nodes, so ``Q5`` (human) and
``Q6581097`` (male) are in the graph to be matched, however the item got
there; their own statements are not imported (one hop). A literal (a
date, a string, an external identifier) is not a statement here.

Against IMKG: every truthy 2b statement whose subject is a linked item is
here, and so is every statement from a linked item to an item nothing
else links — which 2b drops, and which lets a query match ``P31 → Q5``
without Q5 having to be linked by some frame first. What IMKG had and
this has not: 2a's INCOMING statements, ``(x) -[P144 based on]-> (a
meme's own item)`` — finding them means parsing every item in the dump,
not just the wanted ones, hours more per pass — and 2b statements whose
subject is only a statement's value. None of the paper's queries reads
either.

Reading the dump
----------------
One pass, through the lexicon build's reader (kg/wikidata.iter_dump_lines,
``pigz``). Each line's id is read from
its first bytes, so only the wanted items — and the ~12k property lines,
for their labels — are parsed as JSON. ~1 h for the 156 GB dump on this
host (the lexicon build took 60 min).
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Callable, Iterable, Sequence

from modules.kg import wikidata as wd

log = logging.getLogger(__name__)

STATEMENTS_VERSION = "1.0.0"
WDT = "http://www.wikidata.org/prop/direct/"
PROPERTY_RE = re.compile(r"^P[1-9][0-9]*$")
LANGUAGES = ("en", "mul")

_HEAD = 400                                   # bytes searched for the id
_ID_RE = re.compile(rb'"id"\s*:\s*"([QP])([0-9]+)"')
_BATCH_LINES = 2000

# Read by the pool's workers. Set before the pool forks, never after.
_WANTED: frozenset[int] = frozenset()
_WITH_STATEMENTS = True


def is_statement_type(edge_type: str) -> bool:
    """A property-graph edge type that is a Wikidata property (``P31``)."""
    return bool(PROPERTY_RE.match(edge_type or ""))


def truthy_item_values(claims: dict) -> list[tuple[str, int]]:
    """(property, item number) for every truthy item-valued statement, in
    property order, each value once per property."""
    out: list[tuple[str, int]] = []
    for pid in sorted(claims, key=lambda p: int(p[1:]) if p[1:].isdigit() else 0):
        if not PROPERTY_RE.match(pid):
            continue
        statements = claims[pid] or []
        best = ([s for s in statements if s.get("rank") == "preferred"]
                or [s for s in statements if s.get("rank") == "normal"])
        seen: set[int] = set()
        for s in best:
            snak = s.get("mainsnak") or {}
            # the value says what it is; ``datatype`` is checked when present
            if snak.get("snaktype") != "value" or snak.get("datatype") not in (
                    None, "wikibase-item"):
                continue
            value = (snak.get("datavalue") or {}).get("value") or {}
            if value.get("entity-type") != "item" or value.get("numeric-id") is None:
                continue
            q = int(value["numeric-id"])
            if q not in seen:
                seen.add(q)
                out.append((pid, q))
    return out


def _label(entity: dict) -> tuple[str | None, str | None]:
    return (wd._first(entity.get("labels") or {}, LANGUAGES),
            wd._first(entity.get("descriptions") or {}, LANGUAGES))


def _scan_batch(lines: list[bytes]) -> tuple[list[dict], int]:
    """The wanted items (and every property) in one batch of dump lines."""
    found: list[dict] = []
    for line in lines:
        m = _ID_RE.search(line, 0, _HEAD)
        if not m:
            continue
        kind, num = m.group(1), int(m.group(2))
        if kind == b"Q" and num not in _WANTED:
            continue
        if kind == b"P" and not _WITH_STATEMENTS:
            continue
        entity = json.loads(line.strip().rstrip(b","))
        label, description = _label(entity)
        row = {"id": entity["id"], "label": label, "description": description}
        if kind == b"Q" and _WITH_STATEMENTS:
            row["statements"] = truthy_item_values(entity.get("claims") or {})
        found.append(row)
    return found, len(lines)


def scan(dump_path: str, items: Iterable[int], *, statements: bool = True,
         workers: int = 0, limit_lines: int = 0,
         progress_every: int = 5_000_000) -> dict[str, Any]:
    """One pass over the dump.

    ``statements=True``: each wanted item's truthy item-valued statements,
    label and description, and every property's label. ``False``: only the
    wanted items' labels (the second, cheaper use: the values the first
    pass found that the lexicon cannot name).

    ``workers=0`` (the default) reads in this process. Unlike the lexicon
    build, which parses 17M items, only the wanted lines are parsed here;
    the rest is decompression (pigz, multi-threaded) and a byte search per
    line, so a pool buys little — and an Airflow task process may be a
    daemon, which cannot start one.

    Returns {"items": {qid: {...}}, "properties": {pid: label}, "lines",
    "seconds"}."""
    global _WANTED, _WITH_STATEMENTS
    _WANTED = frozenset(int(q) for q in items)
    _WITH_STATEMENTS = statements
    started = time.monotonic()
    out_items: dict[str, dict] = {}
    properties: dict[str, str] = {}
    lines = 0
    batches = wd._batches(wd.iter_dump_lines(dump_path, limit=limit_lines), _BATCH_LINES)
    for found, n in wd._bounded_map(_scan_batch, batches, workers):
        lines += n
        for row in found:
            if row["id"].startswith("P"):
                if row["label"]:
                    properties[row["id"]] = row["label"]
            else:
                out_items[row["id"]] = row
        if progress_every and lines // progress_every != (lines - n) // progress_every:
            log.info("  %s lines, %d of %d items found (%.0f lines/s)", f"{lines:,}",
                     len(out_items), len(_WANTED), lines / max(time.monotonic() - started, 1e-9))
    return {"items": out_items, "properties": properties, "lines": lines,
            "seconds": round(time.monotonic() - started, 1)}


def statement_edges(statements: dict[str, Sequence[Sequence]]) -> list[dict]:
    """{subject qid: [(property, value qid)]} -> property-graph edges,
    ``wd:Q..`` -[P31]-> ``wd:Q..``, in a stable order."""
    edges = []
    for subject in sorted(statements, key=lambda q: int(q[1:])):
        for pid, value in statements[subject]:
            edges.append({"src": wd_node(subject), "dst": wd_node(value), "type": pid})
    return edges


def value_nodes(statements: dict[str, Sequence[Sequence]], linked: Iterable[str],
                names: Callable[[str], tuple[str | None, str | None]]) -> list[dict]:
    """The ``wikidata_entity`` nodes for the statement values a build does
    not already hold — one hop: a value is named (``names`` gives its label
    and description), its own statements are not read."""
    have = set(linked)
    values = sorted({f"Q{int(v)}" for st in statements.values() for _p, v in st} - have,
                    key=lambda q: int(q[1:]))
    nodes = []
    for q in values:
        label, description = names(q)
        nodes.append({"id": wd_node(q), "kind": "wikidata_entity", "qid": q,
                      "label": label, "description": description})
    return nodes


def wd_node(qid: str | int) -> str:
    """The graph's node id for an item (build.wikidata_node_id's form)."""
    return f"wd:{qid}" if str(qid).startswith("Q") else f"wd:Q{int(qid)}"
