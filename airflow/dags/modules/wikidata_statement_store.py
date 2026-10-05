"""
wikidata_statement_store.py — MongoDB persistence for linked items' statements
==============================================================================
The ONLY place kym_wikidata_statements touches the database. Knows nothing
about the dump — kg/wikidata_statements.py reads it.

Collections
-----------
``entities``, ``template_entities``, ``frame_image_entities``  (read-only)
    every Wikidata item some frame, template or frame image links to: the
    items whose statements are imported (more than the graph keeps — a
    statement is cheap to hold, and curation can change its mind).

``wikidata_statements``  ONE DOC PER ITEM (``_id`` = "Q42")
    statements    [[property, value QID], ...], truthy and item-valued
    label, description
    dump, statements_version   — the stamps: a new dump or a new rule
                  re-extracts
    extracted_at

``wikidata_labels``  the labels the lexicon cannot give
    ``_id`` = "Q.." for a statement VALUE the lexicon does not hold (an
    item with no Wikipedia article), or "P.." for a property.

Environment: MONGODB_URI, MONGODB_DB, and the collection overrides
MONGODB_WIKIDATA_STATEMENTS_COLLECTION, MONGODB_WIKIDATA_LABELS_COLLECTION.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from modules.mongo_base import MongoStoreBase, as_utc, now_utc

log = logging.getLogger("wikidata_statement_store")

__all__ = [
    "WikidataStatementStore", "get_store", "linked_qids", "pending", "save_items",
    "save_labels", "labels_known", "statements_for", "labels_for",
    "property_labels", "graph_stamps", "stats",
]

LOOKUP_BATCH = 5000
BULK_BATCH = 1000


def _is_qid(q: Any) -> bool:
    return isinstance(q, str) and q.startswith("Q") and q[1:].isdigit()


class WikidataStatementStore(MongoStoreBase):
    """Owner of wikidata_statements and wikidata_labels."""

    def _configure(self) -> None:
        self.entities = self.collection("MONGODB_ENTITIES_COLLECTION", "entities")
        self.template_entities = self.collection("MONGODB_TEMPLATE_ENTITIES_COLLECTION",
                                                 "template_entities")
        self.frame_image_entities = self.collection(
            "MONGODB_FRAME_IMAGE_ENTITIES_COLLECTION", "frame_image_entities")
        self.statements = self.collection("MONGODB_WIKIDATA_STATEMENTS_COLLECTION",
                                          "wikidata_statements")
        self.labels = self.collection("MONGODB_WIKIDATA_LABELS_COLLECTION", "wikidata_labels")
        self.statements.create_index("extracted_at")

    # -- what to read ---------------------------------------------------------

    def linked_qids(self) -> set[str]:
        """Every item a frame's text, a template's image or a frame's image
        links to."""
        out: set[str] = set()
        out.update(q for q in self.entities.distinct("self_qid") if _is_qid(q))
        out.update(q for q in self.entities.distinct("mentions.qid") if _is_qid(q))
        for coll in (self.template_entities, self.frame_image_entities):
            out.update(q for q in coll.distinct("links.mentions.qid") if _is_qid(q))
        return out

    def pending(self, stamps: dict[str, str]) -> list[str]:
        """Linked items without statements under these stamps (dump, rule)."""
        linked = sorted(self.linked_qids(), key=lambda q: int(q[1:]))
        fresh = {d["_id"] for d in self.statements.find(stamps, {"_id": 1})}
        return [q for q in linked if q not in fresh]

    # -- writes ---------------------------------------------------------------

    def save_items(self, rows: Iterable[dict], stamps: dict[str, str]) -> int:
        """One doc per item, REPLACED: what the dump says now, not merged
        with what an older one said. An item the dump no longer has (merged
        or deleted on Wikidata) is saved with no statements, so it is not
        asked for again under the same stamps."""
        from pymongo import ReplaceOne

        now = now_utc()
        ops = [ReplaceOne({"_id": r["id"]}, {
            "_id": r["id"], "statements": [list(s) for s in r.get("statements") or []],
            "label": r.get("label"), "description": r.get("description"),
            **stamps, "extracted_at": now}, upsert=True) for r in rows]
        for i in range(0, len(ops), BULK_BATCH):
            self.statements.bulk_write(ops[i:i + BULK_BATCH], ordered=False)
        return len(ops)

    def save_labels(self, rows: Iterable[dict], dump: str) -> int:
        from pymongo import ReplaceOne

        now = now_utc()
        ops = [ReplaceOne({"_id": r["id"]}, {
            "_id": r["id"], "label": r.get("label"), "description": r.get("description"),
            "dump": dump, "saved_at": now}, upsert=True) for r in rows]
        for i in range(0, len(ops), BULK_BATCH):
            self.labels.bulk_write(ops[i:i + BULK_BATCH], ordered=False)
        return len(ops)

    def labels_known(self, ids: Iterable[str], dump: str) -> set[str]:
        ids = list(ids)
        out: set[str] = set()
        for i in range(0, len(ids), LOOKUP_BATCH):
            out.update(d["_id"] for d in self.labels.find(
                {"_id": {"$in": ids[i:i + LOOKUP_BATCH]}, "dump": dump}, {"_id": 1}))
        return out

    # -- the KG build's reads -------------------------------------------------

    def statements_for(self, qids: Iterable[str],
                       extracted_at_lte=None) -> dict[str, list[tuple[str, int]]]:
        """{item: [(property, value number)]}, frozen at the build's snapshot."""
        qids = [q for q in dict.fromkeys(qids) if _is_qid(q)]
        out: dict[str, list[tuple[str, int]]] = {}
        for i in range(0, len(qids), LOOKUP_BATCH):
            query: dict[str, Any] = {"_id": {"$in": qids[i:i + LOOKUP_BATCH]},
                                     "statements.0": {"$exists": True}}
            if extracted_at_lte is not None:
                query["extracted_at"] = {"$lte": extracted_at_lte}
            for d in self.statements.find(query, {"statements": 1}):
                out[d["_id"]] = [(p, int(str(v).lstrip("Q"))) for p, v in d["statements"]]
        return out

    def labels_for(self, ids: Iterable[str]) -> dict[str, tuple[str | None, str | None]]:
        """{id: (label, description)} for the ids wikidata_labels holds."""
        ids = list(dict.fromkeys(ids))
        out: dict[str, tuple[str | None, str | None]] = {}
        for i in range(0, len(ids), LOOKUP_BATCH):
            for d in self.labels.find({"_id": {"$in": ids[i:i + LOOKUP_BATCH]}},
                                      {"label": 1, "description": 1}):
                out[d["_id"]] = (d.get("label"), d.get("description"))
        return out

    def property_labels(self) -> dict[str, str]:
        return {d["_id"]: d.get("label") for d in self.labels.find(
            {"_id": {"$regex": "^P[0-9]+$"}}, {"label": 1}) if d.get("label")}

    def graph_stamps(self, extracted_at_lte=None) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if extracted_at_lte is not None:
            query["extracted_at"] = {"$lte": extracted_at_lte}
        agg = list(self.statements.aggregate([
            {"$match": query},
            {"$group": {"_id": None, "items": {"$sum": 1},
                        "statements": {"$sum": {"$size": "$statements"}},
                        "max_extracted_at": {"$max": "$extracted_at"},
                        "dumps": {"$addToSet": "$dump"},
                        "versions": {"$addToSet": "$statements_version"}}}]))
        row = agg[0] if agg else {}
        latest = as_utc(row.get("max_extracted_at"))
        return {"wikidata_statement_items": row.get("items", 0),
                "wikidata_statements": row.get("statements", 0),
                "wikidata_statement_dumps": sorted(x for x in row.get("dumps") or [] if x),
                "wikidata_statement_versions": sorted(x for x in row.get("versions") or [] if x),
                "wikidata_statements_max_extracted_at": latest.isoformat() if latest else None}

    def stats(self) -> dict[str, Any]:
        return {**self.graph_stamps(), "labels": self.labels.count_documents({})}


def get_store(uri: str | None = None, db_name: str | None = None) -> WikidataStatementStore:
    return WikidataStatementStore(uri=uri, db_name=db_name)


# ---------------------------------------------------------------------------
# Facades — the only calls the DAGs make
# ---------------------------------------------------------------------------

def linked_qids() -> set[str]:
    with get_store() as store:
        return store.linked_qids()


def pending(stamps: dict[str, str]) -> list[str]:
    with get_store() as store:
        return store.pending(stamps)


def save_items(rows: Iterable[dict], stamps: dict[str, str]) -> int:
    with get_store() as store:
        return store.save_items(rows, stamps)


def save_labels(rows: Iterable[dict], dump: str) -> int:
    with get_store() as store:
        return store.save_labels(rows, dump)


def labels_known(ids: Iterable[str], dump: str) -> set[str]:
    with get_store() as store:
        return store.labels_known(ids, dump)


def statements_for(qids: Iterable[str], extracted_at_lte=None) -> dict[str, list]:
    with get_store() as store:
        return store.statements_for(qids, extracted_at_lte)


def labels_for(ids: Iterable[str]) -> dict[str, tuple[str | None, str | None]]:
    with get_store() as store:
        return store.labels_for(ids)


def property_labels() -> dict[str, str]:
    with get_store() as store:
        return store.property_labels()


def graph_stamps(extracted_at_lte=None) -> dict[str, Any]:
    with get_store() as store:
        return store.graph_stamps(extracted_at_lte)


def stats() -> dict[str, Any]:
    with get_store() as store:
        return store.stats()
