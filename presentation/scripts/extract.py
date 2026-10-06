"""Everything the deck shows about the graph and its running example, as JSON.

Runs inside the Airflow worker, which has Mongo, Neo4j and the pipeline's
own modules:

    docker compose exec -T airflow-worker python - [build_id] \
        < ../presentation/scripts/extract.py > ../presentation/data/live.json

``build_id`` defaults to the live build, so the 7.1.0 figures are the same
command once that build is published. Read-only: no write to any store.
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

sys.path.insert(0, "/opt/airflow/dags")

from modules.kg import loaders  # noqa: E402
from modules.mongo_base import MongoStoreBase  # noqa: E402

# The running example, by the address IMKG and KG 7.0.0 used. From 7.1.0 the
# frame is the address KYM gives the entry today (gap 14: Doge moved to
# /sensitive/memes/doge) and lists the old one as also_at — resolved below.
EXAMPLE_ADDRESS = "https://knowyourmeme.com/memes/doge"
KG_DATA_DIR = os.getenv("KG_DATA_DIR", "/opt/airflow/data/kg")


class _Mongo(MongoStoreBase):
    def _configure(self) -> None:
        pass


db = _Mongo().db
cfg = loaders.Neo4jConfig.from_env()
driver = loaders.neo4j_driver(cfg)


def cy(cypher: str, **params) -> list[dict]:
    return loaders._run(driver, cfg, cypher, **params)


build = sys.argv[1] if len(sys.argv) > 1 else (db.kg_builds.find_one({"_id": "current"}) or {})["build_id"]
B = {"b": build}
_found = cy("MATCH (f:Frame {build_id: $b}) WHERE f.id = $u OR $u IN coalesce(f.also_at, []) "
            "RETURN f.id AS id", b=build, u=EXAMPLE_ADDRESS)
EXAMPLE = _found[0]["id"] if _found else EXAMPLE_ADDRESS
out: dict = {"build_id": build, "extracted_at": datetime.now(timezone.utc).isoformat()}

# -- the build: its manifest and the metrics computed after publish ----------
bdoc = db.kg_builds.find_one({"_id": build}) or {}
manifest = bdoc.get("manifest") or {}
out["build"] = {
    "version": (bdoc.get("stamps") or {}).get("kg_build_version"),
    "published_at": str(bdoc.get("published_at") or ""),
    "counts": manifest.get("counts", {}),
    "validation": {k: v for k, v in (bdoc.get("validation") or {}).items() if k != "samples"},
}
mpath = os.path.join(KG_DATA_DIR, "builds", build, "metrics.json")
if os.path.exists(mpath):
    with open(mpath) as fh:
        m = json.load(fh)
    out["metrics_core"] = {"replication": m.get("replication"),
                           "integrity": {k: v for k, v in (m.get("integrity") or {}).items()
                                         if not k.endswith("_sample")}}

# -- the graph from above ------------------------------------------------------
out["frames_by_year_category"] = cy(
    "MATCH (f:Frame {build_id: $b}) RETURN f.year AS year, f.category AS category, count(*) AS n", **B)
out["origin_by_year"] = cy(
    "MATCH (f:Frame {build_id: $b})-[:hasOrigin]->(o) RETURN f.year AS year, o.label AS origin, count(*) AS n", **B)
out["events_by_year"] = cy(
    "MATCH (e:Event {build_id: $b}) WHERE e.date_start IS NOT NULL "
    "RETURN toInteger(substring(e.date_start, 0, 4)) AS year, e.source_section AS section, count(*) AS n", **B)
out["event_platforms_by_year"] = cy(
    "MATCH (e:Event {build_id: $b}) WHERE e.date_start IS NOT NULL AND e.location_type IN ['platform', 'both'] "
    "UNWIND e.locations AS place "
    "RETURN toInteger(substring(e.date_start, 0, 4)) AS year, toLower(place) AS place, count(*) AS n", **B)
out["top_items_by_frames"] = cy(
    "MATCH (f:Frame {build_id: $b})-[:fromTitle|fromTags|fromAbout]->(e:WikidataEntity) "
    "RETURN e.qid AS qid, e.label AS label, e.description AS description, count(DISTINCT f) AS frames "
    "ORDER BY frames DESC LIMIT 25", **B)
out["top_tags"] = cy(
    "MATCH (f:Frame {build_id: $b})-[:hasTag]->(t) RETURN t.label AS tag, count(*) AS frames "
    "ORDER BY frames DESC LIMIT 25", **B)
out["top_entry_types"] = cy(
    "MATCH (f:Frame {build_id: $b})-[:hasEntryType]->(t) RETURN t.label AS type, count(*) AS frames "
    "ORDER BY frames DESC LIMIT 25", **B)
out["top_templates"] = cy(
    "MATCH (f:Frame {build_id: $b})-[:hasTemplate]->(t:Template) "
    "RETURN t.label AS template, t.template_id AS template_id, count(DISTINCT f) AS frames "
    "ORDER BY frames DESC LIMIT 15", **B)
out["top_image_items"] = cy(
    "MATCH (t:Template {build_id: $b})-[:fromImage]->(e:WikidataEntity) "
    "RETURN e.label AS label, e.qid AS qid, count(DISTINCT t) AS templates ORDER BY templates DESC LIMIT 15", **B)
out["layers"] = cy(
    "MATCH (f:Frame {build_id: $b}) "
    "WITH f, EXISTS { (f)-[:fromTitle|fromTags|fromAbout]->() } AS wd, "
    "     EXISTS { (f)-[:hasTemplate]->() } AS tpl, EXISTS { (f)-[:hasEvent]->() } AS ev "
    "RETURN count(*) AS frames, sum(CASE WHEN wd THEN 1 ELSE 0 END) AS with_wikidata, "
    "sum(CASE WHEN tpl THEN 1 ELSE 0 END) AS with_template, sum(CASE WHEN ev THEN 1 ELSE 0 END) AS with_events, "
    "sum(CASE WHEN wd AND tpl AND ev THEN 1 ELSE 0 END) AS with_all, "
    "sum(CASE WHEN NOT wd AND NOT tpl AND NOT ev THEN 1 ELSE 0 END) AS with_none", **B)[0]
out["metagraph"] = cy(
    "MATCH (a:KGNode {build_id: $b})-[r]->(c:KGNode) "
    "RETURN a.kind AS src, type(r) AS rel, c.kind AS dst, count(*) AS n", **B)
out["series_sizes"] = cy(
    "MATCH (c:Frame {build_id: $b})-[:partOfSeries]->(p) "
    "RETURN p.id AS parent, coalesce(p.label, p.id) AS label, p.kind AS kind, count(c) AS children "
    "ORDER BY children DESC LIMIT 15", **B)
out["sibling_pairs"] = cy(
    "MATCH (a:Frame {build_id: $b})-[:sharesSameSeries]->(c) "
    "RETURN count(*) AS pairs, "
    "sum(CASE WHEN EXISTS { (a)-[:citesMediaFrame]->(c) } OR EXISTS { (c)-[:citesMediaFrame]->(a) } "
    "    THEN 1 ELSE 0 END) AS linked_by_page", **B)[0]
out["sensitive"] = cy(
    "MATCH (f:Frame {build_id: $b}) WHERE f.id CONTAINS '/sensitive/' "
    "OPTIONAL MATCH (g:Frame {build_id: $b, label: f.label}) WHERE NOT g.id CONTAINS '/sensitive/' "
    "RETURN count(DISTINCT f) AS sensitive_frames, count(DISTINCT CASE WHEN g IS NOT NULL THEN f END) AS with_public_twin", **B)[0]

# -- per-stage figures the dashboard does not keep -----------------------------
out["doms_bytes"] = next(iter(db.doms.aggregate([
    {"$group": {"_id": None, "raw": {"$sum": "$content_length"},
                "stored": {"$sum": {"$binarySize": "$html"}}, "pages": {"$sum": 1}}}])), {})
out["doms_bytes"].pop("_id", None)
lex_path = os.getenv("WIKIDATA_LEXICON", "/opt/airflow/data/wikidata/lexicon.sqlite")
if os.path.exists(lex_path):
    import sqlite3
    con = sqlite3.connect(f"file:{lex_path}?mode=ro", uri=True)
    out["lexicon"] = {"meta": dict(con.execute("SELECT key, value FROM meta")),
                      "entities": con.execute("SELECT count(*) FROM entity").fetchone()[0],
                      "aliases": con.execute("SELECT count(*) FROM alias").fetchone()[0],
                      "bytes": os.path.getsize(lex_path)}
    con.close()
out["event_reviews"] = list(db.event_reviews.aggregate([
    {"$group": {"_id": {"stratum": "$stratum", "verdict": "$verdict"}, "n": {"$sum": 1}}}]))

# -- the running example, stage by stage ---------------------------------------
ex: dict = {"url": EXAMPLE, "also_at": sorted(
    d["url"] for d in db.urls.find({"duplicate_of": EXAMPLE}, {"url": 1}))}
u = db.urls.find_one({"url": EXAMPLE}) or {}
ex["discovery"] = {k: u.get(k) for k in ("namespace", "lastmod", "Confirmed", "last_scraped")}
d = db.doms.find_one({"url": EXAMPLE}) or {}
ex["dom"] = {"raw_bytes": d.get("content_length"), "stored_bytes": len(d.get("html") or b""),
             "encoding": d.get("encoding"), "fetched_at": str(d.get("fetched_at"))}
e = db.entries.find_one({"url": EXAMPLE}) or {}
ex["entry"] = {
    "title": e.get("title"), "category": e.get("category"), "status": e.get("status"),
    "year": e.get("year"), "origin": e.get("origin"), "region": e.get("region"),
    "entry_type": e.get("entry_type"), "tags": e.get("tags"), "series_parent": e.get("series_parent"),
    "corpus_status": e.get("corpus_status"), "parser_version": e.get("parser_version"),
    "kym_added": e.get("kym_added"), "og_image": e.get("og_image"),
    "external_references": len(e.get("external_references") or []),
    "additional_references": len(e.get("additional_references") or []),
    "sections": [{"kind": s.get("kind"), "heading": s.get("heading"),
                  "paragraphs": len(s.get("text") or []), "images": len(s.get("images") or []),
                  "links": len(s.get("links") or [])} for s in e.get("sections") or []],
    "about": " ".join((next((s for s in e.get("sections") or [] if s.get("kind") == "about"), {}) or {}).get("text") or [])[:1200],
    "origin_text": " ".join((next((s for s in e.get("sections") or [] if s.get("kind") == "origin"), {}) or {}).get("text") or [])[:1500],
}
ent = db.entities.find_one({"frame_url": EXAMPLE}) or {}
cur = db.entity_curation.find_one({"frame_url": EXAMPLE}) or {}
decisions = {dd["key"]: dd for dd in cur.get("decisions") or []}
roles = ((cur.get("judge") or {}).get("roles") or {})
mentions = []
for mm in ent.get("mentions") or []:
    key = f"{mm.get('field')}|"
    dec = next((dd for k, dd in decisions.items()
                if dd.get("qid") == mm.get("qid") and dd.get("field") == mm.get("field")), {})
    mentions.append({k: mm.get(k) for k in ("field", "text", "qid", "label", "description", "score", "method")}
                    | {"keep": dec.get("keep"), "basis": dec.get("basis"), "role": roles.get(mm.get("qid"))})
ex["entities"] = {"mentions": mentions, "self_qid": ent.get("self_qid"), "rejected_count": ent.get("rejected_count"),
                  "in_graph": cur.get("in_graph_count"), "judge_model": (cur.get("judge") or {}).get("judge_model")}
evs = []
for doc in db.events.find({"frame_url": EXAMPLE}).sort("source_section", 1):
    for i, ev in enumerate(doc.get("events") or []):
        evs.append({k: ev.get(k) for k in ("date", "date_precision", "date_text", "locations", "location_type",
                                           "actors", "certainty", "source_text", "event_id")}
                   | {"section": doc.get("source_section"), "order": i, "model": doc.get("model")})
ex["events"] = evs
ft = db.frame_templates.find_one({"frame_url": EXAMPLE}) or {}
names = {}
ids = {t["template_id"] for t in ft.get("selected") or []} | {
    mid for t in ft.get("selected") or [] for mid in t.get("members") or []}
for t in db.imgflip_templates.find({"_id": {"$in": list(ids)}}, {"name": 1, "blank_path": 1}):
    names[t["_id"]] = {"name": t.get("name"), "blank_path": t.get("blank_path")}
sel = []
for t in ft.get("selected") or []:
    te = db.template_entities.find_one({"_id": t["template_id"]}) or {}
    regions = ((te.get("detection") or {}).get("regions") or [])
    links = [{k: lm.get(k) for k in ("text", "qid", "label", "score", "method", "in_graph", "source")}
             | {"region": (lm.get("region") or {}).get("index")}
             for lm in ((te.get("links") or {}).get("mentions") or [])]
    sel.append({**{k: t.get(k) for k in ("template_id", "R", "method", "mmr_rank", "s_text", "s_vis", "s_rank")},
                "members": [{"id": mid, "name": (names.get(mid) or {}).get("name")} for mid in t.get("members") or []],
                **(names.get(t["template_id"]) or {}), "regions": regions, "links": links})
ex["templates"] = {"candidates": len(ft.get("candidates") or []), "queries": ft.get("queries"),
                   "pages_fetched": ft.get("pages_fetched"), "accepted_groups": ft.get("accepted_groups"),
                   "gold": ft.get("gold"), "selected": sel, "best_rejected": ft.get("best_rejected")}
X = {"b": build, "u": EXAMPLE}
ex["kg"] = {
    "edges_by_type": cy("MATCH (f:Frame {build_id: $b, id: $u})-[r]-(x) "
                        "RETURN type(r) AS rel, startNode(r) = f AS outgoing, x.kind AS kind, count(*) AS n", **X),
    "wikidata": cy("MATCH (f:Frame {build_id: $b, id: $u})-[r:fromTitle|fromTags|fromAbout]->(e) "
                   "RETURN type(r) AS rel, e.qid AS qid, e.label AS label, r.relevance_bases AS bases", **X),
    "templates": cy("MATCH (f:Frame {build_id: $b, id: $u})-[:hasTemplate]->(t) "
                    "OPTIONAL MATCH (t)-[:fromImage]->(e) "
                    "RETURN t.label AS template, t.template_id AS template_id, collect(e.label) AS shows", **X),
    "ancestors": cy("MATCH p = (f:Frame {build_id: $b, id: $u})-[:partOfSeries*1..10]->(a) "
                    "RETURN [n IN nodes(p) | coalesce(n.label, n.id)] AS chain ORDER BY length(p) DESC LIMIT 1", **X),
    "descendants": cy("MATCH p = (d:Frame {build_id: $b})-[:partOfSeries*1..10]->(f:Frame {build_id: $b, id: $u}) "
                      "RETURN [n IN nodes(p) | n.label] AS chain", **X),
    "siblings": cy("MATCH (f:Frame {build_id: $b, id: $u})-[:sharesSameSeries]-(s) "
                   "RETURN s.label AS label, s.year AS year, "
                   "EXISTS { (f)-[:citesMediaFrame]->(s) } OR EXISTS { (s)-[:citesMediaFrame]->(f) } AS linked", **X),
    "tags": [r["t"] for r in cy("MATCH (f:Frame {build_id: $b, id: $u})-[:hasTag]->(t) RETURN t.label AS t", **X)],
    "entry_types": [r["t"] for r in cy("MATCH (f:Frame {build_id: $b, id: $u})-[:hasEntryType]->(t) RETURN t.label AS t", **X)],
    "story": cy("MATCH (f:Frame {build_id: $b, id: $u})-[:hasEvent]->(e) "
                "OPTIONAL MATCH (e)-[:nextInStory]->(n) "
                "RETURN e.id AS id, e.date AS date, e.date_precision AS precision, e.locations AS locations, "
                "e.source_section AS section, n.id AS next", **X),
}

# A few N-Triples about the example, one per predicate, as the RDF actually reads.
nt = os.path.join(KG_DATA_DIR, "builds", build, "graph.nt")
seen, triples = set(), []
subject = f"<{EXAMPLE}> "
if os.path.exists(nt):
    with open(nt, encoding="utf-8") as fh:
        for line in fh:
            if line.startswith(subject):
                pred = line.split(" ", 2)[1]
                if pred not in seen:
                    seen.add(pred)
                    triples.append(line.strip()[:400])
            elif line.startswith("<< " + subject) and len(triples) < 60:
                pred = "<<" + line.split(" ", 3)[2]
                if pred not in seen:
                    seen.add(pred)
                    triples.append(line.strip()[:400])
ex["rdf_sample"] = triples
out["example"] = ex
driver.close()
json.dump(out, sys.stdout, default=str, indent=1)
