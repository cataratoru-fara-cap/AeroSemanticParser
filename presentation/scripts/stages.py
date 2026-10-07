"""Per-stage figures, computed by the dashboard's own functions (dashboard/lib/data.py).

Runs inside the dashboard container, which has the Mongo settings and the code:

    docker cp scripts/stages.py kym_dashboard:/tmp/stages.py
    docker exec -w /app kym_dashboard python /tmp/stages.py > data/stages.json

Read-only. Keys are made JSON-safe (a tuple key becomes "a | b").

The figures cover the corpus: Mongo still holds what the stages made of the
entries retired as duplicates (gap 14: the 836 addresses marked
`urls.duplicate_of`), so the per-entry collections are read through a view
that leaves those addresses out. Collection (urls, doms) is counted whole:
fetching both addresses is how the duplicates were found.
"""
import json, sys, warnings, logging
warnings.filterwarnings("ignore"); logging.disable(logging.WARNING)
sys.path.insert(0, ".")
from lib import data

PER_ENTRY = {"entities", "entity_curation", "entity_curation_failures", "events", "event_failures",
             "frame_templates"}          # one or more documents per entry, keyed by frame_url
RETIRED = [d["url"] for d in data._coll("MONGODB_URLS_COLLECTION", "urls").find(
    {"duplicate_of": {"$ne": None}}, {"url": 1})]
KEEP = {"frame_url": {"$nin": RETIRED}}


class _Corpus:
    """A collection seen without the retired entries' documents (read calls only)."""
    def __init__(self, coll):
        self._c = coll

    def count_documents(self, flt, **kw):
        return self._c.count_documents({"$and": [flt, KEEP]} if flt else KEEP, **kw)

    def estimated_document_count(self, **kw):
        return self._c.count_documents(KEEP)

    def aggregate(self, pipeline, **kw):
        return self._c.aggregate([{"$match": KEEP}] + list(pipeline), **kw)

    def distinct(self, key, flt=None, **kw):
        return self._c.distinct(key, {"$and": [flt, KEEP]} if flt else KEEP, **kw)

    def find(self, flt=None, *args, **kw):
        return self._c.find({"$and": [flt, KEEP]} if flt else KEEP, *args, **kw)

    def find_one(self, flt=None, *args, **kw):
        return self._c.find_one({"$and": [flt, KEEP]} if flt else KEEP, *args, **kw)

    def __getattr__(self, name):
        return getattr(self._c, name)


_coll = data._coll
data._coll = lambda env, default: _Corpus(_coll(env, default)) if default in PER_ENTRY else _coll(env, default)

out = {"retired_addresses": len(RETIRED)}
for name in ("discovery_state", "scrape_state", "parse_state", "event_state", "entity_state",
             "curation_state", "template_state", "template_reading", "derived_state", "kg_state",
             "event_progress", "stage_freshness"):
    try:
        fn = getattr(data, name)
        out[name] = getattr(fn, "__wrapped__", fn)()      # past st.cache_data
    except Exception as e:
        out[name] = {"error": repr(e)}
def clean(o):
    if isinstance(o, dict):
        return {(" | ".join(map(str, k)) if isinstance(k, tuple) else str(k)): clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(x) for x in o]
    return o
json.dump(clean(out), sys.stdout, default=str, indent=1)
