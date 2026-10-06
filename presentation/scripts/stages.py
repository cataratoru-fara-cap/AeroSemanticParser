"""Per-stage figures, computed by the dashboard's own functions (dashboard/lib/data.py).

Runs inside the dashboard container, which has the Mongo settings and the code:

    docker cp scripts/stages.py kym_dashboard:/tmp/stages.py
    docker exec -w /app kym_dashboard python /tmp/stages.py > data/stages.json

Read-only. Keys are made JSON-safe (a tuple key becomes "a | b").
"""
import json, sys, warnings, logging
warnings.filterwarnings("ignore"); logging.disable(logging.WARNING)
sys.path.insert(0, ".")
from lib import data
out = {}
for name in ("discovery_state", "scrape_state", "parse_state", "event_state", "entity_state",
             "curation_state", "template_state", "template_reading", "derived_state", "kg_state",
             "event_progress", "stage_freshness"):
    try:
        out[name] = getattr(data, name)()
    except Exception as e:
        out[name] = {"error": repr(e)}
def clean(o):
    if isinstance(o, dict):
        return {(" | ".join(map(str, k)) if isinstance(k, tuple) else str(k)): clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(x) for x in o]
    return o
json.dump(clean(out), sys.stdout, default=str, indent=1)
