"""
kg/census.py — frequency + co-occurrence census over one corpus field
========================================================================
Pure: no Mongo, no Airflow. Takes an iterable of `entries` docs and emits
the raw counts needed to derive a hierarchy over a field — per-value
frequency, the per-entry value-count distribution, and pairwise
co-occurrence. Deliberately does NOT compute containment/PMI: those
derivations happen from this output so every threshold choice stays
visible and reviewable instead of hidden in a script default.

Single-valued fields (``origin``)
----------------------------------
Most censused fields are list-valued (``entry_type``, ``tags``) — a frame
can have several. ``origin`` (the infobox platform/franchise/etc. string,
``frame.from`` in the KG) is a single string per frame. ``FieldSpec.
multi_valued=False`` routes it through the same counting logic without
exploding the string into characters (``set("Twitter")`` is not what
anyone wants). A single-valued field's ``pair_cooccurrence`` is always
``[]`` by construction — a value cannot co-occur with itself within one
entry — which is correct, not a bug in either the field or this module.

One implementation, two fields
------------------------------
This replaces two near-identical modules (``kg_census.py`` for
``entry_type`` and ``kg_census_tags.py`` for ``tags``) that emitted
*structurally parallel JSON under different key names*:

    entry_type              tags
    ----------------------  ---------------------
    type_counts             top_tag_counts
    entries_with_entry_type entries_with_tags
    distinct_types          distinct_tags_total

Nothing consumed the tag census as a result — `semantics.py` hardcodes the
entry_type spellings, so the tag output was structurally incapable of
feeding the same describe -> embed -> analyze pipeline despite being the
same kind of data. Both now emit ONE key set (``value_counts``,
``entries_with_value``, ``distinct_values``, …) with a ``field`` stamp
saying which field was censused.

``load_census`` reads either shape, so the definitions and embeddings
already computed against the legacy entry_type file stay valid.

Memory
------
With ``top_k = 0`` this is a single streaming pass. With ``top_k > 0`` the
top-K set cannot be known until every document has been seen, so per-entry
value lists are buffered for the co-occurrence pass — bounded by the
corpus, not the vocabulary (~24k entries x ~10 tags on the current corpus).
Frequency counts are always uncapped; that part is cheap regardless.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m modules.kg.census --field tags --output /opt/airflow/data/kg_census_tags.json

Connection settings (CLI only — the library itself never touches Mongo):
    MONGODB_URI                 (default: mongodb://localhost:27017)
    MONGODB_DB                  (default: memes)
    MONGODB_ENTRIES_COLLECTION  (default: entries)
"""
from __future__ import annotations

import argparse
import itertools
import json
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Iterable

__all__ = ["FieldSpec", "FIELDS", "run_census", "load_census", "CENSUS_VERSION"]

# Bumped when the emitted key set changes, so a stale census on disk is
# detectable rather than silently mismatched against new code. "3": added
# the "origin" field (single-valued) and widened tags' default_top_k
# 300 -> 1000 — old tag censuses at top_k=300 are a different, smaller view.
CENSUS_VERSION = "3"


def _lower_strip(value: str) -> str:
    return value.strip().lower()


@dataclass(frozen=True)
class FieldSpec:
    """How one corpus field is censused.

    ``normalize`` is the difference that actually mattered between the two
    original modules: tags are a folksonomy and were lowercased/stripped,
    entry_type slugs are a controlled vocabulary and were taken verbatim.
    Encoding it here keeps that distinction visible instead of implicit in
    which file you happened to run. It is deliberately just
    ``_lower_strip`` for tags, not the plural-folded form kg/build.py's
    graph nodes use — a curator needs to see the RAW fragmentation
    (``catchphrase`` vs ``catchphrases``) to know folding is needed at
    all. The pipeline that feeds ``coOccursWith`` edges passes the folded
    form explicitly via ``run_census(..., normalize=...)``, so the graph's
    node identity and the co-occurrence edges built from it stay aligned.

    ``multi_valued=False`` is for a field that holds one string per frame
    (``origin``), not a list — see the module docstring.
    """
    normalize: Callable[[str], str] | None = None
    multi_valued: bool = True
    default_top_k: int = 0
    default_min_pair_count: int = 3


FIELDS: dict[str, FieldSpec] = {
    # Controlled vocabulary, 119 values — no normalization, no top-K needed.
    "entry_type": FieldSpec(normalize=None, default_top_k=0,
                            default_min_pair_count=3),
    # Folksonomy, 100k+ values with heavy singular/plural duplication. A full
    # pairwise matrix would be statistically noisy (83k tags occur exactly
    # once) and impractically large to review, hence the top-K restriction.
    # top_k=1000 (was 300): kg/tag_normalize.py folding needs to see enough
    # of the distribution to be worth curating a denylist against.
    "tags": FieldSpec(normalize=_lower_strip, default_top_k=1000,
                      default_min_pair_count=5),
    # Single string per frame (the infobox "Origin" field — platform,
    # country, franchise, company, person; see kg/origin.py). normalize=None:
    # the census must show curators the RAW fragmentation ("Twitter" vs
    # "Twitter / X" vs "twitter.com") so the alias file can be written
    # against real data, not a pre-cleaned view of it.
    "origin": FieldSpec(normalize=None, multi_valued=False,
                        default_top_k=0, default_min_pair_count=0),
}


_UNSET = object()   # distinguishes "use the field's own normalize" from normalize=None


def run_census(docs: Iterable[dict], field: str, *,
               top_k: int | None = None,
               min_pair_count: int | None = None,
               normalize: Callable[[str], str] | None | object = _UNSET) -> dict:
    """Census one field across ``docs``.

    ``docs`` is any iterable of `entries` documents — a Mongo cursor, a
    list, a generator. The caller owns the connection; this stays pure.

    ``normalize`` overrides ``FIELDS[field].normalize`` for this call —
    used by the pipeline that builds ``coOccursWith`` edges to pass the
    plural-folded form (kg/tag_normalize.py, with its curated denylist)
    so the census's value identity matches the graph's ``tag_concept``
    node identity. Left at its sentinel default, the field's own spec is
    used unchanged; passing ``None`` explicitly disables normalization.
    """
    try:
        spec = FIELDS[field]
    except KeyError:
        raise ValueError(
            f"unknown census field {field!r}; known: {sorted(FIELDS)}") from None

    top_k = spec.default_top_k if top_k is None else top_k
    min_pair_count = (spec.default_min_pair_count if min_pair_count is None
                      else min_pair_count)
    norm = spec.normalize if normalize is _UNSET else normalize

    corpus_size = 0
    with_value = 0
    value_counts: Counter[str] = Counter()
    values_per_entry: Counter[int] = Counter()
    pair_counts: Counter[tuple[str, str]] = Counter()
    buffered: list[list[str]] = []   # only populated when top_k > 0

    for doc in docs:
        corpus_size += 1
        value = doc.get(field)
        raw = (value or []) if spec.multi_valued else (
            [value] if value not in (None, "") else [])
        if norm is not None:
            values = sorted({v for v in (norm(x) for x in raw) if v})
        else:
            values = sorted(set(raw))
        if not values:
            values_per_entry[0] += 1
            continue
        with_value += 1
        values_per_entry[len(values)] += 1
        value_counts.update(values)
        if top_k:
            buffered.append(values)
        else:
            for a, b in itertools.combinations(values, 2):
                pair_counts[(a, b)] += 1

    if top_k:
        allowed = {v for v, _ in value_counts.most_common(top_k)}
        for values in buffered:
            restricted = [v for v in values if v in allowed]
            for a, b in itertools.combinations(restricted, 2):
                pair_counts[(a, b)] += 1

    pairs = [{"a": a, "b": b, "count": c}
             for (a, b), c in pair_counts.items() if c >= min_pair_count]
    pairs.sort(key=lambda r: (-r["count"], r["a"], r["b"]))

    reported = (dict(value_counts.most_common(top_k)) if top_k
                else dict(value_counts.most_common()))

    return {
        "census_version": CENSUS_VERSION,
        "field": field,
        "corpus_size": corpus_size,
        "entries_with_value": with_value,
        "distinct_values": len(value_counts),
        "value_counts": reported,
        "values_per_entry_distribution": dict(sorted(values_per_entry.items())),
        "top_k_used_for_cooccurrence": top_k,
        "pair_cooccurrence": pairs,
        "pair_count_min_filter": min_pair_count,
        "total_pairs_seen": len(pair_counts),
        "total_pairs_returned": len(pairs),
    }


# Legacy key -> unified key, for censuses written before the unification.
_LEGACY_KEYS = {
    "type_counts": "value_counts",
    "top_tag_counts": "value_counts",
    "entries_with_entry_type": "entries_with_value",
    "entries_with_tags": "entries_with_value",
    "distinct_types": "distinct_values",
    "distinct_tags_total": "distinct_values",
    "types_per_entry_distribution": "values_per_entry_distribution",
    "tags_per_entry_distribution": "values_per_entry_distribution",
}


def load_census(path: str) -> dict:
    """Read a census file in either the unified or the legacy shape.

    This shim is what lets the 119 entry-type definitions and their
    1024-dim embeddings — real GPU time on a shared lab server — keep
    working against the new key names without being regenerated.
    """
    with open(path) as fh:
        raw = json.load(fh)

    if raw.get("census_version") == CENSUS_VERSION and "value_counts" in raw:
        # Still normalise: JSON stringifies the integer distribution keys
        # whatever the census version, so the current shape needs this too.
        return _restore_int_keys(dict(raw))

    out = dict(raw)
    for legacy, unified in _LEGACY_KEYS.items():
        if legacy in out and unified not in out:
            out[unified] = out.pop(legacy)
    out.setdefault("field", "tags" if "top_k_used_for_cooccurrence" in raw
                   else "entry_type")
    out.setdefault("census_version", "1")
    return _restore_int_keys(out)


def _restore_int_keys(census: dict) -> dict:
    """Undo JSON's string-only object keys.

    ``values_per_entry_distribution`` is keyed by a COUNT, so its keys are
    integers in memory — but JSON has no integer keys, and json.dump
    silently stringifies them. Without this, the same census has int keys
    fresh from run_census() and str keys after a save/load round trip, so
    a consumer indexing it works in one path and KeyErrors in the other.
    Normalising here keeps load_census(dump(x)) == x.
    """
    dist = census.get("values_per_entry_distribution")
    if isinstance(dist, dict):
        census["values_per_entry_distribution"] = {
            int(k): v for k, v in dist.items()}
    return census


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--field", choices=sorted(FIELDS), default="entry_type",
                    help="Which corpus field to census (default entry_type).")
    ap.add_argument("--top-k", type=int, default=None,
                    help="Restrict co-occurrence to the N most frequent values "
                         "(0 = no restriction). Default: per-field.")
    ap.add_argument("--min-pair-count", type=int, default=None,
                    help="Drop pairs co-occurring fewer than this many times. "
                         "Default: per-field.")
    ap.add_argument("--output", type=str, default=None,
                    help="Write full JSON here; otherwise dump to stdout.")
    args = ap.parse_args()

    # Mongo is reached only here, in the CLI — importing this module never
    # requires a driver, and the DAG feeds run_census() from kg_store instead.
    import os
    from pymongo import MongoClient

    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
    try:
        coll = (client[os.getenv("MONGODB_DB", "memes")]
                [os.getenv("MONGODB_ENTRIES_COLLECTION", "entries")])
        cursor = coll.find({}, {"_id": 0, args.field: 1})
        result = run_census(cursor, args.field, top_k=args.top_k,
                            min_pair_count=args.min_pair_count)
    finally:
        client.close()

    if args.output:
        with open(args.output, "w") as fh:
            json.dump(result, fh, indent=2)
        print(f"Wrote full census to {args.output}")
    else:
        print(json.dumps(result, indent=2))

    print(f"field={result['field']} "
          f"corpus_size={result['corpus_size']} "
          f"entries_with_value={result['entries_with_value']} "
          f"distinct_values={result['distinct_values']} "
          f"total_pairs_seen={result['total_pairs_seen']} "
          f"total_pairs_returned={result['total_pairs_returned']}")


if __name__ == "__main__":
    main()
