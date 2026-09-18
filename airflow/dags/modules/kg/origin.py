"""
kg/origin.py — canonicalizing the frame's "origin" field into concepts
==========================================================================
Pure: no Mongo, no Airflow. ``frame.from``/``m4s:from`` (parsed field
``origin``) is the KYM infobox's free-text "Origin" line. It is NOT a
platform field: querying the real corpus directly shows it holds genuine
platforms (Twitter, YouTube, 4chan, Reddit, ...), countries ("United
States", "Japan"), franchises ("The Simpsons", "Avengers: Endgame"),
companies ("Nintendo", "Valve"), games ("Elden Ring"), and even people
("Donald Trump"). 5,900 distinct raw strings, with real duplication in
every one of those categories, not just platform-name casing: "United
States" / "United States of America" / "USA" / "America" / "American";
"Avengers: Endgame" / "Avengers: Endgame (Film)"; "SpongeBob SquarePants"
/ "Spongebob Squarepants" / "SpongeBob SquarePants (Television Series)".

This module canonicalizes ALL of that into ``origin_concept`` nodes via a
curated alias map (``dags/kg_config/origin_taxonomy.yaml``'s ``aliases:``
section) — every raw value gets *some* canonical slug, curated or a
deterministic fallback, never dropped. A curated ``rdfs:subClassOf``-style
hierarchy is layered on top, same as kg/taxonomy.py's entry_type edges,
but ONLY over the subset of canonical slugs that are actually platforms
(twitter -> social-network, 4chan -> imageboard, ...). A country, a
franchise, a company or a person has no natural single parent — inventing
one would be exactly the fabricated relation ``entry_type_taxonomy.yaml``'s
own ``demoted``/``do_not_encode_as_broader`` buckets exist to reject.
Deduplication is the goal for those; a forced taxonomy is not.

Two different shapes, one file
-------------------------------
The alias map is a many-to-one synonymy map, not an is-a relation — fitting
it into kg/taxonomy.py's BroaderEdge/bucket dataclass would fabricate fake
"broader" semantics for what is just canonicalization. The hierarchy layer
IS entry_type-shaped, so it reuses kg/taxonomy.py's bucket parsing and
consistency checks as-is (via the private ``taxonomy._parse``), just with a
different concept-id prefix (``origin:`` instead of ``type:``).
``taxonomy.load()`` refuses unknown top-level keys, so this module reads
the YAML itself, pops ``aliases`` off, and hands the rest to ``_parse``.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

from . import taxonomy

__all__ = ["OriginTaxonomy", "load", "resolve", "concept_edges",
          "check_consistency", "validate", "canonical_census"]

CONCEPT_PREFIX = "origin:"

_SLUG_STRIP = re.compile(r"[^a-z0-9]+")


def _slugify(value: str) -> str:
    """A deterministic fallback slug for a raw origin value with no
    curated alias: lowercase, punctuation/whitespace collapsed to '-',
    trimmed. Never raises, never returns empty for a non-empty input
    (falls back to a hash if the value is punctuation-only)."""
    slug = _SLUG_STRIP.sub("-", value.strip().lower()).strip("-")
    return slug or f"origin-{hashlib.sha1(value.encode('utf-8')).hexdigest()[:8]}"


@dataclass(frozen=True)
class OriginTaxonomy:
    version: str                       # sha256 of the curated file's bytes
    path: str
    aliases: dict[str, str]            # raw origin string -> canonical slug
    hierarchy: taxonomy.Taxonomy       # subTypeOf edges among platform slugs

    def slugs(self) -> set[str]:
        return set(self.aliases.values()) | self.hierarchy.slugs()


def load(path: str) -> OriginTaxonomy:
    """Parse and structurally validate the curated origin file."""
    import yaml   # declared in requirements.txt; lazy so import stays cheap

    with open(path, "rb") as fh:
        raw_bytes = fh.read()
    version = hashlib.sha256(raw_bytes).hexdigest()

    try:
        doc = yaml.safe_load(raw_bytes.decode("utf-8"))
    except yaml.YAMLError as exc:
        raise taxonomy.TaxonomyError(f"{path} is not valid YAML: {exc}") from None

    if not isinstance(doc, dict):
        raise taxonomy.TaxonomyError(f"{path}: top level must be a mapping")

    raw_aliases = doc.pop("aliases", None) or {}
    if not isinstance(raw_aliases, dict):
        raise taxonomy.TaxonomyError(f"{path}: 'aliases' must be a mapping")
    aliases = {str(k): str(v) for k, v in raw_aliases.items()}

    hierarchy = taxonomy._parse(doc, version=version, path=str(path))
    tax = OriginTaxonomy(version=version, path=str(path), aliases=aliases,
                         hierarchy=hierarchy)
    check_consistency(tax)
    return tax


def check_consistency(tax: OriginTaxonomy) -> None:
    """Alias-specific guard, on top of ``taxonomy.check_consistency``
    (already run on ``tax.hierarchy`` inside ``taxonomy._parse``, which
    ``load()`` calls before constructing ``tax`` — so by the time this
    runs, the hierarchy itself is already self-consistent).

    Checks only the NARROWER side of every edge against the alias targets:
    a narrower slug (e.g. ``twitter``) must be something a real raw origin
    value canonicalizes to — a typo'd narrower slug the curator meant to
    write differently would otherwise silently mint a hierarchy edge over
    a node nothing ever produces. The BROADER side is deliberately not
    checked: umbrella concepts like ``social-network``/``imageboard`` are
    synthetic parents that no raw origin value ever aliases to directly —
    the same "true parent that doesn't exist as a value yet" allowance
    entry_type_taxonomy.yaml's own ``missing_umbrellas`` bucket documents.
    """
    canonical = set(tax.aliases.values())
    for e in tax.hierarchy.edges:
        if e.narrower not in canonical:
            raise taxonomy.TaxonomyError(
                f"origin taxonomy: hierarchy narrower {e.narrower!r} is not "
                f"a canonicalization target of any alias — a typo, or a "
                f"missing aliases row")


def resolve(raw_origin: str, aliases: dict[str, str]) -> str:
    """A raw ``frame.from`` value -> its canonical origin slug.

    Exact match first, then case-insensitive, then a deterministic
    slugify fallback. Every value resolves to something — an unreviewed
    long-tail origin still gets its own addressable node rather than being
    folded into a lossy generic "Other" (irreversible without a rebuild;
    everything else in this pipeline keeps the raw distinction instead).
    """
    if raw_origin in aliases:
        return aliases[raw_origin]
    lowered = raw_origin.strip().lower()
    for raw, slug in aliases.items():
        if raw.strip().lower() == lowered:
            return slug
    return _slugify(raw_origin)


def concept_edges(tax: OriginTaxonomy) -> list[dict[str, str]]:
    """Every curated hierarchy edge, unconditionally. The DAG uses
    ``encodable_edges`` instead (restricted to slugs the corpus actually
    uses, matching taxonomy.encodable_edges's convention for entry_type);
    this is for tests and offline inspection."""
    return tax.hierarchy.concept_edges(prefix=CONCEPT_PREFIX)


def encodable_edges(tax: OriginTaxonomy,
                    raw_origin_census: dict) -> list[dict[str, str]]:
    """concept_edges(), restricted to edges whose NARROWER canonical slug
    the corpus actually uses this build.

    Deliberately NOT taxonomy.encodable_edges, which requires BOTH sides
    known — correct for entry_type, where every curated slug is also a
    real corpus value, but wrong here: an origin hierarchy's broader side
    (mk:origin/social-network, .../imageboard, ...) is a synthetic
    umbrella no raw origin value ever resolves to (see
    check_consistency), so requiring it in the census would silently drop
    every edge this file declares.
    """
    census = canonical_census(raw_origin_census, tax.aliases)
    known = set(census.get("value_counts") or {})
    return [{"src": f"{CONCEPT_PREFIX}{e.narrower}",
             "dst": f"{CONCEPT_PREFIX}{e.broader}",
             "type": taxonomy.CONCEPT_EDGE_TYPE}
            for e in tax.hierarchy.edges if e.narrower in known]


def canonical_census(raw_origin_census: dict, aliases: dict[str, str]) -> dict:
    """Re-bucket a raw ``origin`` census (kg/census.py, field="origin")
    through the alias map, so ``validate()`` can check the CURATED slugs
    against real corpus usage without a second Mongo scan — every raw
    value's count folds into its resolved canonical slug's count."""
    from collections import Counter

    counts: Counter[str] = Counter()
    for raw, n in (raw_origin_census.get("value_counts") or {}).items():
        counts[resolve(raw, aliases)] += n
    return {"value_counts": dict(counts)}


def validate(tax: OriginTaxonomy, raw_origin_census: dict) -> dict[str, Any]:
    """Same report shape as ``taxonomy.validate``, against the canonical
    (alias-resolved) view of the corpus — asymmetric per encodable_edges:
    only a narrower slug absent from this build's corpus counts as a drop
    or a "missing" slug; the broader (umbrella) side is never checked."""
    census = canonical_census(raw_origin_census, tax.aliases)
    known = set(census.get("value_counts") or {})
    edges = tax.hierarchy.edges
    dropped = [{"narrower": e.narrower, "broader": e.broader, "bucket": e.bucket,
               "missing": [e.narrower]}
              for e in edges if e.narrower not in known]
    missing = sorted({e.narrower for e in edges if e.narrower not in known})
    return {
        "taxonomy_version": tax.version,
        "aliases_declared": len(tax.aliases),
        "edges_declared": len(edges),
        "edges_encoded": len(edges) - len(dropped),
        "edges_dropped": dropped,
        "slugs_missing_from_census": missing,
        "withheld_pairs": [w["pair"] for w in tax.hierarchy.withheld],
        "buckets": dict(tax.hierarchy.bucket_counts),
    }
