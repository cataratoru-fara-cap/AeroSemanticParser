"""
kg/tag_normalize.py — plural folding for the tags folksonomy
==============================================================
Pure: no Mongo, no Airflow. `kg/build.py` and `kg/census.py` both need the
exact same tag -> canonical-slug mapping (one for the node id it mints, one
for the counts it censuses); previously each had its own inline
`.strip().lower()` and neither folded plurals, so `catchphrase`/`catchphrases`,
`exploitable`/`exploitables`, `image macro`/`image macros` and `meme`/`memes`
were four separate nodes each, real corpus counts: 645/632, 895/798, 800/514,
1050/631.

Why algorithmic, not a curated alias file (the `origin_taxonomy.yaml`
pattern): 102,583 distinct tags makes hand curation infeasible. A small
suffix rule handles the common case; a curated denylist catches the real
false positives it would otherwise cause.

Deliberately conservative: only the unambiguous `+s`/`+es` plural forms are
folded. A `y` -> `ies` rule was tried and rejected — it cannot distinguish
a real consonant+y plural ("parody" -> "parodies", fold back to "parody",
correct) from a noun that already ends in vowel+"ie" ("movie" -> "movies",
which the same suffix check would wrongly fold to "movy"). Missing a merge
is a rounding error; a wrong merge corrupts two tags' worth of data. Leaving
`-ies` tags unfolded is the safer default; add specific ones to the denylist
or handle them by hand later if it matters.

The other real failure mode found by scanning the current top-300 tag
census (`data/kg_census_tags.json`) is proper nouns that happen to end in
"s": "Star Wars", "Game of Thrones", "The Simpsons", "Avengers", "Anonymous",
"Christmas", "United States", "Coronavirus", "Trans", "News", "Politics",
"SpongeBob SquarePants" would all be corrupted by blind stripping. These
seed `dags/kg_config/tag_normalization_exceptions.yaml`'s `do_not_fold`
list; re-review it once the tags census is regenerated at top_k=1000 (this
module's own `fold()` is oblivious to frequency, so the review only ever
adds to the denylist, never changes behavior retroactively for tags already
in the graph under their folded form).
"""
from __future__ import annotations

__all__ = ["fold", "load_denylist"]

_MIN_LENGTH = 4          # "gas", "bus" etc. are shorter than this and untouched
_SIBILANT_ES = ("sses", "xes", "zes", "ches", "shes")


def _fold(word: str) -> str:
    if len(word) < _MIN_LENGTH:
        return word
    if word.endswith(_SIBILANT_ES):
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def load_denylist(path: str) -> frozenset[str]:
    """Read the curated `do_not_fold` list. Pure text I/O, no yaml schema
    beyond a flat list under one key — kept separate from `taxonomy.py`'s
    bucket shape because this is an exceptions list, not a hierarchy."""
    import yaml

    with open(path, "rb") as fh:
        doc = yaml.safe_load(fh.read().decode("utf-8")) or {}
    return frozenset(str(v).strip().lower() for v in (doc.get("do_not_fold") or []))


def fold(tag: str, denylist: frozenset[str] = frozenset()) -> str:
    """A folksonomy tag (already lowercased/stripped) -> its folded form.

    Idempotent: `fold(fold(x)) == fold(x)`, since a folded singular no
    longer ends in a plural suffix this function would touch.
    """
    if not tag or tag in denylist:
        return tag
    return _fold(tag)
