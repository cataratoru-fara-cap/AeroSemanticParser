"""A tiny Wikidata dump, in the dump's own JSON shape, for the lexicon and
linker tests (test_kg_wikidata.py, test_kg_entities.py).

Each item exists to exercise one rule of kg/wikidata.py's filter or one
branch of kg/entities.py's scoring; the comment beside it says which. The
QIDs of real items are their real QIDs, so a failure message reads like the
real thing; invented ones are Q99999xx.
"""
from __future__ import annotations

import gzip
import json

_LANGS = ("en", "fr", "de", "es", "it", "ja", "nl", "pl", "pt", "ru", "sv",
          "zh", "ar", "cs", "fi", "he", "hu", "ko", "no", "tr")


def item(qid: str, label: str | None = None, *, desc: str | None = None,
         aliases=(), p31=(), p279=(), sitelinks: int = 0,
         kym_slug: str | None = None, kym_id: str | None = None,
         mul: str | None = None, fr: str | None = None) -> dict:
    labels = {}
    if label:
        labels["en"] = {"language": "en", "value": label}
    if mul:
        labels["mul"] = {"language": "mul", "value": mul}
    if fr:
        labels["fr"] = {"language": "fr", "value": fr}

    def claim(prop, value):
        dv = ({"value": value, "type": "string"} if isinstance(value, str)
              and not value.startswith("Q") else
              {"value": {"entity-type": "item", "numeric-id": int(value[1:]),
                         "id": value}, "type": "wikibase-entityid"})
        return {"mainsnak": {"snaktype": "value", "property": prop,
                             "datavalue": dv}, "type": "statement",
                "rank": "normal"}

    claims: dict = {}
    for prop, values in (("P31", p31), ("P279", p279)):
        if values:
            claims[prop] = [claim(prop, v) for v in values]
    if kym_slug:
        claims["P13484"] = [claim("P13484", kym_slug)]
    if kym_id:
        claims["P6760"] = [claim("P6760", kym_id)]
    links = {f"{lang}wiki": {"site": f"{lang}wiki", "title": label or qid}
             for lang in _LANGS[:min(sitelinks, len(_LANGS))]}
    # Past 20 Wikipedias, pad with plausible codes: only the count matters.
    for n in range(len(_LANGS), sitelinks):
        links[f"x{n}wiki"] = {"site": f"x{n}wiki", "title": label or qid}
    links["commonswiki"] = {"site": "commonswiki", "title": "Category:x"}
    return {"type": "item", "id": qid, "labels": labels,
            "descriptions": ({"en": {"language": "en", "value": desc}} if desc else {}),
            "aliases": {"en": [{"language": "en", "value": a} for a in aliases]}
            if aliases else {},
            "claims": claims, "sitelinks": links if sitelinks else {},
            "modified": "2026-09-14T04:37:01Z"}


ITEMS = [
    # classes (the NER-type walk goes through these)
    item("Q5", "human", desc="common name of Homo sapiens", p279=["Q215627"], sitelinks=200),
    item("Q215627", "person", desc="being that has certain capacities", sitelinks=100),
    item("Q6256", "country", desc="distinct territorial body", p279=["Q56061"], sitelinks=150),
    item("Q56061", "administrative territorial entity", sitelinks=60),
    item("Q35127", "website", desc="set of related web pages", sitelinks=150),
    item("Q2927074", "Internet meme", desc="concept that spreads via the Internet",
         sitelinks=60),
    # the obvious ones
    item("Q144", "dog", desc="domesticated mammal of the family Canidae",
         aliases=["domestic dog"], sitelinks=283),
    item("Q39315", "Shiba Inu", desc="Japanese dog breed", aliases=["Shiba", "Shiba-Inu"],
         sitelinks=47),
    item("Q17", "Japan", desc="island country in East Asia", p31=["Q6256"], sitelinks=300),
    item("Q1136", "Reddit", desc="social news aggregation website", p31=["Q35127"],
         kym_slug="reddit", sitelinks=60),
    item("Q531", "4chan", desc="anonymous English-language imageboard website",
         p31=["Q35127"], sitelinks=50),
    item("Q28472", "hair", desc="protein filament that grows from the skin", sitelinks=120),
    # the meme itself: its KYM slug is the frame's URL, so it is the title's entity
    item("Q15894956", "Doge", desc="Internet meme featuring a Shiba Inu dog",
         p31=["Q2927074"], kym_slug="doge", kym_id="13564", sitelinks=20),
    # ...and the other Doge, which is more linked: context and the slug must win
    item("Q219", "doge", desc="elected chief of state in Venice and Genoa", sitelinks=40),
    # an ambiguous pair only CONTEXT can separate
    item("Q308", "Mercury", desc="smallest planet in the Solar System", sitelinks=250),
    item("Q925", "mercury", desc="chemical element, a toxic liquid metal", sitelinks=240),
    # kept although no Wikipedia has it: it has a KYM slug
    item("Q9999903", "Kabosu", desc="Shiba Inu dog, the face of Doge",
         kym_slug="kabosu"),
    # kept via a mul label only (Wikidata's language-neutral name label)
    item("Q9999905", mul="Atsuko Sato", desc="Japanese kindergarten teacher",
         p31=["Q5"], sitelinks=2),
    # an alias-only match with no popularity and no context: under threshold
    item("Q9999907", "Gronk Nimbus", aliases=["nimbus"], desc="fictional robot",
         sitelinks=1),
    # DROPPED: a disambiguation page, however many sitelinks
    item("Q4167410", "Wikimedia disambiguation page", sitelinks=150),
    item("Q9999901", "Doge", desc="Wikimedia disambiguation page", p31=["Q4167410"],
         sitelinks=30),
    # DROPPED: no sitelinks and no KYM identifier (the scholarly-article case)
    item("Q9999902", "Dog", desc="scholarly article", p31=["Q13442814"]),
    # DROPPED: no English or mul label
    item("Q9999904", fr="Chien de garde", sitelinks=3),
    # DROPPED as an entity, but its P279 edge must survive for the type walk
    item("Q9999906", "obscure subclass of person", p279=["Q215627"]),
]

PROPERTY = {"type": "property", "id": "P31", "labels": {
    "en": {"language": "en", "value": "instance of"}}, "claims": {},
    "datatype": "wikibase-item"}

KEPT = {"Q5", "Q215627", "Q6256", "Q56061", "Q35127", "Q2927074", "Q144",
        "Q39315", "Q17", "Q1136", "Q531", "Q28472", "Q15894956", "Q219",
        "Q308", "Q925", "Q9999903", "Q9999905", "Q9999907", "Q4167410"}


def dump_lines(items=None, prop: bool = True) -> list[str]:
    """The dump's layout: '[', one entity per line with a trailing comma
    (none on the last), ']'."""
    entities = list(ITEMS if items is None else items) + ([PROPERTY] if prop else [])
    body = [json.dumps(e, ensure_ascii=False, separators=(",", ":")) for e in entities]
    return ["["] + [line + "," for line in body[:-1]] + [body[-1], "]"]


def write_dump(path: str, items=None) -> str:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(dump_lines(items)) + "\n")
    return path
