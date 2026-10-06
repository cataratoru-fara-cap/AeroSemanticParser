"""kg/wikidata_statements.py: the statements IMKG imported. Pinned: only TRUTHY
item-valued statements (preferred if any, else normal; never deprecated; no
literals, no "no value"); one pass over a dump finds the wanted items and every
property's label and parses nothing else; the labels-only pass; statement
edges are named by their property."""
import gzip
import json

import pytest

from modules.kg import wikidata_statements as ws


def stmt(pid, value, rank="normal", snaktype="value", datatype="wikibase-item"):
    snak = {"snaktype": snaktype, "property": pid, "datatype": datatype}
    if snaktype == "value":
        snak["datavalue"] = ({"value": {"entity-type": "item", "numeric-id": int(value[1:]), "id": value},
                              "type": "wikibase-entityid"} if datatype == "wikibase-item"
                             else {"value": value, "type": "string"})
    return {"mainsnak": snak, "type": "statement", "rank": rank}


def item(qid, label, claims=None):
    return {"type": "item", "id": qid, "labels": {"en": {"language": "en", "value": label}}, "descriptions": {},
            "claims": claims or {}, "sitelinks": {}}


TRUMP = item("Q22686", "Donald Trump", {
    "P31": [stmt("P31", "Q5")], "P21": [stmt("P21", "Q6581097")],
    "P106": [stmt("P106", "Q82955", rank="preferred"), stmt("P106", "Q131524")],
    "P27": [stmt("P27", "Q30", rank="deprecated")], "P569": [stmt("P569", "+1946-06-14", datatype="time")],
    "P1196": [stmt("P1196", None, snaktype="novalue")]})
NO_DATATYPE = stmt("P31", "Q5")
del NO_DATATYPE["mainsnak"]["datatype"]


@pytest.mark.parametrize("claims, want", [
    (TRUMP["claims"], [("P21", 6581097), ("P31", 5), ("P106", 82955)]),   # preferred beats normal; the rest out
    ({"P31": [stmt("P31", "Q5"), stmt("P31", "Q5")]}, [("P31", 5)]),     # a value named twice is kept once
    ({"P31": [NO_DATATYPE]}, [("P31", 5)]),                               # read by its value
])
def test_truthy_item_values(claims, want):
    assert ws.truthy_item_values(claims) == want


@pytest.mark.parametrize("name, is_one", [("P31", True), ("partOfSeries", False), ("P", False), ("P0", False),
                                          ("p31", False), ("PX", False), ("", False)])
def test_statement_types(name, is_one):
    assert ws.is_statement_type(name) is is_one


@pytest.fixture(scope="module")
def dump(tmp_path_factory):
    path = tmp_path_factory.mktemp("statements") / "dump.json.gz"
    lines = [TRUMP, item("Q5", "human"), item("Q999", "not wanted", {"P31": [stmt("P31", "Q5")]}),
             {"type": "property", "datatype": "wikibase-item", "id": "P31",
              "labels": {"en": {"language": "en", "value": "instance of"}}, "claims": {}},
             {"type": "lexeme", "id": "L1", "lemmas": {}}]
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("[\n" + ",\n".join(json.dumps(x, separators=(",", ":")) for x in lines) + "\n]\n")
    return str(path)


def test_one_pass_finds_the_wanted_items_and_the_property_labels(dump):
    got = ws.scan(dump, [22686], workers=0)
    assert list(got["items"]) == ["Q22686"] and got["items"]["Q22686"]["label"] == "Donald Trump"
    assert got["items"]["Q22686"]["statements"][1] == ("P31", 5)
    assert (got["properties"], got["lines"]) == ({"P31": "instance of"}, 7)       # "[", 5 entities, "]"


def test_the_labels_pass(dump):
    got = ws.scan(dump, [5], statements=False, workers=0)
    assert (got["items"], got["properties"]) == ({"Q5": {"id": "Q5", "label": "human", "description": None}}, {})


def test_statement_edges_and_value_nodes():
    assert ws.statement_edges({"Q22686": [("P31", 5), ("P21", 6581097)]}) == [
        {"src": "wd:Q22686", "dst": "wd:Q5", "type": "P31"}, {"src": "wd:Q22686", "dst": "wd:Q6581097", "type": "P21"}]
    statements = {"Q22686": [("P31", 5), ("P21", 6581097), ("P26", 432473)], "Q432473": [("P31", 5)]}
    names = {"Q5": ("human", "species"), "Q6581097": ("male", None)}
    # one hop and named: Q432473 is linked already, so no new node; Q5 is named once although two items say it
    assert ws.value_nodes(statements, {"Q22686", "Q432473"}, lambda q: names.get(q, (None, None))) == [
        {"id": "wd:Q5", "kind": "wikidata_entity", "qid": "Q5", "label": "human", "description": "species"},
        {"id": "wd:Q6581097", "kind": "wikidata_entity", "qid": "Q6581097", "label": "male", "description": None}]
