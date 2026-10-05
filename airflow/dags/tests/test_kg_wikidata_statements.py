"""Tests for kg/wikidata_statements.py — the statements IMKG imported.

What these pin: only TRUTHY item-valued statements (preferred if any, else
normal; never deprecated; no literals, no "no value"); one pass over a
dump finds the wanted items and every property's label and parses nothing
else; the labels-only pass; statement edges are named by their property.
"""
import gzip
import json
import os
import tempfile
import unittest

from modules.kg import wikidata_statements as ws


def stmt(pid, value, rank="normal", snaktype="value", datatype="wikibase-item"):
    snak = {"snaktype": snaktype, "property": pid, "datatype": datatype}
    if snaktype == "value":
        snak["datavalue"] = ({"value": {"entity-type": "item", "numeric-id": int(value[1:]),
                                        "id": value}, "type": "wikibase-entityid"}
                             if datatype == "wikibase-item" else
                             {"value": value, "type": "string"})
    return {"mainsnak": snak, "type": "statement", "rank": rank}


def item(qid, label, claims=None):
    return {"type": "item", "id": qid, "labels": {"en": {"language": "en", "value": label}},
            "descriptions": {}, "claims": claims or {}, "sitelinks": {}}


TRUMP = item("Q22686", "Donald Trump", {
    "P31": [stmt("P31", "Q5")],
    "P21": [stmt("P21", "Q6581097")],
    "P106": [stmt("P106", "Q82955", rank="preferred"), stmt("P106", "Q131524")],
    "P27": [stmt("P27", "Q30", rank="deprecated")],
    "P569": [stmt("P569", "+1946-06-14", datatype="time")],
    "P1196": [stmt("P1196", None, snaktype="novalue")],
})


class TruthyTests(unittest.TestCase):
    def test_truthy_item_values_only(self):
        self.assertEqual(ws.truthy_item_values(TRUMP["claims"]), [
            ("P21", 6581097), ("P31", 5),
            ("P106", 82955)])          # preferred beats normal; deprecated, dates, novalue out

    def test_a_value_named_twice_is_kept_once(self):
        self.assertEqual(ws.truthy_item_values({"P31": [stmt("P31", "Q5"), stmt("P31", "Q5")]}),
                         [("P31", 5)])

    def test_a_snak_without_a_datatype_is_read_by_its_value(self):
        s = stmt("P31", "Q5")
        del s["mainsnak"]["datatype"]
        self.assertEqual(ws.truthy_item_values({"P31": [s]}), [("P31", 5)])

    def test_statement_types(self):
        self.assertTrue(ws.is_statement_type("P31"))
        for not_one in ("partOfSeries", "P", "P0", "p31", "PX", ""):
            self.assertFalse(ws.is_statement_type(not_one), not_one)


class ScanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.dump = os.path.join(cls.tmp.name, "dump.json.gz")
        lines = [TRUMP, item("Q5", "human"), item("Q999", "not wanted", {"P31": [stmt("P31", "Q5")]}),
                 {"type": "property", "datatype": "wikibase-item", "id": "P31",
                  "labels": {"en": {"language": "en", "value": "instance of"}}, "claims": {}},
                 {"type": "lexeme", "id": "L1", "lemmas": {}}]
        with gzip.open(cls.dump, "wt", encoding="utf-8") as fh:
            fh.write("[\n" + ",\n".join(json.dumps(x, separators=(",", ":")) for x in lines) + "\n]\n")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_one_pass_finds_the_wanted_items_and_the_property_labels(self):
        got = ws.scan(self.dump, [22686], workers=0)
        self.assertEqual(list(got["items"]), ["Q22686"])
        self.assertEqual(got["items"]["Q22686"]["label"], "Donald Trump")
        self.assertEqual(got["items"]["Q22686"]["statements"][1], ("P31", 5))
        self.assertEqual(got["properties"], {"P31": "instance of"})
        self.assertEqual(got["lines"], 7)              # "[", 5 entities, "]"

    def test_the_labels_pass(self):
        got = ws.scan(self.dump, [5], statements=False, workers=0)
        self.assertEqual(got["items"], {"Q5": {"id": "Q5", "label": "human", "description": None}})
        self.assertEqual(got["properties"], {})

    def test_statement_edges_are_named_by_their_property(self):
        edges = ws.statement_edges({"Q22686": [("P31", 5), ("P21", 6581097)]})
        self.assertEqual(edges, [{"src": "wd:Q22686", "dst": "wd:Q5", "type": "P31"},
                                 {"src": "wd:Q22686", "dst": "wd:Q6581097", "type": "P21"}])

    def test_value_nodes_are_one_hop_and_named(self):
        statements = {"Q22686": [("P31", 5), ("P21", 6581097), ("P26", 432473)],
                      "Q432473": [("P31", 5)]}
        names = {"Q5": ("human", "species"), "Q6581097": ("male", None)}
        nodes = ws.value_nodes(statements, {"Q22686", "Q432473"},
                               lambda q: names.get(q, (None, None)))
        # Q432473 is already in the graph (it is linked), so it is not a new
        # node; Q5 is named once although two items say it.
        self.assertEqual(nodes, [
            {"id": "wd:Q5", "kind": "wikidata_entity", "qid": "Q5",
             "label": "human", "description": "species"},
            {"id": "wd:Q6581097", "kind": "wikidata_entity", "qid": "Q6581097",
             "label": "male", "description": None}])


if __name__ == "__main__":
    unittest.main(verbosity=2)
