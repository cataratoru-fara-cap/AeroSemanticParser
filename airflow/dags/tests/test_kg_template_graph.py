"""The template layer in the graph (KG 6.4.0): build.py, rdf.py, serialize.py
and kg_store's reads. No network; mongomock for the store.

What these pin:

  * **A frame's edge to a template carries how well it fits** (hasTemplate,
    RDF-star on mk:hasTemplate), and RDF has IMKG's m4s:templateOf back.
  * **Everything about a template hangs off the template**, not the frame:
    its blank (still templates only), its imgflip page, and what the image
    shows (m4s:fromImage, one occurrence per region, the box as a Media
    Fragments literal).
  * **A template two frames selected is emitted identically by both.**
  * **imgflip:templateId is a plain literal**, as IMKG writes it.
  * **The build reads selections and readings frozen at its snapshot.**
"""
import csv
import os
import tempfile
import unittest
from datetime import datetime, timezone

import mongomock

from modules import kg_store, template_entity_store as tes, template_store as ts
from modules.kg import build, rdf, serialize

FRAME = "https://knowyourmeme.com/memes/distracted-boyfriend"
OTHER = "https://knowyourmeme.com/memes/other"
MK, M4S, WD = rdf.PREFIXES["mk"], rdf.PREFIXES["m4s"], rdf.PREFIXES["wd"]
T_IRI = MK + "template/112126428"


def template(**over):
    t = {"template_id": 112126428, "R": 0.9, "method": "search",
         "name": "Distracted Boyfriend", "alt_names": ["distracted bf"],
         "file_type": "jpg", "url": "https://imgflip.com/meme/Distracted-Boyfriend",
         "blank_url": "https://i.imgflip.com/1ur9b0.jpg", "width": 1200, "height": 800,
         "animated": False, "featured": True,
         "mentions": [
             {"qid": "Q5", "label": "human", "text": "man", "score": 0.6,
              "method": "vlm_generic", "model": "qwen3-vl:32b",
              "region": {"kind": "person", "box": [0.1, 0.05, 0.6, 0.9]}},
             {"qid": "Q5", "label": "human", "text": "woman", "score": 0.6,
              "method": "vlm_generic", "model": "qwen3-vl:32b",
              "region": {"kind": "person", "box": [0.6, 0.1, 0.9, 0.9]}}]}
    t.update(over)
    return t


def entry(url=FRAME):
    return {"url": url, "title": "Distracted Boyfriend", "category": "meme"}


class BuildTests(unittest.TestCase):
    def build(self, **over):
        return build.build_nodes_and_edges(entry(), templates=[template(**over)])

    def test_nodes(self):
        nodes, _ = self.build()
        by_id = {n["id"]: n for n in nodes}
        t = by_id["template:112126428"]
        self.assertEqual((t["kind"], t["label"], t["template_id"], t["alt_names"]),
                         ("template", "Distracted Boyfriend", "112126428", ["distracted bf"]))
        self.assertEqual(by_id["image:https://i.imgflip.com/1ur9b0.jpg"]["width"], 1200)
        self.assertEqual(by_id["https://imgflip.com/meme/Distracted-Boyfriend"]["kind"],
                         "external_ref")
        self.assertEqual(by_id["wd:Q5"]["kind"], "wikidata_entity")

    def test_edges_and_their_occurrences(self):
        _, edges = self.build()
        types = {(e["src"], e["type"], e["dst"]) for e in edges}
        t = "template:112126428"
        self.assertIn((FRAME, "hasTemplate", t), types)
        self.assertIn((t, "templateImage", "image:https://i.imgflip.com/1ur9b0.jpg"), types)
        self.assertIn((t, "imgflipPage", "https://imgflip.com/meme/Distracted-Boyfriend"), types)
        has = next(e for e in edges if e["type"] == "hasTemplate")
        self.assertEqual(has["occurrences"], [{"template_score": 0.9, "template_match": "search"}])
        from_image = [e for e in edges if e["type"] == "fromImage"]
        self.assertEqual(len(from_image), 1)                   # one edge per item...
        self.assertEqual(len(from_image[0]["occurrences"]), 2)  # ...one occurrence per region
        self.assertEqual(from_image[0]["occurrences"][0]["bounding_box"],
                         "xywh=percent:10.0,5.0,50.0,85.0")

    def test_an_animated_template_has_no_image_node(self):
        nodes, edges = self.build(animated=True, file_type="mp4",
                                  blank_url="https://i.imgflip.com/2/3jpogl.jpg")
        self.assertFalse(any(e["type"] == "templateImage" for e in edges))
        self.assertFalse(any(n["kind"] == "image" for n in nodes))

    def test_two_frames_emit_the_template_identically(self):
        n1, e1 = build.build_nodes_and_edges(entry(FRAME), templates=[template()])
        n2, e2 = build.build_nodes_and_edges(entry(OTHER), templates=[template(R=0.7)])
        pick = lambda edges: sorted((e["src"], e["type"], e["dst"], str(e.get("occurrences")))
                                    for e in edges if e["src"].startswith("template:"))
        self.assertEqual(pick(e1), pick(e2))
        t1 = next(n for n in n1 if n["kind"] == "template")
        t2 = next(n for n in n2 if n["kind"] == "template")
        self.assertEqual(t1, t2)


class RdfTests(unittest.TestCase):
    def test_triples(self):
        nodes, edges = build.build_nodes_and_edges(entry(), templates=[template()])
        lines = set(rdf.iter_triples(nodes, edges))
        self.assertIn(f"<{T_IRI}> <{rdf.RDF_TYPE}> <{MK}MemeTemplate> .", lines)
        self.assertIn(f'<{T_IRI}> <https://imgflip.com/templateId> "112126428" .', lines)
        self.assertIn(f"<{FRAME}> <{MK}hasTemplate> <{T_IRI}> .", lines)
        self.assertIn(f"<{T_IRI}> <{M4S}templateOf> <{FRAME}> .", lines)
        self.assertIn(f"<{T_IRI}> <{M4S}fromImage> <{WD}Q5> .", lines)
        self.assertIn(f"<< <{FRAME}> <{MK}hasTemplate> <{T_IRI}> >> <{MK}templateScore> "
                      f'"0.9"^^<{rdf.XSD_DECIMAL}> .', lines)
        self.assertIn(f"<< <{T_IRI}> <{M4S}fromImage> <{WD}Q5> >> <{MK}boundingBox> "
                      f'"xywh=percent:60.0,10.0,30.0,80.0" .', lines)


class SerializeTests(unittest.TestCase):
    def test_rml_files(self):
        nodes, edges = build.build_nodes_and_edges(entry(), templates=[template()])
        with tempfile.TemporaryDirectory() as tmp:
            serialize.write_build(lambda: nodes, lambda: edges, tmp, build_id="kg_test")
            rml = os.path.join(tmp, serialize.RML_DIR)

            def rows(name):
                with open(os.path.join(rml, name), encoding="utf-8") as fh:
                    return list(csv.DictReader(fh))

            self.assertEqual(rows("templates.csv")[0]["iri"], T_IRI)
            self.assertEqual(rows("templates.csv")[0]["template_id"], "112126428")
            self.assertEqual(rows("template_alt_names.csv"),
                             [{"iri": T_IRI, "alt_name": "distracted bf"}])
            self.assertEqual(rows("template_edges.csv"),
                             [{"url": FRAME, "template": "112126428"}])
            self.assertEqual(len(rows("entity_image_occurrences.csv")), 2)
            self.assertEqual(rows("template_occurrences.csv")[0]["template_match"], "search")


class StoreReadTests(unittest.TestCase):
    """kg_store.template_links_for / template_stamps over mongomock stores."""

    def setUp(self):
        client = mongomock.MongoClient()
        db = client["memes"]
        self.t_store = ts.TemplateStore.__new__(ts.TemplateStore)
        self.t_store.client, self.t_store.db = client, db
        self.t_store.frames, self.t_store.templates = db["frame_templates"], db["imgflip_templates"]
        self.e_store = tes.TemplateEntityStore.__new__(tes.TemplateEntityStore)
        self.e_store.client, self.e_store.db = client, db
        self.e_store.detections = db["template_entities"]
        early = datetime(2026, 9, 1, tzinfo=timezone.utc)
        late = datetime(2026, 10, 1, tzinfo=timezone.utc)
        db["frame_templates"].insert_many([
            {"_id": "f1", "frame_url": FRAME, "status": "selected", "selected_at": early,
             "selection_sha": "s1", "selected": [{"template_id": 1, "R": 0.9,
                                                   "method": "search"}]},
            {"_id": "f2", "frame_url": OTHER, "status": "selected", "selected_at": late,
             "selection_sha": "s2", "selected": [{"template_id": 1, "R": 0.8,
                                                   "method": "search"}]}])
        db["imgflip_templates"].insert_one({"_id": 1, "name": "T", "url": "https://imgflip.com/meme/T"})
        db["template_entities"].insert_one({
            "_id": 1, "in_graph_count": 1, "linked_at": early, "detection": {"model": "m"},
            "links": {"mentions": [{"qid": "Q5", "in_graph": True, "text": "man"},
                                   {"qid": "Q6", "in_graph": False}]}})
        # kg_store's re-exports go through each store's get_store()
        self._orig = (ts.get_store, tes.get_store)
        ts.get_store = lambda *a, **k: _NoClose(self.t_store)
        tes.get_store = lambda *a, **k: _NoClose(self.e_store)

    def tearDown(self):
        ts.get_store, tes.get_store = self._orig

    def test_links_are_frozen_at_the_snapshot(self):
        snap = datetime(2026, 9, 15, tzinfo=timezone.utc)
        got = kg_store.template_links_for(["f1", "f2"], snap)
        self.assertEqual(list(got), [FRAME])
        rec = got[FRAME][0]
        self.assertEqual((rec["template_id"], rec["name"], rec["R"]), (1, "T", 0.9))
        self.assertEqual([m["qid"] for m in rec["mentions"]], ["Q5"])
        self.assertEqual(rec["mentions"][0]["model"], "m")

    def test_stamps_move_with_the_selection(self):
        snap = datetime(2026, 12, 1, tzinfo=timezone.utc)
        a = kg_store.template_stamps(snap)
        self.assertEqual((a["templates_frames"], a["templates_links"],
                          a["template_entities_in_graph"]), (2, 2, 1))
        self.t_store.frames.update_one({"_id": "f2"}, {"$set": {"selection_sha": "changed"}})
        b = kg_store.template_stamps(snap)
        self.assertNotEqual(a["templates_selection_digest"], b["templates_selection_digest"])
        self.assertTrue(set(kg_store.TEMPLATE_STAMP_KEYS) <= set(b))


class _NoClose:
    def __init__(self, store):
        self.store = store

    def __enter__(self):
        return self.store

    def __exit__(self, *exc):
        return False


if __name__ == "__main__":
    unittest.main()
