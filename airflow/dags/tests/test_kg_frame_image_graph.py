"""What a frame's own image shows, in the graph (KG 7.1.0): build.py,
rdf.py and serialize.py. No network.

What these pin:
  * **The frame itself gets IMKG's m4s:fromImage** — the subject of IMKG's
    paper query "IMs that depict SpongeBob" — one edge per item, one
    occurrence per region, with the template layer's occurrence fields.
  * **A frame's fromImage and a template's go to different RML files**:
    their subjects are written differently (a URL, mk:template/<id>), and
    both still reach graph.nt with their annotations.
"""
import csv
import os
import tempfile
import unittest

from modules.kg import build, rdf, serialize

FRAME = "https://knowyourmeme.com/memes/spongegar"
M4S, MK, WD = rdf.PREFIXES["m4s"], rdf.PREFIXES["mk"], rdf.PREFIXES["wd"]


def mention(qid="Q83279", label="SpongeBob SquarePants", text="SpongeBob SquarePants",
            box=(0.1, 0.05, 0.6, 0.9), kind="character"):
    return {"qid": qid, "label": label, "text": text, "score": 0.9, "method": "vlm_named",
            "model": "qwen3-vl:32b", "region": {"kind": kind, "box": list(box)}}


def entry():
    return {"url": FRAME, "title": "Spongegar", "category": "meme"}


def template_record():
    return {"template_id": 7, "R": 0.8, "method": "search", "name": "Spongegar",
            "mentions": [mention(qid="Q1", label="caveman", text="caveman")]}


class BuildTests(unittest.TestCase):
    def test_the_frame_gets_fromImage(self):
        nodes, edges = build.build_nodes_and_edges(
            entry(), frame_images=[mention(), mention(box=(0.6, 0.1, 0.9, 0.9))])
        (e,) = [e for e in edges if e["type"] == "fromImage"]
        self.assertEqual((e["src"], e["dst"]), (FRAME, "wd:Q83279"))
        self.assertEqual(len(e["occurrences"]), 2)              # one per region
        self.assertEqual(e["occurrences"][0], {
            "mention_text": "SpongeBob SquarePants", "link_score": 0.9,
            "link_method": "vlm_named", "depiction_kind": "character",
            "bounding_box": "xywh=percent:10.0,5.0,50.0,85.0", "detected_by": "qwen3-vl:32b"})
        self.assertEqual({n["id"]: n["kind"] for n in nodes}["wd:Q83279"], "wikidata_entity")

    def test_no_reading_no_edge(self):
        _, edges = build.build_nodes_and_edges(entry())
        self.assertEqual([e for e in edges if e["type"] == "fromImage"], [])


class SerializeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        nodes, edges = build.build_nodes_and_edges(
            entry(), templates=[template_record()], frame_images=[mention()])
        cls._tmp = tempfile.TemporaryDirectory()
        cls.out = cls._tmp.name
        cls.manifest = serialize.write_build(lambda: iter(nodes), lambda: iter(edges),
                                             cls.out, build_id="kg_test")

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def rows(self, name):
        with open(os.path.join(self.out, serialize.RML_DIR, name), newline="",
                  encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def test_a_frames_and_a_templates_fromImage_are_routed_apart(self):
        self.assertEqual(self.rows("frame_image_edges.csv"), [{"url": FRAME, "qid": "Q83279"}])
        self.assertEqual(self.rows("entity_image_edges.csv"), [{"template": "7", "qid": "Q1"}])
        (occ,) = self.rows("frame_image_occurrences.csv")
        self.assertEqual((occ["src"], occ["dst"], occ["depiction_kind"]),
                         (FRAME, "Q83279", "character"))
        self.assertEqual(len(self.rows("entity_image_occurrences.csv")), 1)

    def test_both_reach_the_rdf_as_m4s_fromImage(self):
        with open(os.path.join(self.out, "graph.nt"), encoding="utf-8") as fh:
            graph = set(fh.read().splitlines())
        pred = f"<{M4S}fromImage>"
        self.assertIn(f"<{FRAME}> {pred} <{WD}Q83279> .", graph)
        self.assertIn(f"<{MK}template/7> {pred} <{WD}Q1> .", graph)
        self.assertIn(f"<< <{FRAME}> {pred} <{WD}Q83279> >> <{MK}depictionKind> \"character\" .",
                      graph)

    def test_the_counts_add_up_across_the_two_files(self):
        files = self.manifest["files"]
        self.assertEqual(files["frame_image_edges.csv"]["rows"]
                         + files["entity_image_edges.csv"]["rows"],
                         self.manifest["counts"]["edges_by_type"]["fromImage"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
