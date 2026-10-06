"""What a frame's own image shows, in the graph (KG 7.1.0): build.py, rdf.py
and serialize.py. Pinned: the frame itself gets IMKG's m4s:fromImage (the
subject of the paper's query "IMs that depict SpongeBob"), one edge per item,
one occurrence per region, with the template layer's occurrence fields; a
frame's fromImage and a template's go to different RML files (their subjects
are written differently, a URL and mk:template/<id>), and both still reach
graph.nt with their annotations."""
import csv

import pytest

from modules.kg import build, rdf, serialize

FRAME = "https://knowyourmeme.com/memes/spongegar"
M4S, MK, WD = rdf.PREFIXES["m4s"], rdf.PREFIXES["mk"], rdf.PREFIXES["wd"]
ENTRY = {"url": FRAME, "title": "Spongegar", "category": "meme"}


def mention(qid="Q83279", text="SpongeBob SquarePants", box=(0.1, 0.05, 0.6, 0.9)):
    return {"qid": qid, "label": text, "text": text, "score": 0.9, "method": "vlm_named", "model": "qwen3-vl:32b",
            "region": {"kind": "character", "box": list(box)}}


def test_the_frame_gets_from_image():
    nodes, edges = build.build_nodes_and_edges(ENTRY, frame_images=[mention(), mention(box=(0.6, 0.1, 0.9, 0.9))])
    (e,) = [e for e in edges if e["type"] == "fromImage"]
    assert (e["src"], e["dst"], len(e["occurrences"])) == (FRAME, "wd:Q83279", 2)          # one per region
    assert e["occurrences"][0] == {"mention_text": "SpongeBob SquarePants", "link_score": 0.9,
                                   "link_method": "vlm_named", "depiction_kind": "character",
                                   "bounding_box": "xywh=percent:10.0,5.0,50.0,85.0", "detected_by": "qwen3-vl:32b"}
    assert {n["id"]: n["kind"] for n in nodes}["wd:Q83279"] == "wikidata_entity"
    assert not [e for e in build.build_nodes_and_edges(ENTRY)[1] if e["type"] == "fromImage"]   # no reading, no edge


@pytest.fixture(scope="module")
def out(tmp_path_factory):
    nodes, edges = build.build_nodes_and_edges(
        ENTRY, frame_images=[mention()],
        templates=[{"template_id": 7, "R": 0.8, "method": "search", "name": "Spongegar",
                    "mentions": [mention(qid="Q1", text="caveman")]}])
    path = tmp_path_factory.mktemp("frame_image_graph")
    return path, serialize.write_build(lambda: iter(nodes), lambda: iter(edges), str(path), build_id="kg_test")


def rows(path, name):
    with open(path / serialize.RML_DIR / name, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_a_frames_and_a_templates_from_image_are_routed_apart_and_add_up(out):
    path, manifest = out
    assert rows(path, "frame_image_edges.csv") == [{"url": FRAME, "qid": "Q83279"}]
    assert rows(path, "entity_image_edges.csv") == [{"template": "7", "qid": "Q1"}]
    (occ,) = rows(path, "frame_image_occurrences.csv")
    assert (occ["src"], occ["dst"], occ["depiction_kind"]) == (FRAME, "Q83279", "character")
    assert len(rows(path, "entity_image_occurrences.csv")) == 1
    files = manifest["files"]
    assert files["frame_image_edges.csv"]["rows"] + files["entity_image_edges.csv"]["rows"] == \
        manifest["counts"]["edges_by_type"]["fromImage"]


def test_both_reach_the_rdf_as_m4s_from_image(out):
    graph = set((out[0] / "graph.nt").read_text(encoding="utf-8").splitlines())
    pred = f"<{M4S}fromImage>"
    assert {f"<{FRAME}> {pred} <{WD}Q83279> .", f"<{MK}template/7> {pred} <{WD}Q1> .",
            f'<< <{FRAME}> {pred} <{WD}Q83279> >> <{MK}depictionKind> "character" .'} <= graph
