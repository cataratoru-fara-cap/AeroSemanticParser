"""The template layer in the graph (KG 6.4.0): build.py, rdf.py, serialize.py
and kg_store's reads (mongomock). Pinned: a frame's edge to a template carries
how well it fits (hasTemplate, RDF-star on mk:hasTemplate) and RDF has IMKG's
m4s:templateOf back; everything about a template hangs off the template (its
blank, still templates only; its imgflip page; m4s:fromImage, one occurrence
per region, the box a Media Fragments literal); a template two frames selected
is emitted identically by both; imgflip:templateId is a plain literal, as IMKG
writes it; the build reads selections and readings frozen at its snapshot."""
import csv
from datetime import datetime, timezone

import pytest

from helpers import mock_stores, serving
from modules import kg_store, template_entity_store as tes, template_store as ts
from modules.kg import build, rdf, serialize

FRAME = "https://knowyourmeme.com/memes/distracted-boyfriend"
OTHER = "https://knowyourmeme.com/memes/other"
MK, M4S, WD = rdf.PREFIXES["mk"], rdf.PREFIXES["m4s"], rdf.PREFIXES["wd"]
T, T_IRI, BLANK = "template:112126428", MK + "template/112126428", "https://i.imgflip.com/1ur9b0.jpg"
PAGE = "https://imgflip.com/meme/Distracted-Boyfriend"


def template(**over):
    region = lambda text, box: {"qid": "Q5", "label": "human", "text": text, "score": 0.6, "method": "vlm_generic",
                                "model": "qwen3-vl:32b", "region": {"kind": "person", "box": box}}
    return {"template_id": 112126428, "R": 0.9, "method": "search", "name": "Distracted Boyfriend",
            "alt_names": ["distracted bf"], "file_type": "jpg", "url": PAGE, "blank_url": BLANK, "width": 1200,
            "height": 800, "animated": False, "featured": True,
            "mentions": [region("man", [0.1, 0.05, 0.6, 0.9]), region("woman", [0.6, 0.1, 0.9, 0.9])], **over}


def built(url=FRAME, **over):
    return build.build_nodes_and_edges({"url": url, "title": "Distracted Boyfriend", "category": "meme"},
                                       templates=[template(**over)])


def test_nodes_and_edges():
    nodes, edges = built()
    by_id = {n["id"]: n for n in nodes}
    t = by_id[T]
    assert (t["kind"], t["label"], t["template_id"], t["alt_names"]) == \
        ("template", "Distracted Boyfriend", "112126428", ["distracted bf"])
    assert (by_id["image:" + BLANK]["width"], by_id[PAGE]["kind"], by_id["wd:Q5"]["kind"]) == \
        (1200, "external_ref", "wikidata_entity")
    assert {(FRAME, "hasTemplate", T), (T, "templateImage", "image:" + BLANK), (T, "imgflipPage", PAGE)} <= \
        {(e["src"], e["type"], e["dst"]) for e in edges}
    assert next(e for e in edges if e["type"] == "hasTemplate")["occurrences"] == \
        [{"template_score": 0.9, "template_match": "search"}]
    [from_image] = [e for e in edges if e["type"] == "fromImage"]          # one edge per item...
    assert len(from_image["occurrences"]) == 2                              # ...one occurrence per region
    assert from_image["occurrences"][0]["bounding_box"] == "xywh=percent:10.0,5.0,50.0,85.0"


def test_an_animated_template_has_no_image_node():
    nodes, edges = built(animated=True, file_type="mp4", blank_url="https://i.imgflip.com/2/3jpogl.jpg")
    assert not any(e["type"] == "templateImage" for e in edges) and not any(n["kind"] == "image" for n in nodes)


def test_two_frames_emit_the_template_identically():
    (n1, e1), (n2, e2) = built(FRAME), built(OTHER, R=0.7)
    pick = lambda edges: sorted((e["src"], e["type"], e["dst"], str(e.get("occurrences")))
                                for e in edges if e["src"].startswith("template:"))
    assert pick(e1) == pick(e2)
    assert next(n for n in n1 if n["kind"] == "template") == next(n for n in n2 if n["kind"] == "template")


def test_triples():
    lines = set(rdf.iter_triples(*built()))
    assert {f"<{T_IRI}> <{rdf.RDF_TYPE}> <{MK}MemeTemplate> .",
            f'<{T_IRI}> <https://imgflip.com/templateId> "112126428" .',
            f"<{FRAME}> <{MK}hasTemplate> <{T_IRI}> .", f"<{T_IRI}> <{M4S}templateOf> <{FRAME}> .",
            f"<{T_IRI}> <{M4S}fromImage> <{WD}Q5> .",
            f'<< <{FRAME}> <{MK}hasTemplate> <{T_IRI}> >> <{MK}templateScore> "0.9"^^<{rdf.XSD_DECIMAL}> .',
            f'<< <{T_IRI}> <{M4S}fromImage> <{WD}Q5> >> <{MK}boundingBox> "xywh=percent:60.0,10.0,30.0,80.0" .',
            } <= lines


def test_rml_files(tmp_path):
    nodes, edges = built()
    serialize.write_build(lambda: nodes, lambda: edges, str(tmp_path), build_id="kg_test")

    def rows(name):
        with open(tmp_path / serialize.RML_DIR / name, encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    assert (rows("templates.csv")[0]["iri"], rows("templates.csv")[0]["template_id"]) == (T_IRI, "112126428")
    assert rows("template_alt_names.csv") == [{"iri": T_IRI, "alt_name": "distracted bf"}]
    assert rows("template_edges.csv") == [{"url": FRAME, "template": "112126428"}]
    assert len(rows("entity_image_occurrences.csv")) == 2 and rows("template_occurrences.csv")[0]["template_match"] == "search"


# -- kg_store.template_links_for / template_stamps -----------------------------------------------

EARLY, LATE = datetime(2026, 9, 1, tzinfo=timezone.utc), datetime(2026, 10, 1, tzinfo=timezone.utc)


@pytest.fixture
def stores():
    t_store, e_store = mock_stores(ts.TemplateStore, tes.TemplateEntityStore)
    t_store.frames.insert_many([
        {"_id": "f1", "frame_url": FRAME, "status": "selected", "selected_at": EARLY, "selection_sha": "s1",
         "selected": [{"template_id": 1, "R": 0.9, "method": "search"}]},
        {"_id": "f2", "frame_url": OTHER, "status": "selected", "selected_at": LATE, "selection_sha": "s2",
         "selected": [{"template_id": 1, "R": 0.8, "method": "search"}]}])
    t_store.templates.insert_one({"_id": 1, "name": "T", "url": "https://imgflip.com/meme/T"})
    e_store.detections.insert_one({"_id": 1, "in_graph_count": 1, "linked_at": EARLY, "detection": {"model": "m"},
                                   "links": {"mentions": [{"qid": "Q5", "in_graph": True, "text": "man"},
                                                          {"qid": "Q6", "in_graph": False}]}})
    with serving(ts, t_store), serving(tes, e_store):
        yield t_store


def test_links_are_frozen_at_the_snapshot(stores):
    got = kg_store.template_links_for(["f1", "f2"], datetime(2026, 9, 15, tzinfo=timezone.utc))
    assert list(got) == [FRAME]
    [rec] = got[FRAME]
    assert (rec["template_id"], rec["name"], rec["R"]) == (1, "T", 0.9)
    assert [(m["qid"], m["model"]) for m in rec["mentions"]] == [("Q5", "m")]


def test_stamps_move_with_the_selection(stores):
    snap = datetime(2026, 12, 1, tzinfo=timezone.utc)
    a = kg_store.template_stamps(snap)
    assert (a["templates_frames"], a["templates_links"], a["template_entities_in_graph"]) == (2, 2, 1)
    stores.frames.update_one({"_id": "f2"}, {"$set": {"selection_sha": "changed"}})
    b = kg_store.template_stamps(snap)
    assert a["templates_selection_digest"] != b["templates_selection_digest"]
    assert set(kg_store.TEMPLATE_STAMP_KEYS) <= set(b)
