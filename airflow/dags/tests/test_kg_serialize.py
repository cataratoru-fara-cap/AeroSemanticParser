"""kg/serialize.py: one build, every representation, one stream. The point:
the exporters this replaced derived the property-graph CSVs and the RML CSVs in
two loops that disagreed by 14,563 edges unnoticed; every file here projects
one stream, and the projections must describe the same edges."""
import csv
from collections import Counter
from pathlib import Path

import pytest

from helpers import KG_CONFIG
from modules.kg import ntdiff, rdf, serialize
from modules.kg.build import EDGE_TYPES
from modules.kg.siblings import SIBLING_EDGE_TYPES
from modules.kg.taxonomy import CONCEPT_EDGE_TYPES

F1 = "https://knowyourmeme.com/memes/doge"
F2 = "https://knowyourmeme.com/memes/cheems"
STUB = "https://knowyourmeme.com/memes/shiba-inu"
EXT = "https://en.wikipedia.org/wiki/Doge"
IMG = "https://i.kym-cdn.com/photos/images/original/000/1.jpg"
TIKTOK = "https://www.tiktok.com/@a/video/1"
E1, E2 = "abc123def456-0011223344", "abc123def456-5566778899"
EV, EV2 = f"https://meme4.science/atlas/event/{E1}", f"https://meme4.science/atlas/event/{E2}"
WD = "http://www.wikidata.org/entity/"
ONTOLOGY = str(KG_CONFIG / "memeatlas.ttl")

NODES = [
    {"id": F1, "kind": "frame", "label": 'Doge "the" dog', "category": "meme", "status": "confirmed", "year": 2013,
     "from": "Tumblr", "about": "line one\n\nline\ttwo", "added": "2011-03-13T07:06:40Z", "badges": ["Sensitive"],
     "corpus_missing": ["region", "tags"], "section_texts": ["History\n\np1\n\np2", "Reception\n\np3"]},
    {"id": F2, "kind": "frame", "label": "Cheems", "category": "meme", "status": "confirmed"},
    {"id": STUB, "kind": "frame_stub", "label": None, "category": "meme", "status": None},
    {"id": "type:image-macro", "kind": "entry_type_concept", "label": "image-macro"},
    {"id": "type:meme", "kind": "entry_type_concept", "label": "meme"},
    {"id": "tag:doge", "kind": "tag_concept", "label": "doge"},
    {"id": "tag:dog", "kind": "tag_concept", "label": "dog"},
    {"id": EXT, "kind": "external_ref", "label": None},
    {"id": "region:Japan", "kind": "region_concept", "label": "Japan"},
    {"id": "image:" + IMG, "kind": "image", "width": 600, "height": 400},
    {"id": "origin:twitter", "kind": "origin_concept", "label": "twitter"},
    {"id": "origin:social-network", "kind": "origin_concept", "label": "social-network"},
    {"id": "badge:sensitive", "kind": "badge_concept", "label": "Sensitive"},
    # an event dated at day precision, two actors, a quote (the escaping path)
    {"id": f"event:{E1}", "kind": "event", "source_section": "origin",
     "source_text": 'The photo "Kabosu" was posted to Tumblr on February 23rd, 2010. [3]',
     "date": "2010-02-23", "date_precision": "day", "date_basis": "stated", "date_text": "February 23rd, 2010",
     "date_start": "2010-02-23T00:00:00Z", "date_end": "2010-02-23T23:59:59Z",
     "locations": ["Tumblr", "the Doge fan group"], "location_type": "platform", "certainty": "confirmed",
     "actors": ["Atsuko Sato", "u/kabosu"], "extraction_model": "ministral-3:14b", "extraction_version": "1.0.0"},
    {"id": TIKTOK, "kind": "external_ref", "label": None},     # an embedded post's own node
    # an undated one: no interval, no actors
    {"id": f"event:{E2}", "kind": "event", "source_text": "Shortly afterwards it spread to 4chan.",
     "source_section": "spread", "date_precision": "none", "locations": ["4chan"], "location_type": "platform",
     "certainty": "unconfirmed", "extraction_model": "ministral-3:14b", "extraction_version": "1.0.0"},
    # two Wikidata items, one named from two fields
    {"id": "wd:Q39315", "kind": "wikidata_entity", "qid": "Q39315", "label": "Shiba Inu", "description": "dog breed"},
    {"id": "wd:Q15894956", "kind": "wikidata_entity", "qid": "Q15894956", "label": 'Doge "meme"',
     "description": "Internet meme"},
]
EDGES = [
    {"src": F1, "type": "hasEntryType", "dst": "type:image-macro"},
    {"src": F1, "type": "hasTag", "dst": "tag:doge"},
    {"src": F1, "type": "hasTag", "dst": "tag:dog"},
    {"src": F2, "type": "hasTag", "dst": "tag:doge"},
    {"src": F1, "type": "partOfSeries", "dst": STUB},
    {"src": F2, "type": "sharesSameSeries", "dst": F1},          # a sibling pair, once
    {"src": F1, "type": "citesMediaFrame", "dst": F2, "occurrences": [{"anchor_text": "Cheems", "in_section": "Notes"}]},
    {"src": F1, "type": "citesExternal", "dst": EXT, "occurrences": [
        {"citation_text": 'The "Doge" article', "citation_index": 1}, {"site_name": "Wikipedia"}]},
    {"src": "type:image-macro", "type": "subTypeOf", "dst": "type:meme"},
    {"src": F1, "type": "hasRegion", "dst": "region:Japan"},
    {"src": F1, "type": "hasImage", "dst": "image:" + IMG, "occurrences": [
        {"role": "page"}, {"role": "section", "in_section": "Notes", "alt_text": "doge", "caption": "wow"}]},
    {"src": F1, "type": "hasOrigin", "dst": "origin:twitter"},
    {"src": F1, "type": "hasBadge", "dst": "badge:sensitive"},
    {"src": "origin:twitter", "type": "subTypeOf", "dst": "origin:social-network"},
    {"src": F1, "type": "hasEvent", "dst": f"event:{E1}"},
    {"src": F1, "type": "hasEvent", "dst": f"event:{E2}"},
    {"src": f"event:{E1}", "type": "eventLink", "dst": F2},
    {"src": f"event:{E1}", "type": "eventCitation", "dst": EXT},
    {"src": f"event:{E1}", "type": "eventImage", "dst": "image:" + IMG},
    {"src": f"event:{E2}", "type": "eventEmbed", "dst": TIKTOK},
    {"src": f"event:{E2}", "type": "eventDateAnchor", "dst": f"event:{E1}"},
    {"src": f"event:{E1}", "type": "nextInStory", "dst": f"event:{E2}"},
    {"src": F1, "type": "fromTitle", "dst": "wd:Q15894956", "occurrences": [
        {"mention_text": "Doge", "link_score": 1.0, "link_method": "kym_id"}]},
    {"src": F1, "type": "fromTags", "dst": "wd:Q39315", "occurrences": [
        {"mention_text": "shiba inu", "link_score": 0.62, "link_method": "tag"}]},
    {"src": F1, "type": "fromAbout", "dst": "wd:Q39315", "occurrences": [
        {"mention_text": "Shiba Inus", "link_score": 0.83, "link_method": "ner", "ner_label": "ORG"},
        {"mention_text": "Shiba Inu", "link_score": 0.9, "link_method": "propn"}]},
]


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write(tmp, nodes=NODES, edges=EDGES, **kw):
    return serialize.write_build(lambda: iter(nodes), lambda: iter(edges) if isinstance(edges, list) else edges(),
                                 str(tmp), **{"build_id": "b", **kw})


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    out = tmp_path_factory.mktemp("build")
    manifest = write(out, build_id="kg_test", stamps={"kg_build_version": "2.0.0"}, ontology_path=ONTOLOGY)
    return out, manifest


def rml(built, name):
    return read_csv(built[0] / serialize.RML_DIR / name)


def nt(built):
    return (built[0] / "graph.nt").read_text(encoding="utf-8")


# -- layout and manifest -----------------------------------------------------------

def test_every_file_exists_atomically_and_matches_the_manifest(built):
    out, manifest = built
    assert all((out / n).is_file() for n in ("graph.nt", "kg_view_nodes.csv", "kg_view_edges.csv",
                                            "manifest.json", "ontology.ttl"))
    assert all((out / serialize.RML_DIR / n).is_file() for n, _ in serialize.EDGE_TYPE_TO_RML_FILE.values())
    assert {p.name for p in (out / serialize.RML_DIR).iterdir()} == serialize.all_rml_files()
    assert list(out.rglob("*.tmp")) == []
    for name, meta in manifest["files"].items():
        assert meta["sha256"] == serialize._sha256(str(out / meta["path"])), name
    assert manifest["view_filters"] == {"exclude_kinds": [], "top_tags": 0}
    assert (manifest["stamps"]["build_id"], manifest["stamps"]["kg_build_version"]) == ("kg_test", "2.0.0")
    assert serialize.load_manifest(str(out)) == manifest
    assert (manifest["counts"]["edges"], manifest["counts"]["nodes"], manifest["counts"]["edges_by_type"]["hasTag"]) \
        == (len(EDGES), len(NODES), 3)
    assert (manifest["files"]["cites_occurrences.csv"]["rows"], manifest["files"]["image_occurrences.csv"]["rows"]) == (2, 2)


def test_the_rml_table_covers_exactly_the_edge_vocabulary():
    # coOccursWith excluded (5.0.1): no RDF/RML representation for any pair
    assert set(serialize.EDGE_TYPE_TO_RML_FILE) == set(EDGE_TYPES) | set(CONCEPT_EDGE_TYPES) | set(SIBLING_EDGE_TYPES)


# -- what each RML file holds (what kg_mapping.yarrrml.yml expects) ----------------------

@pytest.mark.parametrize("name, rows", [
    ("frame_aliases.csv", []),
    ("images.csv", [{"iri": IMG, "width": "600", "height": "400"}]),
    ("image_edges.csv", [{"url": F1, "image": IMG}]),
    ("region_edges.csv", [{"url": F1, "region": "Japan"}]),                   # prefixes stripped
    ("entry_type_edges.csv", [{"url": F1, "slug": "image-macro"}]),
    ("subtype_edges.csv", [{"narrower": "image-macro", "broader": "meme"}]),  # entry types only
    ("origin_subtype_edges.csv", [{"narrower": "twitter", "broader": "social-network"}]),
    ("sibling_edges.csv", [{"url": F2, "sibling_url": F1}]),                  # read both ways by the mapping
    ("series_edges.csv", [{"url": F1, "parent_url": STUB}]),
    ("cites_edges.csv", [{"url": F1, "target_url": EXT}]),
    ("origin_edges.csv", [{"url": F1, "origin": "twitter"}]),
    ("badge_edges.csv", [{"url": F1, "badge": "sensitive"}]),
    # events: the bare id, templated back onto https://meme4.science/atlas/event/
    ("event_link_edges.csv", [{"event": E1, "target_url": F2}]),
    ("event_citation_edges.csv", [{"event": E1, "target_url": EXT}]),
    ("event_image_edges.csv", [{"event": E1, "image": IMG}]),
    ("event_embed_edges.csv", [{"event": E2, "target_url": TIKTOK}]),
    ("event_story_edges.csv", [{"event": E1, "next": E2}]),
    # entities: the bare QID, templated back onto http://www.wikidata.org/entity/
    ("entity_title_edges.csv", [{"url": F1, "qid": "Q15894956"}]),
    ("entity_tag_edges.csv", [{"url": F1, "qid": "Q39315"}]),
    ("entity_about_edges.csv", [{"url": F1, "qid": "Q39315"}]),
])
def test_an_rml_file_holds_exactly(built, name, rows):
    assert rml(built, name) == rows


def test_frames_carry_imkg_fields_and_leave_absent_ones_empty(built):
    rows = rml(built, "frames.csv")
    assert {r["url"] for r in rows} == {F1, F2}                              # real frames only
    assert list(rows[0]) == [c for c, _ in serialize.RML_NODE_FILES["frames.csv"][1]]
    by = {r["url"]: r for r in rows}
    assert (by[F1]["category_class"], by[F1]["year"], by[F1]["about"], by[F2]["year"]) == \
        ("Meme", "2013", "line one\n\nline\ttwo", "")
    # badges are an edge since 5.0.0, not a frame list
    assert {r["missing"] for r in rml(built, "frame_corpus_missing.csv")} == {"region", "tags"}
    assert [r["text"] for r in rml(built, "frame_section_texts.csv")] == ["History\n\np1\n\np2", "Reception\n\np3"]


def test_concepts_files_and_tags(built):
    assert {r["slug"]: r["label"] for r in rml(built, "types.csv")} == {"image-macro": "image macro", "meme": "meme"}
    assert {(r["url"], r["tag"]) for r in rml(built, "tag_edges.csv")} == {(F1, "doge"), (F1, "dog"), (F2, "doge")}
    # origin labels go through concept_pref_label ("-" -> " "), badges do not
    assert {r["slug"]: r["label"] for r in rml(built, "origin_concepts.csv")} == \
        {"twitter": "twitter", "social-network": "social network"}
    assert {r["slug"]: r["label"] for r in rml(built, "badge_concepts.csv")} == {"sensitive": "Sensitive"}
    schemes = {r["iri"]: r["label"] for r in rml(built, "scheme.csv")}
    assert set(schemes) == {rdf.SCHEME_IRI, rdf.ORIGIN_SCHEME_IRI, rdf.BADGE_SCHEME_IRI}
    assert schemes[rdf.ORIGIN_SCHEME_IRI] == rdf.ORIGIN_SCHEME_LABEL


def test_one_occurrence_row_per_mention_with_empty_cells(built):
    cites = rml(built, "cites_occurrences.csv")
    assert [(c["src"], c["dst"], c["citation_text"], c["citation_index"], c["site_name"]) for c in cites] == \
        [(F1, EXT, 'The "Doge" article', "1", ""), (F1, EXT, "", "", "Wikipedia")]
    assert [(r["dst"], r["role"], r["caption"]) for r in rml(built, "image_occurrences.csv")] == \
        [(IMG, "page", ""), (IMG, "section", "wow")]
    assert rml(built, "cites_frame_occurrences.csv")[0]["anchor_text"] == "Cheems"
    about = rml(built, "entity_about_occurrences.csv")
    assert [(r["mention_text"], r["link_score"], r["link_method"], r["ner_label"]) for r in about] == \
        [("Shiba Inus", "0.83", "ner", "ORG"), ("Shiba Inu", "0.9", "propn", "")]
    assert {r["dst"] for r in about} == {"Q39315"}


def test_events_on_disk(built):
    rows = {r["iri"]: r for r in rml(built, "events.csv")}
    assert set(rows) == {EV, EV2}
    assert (rows[EV]["date_start"], rows[EV]["certainty"], rows[EV]["date_basis"]) == \
        ("2010-02-23T00:00:00Z", "confirmed", "stated")
    # an undated event leaves its interval cells empty: morph-kgc then emits nothing
    assert (rows[EV2]["date_start"], rows[EV2]["date_end"], rows[EV2]["date_precision"]) == ("", "", "none")
    # no bare date (pandas could read it as a number) and no single location column
    assert not {"date", "location"} & set(rml(built, "events.csv")[0])
    assert sorted((r["iri"], r["actor"]) for r in rml(built, "event_actors.csv")) == \
        [(EV, "Atsuko Sato"), (EV, "u/kabosu")]
    # 6.2.0: a platform and a venue on it are two places, two rows
    assert sorted((r["iri"], r["location"]) for r in rml(built, "event_locations.csv")) == \
        sorted([(EV, "Tumblr"), (EV, "the Doge fan group"), (EV2, "4chan")])
    edges = rml(built, "event_edges.csv")
    assert sorted(r["event"] for r in edges) == [E1, E2] and {r["url"] for r in edges} == {F1}
    assert {r["iri"]: r["label"] for r in rml(built, "wikidata_entities.csv")} == \
        {WD + "Q39315": "Shiba Inu", WD + "Q15894956": 'Doge "meme"'}


# -- the projections agree (the drift regression) --------------------------------------

def test_rml_rows_equal_the_input_edges_per_type(built):
    # subTypeOf is split across two files by prefix; coOccursWith has no file
    for etype, (name, _) in serialize.EDGE_TYPE_TO_RML_FILE.items():
        want = sum(1 for e in EDGES if e["type"] == etype
                   and not (etype == "subTypeOf" and e["src"].startswith("origin:")))
        assert len(rml(built, name)) == want, etype
    assert len(rml(built, serialize.ORIGIN_SUBTYPE_RML_FILE[0])) == \
        sum(1 for e in EDGES if e["type"] == "subTypeOf" and e["src"].startswith("origin:"))
    assert Counter(r["type"] for r in read_csv(built[0] / "kg_view_edges.csv")) == Counter(e["type"] for e in EDGES)


def test_every_input_edge_and_occurrence_is_in_the_rdf(built):
    graph = set(nt(built).splitlines())
    # relates + cites + image values, then entity mentions: title 3, tags 3, About 4 + 3
    assert len([line for line in graph if line.startswith("<<")]) == 2 + 3 + 5 + 3 + 3 + 7
    for edge in EDGES:
        expected = set(rdf.iter_triples([], [edge]))
        assert expected and expected <= graph, edge


def test_the_rdf_carries_events_and_wikidata_links(built):
    text = nt(built)
    mk, m4s = rdf.PREFIXES["mk"], rdf.PREFIXES["m4s"]
    for line in (f"<{EV}> <{rdf.RDF_TYPE}> <{mk}Event> .", f"<{F1}> <{mk}hasEvent> <{EV}> .",
                 f'<{EV}> <{mk}eventStart> "2010-02-23T00:00:00Z"^^<{rdf.XSD_DATETIME}> .',
                 f"<{EV}> <{mk}eventImage> <{IMG}> .", f"<{EV}> <{mk}eventCitation> <{EXT}> .",
                 f"<{EV}> <{mk}nextInStory> <{EV2}> .",
                 f"<{F1}> <{m4s}fromAbout> <{WD}Q39315> .", f"<{F1}> <{m4s}fromTags> <{WD}Q39315> .",
                 f"<{F1}> <{mk}fromTitle> <{WD}Q15894956> .",
                 f'<< <{F1}> <{m4s}fromAbout> <{WD}Q39315> >> <{mk}linkScore> "0.9"^^<{rdf.XSD_DECIMAL}> .'):
        assert line in text, line
    for absent in (f"<{EV2}> <{mk}eventStart>", "eventSummary", f"<{WD}Q39315> <{rdf.RDF_TYPE}>"):
        assert absent not in text, absent


# -- set semantics, view filters, atomicity, stability ------------------------------------

def test_a_repeated_edge_appears_once_in_every_projection(tmp_path):
    write(tmp_path, edges=EDGES + [dict(EDGES[1])])
    assert len(read_csv(tmp_path / serialize.RML_DIR / "tag_edges.csv")) == 3
    assert sum(1 for r in read_csv(tmp_path / "kg_view_edges.csv") if r["type"] == "hasTag") == 3
    assert ntdiff.digest(str(tmp_path / "graph.nt"))[2]["tag"] == 3


def test_exclude_kinds_filters_the_view_only(tmp_path):
    m = write(tmp_path, exclude_kinds=["tag_concept"])
    assert not any(r["kind"] == "tag_concept" for r in read_csv(tmp_path / "kg_view_nodes.csv"))
    assert not any(r["type"] == "hasTag" for r in read_csv(tmp_path / "kg_view_edges.csv"))
    assert len(read_csv(tmp_path / serialize.RML_DIR / "tag_edges.csv")) == 3     # RML is never filtered
    assert m["view_filters"]["exclude_kinds"] == ["tag_concept"]
    with pytest.raises(ValueError):
        write(tmp_path / "x", exclude_kinds=["nope"])


def test_top_tags_keeps_the_highest_degree_tags(tmp_path):
    write(tmp_path, top_tags=1)
    assert {r["id"] for r in read_csv(tmp_path / "kg_view_nodes.csv") if r["kind"] == "tag_concept"} == {"tag:doge"}


def test_a_failing_source_leaves_no_final_named_files(tmp_path):
    def bad_edges():
        yield EDGES[0]
        raise RuntimeError("cursor died")
    with pytest.raises(RuntimeError):
        write(tmp_path, edges=bad_edges)
    # the node pass completed (frames.csv may exist); nothing from the failed pass on is final
    finals = [p.name for p in tmp_path.rglob("*") if p.is_file() and not p.name.endswith(".tmp")]
    assert not {"manifest.json", "graph.nt", "kg_view_edges.csv"} & set(finals)
    assert not any(n.endswith("_edges.csv") for n in finals)


def test_the_output_is_byte_stable(tmp_path):
    a = write(tmp_path / "a", build_id="same", stamps={"x": 1})
    b = write(tmp_path / "b", build_id="same", stamps={"x": 1})
    assert {n: f["sha256"] for n, f in a["files"].items()} == {n: f["sha256"] for n, f in b["files"].items()}


def test_cooccurs_with_is_always_property_graph_only(tmp_path):
    # 5.0.1: no pair reaches RDF, so no RML file exists for it at all; still counted
    nodes = [{"id": "tag:meme", "kind": "tag_concept", "label": "meme"},
             {"id": "tag:dank-meme", "kind": "tag_concept", "label": "dank meme"}]
    m = write(tmp_path, nodes, [{"src": "tag:meme", "type": "coOccursWith", "dst": "tag:dank-meme"}],
              assume_unique=True)
    assert not (tmp_path / serialize.RML_DIR / "entry_type_cooccurs_edges.csv").exists()
    assert read_csv(tmp_path / "kg_view_edges.csv") == [{"source": "tag:meme", "target": "tag:dank-meme",
                                                         "type": "coOccursWith"}]
    assert "coOccursWith" not in (tmp_path / "graph.nt").read_text(encoding="utf-8")
    assert m["counts"]["edges_by_type"]["coOccursWith"] == 1
