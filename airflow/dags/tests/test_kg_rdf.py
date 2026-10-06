"""kg/rdf.py: (nodes, edges) -> canonical N-Triples in IMKG's vocabulary.

Scope is pinned on purpose: the RML validation path emits triples only for rows
in one of its CSVs, so typing frame_stub nodes here would make every diff run
report ~9,500 phantom divergences."""
import hashlib

import pytest

from modules.kg import build, ntdiff, rdf, siblings, taxonomy
from modules.kym_models import Category

FRAME = "https://knowyourmeme.com/memes/doge"
PARENT = "https://knowyourmeme.com/memes/shiba-inu"
OTHER = "https://knowyourmeme.com/memes/cheems"
EXTERNAL = "https://en.wikipedia.org/wiki/Doge"
IMG = "https://i.kym-cdn.com/x.jpg"
M4S, MK, KYM, SKOS = rdf.PREFIXES["m4s"], rdf.PREFIXES["mk"], rdf.PREFIXES["kym"], rdf.PREFIXES["skos"]
RDFS, XSD = rdf.PREFIXES["rdfs"], rdf.PREFIXES["xsd"]
RDF_TYPE = rdf.PREFIXES["rdf"] + "type"
EID, EIRI = "event:abc123def456-0011223344", "https://meme4.science/atlas/event/abc123def456-0011223344"
WIRI = "http://www.wikidata.org/entity/Q39315"


def frame(nid=FRAME, label="Doge", category="meme", status="confirmed"):
    return {"id": nid, "kind": "frame", "label": label, "category": category, "status": status}


def triples(nodes, edges, **kw):
    return list(rdf.iter_triples(nodes, edges, **kw))


# -- literals and IRIs -------------------------------------------------------------

@pytest.mark.parametrize("text, want", [('a "b" c', 'a \\"b\\" c'),   # 1,141 real titles have one
                                        ("a\\b", "a\\\\b"), ("a\nb\rc\td", "a\\nb\\rc\\td")])
def test_escaping(text, want):
    assert rdf.escape_literal(text) == want


def test_an_escaped_title_is_a_valid_literal():
    got = triples([frame(label='The "Best" Meme \\ Ever')], [])
    assert '"The \\"Best\\" Meme \\\\ Ever"' in next(t for t in got if "rdf-schema#label" in t)
    assert all(t.endswith(" .") for t in got)


@pytest.mark.parametrize("node_id, iri", [
    ("type:image-macro", rdf.TYPES_BASE + "image-macro"), (FRAME, FRAME), (EID, EIRI),
    ("wd:Q39315", WIRI),     # Wikidata's canonical entity IRI, not IMKG's /wiki/ page URL
])
def test_node_iris(node_id, iri):
    assert rdf.node_iri(node_id) == iri


# -- nodes -------------------------------------------------------------------------------

def test_a_frame_reads_as_an_imkg_media_frame_term_for_term():
    assert set(triples([frame()], [])) == {
        f"<{FRAME}> <{RDF_TYPE}> <{M4S}MediaFrame> .", f"<{FRAME}> <{RDF_TYPE}> <{KYM}Meme> .",
        f'<{FRAME}> <{M4S}title> "Doge" .', f'<{FRAME}> <{RDFS}label> "Doge" .', f'<{FRAME}> <{M4S}status> "confirmed" .'}
    assert triples([frame(label=None, category=None, status=None)], []) == [f"<{FRAME}> <{RDF_TYPE}> <{M4S}MediaFrame> ."]


def test_imkg_literals_and_datatypes():
    got = set(triples([{"id": FRAME, "kind": "frame", "year": 2013, "from": "Tumblr", "about": "a\nb",
                        "added": "2011-03-13T07:06:40Z", "last_updated": "2023-11-14T22:13:20Z",
                        "aliases": ["Shibe", "Such Wow"], "corpus_missing": ["region"],
                        "section_texts": ["History\n\np1\tp2", "Reception\n\np3"]}], []))
    assert {f'<{FRAME}> <{M4S}year> "2013"^^<{XSD}integer> .', f'<{FRAME}> <{M4S}from> "Tumblr" .',
            f'<{FRAME}> <{M4S}added> "2011-03-13T07:06:40Z"^^<{XSD}dateTime> .',
            f'<{FRAME}> <{M4S}last_update_source> "2023-11-14T22:13:20Z"^^<{XSD}dateTime> .',
            f'<{FRAME}> <{MK}sectionText> "History\\n\\np1\\tp2" .',
            f'<{FRAME}> <{MK}sectionText> "Reception\\n\\np3" .'} <= got
    assert any(f"<{M4S}about>" in t for t in got)
    # a list property is one triple per value (badges are an edge since 5.0.0)
    assert (sum("altLabel" in t for t in got), sum(f"<{MK}corpusMissing>" in t for t in got)) == (2, 1)


def test_category_classes_are_imkgs_badge_text():
    # gap 02: on all 23,879 corpus pages the badge reads exactly these six
    assert {c.value: rdf.category_class(c.value) for c in Category if c.value != "unknown"} == {
        "meme": "Meme", "event": "Event", "subculture": "Subculture", "person": "Person", "site": "Site",
        "culture": "Culture"}
    assert rdf.category_class(Category.person) == "Person"
    assert [rdf.category_class(v) for v in ("unknown", None, "editorial")] == [None, None, None]   # never guessed


@pytest.mark.parametrize("node", [
    {"id": PARENT, "kind": "frame_stub", "label": None, "category": "meme", "status": None},  # no RML row
    {"id": "tag:doge", "kind": "tag_concept", "label": "doge"},
    {"id": EXTERNAL, "kind": "external_ref", "label": None},
    {"id": "region:Japan", "kind": "region_concept", "label": "Japan"},
])
def test_unclassed_kinds_get_no_triples(node):
    assert triples([node], []) == []


def test_every_node_kind_is_classed_or_deliberately_not():
    assert set(rdf.NODE_CLASSES) | {"frame_stub", "tag_concept", "region_concept", "external_ref"} == set(build.NODE_KINDS)


def test_an_image_iri_is_the_file_and_carries_only_its_size():
    assert set(triples([{"id": f"image:{IMG}", "kind": "image", "width": 600, "caption": "not a node property"}], [])) \
        == {f"<{IMG}> <{RDF_TYPE}> <{MK}Image> .", f'<{IMG}> <{MK}width> "600"^^<{XSD}integer> .'}


# -- concept schemes (5.0.0: a table of three, each declared once on first sight) -------

def test_an_entry_type_is_a_class_and_a_skos_concept_in_its_declared_scheme():
    got = triples([{"id": "type:exploitable", "kind": "entry_type_concept", "label": "exploitable"}], [])
    # 4 for the concept + 2 declaring the scheme: inScheme to an undeclared scheme is incomplete SKOS
    assert len(got) == 6 and not any("category" in t for t in got)
    assert all(any(w in t for t in got) for w in ("rdf-schema#Class", "inScheme", "prefLabel"))
    got = triples([{"id": f"type:{x}", "kind": "entry_type_concept", "label": x} for x in "abc"], [])
    assert len([t for t in got if t.startswith(f"<{rdf.SCHEME_IRI}>")]) == 2       # declared once
    assert not any(rdf.SCHEME_IRI in t for t in triples([frame()], []))
    # types.csv renders "-" as " " (29 of 119 slugs); the published graph has the spaced form
    assert rdf.concept_pref_label("ai-generated") == "ai generated"
    got = triples([{"id": "type:ai-generated", "kind": "entry_type_concept", "label": "ai-generated"}], [])
    assert '"ai generated"' in next(t for t in got if "prefLabel" in t)


def test_an_origin_is_a_concept_in_its_own_scheme_and_no_class():
    got = set(triples([{"id": "origin:twitter", "kind": "origin_concept", "label": "twitter"}], []))
    assert got == {f"<{MK}origin/twitter> <{RDF_TYPE}> <{rdf.SKOS}Concept> .",
                   f"<{rdf.ORIGIN_SCHEME_IRI}> <{RDF_TYPE}> <{rdf.SCHEME_CLASS}> .",
                   f'<{rdf.ORIGIN_SCHEME_IRI}> <{rdf.RDFS}label> "{rdf.ORIGIN_SCHEME_LABEL}" .',
                   f"<{MK}origin/twitter> <{rdf.SKOS}inScheme> <{rdf.ORIGIN_SCHEME_IRI}> .",
                   f'<{MK}origin/twitter> <{rdf.SKOS}prefLabel> "twitter" .'}
    assert f"<{rdf.RDFS}Class>" not in " ".join(got)       # hasOrigin is a plain object property
    got = triples([{"id": "origin:twitter", "kind": "origin_concept", "label": "twitter"},
                   {"id": "origin:4chan", "kind": "origin_concept", "label": "4chan"}], [])
    assert sum(f"<{rdf.ORIGIN_SCHEME_IRI}> <{RDF_TYPE}>" in t for t in got) == 1


def test_a_badge_has_its_own_scheme_and_three_schemes_coexist():
    got = set(triples([{"id": "badge:sensitive", "kind": "badge_concept", "label": "Sensitive"}], []))
    assert {f"<{rdf.BADGE_SCHEME_IRI}> <{RDF_TYPE}> <{rdf.SCHEME_CLASS}> .",
            f"<{MK}badge/sensitive> <{rdf.SKOS}inScheme> <{rdf.BADGE_SCHEME_IRI}> ."} <= got
    got = " ".join(triples([{"id": "type:creator", "kind": "entry_type_concept", "label": "creator"},
                            {"id": "origin:twitter", "kind": "origin_concept", "label": "twitter"},
                            {"id": "badge:sensitive", "kind": "badge_concept", "label": "Sensitive"}], []))
    assert all(f"<{iri}>" in got for iri in (rdf.SCHEME_IRI, rdf.ORIGIN_SCHEME_IRI, rdf.BADGE_SCHEME_IRI))


# -- edges ---------------------------------------------------------------------------------

@pytest.mark.parametrize("edge, want", [
    # a series is skos:broader with its inverse, both ways like IMKG
    ({"src": FRAME, "type": "partOfSeries", "dst": PARENT},
     [f"<{FRAME}> <{SKOS}broader> <{PARENT}> .", f"<{PARENT}> <{SKOS}narrower> <{FRAME}> ."]),
    # 6.6.0: siblings stored once per pair, IMKG's rdfs:seeAlso both ways
    ({"src": OTHER, "type": "sharesSameSeries", "dst": FRAME},
     [f"<{OTHER}> <{RDFS}seeAlso> <{FRAME}> .", f"<{FRAME}> <{RDFS}seeAlso> <{OTHER}> ."]),
    ({"src": FRAME, "type": "citesMediaFrame", "dst": PARENT}, [f"<{FRAME}> <{MK}citesMediaFrame> <{PARENT}> ."]),
    ({"src": FRAME, "type": "hasEntryType", "dst": "type:exploitable"},
     [f"<{FRAME}> <{RDF_TYPE}> <{rdf.TYPES_BASE}exploitable> ."]),
    ({"src": FRAME, "type": "hasRegion", "dst": "region:Japan"}, [f'<{FRAME}> <{MK}region> "Japan" .']),
    ({"src": FRAME, "type": "hasImage", "dst": f"image:{IMG}"}, [f"<{FRAME}> <{MK}hasImage> <{IMG}> ."]),
    ({"src": FRAME, "type": "hasTag", "dst": "tag:doge"}, [f'<{FRAME}> <{M4S}tag> "doge" .']),
    # the taxonomy is rdfs:subClassOf: skos:broader already means a series between frames
    ({"src": "type:model", "type": "subTypeOf", "dst": "type:influencer"},
     [f"<{rdf.TYPES_BASE}model> <{RDFS}subClassOf> <{rdf.TYPES_BASE}influencer> ."]),
    # one edge type, dispatched on the id prefix
    ({"src": "origin:twitter", "type": "subTypeOf", "dst": "origin:social-network"},
     [f"<{MK}origin/twitter> <{RDFS}subClassOf> <{MK}origin/social-network> ."]),
    ({"src": FRAME, "type": "hasOrigin", "dst": "origin:twitter"}, [f"<{FRAME}> <{MK}hasOrigin> <{MK}origin/twitter> ."]),
    ({"src": FRAME, "type": "hasBadge", "dst": "badge:sensitive"}, [f"<{FRAME}> <{MK}badge> <{MK}badge/sensitive> ."]),
    ({"src": FRAME, "type": "nope", "dst": PARENT}, []),
    # 5.0.1: coOccursWith never reaches RDF, whatever the pair
    ({"src": "type:streamer", "type": "coOccursWith", "dst": "type:creator"}, []),
    ({"src": "tag:meme", "type": "coOccursWith", "dst": "tag:dank-meme"}, []),
    ({"src": "type:streamer", "type": "coOccursWith", "dst": "tag:dank-meme"}, []),
])
def test_edge_triples(edge, want):
    assert triples([], [edge]) == want


def test_every_edge_type_has_a_predicate_but_cooccurs_with():
    assert "coOccursWith" not in rdf.EDGE_PREDICATES
    assert set(build.EDGE_TYPES) | set(taxonomy.CONCEPT_EDGE_TYPES) | set(siblings.SIBLING_EDGE_TYPES) <= set(rdf.EDGE_PREDICATES)


# -- occurrences: RDF-star annotations on the quoted frame-level edge -------------------------

def test_each_distinct_value_annotates_the_quoted_edge():
    got = triples([], [{"src": FRAME, "type": "citesExternal", "dst": EXTERNAL, "occurrences": [
        {"anchor_text": "wiki", "in_section": "About"}, {"anchor_text": "wiki", "in_section": "Spread"},
        {"citation_text": 'The "Doge" article', "citation_index": 3}]}])
    q = f"<< <{FRAME}> <{MK}citesExternal> <{EXTERNAL}> >>"
    assert got[0] == f"<{FRAME}> <{MK}citesExternal> <{EXTERNAL}> ."
    assert sorted(got[1:]) == sorted([f'{q} <{MK}anchorText> "wiki" .', f'{q} <{MK}inSection> "About" .',
                                      f'{q} <{MK}inSection> "Spread" .',
                                      f'{q} <{MK}citationText> "The \\"Doge\\" article" .',
                                      f'{q} <{MK}citationIndex> "3"^^<{XSD}integer> .'])
    assert ntdiff.predicate_of(ntdiff.normalize_line(got[1])) in {"anchorText", "inSection", "citationText",
                                                                  "citationIndex"}


@pytest.mark.parametrize("occurrences, n", [
    ([{"anchor_text": "x"}, {"anchor_text": "x"}], 2),       # repeats go even with dedupe off
    ([{"anchor_text": "", "nonsense": "x", "citation_index": None}], 1),
])
def test_repeats_empty_values_and_unknown_fields(occurrences, n):
    assert len(triples([], [{"src": FRAME, "type": "citesMediaFrame", "dst": PARENT, "occurrences": occurrences}],
                       dedupe=False)) == n


def test_citation_index_zero_is_a_value_and_image_annotations_point_at_the_file():
    assert any('"0"^^' in t for t in triples([], [{"src": FRAME, "type": "citesExternal", "dst": EXTERNAL,
                                                    "occurrences": [{"citation_index": 0}]}]))
    got = triples([], [{"src": FRAME, "type": "hasImage", "dst": f"image:{IMG}",
                        "occurrences": [{"role": "section", "caption": "wow"}]}])
    q = f"<< <{FRAME}> <{MK}hasImage> <{IMG}> >>"
    assert {f'{q} <{MK}imageRole> "section" .', f'{q} <{MK}caption> "wow" .'} <= set(got)
    line = triples([], [{"src": FRAME, "type": "citesExternal", "dst": EXTERNAL,
                         "occurrences": [{"anchor_text": "wiki"}]}])[1]
    assert ntdiff.predicate_of(ntdiff.normalize_line(line)) == "anchorText"


# -- events (6.0.0: the first minted content IRI) ----------------------------------------------

def event_lines(**over):
    node = {"id": EID, "kind": "event", "source_text": "Posted in 2010.", "source_section": "origin", "date": "2010",
            "date_precision": "year", "date_text": "2010", "date_start": "2010-01-01T00:00:00Z",
            "date_end": "2010-12-31T23:59:59Z", "locations": ["Tumblr"], "location_type": "platform",
            "certainty": "unconfirmed", "actors": ["Atsuko Sato", "u/k"], "extraction_model": "ministral-3:14b",
            "extraction_version": "1.0.0", **over}
    return triples([frame(), node], [{"src": FRAME, "type": "hasEvent", "dst": EID}])


def test_an_event_is_an_mk_event_with_its_interval_hedging_and_provenance():
    out = event_lines()
    assert {f"<{EIRI}> <{rdf.RDF_TYPE}> <{MK}Event> .", f"<{FRAME}> <{MK}hasEvent> <{EIRI}> .",
            f'<{EIRI}> <{MK}eventStart> "2010-01-01T00:00:00Z"^^<{rdf.XSD_DATETIME}> .',
            f'<{EIRI}> <{MK}eventEnd> "2010-12-31T23:59:59Z"^^<{rdf.XSD_DATETIME}> .',
            f'<{EIRI}> <{MK}certainty> "unconfirmed" .', f'<{EIRI}> <{MK}extractionModel> "ministral-3:14b" .',
            f'<{EIRI}> <{MK}sourceText> "Posted in 2010." .'} <= set(out)
    assert not [t for t in out if "semanticweb.cs.vu.nl" in t]          # aligned to sem:Event, never emitted
    assert len([t for t in out if f"<{MK}eventActor>" in t]) == 2
    assert not [t for t in out if '"2010" .' in t and "dateText" not in t]   # the bare date: no triple
    undated = event_lines(date=None, date_precision="none", date_start=None, date_end=None, date_text=None)
    assert not [t for t in undated if "eventStart" in t or "eventEnd" in t]
    assert f'<{EIRI}> <{MK}datePrecision> "none" .' in undated


def test_attached_media_hang_off_the_event():
    img = "https://i.kym-cdn.com/photos/images/original/000/1.jpg"
    out = triples([frame()], [{"src": EID, "type": "eventImage", "dst": "image:" + img},
                              {"src": EID, "type": "eventLink", "dst": EXTERNAL}])
    assert {f"<{EIRI}> <{MK}eventImage> <{img}> .", f"<{EIRI}> <{MK}eventLink> <{EXTERNAL}> ."} <= set(out)


# -- Wikidata items (6.1.0: somebody else's resource) ----------------------------------------

def test_a_wikidata_item_gets_its_label_only_and_imkgs_predicates():
    item = {"id": "wd:Q39315", "kind": "wikidata_entity", "qid": "Q39315", "label": "Shiba Inu", "description": "dog breed"}
    occ = {"mention_text": "Shiba Inus", "link_score": 0.83, "link_method": "ner", "ner_label": "ORG"}
    out = triples([frame(), item], [{"src": FRAME, "type": t, "dst": "wd:Q39315", "occurrences": [occ]}
                                    for t in ("fromAbout", "fromTags", "fromTitle")])
    assert [t for t in out if t.startswith(f"<{WIRI}>")] == [f'<{WIRI}> <{rdf.RDFS}label> "Shiba Inu" .']  # no class
    q = f"<< <{FRAME}> <{M4S}fromAbout> <{WIRI}> >>"
    assert {f"<{FRAME}> <{M4S}fromAbout> <{WIRI}> .", f"<{FRAME}> <{M4S}fromTags> <{WIRI}> .",
            f"<{FRAME}> <{MK}fromTitle> <{WIRI}> .", f'{q} <{MK}mentionText> "Shiba Inus" .',
            f'{q} <{MK}linkScore> "0.83"^^<{rdf.XSD_DECIMAL}> .', f'{q} <{MK}linkMethod> "ner" .',
            f'{q} <{MK}nerLabel> "ORG" .'} <= set(out)
    assert not [t for t in out if "dog breed" in t]          # the description stays in the property graph


# -- set semantics, provenance, the file ----------------------------------------------------------

def test_rdf_is_a_set_and_the_edge_list_a_bag():
    tag = {"src": FRAME, "type": "hasTag", "dst": "tag:doge"}
    assert len(triples([], [tag, dict(tag)])) == 1
    assert len(triples([], [tag, dict(tag)], dedupe=False)) == 2
    assert len(triples([frame(), frame()], [])) == 5
    assert len(triples([frame(), frame(nid=PARENT, label="Doge")], [])) == 10      # per triple, not per literal


def test_stamps_become_triples_about_the_build():
    got = triples([], [], stamps={"build_id": "kg_1", "snapshot_at": "t", "kg_build_version": "2.0.0",
                                  "taxonomy_version": "abc"})
    assert len(got) == 4 and all("currentBuild" in t for t in got)
    assert triples([], [], stamps={}) == []


def test_write_nt_counts_hashes_and_is_byte_stable(tmp_path):
    got = rdf.write_nt([frame()], [], str(tmp_path / "a.nt"))
    body = (tmp_path / "a.nt").read_bytes()
    assert (got["triples"], got["sha256"], body.decode().count("\n")) == (5, hashlib.sha256(body).hexdigest(), 5)
    assert rdf.write_nt([frame()], [], str(tmp_path / "b.nt"))["sha256"] == got["sha256"]
