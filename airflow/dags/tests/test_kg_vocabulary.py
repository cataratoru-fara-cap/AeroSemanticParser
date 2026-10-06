"""The vocabulary invariant: one graph, two derivations, one declared vocabulary.

kg/rdf.py produces the RDF and kg_config/kg_mapping.yarrrml.yml re-derives it
(morph-kgc, kym_kg_validate). The diff gate compares their OUTPUT on the data a
build contains; these tests compare the DEFINITIONS, at full-IRI level (local
names collide across m4s:/mk:/skos:), so a term added on one side fails here
before any build exercises it: predicates, constant classes and class
templates, the CSVs read and every $(column), RDF-star annotations quoting an
edge over the same source, every mk: term declared in memeatlas.ttl and none
dead, and IMKG's own terms used, never re-declared under mk:.
"""
import re
from pathlib import Path

import pytest
import yaml

from helpers import KG_CONFIG
from modules.kg import build, cooccurs, rdf, serialize, siblings, taxonomy

MAPPING = KG_CONFIG / "kg_mapping.yarrrml.yml"       # yatter refuses any extension but .yml/.yaml
ONTOLOGY = KG_CONFIG / "memeatlas.ttl"
MK, M4S = rdf.PREFIXES["mk"], rdf.PREFIXES["m4s"]
_TEMPLATE = re.compile(r"\$\(([^)]+)\)")


def expand(term, prefixes):
    term = term.split("~", 1)[0]
    if term.startswith(("http://", "https://")):
        return term
    prefix, _, local = term.partition(":")
    return prefixes[prefix] + local


def source_file(rule):
    [source] = rule["sources"]
    return Path(source[0].split("~", 1)[0]).name


def declared_mk_terms():
    """Subjects of `mk:Term a ...` lines in memeatlas.ttl."""
    return {MK + m.group(1) for m in re.finditer(r"^mk:([A-Za-z][A-Za-z0-9]*)\s+a\s",
                                                 ONTOLOGY.read_text(encoding="utf-8"), re.MULTILINE)}


def emitted():
    return rdf.all_predicates(include_provenance=True) | rdf.constant_classes()


@pytest.fixture(scope="module")
def mapping():
    doc = yaml.safe_load(MAPPING.read_text(encoding="utf-8"))
    prefixes = {**rdf.PREFIXES, **doc["prefixes"]}
    m = {"doc": doc, "prefixes": prefixes, "predicates": set(), "constant_classes": set(), "class_templates": set(),
         "predicate_templates": set()}
    for rule in doc["mappings"].values():
        for pred, obj, *_ in rule.get("po") or []:
            if pred == "a":
                if "$(" in obj:
                    m["class_templates"].add(obj.split("$(", 1)[0])
                else:
                    m["constant_classes"].add(expand(obj, prefixes))
            elif "$(" in pred:
                m["predicate_templates"].add(expand(pred, prefixes).split("$(", 1)[0])
            else:
                m["predicates"].add(expand(pred, prefixes))
    return m


# -- the mapping and rdf.py define the same graph ------------------------------------------

def test_the_mapping_and_rdf_py_agree_on_prefixes_predicates_and_classes(mapping):
    assert {p: rdf.PREFIXES.get(p) for p in mapping["doc"]["prefixes"]} == mapping["doc"]["prefixes"]
    ours = rdf.all_predicates(include_provenance=False)
    assert mapping["predicates"] - ours == set(), "mapping predicates rdf.py never emits"
    assert ours - mapping["predicates"] == set(), "rdf.py predicates the mapping never emits"
    # 7.1.0: wdt:$(property), the linked items' statements, is the only data-driven predicate
    assert mapping["predicate_templates"] == {rdf.WDT}
    assert mapping["constant_classes"] == rdf.constant_classes()
    assert mapping["class_templates"] == {rdf.KYM_CLASS_BASE, rdf.TYPES_BASE}


def csv_headers():
    headers = {name: tuple(c for c, _ in cols) for name, (_, cols) in serialize.RML_NODE_FILES.items()}
    headers.update({name: ("url", col) for name, (_, col) in serialize.RML_LIST_FILES.items()})
    # event and template list files are keyed by their own IRI, not a frame url
    headers.update({name: ("iri", col) for name, (_, col) in serialize.RML_EVENT_LIST_FILES.items()})
    headers.update({name: ("iri", col) for name, (_, col) in serialize.RML_TEMPLATE_LIST_FILES.items()})
    headers.update(serialize.RML_CONCEPT_FILES)
    headers.update(dict(serialize.EDGE_TYPE_TO_RML_FILE.values()))
    headers.update(dict(serialize.OCCURRENCE_RML_FILES.values()))
    headers.update(dict([serialize.ORIGIN_SUBTYPE_RML_FILE, serialize.FRAME_IMAGE_RML_FILE,
                         serialize.FRAME_IMAGE_OCCURRENCE_RML_FILE, serialize.STATEMENTS_RML_FILE]))
    return headers


def test_the_mapping_reads_exactly_the_files_serialize_writes_and_only_their_columns(mapping):
    rules = mapping["doc"]["mappings"]
    assert {source_file(rule) for rule in rules.values()} == serialize.all_rml_files()
    headers = csv_headers()
    for name, rule in rules.items():
        header = headers[source_file(rule)]
        subjects = rule["subjects"] if isinstance(rule["subjects"], str) else ""
        for text in [subjects] + [str(x) for po in rule.get("po") or [] for x in po]:
            for column in _TEMPLATE.findall(text):
                assert column in header, f"{name}: $({column})"


def test_no_csv_column_collides_with_morph_kgc_internals():
    columns = {c for _, cols in serialize.RML_NODE_FILES.values() for c, _ in cols}
    columns |= {c for files in (serialize.RML_LIST_FILES, serialize.RML_EVENT_LIST_FILES,
                                serialize.RML_TEMPLATE_LIST_FILES) for _, c in files.values()}
    columns |= {c for cols in serialize.RML_CONCEPT_FILES.values() for c in cols}
    columns |= {c for files in (serialize.EDGE_TYPE_TO_RML_FILE, serialize.OCCURRENCE_RML_FILES)
                for _, cols in files.values() for c in cols}
    assert columns & serialize.RESERVED_COLUMNS == set()


def test_annotation_mappings_quote_an_edge_over_the_same_source(mapping):
    rules, quoted = mapping["doc"]["mappings"], set()
    occurrence_predicates = {p for p, _ in rdf.OCCURRENCE_PREDICATES.values()}
    for name, rule in rules.items():
        if isinstance(rule["subjects"], str):
            continue
        [subject] = rule["subjects"]
        [target] = subject.values()
        assert target in rules, f"{name} quotes unknown mapping {target}"
        assert source_file(rule) == source_file(rules[target]), name    # morph-kgc needs no join
        [(pred, _obj)] = rules[target]["po"]
        quoted.add(expand(pred, mapping["prefixes"]))
        assert all(expand(po[0], mapping["prefixes"]) in occurrence_predicates for po in rule["po"]), name
    assert quoted == {rdf.EDGE_PREDICATES[t][0] for t in build.OCCURRENCE_EDGE_TYPES}


def test_edge_types_occurrence_fields_and_their_rdf():
    assert set(rdf.OCCURRENCE_PREDICATES) == set(build.OCCURRENCE_FIELDS)
    # coOccursWith excluded (5.0.1): tags are its only source and have no RDF resource
    ours = set(build.EDGE_TYPES) | set(taxonomy.CONCEPT_EDGE_TYPES) | set(siblings.SIBLING_EDGE_TYPES)
    assert set(serialize.EDGE_TYPE_TO_RML_FILE) == ours == set(rdf.EDGE_PREDICATES)
    groups = [set(build.EDGE_TYPES), set(taxonomy.CONCEPT_EDGE_TYPES), set(cooccurs.COOCCURS_EDGE_TYPES),
              set(siblings.SIBLING_EDGE_TYPES)]
    assert all(not a & b for i, a in enumerate(groups) for b in groups[i + 1:])


def test_events_align_to_sem_without_emitting_it(mapping):
    """mk:Event is a subclass of sem:Event (EventKG's model): a SEM-aware
    consumer gets one entailment step, but no sem: term is ever EMITTED (as
    with schema:, dct:, prov:). Promoting it would add a prefix, a class and
    ~120k triples asserting classes MemeAtlas does not own: deliberate only."""
    ttl = ONTOLOGY.read_text(encoding="utf-8")
    assert "@prefix sem:" in ttl
    assert all(a in ttl for a in ("sem:Event", "sem:hasBeginTimeStamp", "sem:hasEndTimeStamp", "sem:hasActor"))
    assert "sem" not in rdf.PREFIXES and "sem" not in mapping["doc"]["prefixes"]
    assert not {t for t in emitted() if t.startswith("http://semanticweb.cs.vu.nl/2009/11/sem/")}


def test_wikidata_items_are_objects_never_classes_or_predicates(mapping):
    # 6.1.0: a linked item is Wikidata's resource: pointed at and labelled, never typed
    wd = rdf.PREFIXES["wd"]
    assert wd == "http://www.wikidata.org/entity/" and not {t for t in emitted() if t.startswith(wd)}
    assert rdf.NODE_CLASSES["wikidata_entity"] == () and rdf.node_iri("wd:Q42") == wd + "Q42"
    terms = {"fromTitle": "mk:fromTitle", "fromTags": "m4s:fromTags", "fromAbout": "m4s:fromAbout"}
    for etype in build.ENTITY_FIELD_EDGES.values():
        rule = next(r for r in mapping["doc"]["mappings"].values() if r.get("po") and r["po"][0][0] == terms[etype])
        assert rule["po"][0][1].startswith(wd), etype


# -- IMKG's own terms ------------------------------------------------------------------------

def test_imkg_terms_are_used():
    # origin/spread (5.1.0): IMKG keeps narrative sections as literals; fromAbout/
    # fromTags (6.1.0) and templateOf/fromImage (6.4.0) reused verbatim
    preds = rdf.all_predicates()
    for local in ("title", "status", "year", "from", "about", "origin", "spread", "added", "last_update_source",
                  "tag", "fromAbout", "fromTags", "templateOf", "fromImage"):
        assert M4S + local in preds, local
    assert M4S + "MediaFrame" in rdf.constant_classes()
    imkg = {"MediaFrame", "title", "status", "year", "from", "about", "added", "last_update_source", "tag", "origin",
            "spread", "fromAbout", "fromTags", "fromImage", "fromCaption", "templateOf"}
    assert {t[len(MK):] for t in declared_mk_terms()} & imkg == set()      # never shadowed under mk:


def test_series_siblings_and_templates_use_imkgs_terms():
    skos, see_also = rdf.PREFIXES["skos"], rdf.PREFIXES["rdfs"] + "seeAlso"
    assert rdf.EDGE_PREDICATES["partOfSeries"] == (skos + "broader", False, skos + "narrower")
    assert "skos" not in rdf.EDGE_PREDICATES["subTypeOf"][0]
    # 6.6.0: IMKG's sibling triples are rdfs:seeAlso, both ways; no mk: term an
    # IMKG query would not know, and the only edge type that emits it
    assert rdf.EDGE_PREDICATES["sharesSameSeries"] == (see_also, False, see_also)
    assert MK + "sharesSameSeries" not in declared_mk_terms()
    assert {t for t, (p, _, inv) in rdf.EDGE_PREDICATES.items() if see_also in (p, inv)} == {"sharesSameSeries"}
    # 6.4.0: one edge, both RDF directions: m4s:templateOf and mk:hasTemplate
    assert rdf.EDGE_PREDICATES["hasTemplate"] == (MK + "hasTemplate", False, M4S + "templateOf")
    assert rdf.EDGE_PREDICATES["fromImage"][0] == M4S + "fromImage"
    assert rdf.node_iri("template:112126428") == MK + "template/112126428"


# -- the ontology declares exactly what is emitted -----------------------------------------------

def test_every_emitted_mk_term_is_declared_and_none_is_dead():
    declared = declared_mk_terms()
    emitted_mk = {t for t in emitted() if t.startswith(MK)}
    assert ONTOLOGY.is_file() and declared
    assert emitted_mk - declared == set()
    # each scheme is emitted as a resource, not a predicate or a class
    assert declared - emitted_mk - {rdf.SCHEME_IRI, rdf.ORIGIN_SCHEME_IRI, rdf.BADGE_SCHEME_IRI} == set()


def test_the_ontology_parses_as_turtle():
    rdflib = pytest.importorskip("rdflib", reason="rdflib not installed; the Fuseki load checks it live")
    g = rdflib.Graph()
    g.parse(str(ONTOLOGY), format="turtle")
    assert len(g) > 0
