"""kg/taxonomy.py: the curated entry_type taxonomy. Two tests pin findings and
are not redundant: the consistency guard (model -> influencer was asserted in
the graph while the record listed it as contested; it was sampled and promoted
since, and the guard stops the two disagreeing again), and the untruncated
rationales (flow-style YAML split a value at a comma inside parentheses; three
rationales were cut mid-sentence)."""
import textwrap
from dataclasses import replace

import pytest
import yaml

from helpers import KG_CONFIG
from modules.kg import taxonomy as tx

CURATED = str(KG_CONFIG / "entry_type_taxonomy.yaml")
# The two encodable buckets of the reviewed record, the only source of truth:
# ("model", "influencer") is here because the curator resolved it so.
EXPECTED_EDGES = {
    ("streamer", "creator"), ("fan-art", "fan-labor"), ("vlogger", "creator"), ("generator", "application"),
    ("ai-influencer", "influencer"), ("company", "organization"), ("song", "music"), ("album", "music"),
    ("flash-mob", "performance"), ("blockchain", "technology"), ("snowclone", "catchphrase"),
    ("creepypasta", "copypasta"), ("model", "influencer")}


@pytest.fixture(scope="module")
def tax():
    return tx.load(CURATED)


@pytest.fixture(scope="module")
def full_census(tax):
    return {"value_counts": {s: 1 for s in tax.slugs()}}


# -- the curated file ----------------------------------------------------------------

def test_the_curated_file_encodes_exactly_the_reviewed_edges(tax):
    assert {e.pair for e in tax.edges} == EXPECTED_EDGES
    pairs = {tuple(sorted(e.pair)) for e in tax.edges}
    assert ("influencer", "model") in pairs                               # promoted after sampling
    assert not {("controversy", "viral-debate"), ("animal", "fauna")} & pairs   # still contested


def test_every_bucket_is_parsed_and_withheld_pairs_are_carried(tax):
    assert set(tax.bucket_counts) == set(tx.BUCKETS)
    assert (tax.bucket_counts["broader_confirmed"], tax.bucket_counts["contested"],
            tax.bucket_counts["demoted"]) == (5, 2, 3)
    assert len(tax.withheld) == 10            # contested 2 + demoted 3 + do_not_encode 5: never in the graph


def test_rationales_and_numeric_evidence_survive(tax):
    for e in tax.edges:
        text = e.rationale or ""
        assert text.count("(") == text.count(")"), f"unbalanced parens in {e.pair}: {text!r}"
    snowclone = next(e for e in tax.edges if e.narrower == "snowclone")
    assert (snowclone.cooccur, snowclone.containment, snowclone.pmi_bits) == (181, 0.26, 1.0)


def test_qualifiers_umbrellas_notes_and_version(tax):
    assert tax.qualifiers == ("ai-generated", "historical-figure", "shock-media")
    assert not set(tax.qualifiers) & {e.narrower for e in tax.edges}      # crosscutting: no parent
    assert (len(tax.missing_umbrellas), len(tax.notes)) == (3, 1)          # a note is not an umbrella
    assert len(tax.version) == 64 and tax.version == tx.load(CURATED).version   # content-addressed


def test_concept_edges(tax):
    edges = tax.concept_edges()
    # subTypeOf, not "broader": IMKG's skos:broader is a frame series
    assert {e["type"] for e in edges} == set(tx.CONCEPT_EDGE_TYPES) == {"subTypeOf"}
    assert all(e["src"].startswith("type:") and e["dst"].startswith("type:") for e in edges)
    assert {"src": "type:streamer", "dst": "type:creator", "type": "subTypeOf"} in edges   # narrower -> broader


def test_the_prefix_parameter_keeps_the_default(tax, full_census):
    # 5.0.0: kg/origin.py reuses these for its own concept namespace
    assert tax.concept_edges() == tax.concept_edges(prefix="type:")
    edges = tax.concept_edges(prefix="origin:")
    assert edges and all(e["src"].startswith("origin:") and e["dst"].startswith("origin:") for e in edges)
    assert tx.encodable_edges(tax, full_census) == tx.encodable_edges(tax, full_census, prefix="type:")
    assert all(e["src"].startswith("origin:") for e in tx.encodable_edges(tax, full_census, prefix="origin:"))


def test_parse_from_a_dict(tax):
    # the file-free half of load(), reused by kg/origin.py
    parsed = tx._parse({"broader_confirmed": [{"broader": "creator", "narrower": "streamer", "rationale": "x"}]},
                       version="v1", path="<memory>")
    assert (parsed.version, {e.pair for e in parsed.edges}) == ("v1", {("streamer", "creator")})
    with pytest.raises(tx.TaxonomyError):                                  # still checks consistency
        tx._parse({"broader_confirmed": [{"broader": "b", "narrower": "n", "rationale": "x"}],
                   "contested": [{"pair": ["b", "n"]}]}, version="v1", path="<memory>")
    with open(CURATED, "rb") as fh:
        doc = yaml.safe_load(fh.read().decode("utf-8"))
    assert tx._parse(doc, version=tax.version, path=CURATED).edges == tax.edges


# -- the guards that make the shipped bug unrepresentable --------------------------------

@pytest.mark.parametrize("edges, withheld, message", [
    (lambda t: t.edges + (tx.BroaderEdge("viral-debate", "controversy", "broader_semantic_only"),), None, "contested"),
    # contested lists [controversy, viral-debate]: the other order is caught too
    (lambda t: (tx.BroaderEdge("controversy", "viral-debate", "broader_confirmed"),), None, None),
    # three nodes: a 2-cycle would trip the duplicate guard before the cycle check
    (lambda t: (tx.BroaderEdge("a", "b", "broader_confirmed"), tx.BroaderEdge("b", "c", "broader_confirmed"),
                tx.BroaderEdge("c", "a", "broader_confirmed")), (), "cycle"),
    (lambda t: (tx.BroaderEdge("x", "y", "broader_confirmed"), tx.BroaderEdge("x", "y", "broader_semantic_only")),
     (), None),
], ids=["a withheld pair", "order-insensitive", "a cycle", "a duplicate edge"])
def test_an_inconsistent_taxonomy_is_rejected(tax, edges, withheld, message):
    bad = replace(tax, edges=edges(tax), **({} if withheld is None else {"withheld": withheld}))
    with pytest.raises(tx.TaxonomyError, match=message):
        tx.check_consistency(bad)


@pytest.mark.parametrize("body, message", [
    # the original file's failure: an empty taxonomy would silently drop the concept layer
    ("broader_confirmed:\n  - {broader: a, narrower: b, note: is this a key? no}\n", None),
    ("broader_confirmd:\n  - broader: creator\n    narrower: streamer\n", "unknown bucket"),
    ("broader_confirmed:\n  - broader: creator\n    narrower: creator\n", None),           # a self edge
    ('broader_confirmed:\n  - evidence: agree\n    rationale: "no slugs here"\n', None),     # no pair
])
def test_a_malformed_file_raises(tmp_path, body, message):
    path = tmp_path / "tax.yaml"
    path.write_text(textwrap.dedent(body))
    with pytest.raises(tx.TaxonomyError, match=message):
        tx.load(str(path))


# -- against the census ---------------------------------------------------------------------

def test_validation_returns_missing_slugs_as_data(tax, full_census):
    report = tx.validate(tax, {"value_counts": {"streamer": 5, "creator": 9}})
    assert report["edges_encoded"] == 1 and "snowclone" in report["slugs_missing_from_census"]
    assert report["edges_dropped"][0].keys() == {"narrower", "broader", "bucket", "missing"}
    report = tx.validate(tax, full_census)
    assert (report["slugs_missing_from_census"], report["edges_encoded"], report["edges_dropped"]) == ([], 13, [])
    assert tx.encodable_edges(tax, {"value_counts": {"streamer": 5, "creator": 9}}) == \
        [{"src": "type:streamer", "dst": "type:creator", "type": "subTypeOf"}]
    s = tax.summary()
    assert (s["broader_edges"], s["withheld_pairs"], s["taxonomy_version"]) == (13, 10, tax.version)
