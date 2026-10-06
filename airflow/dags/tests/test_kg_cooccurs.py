"""kg/cooccurs.py: coOccursWith edges from a census. Apart from kg/taxonomy.py on
purpose: subTypeOf is curator judgement, coOccursWith a threshold applied to a
statistic. The RDF scope restriction (tag_concept has no RDF resource) is
applied downstream in kg/rdf.py and kg/serialize.py, not here."""
import pytest

from modules.kg import cooccurs


def census(*pairs):
    return {"pair_cooccurrence": [{"a": a, "b": b, "count": c} for a, b, c in pairs]}


def edge(a, b, prefix="type:"):
    return {"src": prefix + a, "dst": prefix + b, "type": "coOccursWith"}


@pytest.mark.parametrize("c, prefix, kw, want", [
    (census(("creator", "streamer", 23)), "type:", {}, [edge("creator", "streamer")]),       # canonical order kept
    (census(("dog", "shiba-inu", 5)), "tag:", {}, [edge("dog", "shiba-inu", "tag:")]),     # both endpoints prefixed
    (census(("a", "b", 10), ("c", "d", 2)), "type:", {"min_count": 5}, [edge("a", "b")]),  # stricter than the census
    (census(("a", "b", 10), ("c", "d", 2)), "type:", {}, [edge("a", "b"), edge("c", "d")]),  # None keeps everything
    (census(("a", "b", 5), ("b", "c", 5), ("a", "c", 5)), "type:", {},
     [edge("a", "b"), edge("b", "c"), edge("a", "c")]),
    (census(), "type:", {}, []),
    ({}, "origin:", {}, []),                # origin's census never has real pairs (single-valued): must not raise
])
def test_edges_from_census(c, prefix, kw, want):
    assert cooccurs.edges_from_census(c, prefix, **kw) == want


def test_the_edge_type():
    assert (cooccurs.COOCCURS_EDGE_TYPES, cooccurs.COOCCURS_EDGE_TYPE) == (("coOccursWith",), "coOccursWith")
