"""kg/siblings.py: sharesSameSeries edges from partOfSeries. Two frames are
siblings when they have the same series parent. The property graph keeps each
pair once (src < dst) and RDF states it both ways; the second half is pinned in
test_kg_rdf.py and test_kg_serialize.py."""
import random

import pytest

from modules.kg import siblings

KYM = "https://knowyourmeme.com/memes/"
SERIES, OTHER_SERIES = KYM + "italian-brainrot-ai-italian-animals", KYM + "cheems"
A, B, C, D = (KYM + s for s in ("bombardiro-crocodilo", "brr-brr-patapim", "tralalero-tralala", "tung-tung-tung-sahur"))


def series(child, parent):
    return {"src": child, "type": "partOfSeries", "dst": parent}


def pairs(edges):
    return [(e["src"], e["dst"]) for e in siblings.sibling_edges(edges)]


def test_every_pair_of_a_series_once_in_id_order():
    assert list(siblings.sibling_edges([series(D, SERIES), series(A, SERIES), series(B, SERIES)])) == [
        {"src": A, "dst": B, "type": "sharesSameSeries"}, {"src": A, "dst": D, "type": "sharesSameSeries"},
        {"src": B, "dst": D, "type": "sharesSameSeries"}]
    assert (siblings.SIBLING_EDGE_TYPES, siblings.SIBLING_EDGE_TYPE) == (("sharesSameSeries",), "sharesSameSeries")


def test_n_frames_give_n_choose_2_edges():
    got = pairs([series(f"{KYM}m{i:02d}", SERIES) for i in range(10)])
    assert len(got) == len(set(got)) == 45 and all(a < b for a, b in got)


@pytest.mark.parametrize("edges, want", [
    pytest.param([series(A, SERIES), series(B, SERIES), series(C, OTHER_SERIES)], [(A, B)], id="different-series"),
    pytest.param([series(A, SERIES)], [], id="a-series-of-one"),
    pytest.param([], [], id="nothing"),
    # SERIES is itself part of a series: a sibling only of OTHER_SERIES's children, never of its own
    pytest.param([series(A, SERIES), series(B, SERIES), series(SERIES, OTHER_SERIES), series(C, OTHER_SERIES)],
                 [(A, B), (SERIES, C)], id="the-parent-is-no-sibling-of-its-children"),
    pytest.param([series(A, SERIES), series(B, SERIES), {"src": C, "type": "citesMediaFrame", "dst": SERIES},
                  series(SERIES, SERIES)], [(A, B)], id="other-types-and-self-loops"),
    pytest.param([series(A, SERIES), series(A, SERIES), series(B, SERIES)], [(A, B)], id="a-repeated-edge"),
])
def test_who_is_a_sibling(edges, want):
    assert sorted(pairs(edges)) == want


def test_output_does_not_depend_on_input_order():
    edges = [series(f"{KYM}m{i:02d}", SERIES if i % 2 else OTHER_SERIES) for i in range(12)]
    expected = pairs(edges)
    random.Random(7).shuffle(edges)
    assert pairs(edges) == expected


def test_the_input_is_drained_before_the_first_pair():
    # kym_kg passes a Mongo cursor and writes while iterating: the cursor must be finished first
    read = []

    def cursor():
        for e in (series(A, SERIES), series(B, SERIES), series(C, SERIES)):
            read.append(e["src"])
            yield e

    next(siblings.sibling_edges(cursor()))
    assert read == [A, B, C]
