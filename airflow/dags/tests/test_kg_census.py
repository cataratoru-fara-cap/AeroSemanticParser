"""kg/census.py: one census, several corpus fields. The contract worth pinning
is the key set: this replaced two near-identical scripts emitting parallel JSON
under different names (type_counts vs top_tag_counts), so the tag census had
no consumers at all. Equivalence with both originals was verified on the live
corpus (23,882 entries) at migration; these pin what that verified."""
import json
import random

import pytest

from modules.kg.census import CENSUS_VERSION, FIELDS, load_census, run_census

DOCS = [{"entry_type": ["meme", "exploitable"], "tags": ["Doge", " doge ", "shiba"]},
        {"entry_type": ["meme"], "tags": ["shiba", "dog"]},
        {"entry_type": [], "tags": []},
        {"entry_type": ["meme", "exploitable"], "tags": ["doge", "dog"]}]
ORIGIN_DOCS = [{"origin": "Twitter"}, {"origin": "Twitter"}, {"origin": "4chan"}, {"origin": ""}, {}]


def test_one_shape_for_every_field():
    a, b = run_census(DOCS, "entry_type"), run_census(DOCS, "tags")
    assert set(a) == set(b)
    assert (a["field"], b["field"], b["census_version"]) == ("entry_type", "tags", CENSUS_VERSION)
    with pytest.raises(ValueError, match="entry_type"):            # names the known ones
        run_census(DOCS, "nope")
    assert run_census(iter(DOCS), "entry_type")["corpus_size"] == 4   # a cursor is traversed once


def test_counting():
    et = run_census(DOCS, "entry_type", min_pair_count=1)
    assert (et["corpus_size"], et["entries_with_value"], et["distinct_values"]) == (4, 3, 2)
    assert et["value_counts"] == {"meme": 3, "exploitable": 2}
    assert et["values_per_entry_distribution"] == {0: 1, 1: 1, 2: 2}          # empty entries: the zero bucket
    assert et["pair_cooccurrence"] == [{"a": "exploitable", "b": "meme", "count": 2}]
    strict = run_census(DOCS, "entry_type", min_pair_count=3)              # filtered, but still reported
    assert (strict["pair_cooccurrence"], strict["total_pairs_seen"], strict["total_pairs_returned"]) == ([], 1, 0)


@pytest.mark.parametrize("docs, field", [
    # the original sorted on count alone: tie order was dict iteration
    ([{"entry_type": ["a", "b"]}, {"entry_type": ["c", "d"]}], "entry_type"),
    (DOCS, "tags"),
])
def test_the_result_does_not_depend_on_document_order(docs, field):
    shuffled = list(docs)
    random.Random(7).shuffle(shuffled)
    a = run_census(docs, field, min_pair_count=1)
    for other in (list(reversed(docs)), shuffled):
        b = run_census(other, field, min_pair_count=1)
        assert (a["pair_cooccurrence"], a["value_counts"]) == (b["pair_cooccurrence"], b["value_counts"])


def test_normalisation_tags_vs_entry_types():
    tg = run_census(DOCS, "tags", min_pair_count=1)
    assert "doge" in tg["value_counts"] and not {"Doge", " doge "} & set(tg["value_counts"])
    assert tg["value_counts"]["doge"] == 2                                 # "Doge" and " doge " count once
    # a controlled vocabulary: casing is meaningful
    assert set(run_census([{"entry_type": ["Meme", "meme"]}], "entry_type", min_pair_count=1)["value_counts"]) == {"Meme", "meme"}
    assert run_census([{"tags": ["  ", "", "real"]}], "tags")["value_counts"] == {"real": 1}


def test_normalize_override():
    # kg/cooccurs.py aligns the census with build.py's folded tag_concept identity
    docs = [{"tags": ["Catchphrases"]}]
    assert "catchphrases" in run_census(docs, "tags")["value_counts"]
    assert "catchphrase" in run_census(docs, "tags", normalize=lambda v: v.strip().lower()[:-1])["value_counts"]
    assert "Doge" in run_census([{"tags": ["Doge"]}], "tags", normalize=None)["value_counts"]


def test_top_k():
    tk = run_census(DOCS, "tags", top_k=1, min_pair_count=1)
    assert (len(tk["value_counts"]), tk["pair_cooccurrence"], tk["top_k_used_for_cooccurrence"]) == (1, [], 1)
    assert run_census(DOCS, "tags", top_k=1)["distinct_values"] == 3        # frequency stays exhaustive
    assert (FIELDS["entry_type"].default_top_k, FIELDS["tags"].default_top_k) == (0, 1000)
    assert run_census(DOCS, "tags")["top_k_used_for_cooccurrence"] == 1000


def test_a_single_valued_field():
    # origin (5.0.0): set("Twitter") would be 7 characters
    oc = run_census(ORIGIN_DOCS, "origin")
    assert oc["value_counts"] == {"Twitter": 2, "4chan": 1}
    assert (oc["entries_with_value"], oc["values_per_entry_distribution"]) == (3, {0: 2, 1: 3})
    oc = run_census(ORIGIN_DOCS, "origin", min_pair_count=0)       # a single value cannot co-occur with itself
    assert (oc["pair_cooccurrence"], oc["total_pairs_seen"]) == ([], 0)
    assert not FIELDS["origin"].multi_valued and FIELDS["origin"].normalize is None


@pytest.mark.parametrize("legacy, want", [
    ({"corpus_size": 22915, "entries_with_entry_type": 18000, "distinct_types": 119, "type_counts": {"meme": 10},
      "types_per_entry_distribution": {"1": 5}}, ({"meme": 10}, 18000, 119, "entry_type")),
    ({"corpus_size": 23882, "entries_with_tags": 20000, "distinct_tags_total": 102583, "top_tag_counts": {"doge": 7},
      "top_k_used_for_cooccurrence": 300}, ({"doge": 7}, 20000, 102583, "tags")),
])
def test_legacy_files_load_in_the_current_shape(tmp_path, legacy, want):
    # keeps the already-computed definitions and embeddings usable
    path = tmp_path / "census.json"
    path.write_text(json.dumps(legacy))
    got = load_census(str(path))
    assert (got["value_counts"], got["entries_with_value"], got["distinct_values"], got["field"]) == want


def test_the_current_shape_passes_through(tmp_path):
    current = run_census(DOCS, "tags")
    path = tmp_path / "census.json"
    path.write_text(json.dumps(current))
    assert load_census(str(path)) == current
