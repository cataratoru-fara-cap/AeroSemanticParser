"""Tests for kg/census.py — one census implementation, two corpus fields.

The contract worth pinning is the *key set*: this module replaced two
near-identical scripts that emitted structurally parallel JSON under
different names (``type_counts`` vs ``top_tag_counts``,
``entries_with_entry_type`` vs ``entries_with_tags``). Because of that
mismatch the tag census had no consumers at all — semantics.py is written
against the entry_type spellings. ``test_key_sets_are_identical`` is what
stops that divergence coming back.

Equivalence with the two originals was verified against the live corpus
(23,882 entries) at migration time; these tests cover the behaviour that
verification pinned.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_census.py -v
"""
import json
import tempfile
import unittest
from pathlib import Path

from modules.kg.census import CENSUS_VERSION, FIELDS, load_census, run_census

DOCS = [
    {"entry_type": ["meme", "exploitable"], "tags": ["Doge", " doge ", "shiba"]},
    {"entry_type": ["meme"], "tags": ["shiba", "dog"]},
    {"entry_type": [], "tags": []},
    {"entry_type": ["meme", "exploitable"], "tags": ["doge", "dog"]},
]


class SharedShapeTests(unittest.TestCase):
    def test_key_sets_are_identical(self):
        a = run_census(DOCS, "entry_type")
        b = run_census(DOCS, "tags")
        self.assertEqual(set(a), set(b))

    def test_field_is_stamped(self):
        self.assertEqual(run_census(DOCS, "entry_type")["field"], "entry_type")
        self.assertEqual(run_census(DOCS, "tags")["field"], "tags")

    def test_version_is_stamped(self):
        self.assertEqual(run_census(DOCS, "tags")["census_version"],
                         CENSUS_VERSION)

    def test_unknown_field_raises_and_names_the_known_ones(self):
        with self.assertRaises(ValueError) as ctx:
            run_census(DOCS, "nope")
        self.assertIn("entry_type", str(ctx.exception))

    def test_accepts_any_iterable_not_just_a_list(self):
        # A Mongo cursor is a one-shot generator; the library must not
        # assume it can be traversed twice.
        result = run_census(iter(DOCS), "entry_type")
        self.assertEqual(result["corpus_size"], 4)


class CountingTests(unittest.TestCase):
    def setUp(self):
        self.et = run_census(DOCS, "entry_type", min_pair_count=1)

    def test_corpus_and_coverage(self):
        self.assertEqual(self.et["corpus_size"], 4)
        self.assertEqual(self.et["entries_with_value"], 3)

    def test_value_counts(self):
        self.assertEqual(self.et["value_counts"], {"meme": 3, "exploitable": 2})
        self.assertEqual(self.et["distinct_values"], 2)

    def test_empty_entries_land_in_the_zero_bucket(self):
        self.assertEqual(self.et["values_per_entry_distribution"],
                         {0: 1, 1: 1, 2: 2})

    def test_pair_cooccurrence(self):
        self.assertEqual(self.et["pair_cooccurrence"],
                         [{"a": "exploitable", "b": "meme", "count": 2}])

    def test_min_pair_count_filters_but_total_seen_still_reported(self):
        strict = run_census(DOCS, "entry_type", min_pair_count=3)
        self.assertEqual(strict["pair_cooccurrence"], [])
        self.assertEqual(strict["total_pairs_seen"], 1)
        self.assertEqual(strict["total_pairs_returned"], 0)

    def test_pair_order_is_deterministic(self):
        # The original sorted on count alone, leaving tie order to Python's
        # dict iteration — the same corpus could produce two different files.
        docs = [{"entry_type": ["a", "b"]}, {"entry_type": ["c", "d"]}]
        first = run_census(docs, "entry_type", min_pair_count=1)
        second = run_census(list(reversed(docs)), "entry_type", min_pair_count=1)
        self.assertEqual(first["pair_cooccurrence"], second["pair_cooccurrence"])


class NormalizationTests(unittest.TestCase):
    """The one real behavioural difference between the two originals."""

    def test_tags_are_lowercased_and_stripped(self):
        tg = run_census(DOCS, "tags", min_pair_count=1)
        self.assertIn("doge", tg["value_counts"])
        self.assertNotIn("Doge", tg["value_counts"])
        self.assertNotIn(" doge ", tg["value_counts"])

    def test_repeats_within_one_entry_count_once(self):
        # doc 0 carries "Doge" and " doge "; they are the same tag, and that
        # entry must contribute a single occurrence.
        tg = run_census(DOCS, "tags", min_pair_count=1)
        self.assertEqual(tg["value_counts"]["doge"], 2)

    def test_entry_type_is_taken_verbatim(self):
        # A controlled vocabulary: casing is meaningful, not noise.
        docs = [{"entry_type": ["Meme", "meme"]}]
        et = run_census(docs, "entry_type", min_pair_count=1)
        self.assertEqual(set(et["value_counts"]), {"Meme", "meme"})

    def test_blank_values_are_dropped_not_counted(self):
        docs = [{"tags": ["  ", "", "real"]}]
        tg = run_census(docs, "tags")
        self.assertEqual(tg["value_counts"], {"real": 1})


class TopKTests(unittest.TestCase):
    def test_top_k_restricts_cooccurrence_and_reported_counts(self):
        tk = run_census(DOCS, "tags", top_k=1, min_pair_count=1)
        self.assertEqual(len(tk["value_counts"]), 1)
        self.assertEqual(tk["pair_cooccurrence"], [])
        self.assertEqual(tk["top_k_used_for_cooccurrence"], 1)

    def test_distinct_values_stays_uncapped(self):
        # Frequency counting is cheap and must stay exhaustive even when
        # co-occurrence is restricted.
        tk = run_census(DOCS, "tags", top_k=1)
        self.assertEqual(tk["distinct_values"], 3)

    def test_field_defaults_differ_by_vocabulary_size(self):
        self.assertEqual(FIELDS["entry_type"].default_top_k, 0)
        self.assertEqual(FIELDS["tags"].default_top_k, 1000)
        self.assertEqual(run_census(DOCS, "tags")["top_k_used_for_cooccurrence"],
                         1000)


class SingleValuedFieldTests(unittest.TestCase):
    """origin: one string per frame, not a list — census.py's other real
    behavioural difference, added in 5.0.0."""

    ORIGIN_DOCS = [
        {"origin": "Twitter"}, {"origin": "Twitter"}, {"origin": "4chan"},
        {"origin": ""}, {},
    ]

    def test_a_string_field_does_not_explode_into_characters(self):
        # The bug this guards: set("Twitter") == a set of 7 characters.
        oc = run_census(self.ORIGIN_DOCS, "origin")
        self.assertEqual(oc["value_counts"], {"Twitter": 2, "4chan": 1})

    def test_pair_cooccurrence_is_always_empty(self):
        # A single-valued field cannot co-occur with itself within one
        # entry — not a bug, see kg/census.py's module docstring.
        oc = run_census(self.ORIGIN_DOCS, "origin", min_pair_count=0)
        self.assertEqual(oc["pair_cooccurrence"], [])
        self.assertEqual(oc["total_pairs_seen"], 0)

    def test_empty_and_missing_values_are_not_counted(self):
        oc = run_census(self.ORIGIN_DOCS, "origin")
        self.assertEqual(oc["entries_with_value"], 3)
        self.assertEqual(oc["values_per_entry_distribution"], {0: 2, 1: 3})

    def test_origin_is_registered_single_valued_and_unnormalized(self):
        self.assertFalse(FIELDS["origin"].multi_valued)
        self.assertIsNone(FIELDS["origin"].normalize)


class NormalizeOverrideTests(unittest.TestCase):
    """The override used by kg/cooccurs.py's pipeline to align the census's
    value identity with kg/build.py's tag_concept node identity (folded,
    not the curator-facing raw-lowercased default)."""

    def test_override_replaces_the_fields_own_normalize(self):
        docs = [{"tags": ["Catchphrases"]}]
        default = run_census(docs, "tags")
        folded = run_census(docs, "tags", normalize=lambda v: v.strip().lower()[:-1])
        self.assertIn("catchphrases", default["value_counts"])
        self.assertIn("catchphrase", folded["value_counts"])

    def test_normalize_none_disables_normalization_even_for_tags(self):
        docs = [{"tags": ["Doge"]}]
        raw = run_census(docs, "tags", normalize=None)
        self.assertIn("Doge", raw["value_counts"])


class DeterminismTests(unittest.TestCase):
    def test_pair_cooccurrence_is_order_independent(self):
        import random
        shuffled = list(DOCS)
        random.Random(7).shuffle(shuffled)
        a = run_census(DOCS, "tags", min_pair_count=1)
        b = run_census(shuffled, "tags", min_pair_count=1)
        self.assertEqual(a["pair_cooccurrence"], b["pair_cooccurrence"])
        self.assertEqual(a["value_counts"], b["value_counts"])


class LegacyLoaderTests(unittest.TestCase):
    """Keeps the already-computed definitions and embeddings usable."""

    def _write(self, payload: dict) -> str:
        tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        json.dump(payload, tmp)
        tmp.close()
        self.addCleanup(lambda: Path(tmp.name).unlink(missing_ok=True))
        return tmp.name

    def test_legacy_entry_type_shape_is_normalised(self):
        path = self._write({"corpus_size": 22915,
                            "entries_with_entry_type": 18000,
                            "distinct_types": 119,
                            "type_counts": {"meme": 10},
                            "types_per_entry_distribution": {"1": 5}})
        got = load_census(path)
        self.assertEqual(got["value_counts"], {"meme": 10})
        self.assertEqual(got["entries_with_value"], 18000)
        self.assertEqual(got["distinct_values"], 119)
        self.assertEqual(got["field"], "entry_type")

    def test_legacy_tag_shape_is_normalised(self):
        path = self._write({"corpus_size": 23882,
                            "entries_with_tags": 20000,
                            "distinct_tags_total": 102583,
                            "top_tag_counts": {"doge": 7},
                            "top_k_used_for_cooccurrence": 300})
        got = load_census(path)
        self.assertEqual(got["value_counts"], {"doge": 7})
        self.assertEqual(got["entries_with_value"], 20000)
        self.assertEqual(got["distinct_values"], 102583)
        self.assertEqual(got["field"], "tags")

    def test_current_shape_passes_through_untouched(self):
        current = run_census(DOCS, "tags")
        path = self._write(current)
        self.assertEqual(load_census(path), current)


if __name__ == "__main__":
    unittest.main(verbosity=2)
