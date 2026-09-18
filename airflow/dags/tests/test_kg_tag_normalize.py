"""Tests for kg/tag_normalize.py -- plural folding for the tags folksonomy.

The four pairs in ``test_the_four_grounding_pairs_fold_together`` are real
corpus counts (2026-09-17 census): catchphrase/catchphrases 645/632,
exploitable/exploitables 895/798, image macro/image macros 800/514,
meme/memes 1050/631 -- the fragmentation this module exists to fix.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_tag_normalize.py -v
"""
import os
import tempfile
import unittest

from modules.kg import tag_normalize


class FoldTests(unittest.TestCase):
    def test_the_four_grounding_pairs_fold_together(self):
        for singular, plural in (
                ("catchphrase", "catchphrases"),
                ("exploitable", "exploitables"),
                ("image macro", "image macros"),
                ("meme", "memes")):
            self.assertEqual(tag_normalize.fold(singular), singular)
            self.assertEqual(tag_normalize.fold(plural), singular, plural)

    def test_short_tags_are_never_touched(self):
        # len < 4: "gas", "bus" would otherwise become "ga"/"bu".
        for tag in ("gas", "bus", "ai", "a"):
            self.assertEqual(tag_normalize.fold(tag), tag)

    def test_double_s_endings_are_not_stripped_by_the_plain_s_rule(self):
        self.assertEqual(tag_normalize.fold("boss"), "boss")

    def test_sibilant_es_endings_fold_two_characters(self):
        self.assertEqual(tag_normalize.fold("glasses"), "glass")
        self.assertEqual(tag_normalize.fold("boxes"), "box")

    def test_denylisted_tag_is_never_folded(self):
        denylist = frozenset({"news", "star wars"})
        self.assertEqual(tag_normalize.fold("news", denylist), "news")
        self.assertEqual(tag_normalize.fold("star wars", denylist), "star wars")

    def test_denylist_does_not_affect_other_tags(self):
        denylist = frozenset({"news"})
        self.assertEqual(tag_normalize.fold("memes", denylist), "meme")

    def test_idempotent(self):
        for tag in ("catchphrases", "catchphrase", "boss", "glasses", ""):
            once = tag_normalize.fold(tag)
            self.assertEqual(tag_normalize.fold(once), once, tag)

    def test_empty_string_is_unchanged(self):
        self.assertEqual(tag_normalize.fold(""), "")


class LoadDenylistTests(unittest.TestCase):
    def _write(self, text: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".yaml")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        self.addCleanup(os.unlink, path)
        return path

    def test_loads_a_flat_list_lowercased(self):
        path = self._write("do_not_fold:\n  - News\n  - Star Wars\n")
        self.assertEqual(tag_normalize.load_denylist(path),
                         frozenset({"news", "star wars"}))

    def test_missing_key_is_an_empty_denylist(self):
        path = self._write("other_key: []\n")
        self.assertEqual(tag_normalize.load_denylist(path), frozenset())

    def test_round_trips_through_fold(self):
        path = self._write("do_not_fold:\n  - news\n")
        denylist = tag_normalize.load_denylist(path)
        self.assertEqual(tag_normalize.fold("news", denylist), "news")


if __name__ == "__main__":
    unittest.main(verbosity=2)
