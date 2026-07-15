"""Fixture tests for kym_parse + kym_models (no network, no Mongo).

Fixture: tests/fixtures/doge.html — a real scraped confirmed-meme page.
Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kym_parse.py -v
"""
import os
import unittest

from pydantic import ValidationError

from modules.kym_models import CorpusPolicy, KYMEntryScrape, corpus_ready
from modules.kym_parse import parse_entry

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "doge.html")


def _load() -> str:
    with open(FIXTURE, encoding="utf-8") as fh:
        return fh.read()


class ParseDogeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entry = parse_entry(_load())

    def test_identity_fields(self):
        e = self.entry
        self.assertEqual(str(e.url), "https://knowyourmeme.com/memes/doge")
        self.assertEqual(e.title, "Doge")
        self.assertEqual(e.category, "meme")
        self.assertEqual(e.status, "confirmed")

    def test_sidebar_fields(self):
        e = self.entry
        self.assertEqual(e.year, 2010)
        self.assertEqual(e.origin, "Tumblr")
        self.assertEqual(e.region, ["Japan"])
        self.assertEqual(
            e.entry_type,
            ["animal", "character", "exploitable", "image-macro", "slang"])

    def test_tags_nonempty_and_deduped(self):
        self.assertIn("shiba inu", self.entry.tags)
        self.assertEqual(len(self.entry.tags), len({t.lower() for t in self.entry.tags}))

    def test_tags_do_not_bleed_from_additional_references(self):
        # Regression: dl#entry_tags holds BOTH the Tags dt/dd and the
        # Additional References dt/dd side by side; a naive 'dl a' selector
        # merges reference site names ("Wikipedia", "Reddit"-the-site, etc.)
        # into tags. Reddit is legitimately both a tag AND a reference name
        # here, so assert on names that only belong to one side.
        self.assertNotIn("Encyclopedia Dramatica", self.entry.tags)
        self.assertNotIn("Wikipedia", self.entry.tags)
        self.assertNotIn("Dictionary.com", self.entry.tags)
        ref_names = {r.name for r in self.entry.additional_references}
        self.assertNotIn("shiba inu", ref_names)
        self.assertEqual(len(self.entry.tags), 22)
        self.assertEqual(len(self.entry.additional_references), 8)

    def test_parent_series(self):
        self.assertEqual(
            str(self.entry.parent),
            "https://knowyourmeme.com/memes/interior-monologue-captioning")

    def test_references(self):
        e = self.entry
        self.assertGreater(len(e.external_references), 40)
        first = e.external_references[0]
        self.assertEqual(first.index, 1)
        self.assertIn("wikipedia.org", str(first.url))
        self.assertIn("Encyclopedia Dramatica",
                      [r.name for r in e.additional_references])

    def test_sections_canonical_kinds(self):
        kinds = {s.kind for s in self.entry.sections}
        for expected in ("about", "origin", "spread", "related_memes",
                         "various_examples", "search_interest",
                         "external_references"):
            self.assertIn(expected, kinds)

    def test_sections_levels_and_order(self):
        # First three canonical sections appear in page order.
        canon = [s.kind for s in self.entry.sections if s.kind != "other"]
        self.assertEqual(canon[:3], ["about", "origin", "spread"])
        self.assertTrue(all(s.level in (2, 3) for s in self.entry.sections))

    def test_images_lazyload_resolved(self):
        imgs = [i for s in self.entry.sections for i in s.images]
        self.assertGreater(len(imgs), 10)
        self.assertTrue(all("kym-cdn.com" in str(i.src) for i in imgs))
        self.assertFalse(any("/assets/blank-" in str(i.src) for i in imgs))

    def test_timestamps(self):
        self.assertIsNotNone(self.entry.kym_added)
        self.assertIsNotNone(self.entry.kym_last_updated)
        self.assertLess(self.entry.kym_added, self.entry.kym_last_updated)

    def test_corpus_gate_passes(self):
        ready, missing = corpus_ready(self.entry)
        self.assertTrue(ready, f"missing: {missing}")

    def test_region_gate_would_pass_here(self):
        ready, _ = corpus_ready(self.entry, CorpusPolicy(require_region=True))
        self.assertTrue(ready)

    def test_badges_empty_when_no_badges_row(self):
        # Doge's sidebar has no "Badges:" dt at all (SFW page) — confirms
        # _badges() degrades to [] rather than erroring or hallucinating.
        self.assertEqual(self.entry.badges, [])

    def test_nsfw_and_children_fields_removed(self):
        # nsfw (URL-inference), children, and siblings (never populated,
        # need a separate fetch, redundant with parent) were removed from
        # the schema — badges['Sensitive'] from the sidebar is now the
        # authoritative content-warning signal instead.
        self.assertNotIn("nsfw", type(self.entry).model_fields)
        self.assertNotIn("children", type(self.entry).model_fields)
        self.assertNotIn("siblings", type(self.entry).model_fields)


class ModelGuardTests(unittest.TestCase):
    def test_missing_origin_and_tags_rejected(self):
        with self.assertRaises(ValidationError):
            KYMEntryScrape.model_validate({
                "url": "https://knowyourmeme.com/memes/x",
                "title": "X", "category": "meme", "status": "confirmed"})


if __name__ == "__main__":
    unittest.main()