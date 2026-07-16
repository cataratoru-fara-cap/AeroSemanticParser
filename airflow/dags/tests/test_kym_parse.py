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


class MalformedUrlRepairTests(unittest.TestCase):
    """Root cause of all 14 production confirmed-meme failures (2026-07):
    KYM editors' wiki-content typos in body links/images. One bad href was
    failing the WHOLE page; the repair layer fixes what's mechanically
    certain and drops single unrecoverable items instead."""

    def test_scheme_typos_repaired(self):
        # exact values from production parse_failures
        from modules.kym_parse import _clean_url
        self.assertEqual(
            _clean_url("https;//knowyourmeme.com/memes/sites/youtube"),
            "https://knowyourmeme.com/memes/sites/youtube")
        self.assertEqual(
            _clean_url("https//knowyourmeme.com/memes/aqua"),
            "https://knowyourmeme.com/memes/aqua")

    def test_styling_prefix_stripped(self):
        from modules.kym_parse import _clean_url
        self.assertEqual(
            _clean_url("--%7Bwidth:170px%7Dhttps://i.kym-cdn.com/x/5dc.gif"),
            "https://i.kym-cdn.com/x/5dc.gif")
        self.assertEqual(
            _clean_url("%7Bwidth:425pxhttps://i.kym-cdn.com/x/302.jpg"),
            "https://i.kym-cdn.com/x/302.jpg")

    def test_healthy_urls_untouched(self):
        from modules.kym_parse import _clean_url
        for url in ("https://knowyourmeme.com/memes/doge",
                    "http://example.com/a?b=c"):
            self.assertEqual(_clean_url(url), url)

    def test_unrecoverable_garbage_dropped(self):
        from modules.kym_parse import _clean_url
        for bad in ("javascript:void(0)", "not a url at all", "", None):
            self.assertIsNone(_clean_url(bad))

    def test_page_survives_typo_links(self):
        # The granularity fix itself: a page whose body contains typo'd and
        # garbage links parses successfully; typos repaired, garbage
        # dropped, nothing page-fatal.
        html = '''<html><head>
<link rel="canonical" href="https://knowyourmeme.com/memes/typo-repro"/>
</head><body>
<h1 class="entry-title">Typo Repro</h1>
<aside class="left"><dl><dt>Status</dt><dd>Confirmed</dd>
<dt>Origin</dt><dd>TikTok</dd></dl></aside>
<dl id="entry_tags"><dt>Tags</dt><dd><a data-tag="m">m</a></dd></dl>
<section class="bodycopy"><h2 id="about">About</h2>
<p><a href="https;//knowyourmeme.com/memes/rickroll">typo</a>
<a href="https://knowyourmeme.com/memes/doge">ok</a>
<a href="javascript:void(0)">junk</a></p></section></body></html>'''
        entry = parse_entry(html)
        urls = [str(l.url) for l in entry.sections[0].links]
        self.assertEqual(urls, ["https://knowyourmeme.com/memes/rickroll",
                                "https://knowyourmeme.com/memes/doge"])


class ModelGuardTests(unittest.TestCase):
    def test_missing_origin_still_rejected(self):
        with self.assertRaises(ValidationError):
            KYMEntryScrape.model_validate({
                "url": "https://knowyourmeme.com/memes/x",
                "title": "X", "category": "meme", "status": "confirmed"})

    def test_missing_tags_no_longer_rejected(self):
        # tags moved from model-required to CorpusPolicy.require_tags: a
        # legitimate confirmed meme without tags should validate fine and
        # only be FLAGGED incomplete, not rejected outright.
        entry = KYMEntryScrape.model_validate({
            "url": "https://knowyourmeme.com/memes/x", "title": "X",
            "category": "meme", "status": "confirmed", "origin": "Twitter"})
        self.assertEqual(entry.tags, [])
        ready, missing = corpus_ready(entry)
        self.assertFalse(ready)
        self.assertIn("tags", missing)

    def test_pre_1500_year_no_longer_rejected(self):
        # year's lower bound was ge=1500, which raised on the WHOLE page for
        # any culture/event/person entry with a genuinely pre-1500 origin
        # year (e.g. a painting, a historical event) — not just the field.
        entry = KYMEntryScrape.model_validate({
            "url": "https://knowyourmeme.com/cultures/renaissance-art",
            "title": "Renaissance Art", "category": "culture",
            "status": "confirmed", "origin": "Italy", "year": "1200"})
        self.assertEqual(entry.year, 1200)

    def test_year_still_rejects_nonsense_values(self):
        with self.assertRaises(ValidationError):
            KYMEntryScrape.model_validate({
                "url": "https://knowyourmeme.com/memes/x", "title": "X",
                "category": "meme", "status": "confirmed",
                "origin": "Twitter", "year": 0})
        with self.assertRaises(ValidationError):
            KYMEntryScrape.model_validate({
                "url": "https://knowyourmeme.com/memes/x", "title": "X",
                "category": "meme", "status": "confirmed",
                "origin": "Twitter", "year": 3000})


if __name__ == "__main__":
    unittest.main()