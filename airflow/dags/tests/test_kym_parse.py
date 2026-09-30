"""Fixture tests for kym_parse + kym_models (no network, no Mongo).

Fixture: tests/fixtures/doge.html — a real scraped confirmed-meme page.
Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kym_parse.py -v
"""
import os
import unittest
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from pydantic import ValidationError

from modules import kym_parse
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

    def test_every_link_knows_where_it_sits(self):
        """Parser 1.6.0. The event layer ties a link to the sentence it
        appears in; that is only possible if the offset points at the
        anchor's own text in its paragraph."""
        links = [(s, l) for s in self.entry.sections for l in s.links]
        self.assertTrue(links)
        for s, l in links:
            self.assertIsNotNone(l.paragraph, l.text)
            self.assertIsNotNone(l.offset, l.text)
            self.assertEqual(s.text[l.paragraph][l.offset:l.offset + len(l.text)],
                             l.text)

    def test_every_image_knows_which_paragraph_it_follows(self):
        for s in self.entry.sections:
            for i in s.images:
                self.assertIsNotNone(i.after_paragraph)
                self.assertLess(i.after_paragraph, max(len(s.text), 1))

    def test_embedded_posts_are_captured(self):
        # Doge embeds two Instagram reels; before 1.6.0 neither existed.
        embeds = [e for s in self.entry.sections for e in s.embeds]
        self.assertEqual({e.platform for e in embeds}, {"instagram"})
        self.assertEqual(len(embeds), 2)
        self.assertTrue(all("instagram.com/reel/" in str(e.url) for e in embeds))
        self.assertTrue(all("?" not in str(e.url) for e in embeds))  # no utm_*

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

    def test_series_parent(self):
        self.assertEqual(
            str(self.entry.series_parent),
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

    def test_no_aliases_when_the_about_bolds_only_the_title(self):
        # "<strong>Doge</strong> (pronounced /ˈdoʊdʒ/ DOHJ) is a slang term"
        self.assertEqual(self.entry.aliases, [])

    def test_one_page_image_and_no_template_image_field(self):
        # Parser 1.7.0 (gap 03): the header photo IMKG called the template
        # image is og:image again on every corpus page.
        self.assertEqual(str(self.entry.og_image),
                         "https://i.kym-cdn.com/entries/icons/original/000/013/564/doge.jpg")
        self.assertNotIn("template_image_url", type(self.entry).model_fields)

    def test_scraped_at_is_the_stored_pages_fetch_time(self):
        when = datetime(2026, 7, 9, 23, 47, 44, tzinfo=timezone.utc)
        self.assertEqual(parse_entry(_load(), fetched_at=when).scraped_at, when)
        self.assertIsNone(self.entry.scraped_at)          # none given, none invented

    def test_badges_empty_when_no_badges_row(self):
        # Doge's sidebar has no "Badges:" dt at all (SFW page) — confirms
        # _badges() degrades to [] rather than erroring or hallucinating.
        self.assertEqual(self.entry.badges, [])

    def test_relation_fields_reverted_to_series_parent_only(self):
        # nsfw (URL-inference) stays removed — badges['Sensitive'] is the
        # signal. related_entries/related_sub_entries (inline-gallery
        # harvesting) was tried and reverted as unreliable + redundant with
        # series_parent; children/siblings were removed earlier still.
        fields = type(self.entry).model_fields
        for gone in ("nsfw", "children", "siblings",
                    "related_entries", "related_sub_entries"):
            self.assertNotIn(gone, fields)
        self.assertIn("series_parent", fields)


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


class EmbedShapeTests(unittest.TestCase):
    """One case per embed shape found on a 400-page sample (2026-09-18)."""

    def embeds(self, html):
        from bs4 import BeautifulSoup
        from modules.kym_parse import _embeds
        soup = BeautifulSoup(f"<div>{html}</div>", "lxml")
        return [(e["platform"], e["url"]) for e in _embeds(soup.div)]

    def test_tiktok_uses_the_cite_attribute(self):
        self.assertEqual(self.embeds(
            '<blockquote class="tiktok-embed" cite="https://www.tiktok.com/@a/video/1">'
            '<a href="https://www.tiktok.com/@a?refer=embed">@a</a></blockquote>'),
            [("tiktok", "https://www.tiktok.com/@a/video/1")])

    def test_a_tweet_is_its_permalink_not_the_first_tco_link(self):
        self.assertEqual(self.embeds(
            '<blockquote class="twitter-tweet-lazy"><p>lol <a href="https://t.co/x">'
            'pic</a></p>&mdash; A (@a) <a href="https://twitter.com/a/status/42?ref_src=tw">'
            'May 1, 2025</a></blockquote>'),
            [("twitter", "https://twitter.com/a/status/42")])

    def test_lazy_iframes_use_data_src(self):
        self.assertEqual(self.embeds(
            '<iframe class="lazy-iframe" data-src="https://www.youtube.com/embed/abc"></iframe>'
            '<iframe class="vine-embed lazy-iframe" data-src="https://vine.co/v/x/embed/simple"></iframe>'),
            [("youtube", "https://www.youtube.com/embed/abc"),
             ("vine", "https://vine.co/v/x/embed/simple")])

    def test_instagram_permalink_loses_its_tracking_query(self):
        self.assertEqual(self.embeds(
            '<blockquote class="instagram-media-lazy" data-instgrm-permalink='
            '"https://www.instagram.com/p/X/?utm_source=ig_embed"></blockquote>'),
            [("instagram", "https://www.instagram.com/p/X/")])

    def test_video_and_imgur(self):
        self.assertEqual(self.embeds(
            '<video controls src="https://img.ifunny.co/videos/a.mp4"></video>'
            '<blockquote class="imgur-embed-pub" data-id="N7VqB1g"></blockquote>'),
            [("video", "https://img.ifunny.co/videos/a.mp4"),
             ("imgur", "https://imgur.com/N7VqB1g")])

    def test_quotations_and_the_trends_chart_are_not_embeds(self):
        self.assertEqual(self.embeds(
            '<blockquote><p>a quoted line</p></blockquote>'
            '<iframe class="google-trends-iframe lazy-iframe" '
            'data-src="https://trends.google.com/trends/embed/x"></iframe>'), [])


class LinkPositionTests(unittest.TestCase):
    def test_two_anchors_on_the_same_words_both_get_a_position(self):
        """Real markup (dat-boi, 2026-09-18): KYM's auto-link nested inside a
        hand-made link. Before 1.6.1 the second anchor had no offset, so the
        event layer could never attach it."""
        from modules.kym_parse import _sections
        from bs4 import BeautifulSoup
        html = ('<section class="bodycopy"><h2 id="origin">Origin</h2>'
                # verbatim shape from the dat-boi page
                '<p>The <a class="internal-link" href="https://knowyourmeme.com/memes/'
                'sites/facebook-meta"><strong><em><a class="auto-link" '
                'href="/memes/sites/facebook">Facebook</a></em></strong></a> page '
                'posted it on <a href="/memes/sites/facebook">Facebook</a> again.'
                '</p></section>')
        [section] = _sections(BeautifulSoup(html, "lxml"))
        para = section["text"][0]
        offsets = [l["offset"] for l in section["links"]]
        self.assertNotIn(None, offsets)
        for link in section["links"]:
            self.assertEqual(para[link["offset"]:link["offset"] + len(link["text"])],
                             link["text"])
        self.assertEqual(offsets[0], offsets[1])            # the same words
        self.assertGreater(offsets[2], offsets[1])          # the later mention


def aliases(lead: str, title: str) -> list[str]:
    html = f'<h2 id="about"><span>About</span></h2>\n{lead}\n<h2 id="origin">Origin</h2><p>x</p>'
    return kym_parse._aliases(BeautifulSoup(html, "html.parser"), title)


class AliasTests(unittest.TestCase):
    """Parser 1.7.0: KYM has no alias row; the About's lead bolds the names
    (gap 03). Each case is a pattern read on the corpus."""

    def test_also_known_as_and_or_continue_the_names(self):
        self.assertEqual(aliases(
            "<p><strong>Distracted Boyfriend</strong>, also known as <strong>Man Looking "
            "at Other Woman</strong> or <strong>Guy Checking Out Another Girl</strong>, "
            "is a stock photo series in which <strong>nobody</strong> is named.</p>",
            "Distracted Boyfriend"),
            ["Man Looking at Other Woman", "Guy Checking Out Another Girl"])

    def test_the_sentence_proper_ends_the_names(self):
        # "continued" is the rest of a catchphrase, not another name
        self.assertEqual(aliases(
            "<p><strong>Yeah, But They Got Him</strong>, continued <strong>What Does "
            "That Mean?</strong>, is a catchphrase.</p>", "Yeah, But They Got Him"), [])

    def test_real_name_and_a_parenthesis(self):
        self.assertEqual(aliases(
            "<p><strong>Pikabu</strong> (<strong>Пикабу</strong>) is a Russian site.</p>",
            "Pikabu"), ["Пикабу"])
        self.assertEqual(aliases(
            "<p><strong>Ice Spice</strong>, real name <strong>Isis Gaston</strong>, "
            "is a rapper.</p>", "Ice Spice"), ["Isis Gaston"])

    def test_a_first_name_that_is_not_the_title_is_an_alias(self):
        self.assertEqual(aliases(
            "<p>The <strong>Sir Toad</strong> or <strong>Frog In Suit Sitting In "
            "Chair</strong> is an image.</p>", "Colonel Toad"),
            ["Sir Toad", "Frog In Suit Sitting In Chair"])

    def test_the_title_is_never_its_own_alias(self):
        self.assertEqual(aliases(
            '<p><strong>"Who is Paul McCartney?"</strong> (<strong>Only One '
            'Trolling</strong>) is a joke.</p>', "Who Is Paul McCartney?"),
            ["Only One Trolling"])
        self.assertEqual(aliases(
            "<p><strong>The Slashdot Effect</strong>, also known as <strong>Slashdotting"
            "</strong>, is traffic.</p>", "Slashdot Effect"), ["Slashdotting"])

    def test_markup_kym_failed_to_render_ends_the_names(self):
        self.assertEqual(aliases(
            '<p><strong>Cat Circle</strong> or <strong>I Wake Up / There Is X" also '
            "known as *Cat Circle Of Life</strong> refers to an image.</p>",
            "I Wake Up / There Is X"), ["Cat Circle"])
        self.assertEqual(aliases(                            # a name's own asterisks
            "<p><strong>*Starts Beatboxing*</strong> or <strong>Please Stop Beatboxing"
            "</strong> is a video.</p>", "*Starts Beatboxing* / Please Stop Beatboxing"),
            ["*Starts Beatboxing*", "Please Stop Beatboxing"])

    def test_previously_known_as(self):
        self.assertEqual(aliases(
            "<p><strong>Tessa Violet Williams</strong>, previously known as "
            "<strong>Meekakitty</strong>, is a singer.</p>", "Tessa Violet"),
            ["Tessa Violet Williams", "Meekakitty"])

    def test_a_lead_that_bolds_a_name_later_gives_none(self):
        self.assertEqual(aliases(
            "<p>In 2014, the phrase <strong>Such Wow</strong>, also known as "
            "<strong>Wow</strong>, spread.</p>", "Doge"), [])

    def test_a_name_split_by_the_markup_is_not_two_names(self):
        self.assertEqual(aliases(
            "<p><strong>Don't F</strong><strong>k With Cats</strong> is a series.</p>",
            "Don't F**k With Cats"), [])

    def test_a_comma_inside_the_bold_still_separates_names(self):
        self.assertEqual(aliases(
            "<p><strong>Dab Pen,</strong> <strong>Cartridges,</strong> or "
            "<strong>Vape Pen</strong> are devices.</p>", "Dab Pen"),
            ["Cartridges", "Vape Pen"])

    def test_a_name_inside_a_link_counts(self):
        self.assertEqual(aliases(
            '<p><a href="/memes/pepe-the-frog"><strong>Pepe the Frog</strong></a>, '
            "a.k.a. <strong>Pepe</strong>, is a frog.</p>", "Pepe the Frog"), ["Pepe"])

    def test_no_about_section(self):
        soup = BeautifulSoup("<h2 id='origin'>Origin</h2><p><strong>X</strong></p>",
                             "html.parser")
        self.assertEqual(kym_parse._aliases(soup, "Y"), [])


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