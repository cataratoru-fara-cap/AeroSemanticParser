"""kym_parse + kym_models against a real scraped confirmed meme
(fixtures/doge.html) and the markup shapes found on the corpus."""
from datetime import datetime, timezone

import pytest
from bs4 import BeautifulSoup
from pydantic import ValidationError

from helpers import FIXTURES
from modules import kym_parse
from modules.kym_models import CorpusPolicy, KYMEntryScrape, corpus_ready
from modules.kym_parse import parse_entry

HTML = (FIXTURES / "doge.html").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def doge():
    return parse_entry(HTML)


def test_identity_sidebar_and_series(doge):
    assert (str(doge.url), doge.title, doge.category, doge.status) == \
        ("https://knowyourmeme.com/memes/doge", "Doge", "meme", "confirmed")
    assert (doge.year, doge.origin, doge.region) == (2010, "Tumblr", ["Japan"])
    assert doge.entry_type == ["animal", "character", "exploitable", "image-macro", "slang"]
    assert str(doge.series_parent) == "https://knowyourmeme.com/memes/interior-monologue-captioning"
    assert doge.kym_added is not None and doge.kym_added < doge.kym_last_updated
    assert doge.badges == []                    # no "Badges:" row on an SFW page: [], not an error
    assert doge.aliases == []                   # the About bolds only the title
    # 1.7.0 (gap 03): the header photo IMKG called the template image is og:image
    assert str(doge.og_image) == "https://i.kym-cdn.com/entries/icons/original/000/013/564/doge.jpg"


def test_the_model_kept_only_series_parent_of_the_relation_fields(doge):
    # nsfw (URL inference) gone for the Sensitive badge; related entries and
    # children/siblings reverted as unreliable and redundant with series_parent
    fields = type(doge).model_fields
    assert not {"nsfw", "children", "siblings", "related_entries", "related_sub_entries",
                "template_image_url"} & set(fields)
    assert "series_parent" in fields


def test_tags_and_references(doge):
    assert "shiba inu" in doge.tags and len(doge.tags) == len({t.lower() for t in doge.tags})
    # dl#entry_tags holds the Tags AND the Additional References side by side:
    # a naive 'dl a' merged reference site names into tags
    assert not {"Encyclopedia Dramatica", "Wikipedia", "Dictionary.com"} & set(doge.tags)
    names = [r.name for r in doge.additional_references]
    assert "shiba inu" not in names and "Encyclopedia Dramatica" in names
    assert (len(doge.tags), len(doge.additional_references)) == (22, 8)
    assert len(doge.external_references) > 40
    assert doge.external_references[0].index == 1 and "wikipedia.org" in str(doge.external_references[0].url)


def test_sections(doge):
    assert {"about", "origin", "spread", "related_memes", "various_examples", "search_interest",
            "external_references"} <= {s.kind for s in doge.sections}
    assert [s.kind for s in doge.sections if s.kind != "other"][:3] == ["about", "origin", "spread"]
    assert all(s.level in (2, 3) for s in doge.sections)


def test_every_link_and_image_knows_where_it_sits(doge):
    # 1.6.0: the event layer ties a link to its sentence through the offset
    links = [(s, link) for s in doge.sections for link in s.links]
    assert links
    for s, link in links:
        assert link.paragraph is not None and link.offset is not None, link.text
        assert s.text[link.paragraph][link.offset:link.offset + len(link.text)] == link.text
    imgs = [(s, i) for s in doge.sections for i in s.images]
    assert len(imgs) > 10
    for s, i in imgs:
        assert i.after_paragraph is not None and i.after_paragraph < max(len(s.text), 1)
        assert "kym-cdn.com" in str(i.src) and "/assets/blank-" not in str(i.src)   # lazy-load resolved


def test_embedded_posts_are_captured(doge):
    # two Instagram reels; none before 1.6.0
    embeds = [e for s in doge.sections for e in s.embeds]
    assert {e.platform for e in embeds} == {"instagram"} and len(embeds) == 2
    assert all("instagram.com/reel/" in str(e.url) and "?" not in str(e.url) for e in embeds)   # no utm_*


def test_the_corpus_gate(doge):
    assert corpus_ready(doge)[0] and corpus_ready(doge, CorpusPolicy(require_region=True))[0]


def test_scraped_at_is_the_stored_pages_fetch_time(doge):
    when = datetime(2026, 7, 9, 23, 47, 44, tzinfo=timezone.utc)
    assert parse_entry(HTML, fetched_at=when).scraped_at == when
    assert doge.scraped_at is None                # none given, none invented


# -- malformed URLs: all 14 production confirmed-meme failures of 2026-07 were
#    wiki typos in one link; one bad href failed the WHOLE page ---------------------

@pytest.mark.parametrize("raw, clean", [
    ("https;//knowyourmeme.com/memes/sites/youtube", "https://knowyourmeme.com/memes/sites/youtube"),
    ("https//knowyourmeme.com/memes/aqua", "https://knowyourmeme.com/memes/aqua"),
    ("--%7Bwidth:170px%7Dhttps://i.kym-cdn.com/x/5dc.gif", "https://i.kym-cdn.com/x/5dc.gif"),
    ("%7Bwidth:425pxhttps://i.kym-cdn.com/x/302.jpg", "https://i.kym-cdn.com/x/302.jpg"),
    ("https://knowyourmeme.com/memes/doge", "https://knowyourmeme.com/memes/doge"),
    ("http://example.com/a?b=c", "http://example.com/a?b=c"),
    ("javascript:void(0)", None), ("not a url at all", None), ("", None), (None, None),
])
def test_urls_are_repaired_or_dropped(raw, clean):
    assert kym_parse._clean_url(raw) == clean


def test_a_page_survives_typo_links():
    entry = parse_entry('''<html><head>
<link rel="canonical" href="https://knowyourmeme.com/memes/typo-repro"/></head><body>
<h1 class="entry-title">Typo Repro</h1>
<aside class="left"><dl><dt>Status</dt><dd>Confirmed</dd><dt>Origin</dt><dd>TikTok</dd></dl></aside>
<dl id="entry_tags"><dt>Tags</dt><dd><a data-tag="m">m</a></dd></dl>
<section class="bodycopy"><h2 id="about">About</h2>
<p><a href="https;//knowyourmeme.com/memes/rickroll">typo</a>
<a href="https://knowyourmeme.com/memes/doge">ok</a>
<a href="javascript:void(0)">junk</a></p></section></body></html>''')
    assert [str(link.url) for link in entry.sections[0].links] == \
        ["https://knowyourmeme.com/memes/rickroll", "https://knowyourmeme.com/memes/doge"]


# -- embeds: one case per shape on a 400-page sample (2026-09-18) --------------------------

@pytest.mark.parametrize("html, want", [
    ('<blockquote class="tiktok-embed" cite="https://www.tiktok.com/@a/video/1">'
     '<a href="https://www.tiktok.com/@a?refer=embed">@a</a></blockquote>',
     [("tiktok", "https://www.tiktok.com/@a/video/1")]),                     # the cite attribute
    ('<blockquote class="twitter-tweet-lazy"><p>lol <a href="https://t.co/x">pic</a></p>&mdash; A (@a) '
     '<a href="https://twitter.com/a/status/42?ref_src=tw">May 1, 2025</a></blockquote>',
     [("twitter", "https://twitter.com/a/status/42")]),                     # the permalink, not a t.co link
    ('<iframe class="lazy-iframe" data-src="https://www.youtube.com/embed/abc"></iframe>'
     '<iframe class="vine-embed lazy-iframe" data-src="https://vine.co/v/x/embed/simple"></iframe>',
     [("youtube", "https://www.youtube.com/embed/abc"), ("vine", "https://vine.co/v/x/embed/simple")]),
    ('<blockquote class="instagram-media-lazy" data-instgrm-permalink='
     '"https://www.instagram.com/p/X/?utm_source=ig_embed"></blockquote>',
     [("instagram", "https://www.instagram.com/p/X/")]),                     # no tracking query
    ('<video controls src="https://img.ifunny.co/videos/a.mp4"></video>'
     '<blockquote class="imgur-embed-pub" data-id="N7VqB1g"></blockquote>',
     [("video", "https://img.ifunny.co/videos/a.mp4"), ("imgur", "https://imgur.com/N7VqB1g")]),
    ('<blockquote><p>a quoted line</p></blockquote><iframe class="google-trends-iframe lazy-iframe" '
     'data-src="https://trends.google.com/trends/embed/x"></iframe>', []),    # quotes and trends: no embeds
])
def test_embed_shapes(html, want):
    soup = BeautifulSoup(f"<div>{html}</div>", "lxml")
    assert [(e["platform"], e["url"]) for e in kym_parse._embeds(soup.div)] == want


def test_two_anchors_on_the_same_words_both_get_a_position():
    # dat-boi: KYM's auto-link nested inside a hand-made link; before 1.6.1 the
    # second anchor had no offset and the event layer could never attach it
    html = ('<section class="bodycopy"><h2 id="origin">Origin</h2>'
            '<p>The <a class="internal-link" href="https://knowyourmeme.com/memes/sites/facebook-meta"><strong><em>'
            '<a class="auto-link" href="/memes/sites/facebook">Facebook</a></em></strong></a> page posted it on '
            '<a href="/memes/sites/facebook">Facebook</a> again.</p></section>')
    [section] = kym_parse._sections(BeautifulSoup(html, "lxml"))
    para, offsets = section["text"][0], [link["offset"] for link in section["links"]]
    assert None not in offsets
    assert all(para[link["offset"]:link["offset"] + len(link["text"])] == link["text"] for link in section["links"])
    assert offsets[0] == offsets[1] < offsets[2]        # the same words, then the later mention


# -- aliases: 1.7.0, from the names the About's lead bolds (gap 03) --------------------------

@pytest.mark.parametrize("lead, title, want", [
    ("<p><strong>Distracted Boyfriend</strong>, also known as <strong>Man Looking at Other Woman</strong> or "
     "<strong>Guy Checking Out Another Girl</strong>, is a stock photo series in which <strong>nobody</strong> "
     "is named.</p>", "Distracted Boyfriend", ["Man Looking at Other Woman", "Guy Checking Out Another Girl"]),
    # the sentence proper ends the names: "continued" is the rest of a catchphrase
    ("<p><strong>Yeah, But They Got Him</strong>, continued <strong>What Does That Mean?</strong>, is a "
     "catchphrase.</p>", "Yeah, But They Got Him", []),
    ("<p><strong>Pikabu</strong> (<strong>Пикабу</strong>) is a Russian site.</p>", "Pikabu", ["Пикабу"]),
    ("<p><strong>Ice Spice</strong>, real name <strong>Isis Gaston</strong>, is a rapper.</p>", "Ice Spice",
     ["Isis Gaston"]),
    ("<p>The <strong>Sir Toad</strong> or <strong>Frog In Suit Sitting In Chair</strong> is an image.</p>",
     "Colonel Toad", ["Sir Toad", "Frog In Suit Sitting In Chair"]),        # a first name not the title
    # the title is never its own alias
    ('<p><strong>"Who is Paul McCartney?"</strong> (<strong>Only One Trolling</strong>) is a joke.</p>',
     "Who Is Paul McCartney?", ["Only One Trolling"]),
    ("<p><strong>The Slashdot Effect</strong>, also known as <strong>Slashdotting</strong>, is traffic.</p>",
     "Slashdot Effect", ["Slashdotting"]),
    # markup KYM failed to render ends the names; a name's own asterisks do not
    ('<p><strong>Cat Circle</strong> or <strong>I Wake Up / There Is X" also known as *Cat Circle Of '
     "Life</strong> refers to an image.</p>", "I Wake Up / There Is X", ["Cat Circle"]),
    ("<p><strong>*Starts Beatboxing*</strong> or <strong>Please Stop Beatboxing</strong> is a video.</p>",
     "*Starts Beatboxing* / Please Stop Beatboxing", ["*Starts Beatboxing*", "Please Stop Beatboxing"]),
    ("<p><strong>Tessa Violet Williams</strong>, previously known as <strong>Meekakitty</strong>, is a "
     "singer.</p>", "Tessa Violet", ["Tessa Violet Williams", "Meekakitty"]),
    ("<p>In 2014, the phrase <strong>Such Wow</strong>, also known as <strong>Wow</strong>, spread.</p>",
     "Doge", []),                                                           # bolded later: none
    ("<p><strong>Don't F</strong><strong>k With Cats</strong> is a series.</p>", "Don't F**k With Cats", []),
    ("<p><strong>Dab Pen,</strong> <strong>Cartridges,</strong> or <strong>Vape Pen</strong> are devices.</p>",
     "Dab Pen", ["Cartridges", "Vape Pen"]),                                # a comma inside the bold separates
    ('<p><a href="/memes/pepe-the-frog"><strong>Pepe the Frog</strong></a>, a.k.a. <strong>Pepe</strong>, '
     "is a frog.</p>", "Pepe the Frog", ["Pepe"]),                          # inside a link counts
])
def test_aliases(lead, title, want):
    html = f'<h2 id="about"><span>About</span></h2>\n{lead}\n<h2 id="origin">Origin</h2><p>x</p>'
    assert kym_parse._aliases(BeautifulSoup(html, "html.parser"), title) == want


def test_no_about_section_no_aliases():
    soup = BeautifulSoup("<h2 id='origin'>Origin</h2><p><strong>X</strong></p>", "html.parser")
    assert kym_parse._aliases(soup, "Y") == []


# -- the model's guards ------------------------------------------------------------------------

BASE = {"url": "https://knowyourmeme.com/memes/x", "title": "X", "category": "meme", "status": "confirmed"}


@pytest.mark.parametrize("fields", [{}, {"origin": "Twitter", "year": 0}, {"origin": "Twitter", "year": 3000}])
def test_a_missing_origin_or_a_nonsense_year_is_rejected(fields):
    with pytest.raises(ValidationError):
        KYMEntryScrape.model_validate({**BASE, **fields})


def test_missing_tags_and_a_pre_1500_year_are_accepted():
    # tags moved to CorpusPolicy.require_tags: flagged incomplete, not rejected
    entry = KYMEntryScrape.model_validate({**BASE, "origin": "Twitter"})
    ready, missing = corpus_ready(entry)
    assert entry.tags == [] and not ready and "tags" in missing
    # year >= 1500 raised on the WHOLE page for a painting or a historical event
    old = KYMEntryScrape.model_validate({"url": "https://knowyourmeme.com/cultures/renaissance-art",
                                         "title": "Renaissance Art", "category": "culture", "status": "confirmed",
                                         "origin": "Italy", "year": "1200"})
    assert old.year == 1200
