"""kg/events.py: narrative section -> event rows, on the real client over a
stubbed HTTP session (no model server is contacted).

Pinned beyond "it works": nothing the model adds reaches the store and nothing
valid is thrown away (the model answers with sentence NUMBERS; every value it
returns is grounded to the section's own words); audit() checks independently
and extract() writes nothing that fails it; links, citations and media attach
by position; nothing is capped; the model policy is criteria, never a
reasoning model; every sentence is in an event (4.0.0); the artifact is
appended, never rewritten. Case ids name the KYM entry a case came from in
review, where it did. Every table case is also audited.
"""
import json

import pytest
from jsonschema import Draft202012Validator

from helpers import KG_CONFIG
from modules import openwebui_client as owc
from modules.kg import events as ev
from modules.mongo_base import url_doc_id
from openwebui_stub import CCDD, UI, Resp, StubSession, by_model, client

SCHEMA_PATH = str(KG_CONFIG / "event_extraction_schema.json")
MINISTRAL = "ministral-3:14b"
REQ = ev.model_request({})

URL = "https://knowyourmeme.com/memes/doge"
KYM_LINK = "https://knowyourmeme.com/memes/sites/tumblr"
REF_3 = "https://web.archive.org/web/2010/https://tumblr.com/post/1"
IMG = "https://i.kym-cdn.com/photos/images/original/000/1.jpg"
TIKTOK = "https://www.tiktok.com/@buhfingeranator43/video/1"

# Two paragraphs as the parser stores them, citation marker included. Since
# 4.0.0 every sentence is in an event: in a reply naming only sentence 1, 2
# ("allegedly first called ...") continues it and 3, 4, 5 are events of their own.
P0 = ("The original photo of Kabosu was posted to Tumblr [3] on February "
      "23rd, 2010 by blogger Atsuko Sato. It was allegedly first called "
      "“doge” on 4chan.")
P1 = ("On May 7th, YouTuber KwandaoRen66 uploaded a video. That same day, "
      "the same YouTuber posted a second one. In 2013 it spread to Reddit.")


def entry(**over):
    return {"url": URL, "title": "Doge", "category": "meme", "parser_version": "1.6.0",
            "external_references": [{"index": 3, "text": "Tumblr post", "url": REF_3},
                                    {"index": 9, "text": "unrelated", "url": "https://example.org/9"}],
            "sections": [
                {"kind": "about", "heading": "About", "text": ["Doge is a meme."]},
                {"kind": "origin", "heading": "Origin", "text": [P0, P1],
                 "links": [{"text": "Tumblr", "url": KYM_LINK, "paragraph": 0,
                            "offset": P0.index("Tumblr")}],
                 "images": [{"src": IMG, "caption": "Kabosu", "after_paragraph": 0}],
                 "embeds": [{"url": TIKTOK, "platform": "tiktok", "after_paragraph": 1}]}],
            **over}


def unit(**over):
    return ev.section_unit(entry(**over), "origin")


def origin(*paragraphs):
    return unit(sections=[{"kind": "origin", "heading": "Origin", "text": list(paragraphs)}])


def schema():
    return ev.load_schema(SCHEMA_PATH)[0]


def validator(u=None):
    return ev.make_validator(ev.item_checker(schema()), u or unit())


def event(**over):
    """A model reply row: date WORDS only (2.1.0), places a list (3.0.0);
    ``location=`` is shorthand for one place or none."""
    row = {"sentences": [1], "date_text": "February 23rd, 2010", "locations": ["Tumblr"],
           "location_type": "platform", "actors": ["Atsuko Sato"], "certainty": "confirmed"}
    if "location" in over:
        one = over.pop("location")
        row["locations"] = [one] if one else []
    return {**row, **over}


def E(*sentences, words=None, places=(), actors=(), **over):
    """A row with only what is given: no date words, places or actors."""
    return event(sentences=list(sentences), date_text=words, locations=list(places),
                 actors=list(actors), **over)


def run(*rows, u=None):
    return validator(u)(json.dumps({"events": list(rows)}))


def at(out, first):
    return next(e for e in out if e["sentences"][0] == first)


def spans(out):
    return [e["sentences"] for e in out]


def dates(out):
    return [e["date"] for e in out]


def pick(row, names):
    return row[names[0]] if len(names) == 1 else tuple(row[n] for n in names)


def rows(*names):
    """Every event's fields, in order: comparing with a list pins the count."""
    return lambda out: [pick(r, names) for r in out]


def first(k, *names):
    """The fields of the event that starts at sentence ``k``."""
    return lambda out: pick(at(out, k), names)


def check(paragraphs, model_rows, view, want):
    u = origin(*paragraphs) if paragraphs else unit()
    out = run(*model_rows, u=u)
    assert view(out) == want
    assert ev.audit({"events": list(out)}, u) == []


def routes(handler):
    route = by_model({MINISTRAL: handler})
    return {("POST", CCDD, owc.CHAT_PATH): route, ("POST", UI, owc.CHAT_PATH): route}


def reply(model_rows):
    return routes(lambda body: Resp(200, {"message": {"content": json.dumps({"events": model_rows})}}))


# -- the grammar --------------------------------------------------------------

def test_the_model_grammar_has_no_derived_no_free_text_and_no_cap():
    fmt = ev.request_format(schema())
    item = fmt["properties"]["events"]["items"]
    derived = {"frame_url", "source_section", "source_text", "links", "images", "embeds", "date",
               "date_precision", "date_basis", "date_anchor", "summary"}   # summary: gone in 2.0.0
    assert not derived & (set(item["properties"]) | set(item["required"]))
    assert "sentences" in item["required"] and "date_text" in item["properties"]
    assert "maxItems" not in fmt["properties"]["events"]
    Draft202012Validator.check_schema(fmt)


def test_the_tracked_schema_still_describes_the_whole_stored_row():
    props = schema()["properties"]
    assert {"source_text", "links", "images", "embeds", "actors"} <= set(props)
    assert props["source_text"]["x-derived"]


# -- sentences ----------------------------------------------------------------

@pytest.mark.parametrize("text, want", [
    ("One. Two! Three?", ["One.", "Two!", "Three?"]),
    ("Mr. Smith met J. K. Rowling in the U.S. on Jan. 3rd. Then he left.",
     ["Mr. Smith met J. K. Rowling in the U.S. on Jan. 3rd.", "Then he left."]),
    # chuckster, murica, she-took-the-fucking-kids: a quotation is not cut
    ('On October 15th, 2019, Redditor x wrote, "If I have to see it one more time lol. '
     'I honestly think this level is the worst." Then it spread.',
     ['On October 15th, 2019, Redditor x wrote, "If I have to see it one more time lol. '
      'I honestly think this level is the worst."', 'Then it spread.']),
    ('In May 2012, a subreddit titled "\'Murica! Fuck Yeah!" was created. It grew.',
     ['In May 2012, a subreddit titled "\'Murica! Fuck Yeah!" was created.', 'It grew.']),
    ("He wrote, “One. Two.” Then left.", ["He wrote, “One. Two.”", "Then left."]),
    # its-a-gay-bar-pamela: an unclosed quote does not swallow the paragraph,
    ('Days later, "It\'s a gay bar t-shirts appeared. On October 19th, x shared one.',
     ['Days later, "It\'s a gay bar t-shirts appeared.', 'On October 19th, x shared one.']),
    # and of three straight quotes, which two pair is unknowable: none do
    ('Days later, "It\'s a gay bar t-shirts appeared. Then x shared "the shirt" online.',
     ['Days later, "It\'s a gay bar t-shirts appeared.', 'Then x shared "the shirt" online.']),
    ("It is where RAdm. Daniel Hagari walks. Then singer K. Michelle shouted. Wojak a.k.a. "
     "That Feel Guy frowns. A Super Smash Bros. Ultimate meme spread.",
     ["It is where RAdm. Daniel Hagari walks.", "Then singer K. Michelle shouted.",
      "Wojak a.k.a. That Feel Guy frowns.", "A Super Smash Bros. Ultimate meme spread."]),
    # "on X." ends a sentence (read as an initial, it cost a pilot section); initials do not
    ("It went viral on X. For example, it did.", ["It went viral on X.", "For example, it did."]),
    ("It moved to the platform X. Then it died.", ["It moved to the platform X.", "Then it died."]),
    ("They met in B. Then they left.", ["They met in B.", "Then they left."]),
    ("It went viral on X. For example, @foo posted it.",
     ["It went viral on X.", "For example, @foo posted it."]),
    ("J. K. Rowling wrote it. It sold well.", ["J. K. Rowling wrote it.", "It sold well."]),
    ("By George R. R. Martin now. It sold well.", ["By George R. R. Martin now.", "It sold well."]),
    ("The first book by author George R.R. Martin debuted in 1996. It sold.",
     ["The first book by author George R.R. Martin debuted in 1996.", "It sold."]),
    # a citation marker stays with its sentence; one ending a paragraph is no
    # sentence (687 orphaned citations across a 2,909-section scan)
    ("It went viral. [4] The next day it died.", ["It went viral. [4]", "The next day it died."]),
    ("A character named Mr. Armstrong first appeared. [2]",
     ["A character named Mr. Armstrong first appeared. [2]"]),
    ("The film premiered in the United States. [1]", ["The film premiered in the United States. [1]"]),
    ("It spread to Reddit. [3] [4]", ["It spread to Reddit. [3] [4]"]),
    ("He posted it on Twitter. [6] Then it spread. [7]",
     ["He posted it on Twitter. [6]", "Then it spread. [7]"]),
])
def test_sentences(text, want):
    assert [text[a:b] for a, b in ev.split_sentences(text)] == want


def test_spans_are_verbatim_and_the_model_reads_numbered_sentences_without_markers():
    assert all(P0[a:b] in P0 for a, b in ev.split_sentences(P0))
    text = ev.numbered_text(unit())
    assert text.startswith("1: The original photo") and "\n\n3: On May 7th" in text
    assert "[3]" not in text
    lines = ev.numbered_text(origin("It spread to Reddit. [3]")).splitlines()
    assert all(line.split(": ", 1)[1].strip() for line in lines if line)


# -- the unit -----------------------------------------------------------------

def test_the_whole_section_is_the_unit_never_truncated():
    long = "A sentence about the meme. " * 400
    u = origin(long)
    assert (u["source_chars"], len(u["sentences"])) == (len(long), 400) and "truncated" not in u


def test_the_unit_resolves_only_marked_citations_and_its_stamp_follows_the_media():
    assert unit()["citations"] == {"3": REF_3}
    moved = entry()
    moved["sections"][1]["images"][0]["after_paragraph"] = 1
    assert ev.section_unit(moved, "origin")["source_sha256"] != unit()["source_sha256"]


def test_the_infobox_origin_is_not_the_section_and_no_section_is_no_unit():
    assert "Kabosu" in unit(origin="Tumblr")["paragraphs"][0]
    assert ev.section_unit(entry(), "spread") is None
    assert ev.section_unit({"title": "no url"}, "origin") is None


# -- grounding: every addition seen in review, fed back in ----------------------

@pytest.mark.parametrize("paragraphs, model_rows, view, want", [
    pytest.param(None, [event()], first(1, "sentences", "source_text"), ([1, 2], P0),
                 id="the evidence is the page's, [3] included; 2 continues 1"),
    pytest.param(None, [event(sentences=[2, 3], date_text=None)],
                 lambda out: (lambda t: ("\n\n" in t, all(p in P0 + "\n\n" + P1 for p in t.split("\n\n"))))(
                     at(out, 2)["source_text"]), (True, True),
                 id="a span across paragraphs is joined like the frame text"),
    pytest.param(None, [event(location="Tumblr (the blogging site)")],
                 lambda out: pick(out[0], ("locations", "location_type")), (["Tumblr"], "platform"),
                 id="an added qualifier grounds to the page's own words"),
    pytest.param(["In May 2013 people posted it. Later, users and Atsuko Sato did too."],
                 [E(1, words="In May 2013", actors=["people"]), E(2, actors=["users", "Atsuko Sato"])],
                 rows("actors"), [[], ["Atsuko Sato"]],
                 id="people, users, they name nobody (retroslop)"),
    pytest.param(None, [event(actors=["Atsuko Sato", "Atsuko Sato (blogger)", "Kabosu's vet"])],
                 lambda out: out[0]["actors"], ["Atsuko Sato"],
                 id="an embellished actor grounds, an invented one does not"),
    pytest.param(None, [E(4, words="That same day", actors=["KwandaoRen66"])],
                 first(4, "actors"), ["KwandaoRen66"],
                 id="an actor named elsewhere in the section is kept (coreference)"),
    # 2.1 lost 34 of 421 pilot dates to the year KYM states once and omits after
    pytest.param(None, [E(3, words="May 7th, 2010")], first(3, "date_text", "date", "date_basis"),
                 ("May 7th", "2010-05-07", "stated"), id="a year the page omits no longer costs the date"),
    pytest.param(None, [E(3, words="June 2nd, 2010")], first(3, "date_text", "date"),
                 ("May 7th", "2010-05-07"), id="contradicted date words: the opening words date it"),
    pytest.param(None, [E(2, words="June 2nd, 2010", places=["4chan"])], first(2, "date_text", "date"),
                 (None, None), id="contradicted date words and no opening date: none"),
    pytest.param(None, [event(date_text="May 7th")], lambda out: out[0]["date_text"], None,
                 id="date words from another sentence ground to nothing"),
    pytest.param(None, [E(2, places=["4chan"], certainty="unconfirmed")], first(2, "locations"),
                 ["4chan"], id="typography and markers are not additions"),
    pytest.param(None, [event(location='"Tumblr"')], first(1, "locations"), ["Tumblr"],
                 id="quote marks are not part of a name"),
    pytest.param(None, [event(sentences=[1, 99])], first(1, "sentences"), [1, 2],
                 id="valid sentences survive an invalid one"),
    pytest.param(None, [event(location=None, actors=["Atsuko Sato"]),
                        event(location="Tumblr", actors=["Kabosu"])],
                 lambda out: ([e["sentences"][0] for e in out].count(1), at(out, 1)["locations"],
                              at(out, 1)["actors"]), (1, ["Tumblr"], ["Atsuko Sato", "Kabosu"]),
                 id="the same event twice is one, with what both readings saw"),
    pytest.param(None, [], lambda out: (spans(out), at(out, 1)["date"], at(out, 5)["date"]),
                 ([[1, 2], [3], [4], [5]], "2010-02-23", "2013"),
                 id="an empty reply still covers every sentence (4.0.0)"),
    pytest.param(["The comic depicts a dog. It is drawn in pencil."], [],
                 rows("sentences", "source_text", "date", "actors", "locations"),
                 [([1, 2], "The comic depicts a dog. It is drawn in pencil.", None, [], [])],
                 id="a section that narrates nothing is one undated event"),
    pytest.param(None, [event(summary="A made-up summary", confidence=0.9)],
                 lambda out: {"summary", "confidence"} & set(at(out, 1)), set(),
                 id="unknown keys never reach the row"),
])
def test_grounding(paragraphs, model_rows, view, want):
    check(paragraphs, model_rows, view, want)


@pytest.mark.parametrize("content, error", [
    (json.dumps({"events": [event(sentences=[99])]}), ValueError),  # no evidence: retried, then dead-lettered
    ("Here are the events:", ValueError),
    (json.dumps({"rows": []}), KeyError),
    (json.dumps({"events": {}}), TypeError),
])
def test_a_broken_reply_fails_the_unit(content, error):
    with pytest.raises(error):
        validator()(content)


# -- dates: the pipeline parses the words, the model never dates anything (2.1.0)

NONE = (None, "none")


@pytest.mark.parametrize("args, want", [
    (("February 23rd, 2010",), ("2010-02-23", "day")),
    (("on Feb 23, 2010",), ("2010-02-23", "day")),
    (("the 23rd of February, 2010",), ("2010-02-23", "day")),
    (("May 2013",), ("2013-05", "month")),
    (("early 2013",), ("2013", "year")),
    (("In 2013",), ("2013", "year")),
    (("On May 7th", 2010), ("2010-05-07", "day")),   # a missing year from the section,
    (("On May 7th",), NONE),                          # never from nowhere
    (("February 31st, 2010",), NONE),
    (("In their post",), NONE), (("",), NONE), ((None,), NONE), (("shortly afterwards",), NONE),
    # a decade dates nothing (was 2010-01-01); a year's possessive still dates
    (("During the first half of the 2010s", None), NONE), (("the mid-2000s", None), NONE),
    (("the late 1990s", None), NONE), (("the 2010s", None), NONE),
    (("2016's election", None), ("2016", "year")),
    # the phrase's own year beats the context; either of two days is their month
    (("Around June 7th or June 8th, 2026", 2017), ("2026-06", "month")),
    # joined dates, at the precision that holds them
    (("Between 2009 and 2013",), NONE), (("Between late 2021 and early 2022",), NONE),
    (("April 13th and 14th, 2018",), ("2018-04", "month")),
    (("Between May 28th and June 6th, 2025",), ("2025", "year")),
    (("In September and October 2020",), ("2020", "year")),
    (("mid-May of 2018",), ("2018-05", "month")), (("September of 2007",), ("2007-09", "month")),
    (("May, 1st, 2019",), ("2019-05-01", "day")), (("May, 2019",), ("2019-05", "month")),
])
def test_date_phrases(args, want):
    assert ev.parse_date_phrase(*args) == want


@pytest.mark.parametrize("phrase, days", [
    ("That same day", 0), ("Later that day", 0), ("on the same day", 0), ("An hour later", 0),
    ("a few minutes later", 0), ("The following day", 1), ("a day later", 1),
    ("three days later", 3), ("2 days later", 2),
    ("shortly after", None), ("the following week", None), ("later that year", None),
    ("In 2013", None), ("eventually", None),
])
def test_relative_phrases(phrase, days):
    assert ev.relative_offset(phrase) == days


def test_date_tokens_words_and_names():
    assert ev._points("spider-woman 2099 fan art") == []            # past 2039: a name
    assert ev._points("it may have been posted in 2013") == [(27, 31, 2013, None, None)]
    assert ev._points("in early may, x posted")[0][3] == 5            # the verb may is not May
    assert ev._date_spans("It may have been drawn earlier.") == []
    assert not ev._same_word("face", "facebook") and ev._same_word("tiktok", "tiktoker")
    assert not ev._names_nobody("Who Is Dog")                         # capitalised: a name


# -- relative dates and the timeline -------------------------------------------

DAY_CHAIN = ["On February 23rd, 2010 it was posted. The following day it spread. Three days later it peaked."]


def anchored(out):
    [a] = [e for e in out if e["date_basis"] == "relative"]
    src = {e["event_id"]: e for e in out}[a["date_anchor"]]
    return a["date"], src["sentences"], src["date"]


@pytest.mark.parametrize("paragraphs, model_rows, view, want", [
    # "that same day" is dated from the event before it, not guessed (2026-09-21)
    pytest.param(None, [E(3, words="On May 7th"), E(4, words="That same day")],
                 lambda out: (at(out, 3)["date"], at(out, 4)["date"], at(out, 3)["date_basis"],
                              at(out, 4)["date_basis"], at(out, 4)["date_anchor"] == at(out, 3)["event_id"]),
                 ("2010-05-07", "2010-05-07", "stated", "relative", True),
                 id="a relative event is dated from the one before it"),
    pytest.param(DAY_CHAIN, [E(1, words="On February 23rd, 2010"), E(2, words="The following day"),
                             E(3, words="Three days later")], dates,
                 ["2010-02-23", "2010-02-24", "2010-02-27"], id="the offset is applied"),
    pytest.param(["The comic depicts a dog. That same day, @x posted it."], [E(2, words="That same day")],
                 lambda out: (spans(out), out[0]["date"], out[0]["date_text"]),
                 ([[1, 2]], None, "That same day"), id="nothing before it: undated, words kept"),
    # 10 of 339 pilot dates took their year from captions, festival names, asides
    pytest.param(['On March 13th, 2025, it appeared. The caption read, "Reject modern memes, '
                  'return to 2010." On April 24th, it spread.'],
                 [E(1, words="March 13th, 2025"), E(3, words="April 24th")], dates,
                 ["2025-03-13", "2025-04-24"], id="a missing year comes from a date, not any number"),
    pytest.param(['On September 23rd, 2025, it started. On October 1st, a video said, "We will '
                  'return to January 1st, 2016."'],
                 [E(1, words="September 23rd, 2025"), E(2, words="October 1st")], dates,
                 ["2025-09-23", "2025-10-01"], id="a year quoted later in the event cannot supply it"),
    # 30 of 5,612 sampled events: the year is stated after the first event
    pytest.param(["On April 13th, SethEverman posted a video. On May 2nd, 2019, it was reposted."],
                 [E(1, words="April 13th"), E(2, words="May 2nd, 2019")], dates,
                 ["2019-04-13", "2019-05-02"], id="a year named once dates what precedes it"),
    pytest.param(["On May 25th, it was posted. It spread in June 2019. By March 2020, it was everywhere."],
                 [E(1, words="May 25th"), E(2, words="June 2019"), E(3, words="March 2020")],
                 lambda out: out[0]["date"], None, id="two years later in the section: neither"),
    # 74 of 5,612 sampled events start on a sentence another event starts on
    pytest.param(["On May 4th, 2013 it appeared. It was popular. That same day it reached Reddit."],
                 [E(1, words="May 4th, 2013"), E(1, 2),
                  E(3, words="That same day", places=["Reddit"], location_type="platform")],
                 anchored, ("2013-05-04", [1], "2013-05-04"),
                 id="the anchor is the event, not whatever shares its sentence"),
    pytest.param(["In May 2013 it appeared. The following day it spread."],
                 [E(1, words="In May 2013"), E(2, words="The following day")], dates,
                 ["2013-05", None], id="a relative phrase cannot shift a month-precision date"),
    pytest.param(["In May 2013 it appeared. That same day it spread."],
                 [E(1, words="In May 2013"), E(2, words="That same day")], rows("date", "date_precision"),
                 [("2013-05", "month"), ("2013-05", "month")], id="same day inherits a coarse precision"),
    pytest.param(["On May 1st, 2020 it began. The following day it grew. That same day it peaked."],
                 [E(1, words="On May 1st, 2020"), E(2, words="The following day"),
                  E(3, words="That same day")],
                 lambda out: (dates(out), out[2]["date_anchor"] == out[1]["event_id"]),
                 (["2020-05-01", "2020-05-02", "2020-05-02"], True), id="a chain follows the page"),
    pytest.param(None, [E(3, words="On May 7th"), E(4)], first(4, "date", "date_basis"),
                 ("2010-05-07", "relative"), id="a relative phrase in the sentences counts unquoted"),
    pytest.param(["The screenshot shows that on June 3rd, 2014, a user posted it."], [E(1)], dates, [None],
                 id="an absolute date in the sentences is NOT taken"),
    pytest.param(None, [E(4, words="That same day"), E(3, words="On May 7th")],
                 lambda out: (at(out, 3)["date"], at(out, 4)["date"], at(out, 4)["date_basis"]),
                 ("2010-05-07", "2010-05-07", "relative"), id="resolution follows the page, not the reply"),
    pytest.param(None, [E(2, words="That same day", places=["4chan"]), event()],
                 lambda out: (at(out, 2)["date"], at(out, 1)["date"]), (None, "2010-02-23"),
                 id="a later event never dates an earlier one"),
    # 3.0.0: the page is one timeline (the 2026-09-24 review of 127 sections)
    pytest.param(["On April 13th, 2013, user mosmi defined it as a meme inspired by the tweet from "
                  "April 2011. On April 15th, BuzzFeed featured it."],
                 [E(1, words="April 13th, 2013"), E(2, words="April 15th")], dates,
                 ["2013-04-13", "2013-04-15"], id="ed-balls: a referenced date does not move the story"),
    pytest.param(["On April 13th, 2013, user mosmi defined it. The entry, citing the tweet from April "
                  "2011, had 900 likes as of May 2019. On April 15th, BuzzFeed featured it."],
                 [E(1, words="April 13th, 2013"), E(2, words="May 2019"), E(3, words="April 15th")],
                 dates, ["2013-04-13", None, "2013-04-15"], id="dates inside an undated event set no year"),
    pytest.param(["The template began seeing most use on Twitter after November 10th, 2021, when user "
                  "@BonerWizard posted the screenshot."], [E(1, words="November 10th, 2021")], dates,
                 ["2021-11-10"], id="use-the-voice: after a bare date is not a bound"),
    pytest.param(["Serafinowicz did not start regularly putting out videos until August 2016, when he "
                  "posted nine."], [E(1, words="August 2016")], dates, ["2016-08"],
                 id="sassy-trump: not until is a start"),
    pytest.param(["The page was used on Twitter until 2020."], [E(1, words="2020")], dates, [None],
                 id="a plain until is an end"),
    # hallway-swimming: two events quoting "the next day" are one time, not a chain
    pytest.param(["On April 2nd, 2013, Cole Pugsley uploaded a version. The video was shared on the "
                  "Huffington Post the next day, followed by Smosh."],
                 [E(1, words="April 2nd, 2013"), E(2, words="the next day", places=["Huffington Post"]),
                  E(2, words="the next day", places=["Smosh"])],
                 lambda out: (dates(out), out[1]["locations"]),
                 (["2013-04-02", "2013-04-03"], ["Huffington Post", "Smosh"]),
                 id="the same words in one sentence are one time"),
    pytest.param(["On April 2nd, 2013, Cole Pugsley uploaded a version. The video was shared on the "
                  "Huffington Post the next day, followed by Smosh."],
                 [E(1, words="April 2nd, 2013"), E(2, words="the next day", places=["Huffington Post"]),
                  E(2, places=["Smosh"])], dates, ["2013-04-02", "2013-04-03"],
                 id="and still one when the twin quotes no words"),
    pytest.param(["The fan art took off on September 24th, 2018. Popular examples from that date include "
                  "pieces by @angstrom."], [E(1, words="September 24th, 2018"), E(2, words="that date")],
                 lambda out: out[1]["date"], "2018-09-24", id="that date is that day"),
    pytest.param(["On November 26th, 2016, the account officialashtonj was created. It has nearly 1,700 "
                  "followers as of December 2nd, 2016. The following day, singer Tyrese posted the photo."],
                 [E(1, words="November 26th, 2016"), E(2, words="December 2nd, 2016"),
                  E(3, words="The following day")],
                 lambda out: (spans(out), dates(out)), ([[1, 2], [3]], ["2016-11-26", "2016-11-27"]),
                 id="honey-bun-baby: an as-of statistic is no date and no anchor"),
    pytest.param(["Sometime prior to August 2019, rapper Stunna Boy sent a DM."],
                 [E(1, words="prior to August 2019")], dates, [None], id="prior to is a bound"),
    pytest.param(["After Cody Ko's April 30th, 2024, video, TikToker @c clipped it."],
                 [E(1, words="April 30th, 2024")], dates, [None], id="after X's dated video is a bound"),
    pytest.param(["By March 26th, 2025, the sound had 9,200 posts."], [E(1, words="By March 26th, 2025")],
                 dates, [None], id="by a date is a bound"),
    pytest.param(["An animation was posted by YouTuber Kaliido_scope on January 4th, 2023."],
                 [E(1, words="January 4th, 2023")], dates, ["2023-01-04"],
                 id="posted by X on a date is a date"),
    pytest.param(["In December 2006, LusoSkav drew it. A month earlier, russxl posted a video. On "
                  "September 9th, 2005, the first YTMND was posted. That month, it appeared on a forum. "
                  "On November 6th, 2017, Apple answered. Meanwhile, @hot_kommodity_ tweeted a GIF."],
                 [E(1, words="December 2006"), E(2, words="A month earlier"), E(3, words="September 9th, 2005"),
                  E(4, words="That month"), E(5, words="November 6th, 2017"), E(6, words="Meanwhile")],
                 rows("date", "date_precision"),
                 [("2006-12", "month"), ("2006-11", "month"), ("2005-09-09", "day"), ("2005-09", "month"),
                  ("2017-11-06", "day"), ("2017-11-06", "day")], id="relative months, years and meanwhile"),
    pytest.param(["It was first posted to Facebook on October 20th, 2019. A capture was posted to YouTube "
                  "by user Frazzle Tazz on the 21st."],
                 [E(1, words="October 20th, 2019"), E(2, words="the 21st")], dates,
                 ["2019-10-20", "2019-10-21"], id="muvvafukka: a bare ordinal takes the timeline's month"),
    # a sentence that opens with its date dates its event without date words (2026-09-24)
    pytest.param(["On May 4th, 2019, X user @a posted a clip. On September 13th, user elie posted a video "
                  "showing the clip."], [E(1, words="May 4th, 2019", actors=["@a"]), E(2, actors=["elie"])],
                 lambda out: (dates(out), out[1]["date_text"]),
                 (["2019-05-04", "2019-09-13"], "September 13th"), id="an opening date is read"),
    pytest.param(["On September 9th, 2005, the first YTMND was posted. That month, the Moran appeared in "
                  "a photoshop series."], [E(1, words="September 9th, 2005"), E(2)],
                 lambda out: pick(out[1], ("date", "date_text")), ("2005-09", "That month"),
                 id="get-a-brain-morans: an opening relative date is read"),
    pytest.param(["On September 9th, 2005, the first YTMND was posted. That month, the Moran appeared in "
                  "a photoshop series."], [E(1, words="September 9th, 2005"), E(2, words="September 9th, 2005")],
                 lambda out: pick(out[1], ("date", "date_text")), ("2005-09", "That month"),
                 id="even when the model copied the sentence before's date"),
    pytest.param(["Users drew Spider-Woman fan art, starting on June 5th, 2023."], [E(1)], dates, [None],
                 id="a date deeper in the sentence is not an opening date"),
    pytest.param(["Early in the 2021 remake of Dune, Lady Jessica speaks."], [E(1)], dates, [None],
                 id="a year inside a title is not an opening date"),
    pytest.param(["As of May 5th, 2013, the video has 900 views."], [E(1)], dates, [None],
                 id="as of is not an opening date"),
    # the 2026-09-25 holdout (60 entries never looked at)
    pytest.param(["On September 13th, 2015, the infographic was posted on 9gag. The graphic remained "
                  "relatively unknown until January 5th, 2017, when Twitter user @RahSenpai posted "
                  "screenshots. That day, Twitter users @a and @b replied with photographs."],
                 [E(1, words="September 13th, 2015", places=["9gag"]),
                  E(2, words="January 5th, 2017", places=["Twitter"], actors=["@RahSenpai"]),
                  E(3, words="That day", places=["Twitter"], actors=["@a", "@b"])],
                 dates, ["2015-09-13", "2017-01-05", "2017-01-05"],
                 id="how-to-break-your-thumb-ligament: until a date when it happened is that date"),
    pytest.param(["On July 1st, 2020, TikToker OkCron uploaded day 1. OkCron continued posting updates "
                  "over the following month."],
                 [E(1, words="July 1st, 2020", places=["TikTok"], actors=["OkCron"]),
                  E(2, words="the following month", actors=["OkCron"])], dates, ["2020-07-01", None],
                 id="no-poop-july: over the following month is a stretch"),
    pytest.param(["On July 1st, 2020, TikToker OkCron uploaded day 1. The following month, OkCron posted "
                  "day 32."],
                 [E(1, words="July 1st, 2020", places=["TikTok"], actors=["OkCron"]),
                  E(2, words="The following month", actors=["OkCron"])], dates, ["2020-07-01", "2020-08"],
                 id="the following month is the next month"),
])
def test_dating(paragraphs, model_rows, view, want):
    check(paragraphs, model_rows, view, want)


def spread_after(origin_text, origin_rows, spread_text, spread_rows):
    """3.0.0: Spread reads its timeline from Origin's stored events."""
    e = entry(sections=[{"kind": "origin", "heading": "Origin", "text": origin_text},
                        {"kind": "spread", "heading": "Spread", "text": spread_text}])
    o, s = ev.section_unit(e, "origin"), ev.section_unit(e, "spread")
    first_rows = run(*origin_rows, u=o)
    s["prior"] = [{k: r[k] for k in ("event_id", "sentences", "date", "date_precision")}
                  for r in first_rows]
    return first_rows, run(*spread_rows, u=s), s


def test_spread_takes_its_year_from_origin():
    # ios-question-mark-box: 2.x dated none of its 8 events; atlorgy lost all 9
    _, spread, _ = spread_after(["On October 31st, 2017, Apple released the iOS 11.1 update."],
                                [E(1, words="October 31st, 2017")],
                                ["On November 5th, Redditor catitobandito asked about it. "
                                 "On November 9th, Apple fixed the bug."],
                                [E(1, words="November 5th"), E(2, words="November 9th")])
    assert dates(spread) == ["2017-11-05", "2017-11-09"]


def test_that_day_at_the_top_of_spread_is_origins_last_day():
    # star-wars-rise-of-skywalker: that day is the poster's release, stated at the end of Origin
    origin_rows, [row], s = spread_after(
        ["On January 9th, 2018, Tumblr user kylosroboarm posted parodies. "
         "On April 12th, 2019, the first poster was revealed."],
        [E(1, words="January 9th, 2018"), E(2, words="April 12th, 2019")],
        ["For example, that day, Facebook user elliott.boydstringer posted a version."],
        [E(1, words="that day")])
    assert (row["date"], row["date_basis"], row["date_anchor"]) == \
        ("2019-04-12", "relative", origin_rows[1]["event_id"])
    assert ev.audit({"events": [row]}, s) == []


@pytest.mark.parametrize("tamper, problem", [
    ({"source_text": "The original photo of a CAT was posted."}, "verbatim"),
    ({"actors": ["Atsuko Sato (blogger)"]}, "actor"),
    ({"locations": ["Tumblr (site)"]}, "location"),
    ({"date": "2010-03-23"}, "resolves to"),
    ({"date_basis": None}, "no basis"),
    ({"links": [{"url": "https://evil.example", "kind": "link"}]}, "not on the page"),
])
def test_the_audit_catches_what_a_broken_validator_would_let_through(tamper, problem):
    record = {"events": list(run(event()))}
    record["events"][0].update(tamper)
    problems = ev.audit(record, unit())
    assert any(problem in p for p in problems), problems


def test_the_audit_refuses_a_bad_relative_date_and_a_sentence_in_no_event():
    u = origin("On May 4th, 2013 it appeared. The following day it spread.")
    out = run(E(1, words="May 4th, 2013"), E(2, words="The following day"), u=u)
    out[1]["date"] = "2013-05-09"
    assert any("relative date" in p for p in ev.audit({"events": out}, u))
    out = [e for e in run(event()) if e["sentences"][0] != 5]
    assert ev.audit({"events": out}, unit()) == ["sentences [5] are in no event"]


# -- who and where (3.0.0, as the review asked for them) -----------------------

SITH = ("For example, that day, Facebook user elliott.boydstringer posted a version in the Star "
        "Wars Sithposting shitposting group.")
GROUP = "Star Wars Sithposting shitposting group"


@pytest.mark.parametrize("paragraphs, model_rows, view, want", [
    pytest.param(["On August 1st, 2019, the Facebook page King K Rool posted a video by user @cappnriki."],
                 [E(1, places=["Facebook", "King K Rool"], actors=["@cappnriki"])],
                 lambda out: [(r["locations"], sorted(r["actors"])) for r in out],
                 [(["Facebook"], ["@cappnriki", "King K Rool"])],
                 id="chuckster: the poster's own page is an actor, not a place"),
    pytest.param(["On October 18th, 2018, Facebook meme page Grodosbova posted a meme."],
                 [E(1, places=["Facebook", "Grodosbova"], actors=["Grodosbova"])],
                 rows("locations", "actors"), [(["Facebook"], ["Grodosbova"])],
                 id="an actor is never also a place"),
    pytest.param(["The practice likely began in an anime forum about the show. A post in an Imgur "
                  "compilation featured it."],
                 [E(1, places=["anime forum about the show"]), E(2, places=["Imgur"])],
                 rows("locations"), [[], ["Imgur"]], id="a common noun after a/an names nothing"),
    pytest.param(["According to Pixiv Encyclopedia, the first occurrence was a game released by Hrathnir."],
                 [E(1, actors=["Pixiv Encyclopedia", "Hrathnir"])], rows("actors"), [["Hrathnir"]],
                 id="a cited source is not an actor"),
    # murica: grounded section-wide to a Tumblr blog's name two sentences away
    pytest.param(["In October 2011, the Tumblr Fuck Yeah Murica launched. In May 2012, a subreddit "
                  "titled Murica! Fuck Yeah! was created."], [E(2, places=["Murica Fuck Yeah"])],
                 lambda out: "Fuck Yeah Murica" in at(out, 2)["locations"], False,
                 id="murica: a place is grounded in the event's own sentence first"),
    pytest.param(["In October 2011, the Tumblr Fuck Yeah Murica launched. In May 2012, a subreddit "
                  "titled Murica! Fuck Yeah! was created."], [E(2, 3, places=["Murica Fuck Yeah"])],
                 first(2, "locations"), ["Murica! Fuck Yeah"], id="murica: across its two sentences"),
    pytest.param(["On July 12th, 2018, the person who uploaded the clip, fishpupper, posted it again."],
                 [E(1, words="July 12th, 2018", actors=["person who uploaded the clip", "fishpupper"])],
                 rows("actors"), [["fishpupper"]], id="a description of someone is not a name"),
    pytest.param(['On July 23rd, 2007, Yahoo Answers user posed a question. Tumblr users and 4chan\'s '
                  'moderators replied. Facebook page "Uzuki\'s ganbarimasu" and the ApeThrowbacks channel '
                  'reposted it.'],
                 [E(1, 2, 3, actors=["Yahoo Answers user", "Tumblr users", "4chan's moderators",
                                     '"Uzuki\'s ganbarimasu"', "ApeThrowbacks channel"])],
                 rows("actors"), [["Uzuki's ganbarimasu", "ApeThrowbacks"]],
                 id="names come without roles, crowds, quotes or channel"),
    pytest.param(["On November 28th, artist Chance the Rapper posted a picture. On April 14th, YouTuber "
                  "Ghost X Channel posted a video."], [E(1, 2, actors=["Chance the Rapper", "Ghost X Channel"])],
                 rows("actors"), [["Chance the Rapper", "Ghost X Channel"]],
                 id="a name that ends like a role is a name"),
    pytest.param(["On April 12th, 2019, the official Star Wars Twitter account posted the poster."],
                 [E(1, places=["Twitter", "official Star Wars Twitter account"], actors=["official Star Wars"])],
                 rows("actors", "locations"), [(["official Star Wars"], ["Twitter"])],
                 id="a poster is listed once by its cleaned name"),
    pytest.param(["By November, Relentlessly Optimistic posted the image."],
                 [E(1, places=["Relentlessly Optimistic"])], rows("locations", "actors"),
                 [([], ["Relentlessly Optimistic"])], id="whoever posted is an actor"),
    pytest.param(["On August 6th, TMZ published footage of Dawkins."], [E(1, places=["TMZ"])],
                 rows("locations", "actors"), [([], ["TMZ"])], id="whoever published is an actor"),
    pytest.param(["On May 15th, a post in an Imgur compilation of Avengers memes featured one of the first "
                  "exploitables."], [E(1, places=["compilation of Avengers memes"])], rows("locations", "actors"),
                 [(["compilation of Avengers memes"], [])], id="a work is not a doer"),
    pytest.param(["In October 2011, the single topic Tumblr Fuck Yeah Murica launched."],
                 [E(1, places=["Fuck Yeah Murica"])], rows("locations", "actors"), [(["Fuck Yeah Murica"], [])],
                 id="a page that launched is not a doer"),
    pytest.param(["The clip was posted to YouTube."], [E(1, places=["YouTube"])], rows("locations"),
                 [["YouTube"]], id="a platform after the verb stays a place"),
    pytest.param(["Meanwhile, the TomoNews US YouTube channel uploaded a video."],
                 [E(1, places=["YouTube", "TomoNews US"])], rows("locations", "actors"),
                 [(["YouTube"], ["TomoNews US"])], id="revenant: a channel named before its platform posts"),
    pytest.param(["It was posted to the personal blog of user Brian on tumblr."],
                 [E(1, places=["personal blog", "tumblr"])], rows("locations"), [["tumblr"]],
                 id="lowercase common words are no place"),
    pytest.param(["On February 13th, 2024, Cody Ko uploaded a video. Ko said Cody was tired."],
                 [E(1, 2, actors=["Cody Ko", "Cody", "Ko"])], rows("actors"), [["Cody Ko"]],
                 id="a part of a name beside the whole is dropped"),
    pytest.param(["On November 13th, 2023, the IDF YouTube channel posted a video."],
                 [E(1, words="November 13th, 2023", places=["YouTube"], actors=["IDF", "IDF YouTube channel"])],
                 rows("actors", "locations"), [(["IDF"], ["YouTube"])], id="a poster named twice is one actor"),
    pytest.param(["It was first posted to Facebook on October 20th, 2019. Butler made a screenshot of "
                  "Hilzinger's face his cover photo."], [E(2, places=["Facebook"])],
                 first(2, "locations"), ["Facebook"], id="muvvafukka: a stem must be most of the word"),
    pytest.param([SITH], [E(1, places=["Facebook"], actors=["elliott.boydstringer", GROUP])],
                 rows("actors", "locations"), [(["elliott.boydstringer"], ["Facebook", GROUP])],
                 id="a group is a location, not an actor"),
    pytest.param([SITH], [E(1, places=["Facebook", GROUP], actors=["elliott.boydstringer"])],
                 lambda out: [len(r["locations"]) for r in out], [2], id="platform and venue are both kept"),
    pytest.param([SITH], [E(1, actors=["Facebook user elliott.boydstringer"])], rows("actors"),
                 [["elliott.boydstringer"]], id="role words are not part of a name"),
    pytest.param(["On March 30th, 2022, an anonymous Soyjak.party user posted it to a personal blog and to "
                  "Soyjak.party."],
                 [E(1, places=["a personal blog", "Soyjak.party"], actors=["an anonymous Soyjak.party user"])],
                 rows("actors", "locations"), [([], ["Soyjak.party"])], id="what names nobody is dropped"),
    pytest.param(["The image was reblogged by Monorail and Funny Junk on September 13th, 2009."],
                 [E(1, places=["Monorail and Funny Junk"])], rows("locations"), [["Monorail", "Funny Junk"]],
                 id="two venues in one string are two places"),
    pytest.param(["On June 15th, 2016, Trump spoke at the Fox Theater in Atlanta, Georgia."],
                 [E(1, places=["Fox Theater in Atlanta, Georgia"], location_type="geo")], rows("locations"),
                 [["Fox Theater in Atlanta, Georgia"]], id="a place name with a comma stays whole"),
    pytest.param(["On June 25th, 2022, iFunnyer Choctaw posted a version."],
                 [E(1, places=["iFunnyer"], actors=["Choctaw"])], rows("locations"), [["iFunny"]],
                 id="the platform in a user word is the place"),
    pytest.param(["Peanut also had a TikTok account under the name @peanut_the_squirrel12, which posted its "
                  "first video."], [E(1, places=["TikTok", "@peanut_the_squirrel12"])],
                 rows("locations", "actors"), [(["TikTok"], ["@peanut_the_squirrel12"])],
                 id="a handle is an actor, never a place"),
    pytest.param(['The organization tweeted at YouTube, "This content has no place on @YouTube or anywhere '
                  'else."'], [E(1, places=["Twitter", "@YouTube"])], rows("actors"), [[]],
                 id="a handle only quoted is not made an actor"),
    pytest.param(["It was posted to the personal blog of Something Awful user Brian Somerville on July "
                  "20th, 2006."],
                 [E(1, words="July 20th, 2006", places=["personal blog of Something Awful user Brian Somerville"],
                    actors=["Brian Somerville"])], rows("locations", "actors"), [([], ["Brian Somerville"])],
                 id="the actor's own page named around them is no place"),
    pytest.param(['On March 1st, Viner Shourouk Abdalla uploaded clips. Instagrammer "fawaz_Alfahad" ripped '
                  'the video.'], [E(1, 2, actors=["Viner Shourouk Abdalla", 'Instagrammer "fawaz_Alfahad'])],
                 rows("actors"), [["Shourouk Abdalla", "fawaz_Alfahad"]], id="Viner and a stray quote are not the name"),
    # an actor or place named only AFTER its event (2026-09-24)
    pytest.param(["On November 13th, 2023, the IDF posted a video. The claim was refuted online that day by "
                  "many. For example, that day, X user @zoo_bear made a post purportedly debunking the claim."],
                 [E(1, words="November 13th, 2023", actors=["IDF"]),
                  E(2, words="that day", places=["X"], actors=["@zoo_bear"])],
                 lambda out: (pick(out[1], ("sentences", "actors", "locations")), "@zoo_bear" in out[1]["source_text"]),
                 (([2, 3], ["@zoo_bear"], ["X"]), True), id="the next sentence it came from joins the event"),
    pytest.param(["On May 16th, 2018, Redditor WhiteCrowWolf posted a question. The following day, on May "
                  "17th, 2018, a Redditor posted in the /r/NoStupidQuestions subreddit."],
                 [E(1, words="May 16th, 2018", places=["/r/NoStupidQuestions"], actors=["WhiteCrowWolf"]),
                  E(2, words="May 17th, 2018", places=["/r/NoStupidQuestions"])],
                 lambda out: (pick(out[0], ("sentences", "locations", "location_type")), out[1]["locations"]),
                 (([1], [], "unknown"), ["/r/NoStupidQuestions"]), id="a later happening's place is dropped"),
    pytest.param(["On May 4th, 2013, user x posted in the Foo Fans group. Later that day, user y posted "
                  "there too."], [E(1, words="May 4th, 2013", places=["Foo Fans group"], actors=["x"]),
                                  E(2, words="Later that day", places=["Foo Fans group"], actors=["y"])],
                 lambda out: out[1]["locations"], ["Foo Fans group"], id="a place named earlier is referred back to"),
    pytest.param(["On May 4th, 2013, x posted a video. That day, @y reposted it on Tumblr."],
                 [E(1, words="May 4th, 2013", places=["Tumblr"], actors=["x"]),
                  E(2, words="That day", places=["Tumblr"], actors=["@y"])],
                 lambda out: pick(out[0], ("sentences", "locations")), ([1], []),
                 id="a next sentence another event narrates is not joined"),
    pytest.param(["On May 4th, 2013, x posted a video. The next day, @y reposted it on Tumblr."],
                 [E(1, words="May 4th, 2013", places=["Tumblr"], actors=["x", "@y"])],
                 lambda out: (pick(at(out, 1), ("sentences", "actors", "locations")),
                              pick(at(out, 2), ("sentences", "actors", "date"))),
                 (([1], ["x"], []), ([2], ["@y"], "2013-05-05")),
                 id="a next sentence a day later is its own event"),
    pytest.param(["On May 4th, 2013, YouTuber Wimpyapple uploaded a clip. On May 9th, 2013, the clip was "
                  "reposted to YouTube."], [E(1, places=["YouTube"])], lambda out: out[0]["locations"],
                 ["YouTube"], id="YouTuber names YouTube"),
    pytest.param(["That day, journalist Tony Webster tweeted a screenshot. On May 9th, 2013, Twitter user "
                  "@y did too."], [E(1, places=["Twitter"])], lambda out: out[0]["locations"], ["Twitter"],
                 id="tweeted names Twitter"),
    pytest.param(["On October 20th, 2019, Caiden Butler posted a video. Butler made a screenshot his cover "
                  "photo."], [E(1, words="October 20th, 2019", actors=["Caiden Butler"]),
                              E(2, actors=["Caiden Butler"])],
                 lambda out: out[1]["actors"], ["Caiden Butler"], id="a full name named earlier beats a surname"),
    pytest.param(["That evening, Warski posted a tweet. On January 1st, 2018, a thread about the Adam "
                  "Warski meme was submitted to /pol/."], [E(1, words="That evening", actors=["Adam Warski"])],
                 lambda out: out[0]["actors"], ["Warski"], id="adam-warski: a name only later aligns to its own words"),
    pytest.param(["That day, Facebook user elliott.boydstringer posted in the Star Wars Sithposting "
                  "shitposting group. Throughout the weekend, others began sharing parodies in the group."],
                 [E(2, places=["Facebook", GROUP])], first(2, "locations"), ["Facebook", GROUP],
                 id="a group named earlier is the group"),
    pytest.param(["On October 13th, 2009, another video was uploaded by Kailyn Jensen. Between 2009 and "
                  "2013, a handful of other videos were uploaded to YouTube."],
                 [E(1, words="October 13th, 2009", places=["YouTube"], actors=["Kailyn Jensen"])],
                 lambda out: (pick(at(out, 1), ("sentences", "locations")), at(out, 2)["sentences"]),
                 (([1], []), [2]), id="a next sentence with its own date is not joined"),
])
def test_who_and_where(paragraphs, model_rows, view, want):
    check(paragraphs, model_rows, view, want)


# -- what joins an event: reception, description, background ------------------
# (2026-09-24: 127 of 1,370 sentences were in no event; 4.0.0 puts every one in one)

POSTED = "On May 4th, 2013, Tumblr user x posted a comic. "
X_POST = E(1, words="May 4th, 2013", places=["Tumblr"], actors=["x"])
STARS = ("Following the poster's release, parodies using the design of the image became much more "
         "common. For example, that day, Facebook user elliott.boydstringer posted a version criticizing "
         "some of the character reveals in the first trailer in the Star Wars Sithposting shitposting "
         "group. The post received more than 2,000 reactions, 410 comments and 500 shares in three days "
         "(shown below, left).")


@pytest.mark.parametrize("paragraphs, model_rows, view, want", [
    pytest.param([STARS, "Throughout the weekend, others began sharing parodies of the poster in the group."],
                 [E(1), E(2, words="that day", places=["Facebook", GROUP], actors=["elliott.boydstringer"]),
                  E(4, words="Throughout the weekend")],
                 lambda out: (spans(out), out[1]["source_text"].endswith("(shown below, left).")),
                 ([[1], [2, 3], [4]], True), id="the reviewer's missed sentence"),
    pytest.param(["On May 4th, 2013, Tumblr user x posted a comic. The comic depicts a cat on a roof. The "
                  "post received more than 10,000 notes in a year (shown below)."], [X_POST],
                 rows("sentences", "date"), [([1, 2, 3], "2013-05-04")], id="the description in between joins too"),
    pytest.param([POSTED + "As of April 2014, the post has more than 18,500 notes."], [X_POST],
                 rows("sentences"), [[1, 2]], id="an as-of count is reception"),
    pytest.param([POSTED + "The post received over 5,300 notes."], [X_POST, E(2)], rows("sentences"),
                 [[1, 2]], id="a stat-only event is folded into the post"),
    pytest.param(["The post received over 5,300 notes. On May 4th, 2013, Tumblr user x posted a comic."],
                 [E(1), E(2, words="May 4th, 2013", places=["Tumblr"], actors=["x"])],
                 rows("sentences", "date"), [([1, 2], "2013-05-04")],
                 id="a stat-only event with nothing before it joins the first"),
    pytest.param(["On May 4th, 2013, iFunny user x posted a meme. This post was liked 124 times and shared "
                  "27 times."], [E(1, words="May 4th, 2013", places=["iFunny"], actors=["x"])],
                 rows("sentences"), [[1, 2]], id="counted in times is reception"),
    pytest.param(["On May 4th, 2013, iFunny user x posted a meme. The post received over 1,300 smiles in one "
                  "year."], [E(1, words="May 4th, 2013", places=["iFunny"], actors=["x"])],
                 rows("sentences"), [[1, 2]], id="counted in smiles is reception"),
    *[pytest.param([f'On May 5th, 2009, Twitter user @x posted it. They wrote, {words}'],
                   [E(1, words="May 5th, 2009", places=["Twitter"], actors=["@x"]), E(2)],
                   rows("sentences"), [[1, 2]], id=f"periodt: the post's own words join it {i}")
      for i, words in enumerate(('"Churches should pay property tax periodt."', '"I posted this yesterday, lol."'))],
    *[pytest.param(["On May 23rd, 2018, the channel PBS Space Time published a video. " + text],
                   [E(1, words="May 23rd, 2018", places=["YouTube"], actors=["PBS Space Time"]), E(2)],
                   rows("sentences"), [[1, 2]], id=f"an event describing the work is folded into it {i}")
      for i, text in enumerate(("The post features host Matthew O'Dowd discussing physics.",
                                "In the video, the computer malfunctions and the boy screams.",
                                "The multi-pane comic strip depicted Computer Reaction Guy."))],
    *[pytest.param(["On March 2, 2008, the picture was posted on icanhascheezburger. " + text],
                   [E(1, words="March 2, 2008"), E(2)], spans, [[1], [2]],
                   id=f"the work's spread is not a description {i}")
      for i, text in enumerate(("The picture was very well-received, and variations flowed forth.",
                                "The comic grew into a popular exploitable shortly after.",
                                "The video was also the source of dozens of photoshops.",
                                "The image, which shows a cat, went viral on Tumblr."))],
    *[pytest.param([POSTED + second], [X_POST], lambda out: (out[0]["sentences"], at(out, 2)["sentences"]),
                   ([1], [2]), id=f"a possible missed happening is its own event {i}")
      for i, second in enumerate((
          "One such post by YouTuber Nathan Zed has gained 22,800 retweets.",
          "One related video, by animator OneyG, received 4 million views.",
          "A post by crybabygrande received 2,000 likes.",
          "The next day, X user @y made a GIF that received 5.1 million views.",
          "A March 5th reupload of the clip received over 400,000 views.",
          "It was then reposted to Reddit, where it gained 1,200 upvotes."))],
    *[pytest.param([POSTED + second], [X_POST], rows("sentences", "date"), [([1, 2], "2013-05-04")],
                   id=f"the work's reception at a later time continues it {i}")
      for i, second in enumerate(("The next day, the video received 5.1 million views.",
                                  "The next day it had 900 upvotes."))],
    pytest.param(["On May 4th, 2023, TikToker x posted a clip. After Link is struck by the Hinox, he falls "
                  "on all fours. The clip gained over 1.6 million views in five days (shown below)."],
                 [E(1, words="May 4th, 2023", places=["TikTok"], actors=["x"])], rows("sentences"),
                 [[1, 2, 3]], id="links-balls: a passive by in a description is crossed"),
    pytest.param([POSTED + "In 2014, Reddit users posted edits. The post received 900 upvotes."], [X_POST],
                 lambda out: (at(out, 1)["sentences"], at(out, 2)["sentences"]), ([1], [2, 3]),
                 id="a gap that narrates something is not crossed"),
    pytest.param(["On February 17th, Viner x posted a clip. In two months, the Vine received over 26,000 "
                  "loops and 700 revines."],
                 [E(1, words="February 17th, 2016", places=["Vine"], actors=["x"]), E(2, words="In two months")],
                 rows("sentences"), [[1, 2]], id="vine counts and duration words are reception"),
    pytest.param(["Doge is a Shiba Inu. On May 4th, 2013, x posted a photo of her."],
                 [E(2, words="May 4th, 2013", actors=["x"])], rows("sentences", "date"),
                 [([1, 2], "2013-05-04")], id="background before the first happening joins it"),
    pytest.param(["On May 4th, 2013, x posted a comic. The comic depicts a dog. The post received 900 upvotes."],
                 [E(1, words="May 4th, 2013", actors=["x"])], rows("sentences", "source_text"),
                 [([1, 2, 3], "On May 4th, 2013, x posted a comic. The comic depicts a dog. "
                   "The post received 900 upvotes.")], id="a description or a count continues the event"),
    # only the model's own sentences are read for an unquoted relative phrase
    pytest.param(["On May 4th, 2013, x posted a comic. Later, y posted a remix. The next day it had 900 upvotes."],
                 [E(1, words="May 4th, 2013", actors=["x"]), E(2, actors=["y"])],
                 lambda out: (spans(out), at(out, 2)["date"]), ([[1], [2, 3]], None),
                 id="a joined sentence never dates the event"),
    pytest.param(["On May 4th, 2013, x posted a comic. On June 1st, 2013, u/zed reposted it to Reddit."],
                 [E(1, words="May 4th, 2013", actors=["x"])],
                 first(2, "sentences", "date", "actors", "locations"), ([2], "2013-06-01", ["u/zed"], []),
                 id="a missed happening is its own event with its own date"),
    *[pytest.param(None, model_rows,
                   lambda out: {i for e in out for i in range(e["sentences"][0], e["sentences"][-1] + 1)},
                   {1, 2, 3, 4, 5}, id=f"every record covers every sentence {i}")
      for i, model_rows in enumerate(([], [event()], [E(3, words="On May 7th")]))],
    # prompt 9 made 118 such events of the review sample's sentences
    *[pytest.param([POSTED + second], [X_POST, E(2)], spans, [[1, 2]],
                   id=f"an event made of commentary is folded {i}")
      for i, second in enumerate(("One of the more prevalent names was \"Operation Crawler\".",
                                  "This is the earliest known version of the meme.",
                                  "The original dialogue was:"))],
    pytest.param([POSTED + "On Instagram, there are over 2 million images tagged #murica as of June 2017."],
                 [X_POST, E(2, words="as of June 2017", places=["Instagram"])], spans, [[1, 2]],
                 id="a count as of a date is folded too"),
    *[pytest.param([POSTED + second], [X_POST, E(2)], spans, [[1], [2]],
                   id=f"an event where someone does something is not folded {i}")
      for i, second in enumerate(("Snopes ultimately labeled the theory as false.",
                                  "The Rake was eventually added to horror story databases.",
                                  "This site spawned many other sites editing the GIF image.",
                                  "Other social media users took footage of the dance and paired it with other music.",
                                  "The picture was very well-received, and variations flowed forth."))],
    pytest.param(["The exact origin of the video is unknown. On May 4th, 2013, Tumblr user x posted it."],
                 [E(1), E(2, words="May 4th, 2013", places=["Tumblr"], actors=["x"])],
                 lambda out: (spans(out), out[0]["date"], out[0]["actors"]), ([[1, 2]], "2013-05-04", ["x"]),
                 id="an opening commentary event joins the first happening"),
])
def test_what_joins_an_event(paragraphs, model_rows, view, want):
    check(paragraphs, model_rows, view, want)


# -- attachments: by position, never by the model ------------------------------

@pytest.mark.parametrize("model_rows, view, want", [
    ([event()], lambda out: {"url": KYM_LINK, "text": "Tumblr", "kind": "link"} in at(out, 1)["links"], True),
    ([event()], lambda out: {"url": REF_3, "text": "[3]", "kind": "citation"} in at(out, 1)["links"], True),
    ([E(2, places=["4chan"])], first(2, "links"), []),
    ([event()], lambda out: ([i["src"] for i in at(out, 1)["images"]], at(out, 1)["embeds"]), ([IMG], [])),
    ([E(5, words="In 2013", places=["Reddit"])], first(5, "embeds", "images"),
     ([{"url": TIKTOK, "platform": "tiktok"}], [])),
    # 2.1.1 audited only the date words and refused good records: 2 of 3 chunks
    ([E(3, words="On May 7th"), E(4)], first(4, "date_basis", "date_text"), ("relative", "That same day")),
], ids=["a link inside the sentence", "the reference its marker cites", "not a link in another sentence",
        "media after the paragraph go with its events", "and the next paragraph's with its own",
        "a relative date read from the sentences passes the audit"])
def test_attachments(model_rows, view, want):
    check(None, model_rows, view, want)


def test_media_before_the_first_paragraph_go_with_the_first():
    e = entry()
    e["sections"][1]["images"][0]["after_paragraph"] = -1
    assert [i["src"] for i in at(run(event(), u=ev.section_unit(e, "origin")), 1)["images"]] == [IMG]


# -- identity and the model policy ---------------------------------------------

def test_identity():
    assert ev.frame_key(URL) == url_doc_id(URL)
    a = ev.event_id(URL, "origin", [1], "2010-02-23", "day")
    assert a == ev.event_id(URL, "origin", [1], "2010-02-23", "day") and "-" in a
    assert a not in {ev.event_id(URL, "origin", [2], "2010-02-23", "day"),
                     ev.event_id(URL, "spread", [1], "2010-02-23", "day"),
                     ev.event_id(URL, "origin", [1], None, "none")}
    with pytest.raises(ValueError):
        int(a)


def test_the_policy_is_criteria_not_a_name():
    assert ev.model_request({}).exclude_capabilities == {"thinking"}
    assert ev.model_request({"KG_EVENTS_MODEL": "x"}).model == "x"
    c, _ = client(StubSession())
    chosen = ev.choose_model(c, ev.model_request({}))
    assert (chosen.name, owc.host_name(chosen.host)) == (MINISTRAL, "ollama-ccdd")
    with pytest.raises(owc.ModelUnavailableError):            # refused, not rerouted
        ev.choose_model(c, ev.model_request({"KG_EVENTS_MODEL": "qwen3.8:27b"}))
    cands = owc.resolve_candidates(c.inventory(), ev.model_request({}), c.cfg.host_order)
    assert cands and not [m.name for m in cands if "thinking" in m.capabilities]


# -- the artifact and extract() -------------------------------------------------

def lines(path):
    return path.read_text(encoding="utf-8").splitlines()


def test_the_artifact_is_appended_never_rewritten_and_a_torn_line_is_skipped(tmp_path):
    out = tmp_path / "events.jsonl"
    ev.append_jsonl(str(out), {"frame_url": "a", "source_section": "origin"})
    head = lines(out)[0]
    for i in range(5):
        ev.append_jsonl(str(out), {"frame_url": f"u{i}", "source_section": "spread"})
    assert len(lines(out)) == 6 and lines(out)[0] == head
    with open(out, "a") as fh:
        fh.write('{"frame_url": "b", "source_sec')
    assert len(list(ev.iter_jsonl(str(out)))) == 6


@pytest.fixture
def extract(tmp_path):
    path = tmp_path / "events.jsonl"

    def go(session, **kw):
        c, _ = client(session)
        return ev.extract(c, [unit()], str(path), REQ, schema_path=SCHEMA_PATH,
                          progress=lambda _: None, **kw)
    go.path = path
    return go


def test_a_unit_is_extracted_audited_stamped_and_written(extract):
    summary = extract(StubSession(reply([event()])))
    assert (summary["extracted"], summary["events"]) == (1, 4)
    record = json.loads(lines(extract.path)[0])
    assert (record["extraction_version"], record["model"]) == (ev.EXTRACTION_VERSION, MINISTRAL)
    assert not {"discarded", "truncated"} & set(record)
    assert record["events"][0]["source_text"][:18] == "The original photo"


def test_an_addition_never_reaches_the_record(extract):
    # "(dog)" is the model's aside; the vet is nobody the page names; neither is counted
    summary = extract(StubSession(reply([event(actors=["Kabosu (dog)", "Kabosu's vet"])])))
    record = json.loads(lines(extract.path)[0])
    assert (summary["events"], record["events"][0]["actors"]) == (4, ["Kabosu"])
    assert "discarded_count" not in record


def test_a_reply_the_grammar_cannot_finish_is_retried_without_it(extract):
    """Ollama's constrained sampler dies mid-string on non-Latin text (1.5% of
    sections); with no format the reply comes back whole."""
    def handler(body):
        if "format" in body:
            return Resp(200, {"message": {"content": '{"events": [{"actors": ["Ст'}})
        return Resp(200, {"message": {"content": "```json\n" + json.dumps({"events": [event()]}) + "\n```"}})
    summary = extract(StubSession(routes(handler)))
    assert (summary["extracted"], summary["failed_count"], summary["events"]) == (1, 0, 4)


def test_a_grammarless_reply_still_has_to_be_the_schemas_shape():
    assert ev._loads('```json\n{"events": []}\n```') == {"events": []}   # the fence is tolerated
    assert ev._loads('  {"events": []} ') == {"events": []}
    with pytest.raises(ValueError):
        ev._loads("```json\nnot json\n```")


def test_the_request_is_numbered_sentences_and_the_extractive_grammar(extract):
    bodies = []
    extract(StubSession(routes(lambda body: bodies.append(body) or Resp(
        200, {"message": {"content": json.dumps({"events": []})}}))))
    [body] = bodies
    user = body["messages"][1]["content"]
    assert "Sentences (1 to 5; every one belongs to an event):\n1: The original photo" in user
    assert "[3]" not in user and "ORIGIN section" in user
    assert body["format"] == ev.request_format(schema())
    assert (body["options"]["temperature"], body["think"], body["model"]) == (0, False, MINISTRAL)


def test_a_record_failing_its_audit_is_never_written(extract, monkeypatch):
    original = ev._attach
    monkeypatch.setattr(ev, "_attach", lambda row, u: {**original(row, u), "actors": ["Somebody Invented"]})
    with pytest.raises(AssertionError):
        extract(StubSession(reply([event()])))
    assert not extract.path.exists()


def test_a_resumed_run_asks_for_nothing(extract):
    session = StubSession(reply([event()]))
    extract(session)
    before = len(session.posts())
    assert extract(session)["attempted"] == 0 and len(session.posts()) == before


def test_garbage_output_lands_in_failures_as_data(extract):
    summary = extract(StubSession(routes(Resp(200, {"message": {"content": "Sure! Here are the events:"}}))))
    assert (summary["extracted"], summary["failed_count"], summary["failed"][0]["error_kind"]) == (0, 1, "invalid")
    assert not extract.path.exists()


def test_on_record_fires_after_the_line_is_on_disk(extract):
    seen = []
    extract(StubSession(reply([event()])), on_record=lambda rec: seen.append(len(lines(extract.path))))
    assert seen == [1]
