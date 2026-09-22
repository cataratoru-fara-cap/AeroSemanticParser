"""Tests for kg/events.py — narrative section -> event rows, on the real
client over a stubbed HTTP session (no model server is ever contacted).

What these pin, beyond "it works" (extraction 2.2.0):

  * **Nothing the model adds reaches the store, and nothing valid is
    thrown away.** The model answers with sentence NUMBERS; the evidence
    text is copied from the page by the pipeline. Every textual value it
    does return is GROUNDED — resolved to the span of the section it names
    and stored in the section's own words. ``ExtractiveTests`` feeds it the
    exact wordings seen in review: a year the page omits ("May 7th, 2010"
    for "On May 7th"), which must survive as the page's words; an aside
    ("Tumblr (the blogging site)"), which must lose the aside; an invented
    actor, which must ground to nothing at all; and a date the sentences
    contradict, which must not be quietly corrected to theirs.
  * **``audit()`` is an independent second check,** and ``extract()``
    refuses to write any record that fails it — so a validator bug cannot
    leak an addition either.
  * **Links, citations, photos and embeds are attached by position,** not
    by the model: a link inside the event's sentences, the reference its
    ``[n]`` marker cites, the media shown after its paragraph.
  * **Nothing is capped:** no truncation, no ceiling on events.
  * **The model policy is criteria, not a name:** never a reasoning model,
    on the requested model or on any fallback.
  * the artifact is APPENDED, never rewritten; ``{"events": []}`` is a
    correct answer; frame_key agrees with mongo_base.url_doc_id.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_events.py -v
"""
import json
import os
import tempfile
import unittest

from jsonschema import Draft202012Validator

from modules import openwebui_client as owc
from modules.kg import events as ev
from test_openwebui_client import CCDD, UI, Resp, StubSession, by_model, client

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "kg_config",
                           "event_extraction_schema.json")
MINISTRAL = "ministral-3:14b"
REQ = ev.model_request({})              # the policy, with the default model

URL = "https://knowyourmeme.com/memes/doge"
KYM_LINK = "https://knowyourmeme.com/memes/sites/tumblr"
REF_3 = "https://web.archive.org/web/2010/https://tumblr.com/post/1"
IMG = "https://i.kym-cdn.com/photos/images/original/000/1.jpg"
TIKTOK = "https://www.tiktok.com/@buhfingeranator43/video/1"

# Two paragraphs, as the parser stores them (citation marker included).
P0 = ("The original photo of Kabosu was posted to Tumblr [3] on February "
      "23rd, 2010 by blogger Atsuko Sato. It was allegedly first called "
      "“doge” on 4chan.")
P1 = ("On May 7th, YouTuber KwandaoRen66 uploaded a video. That same day, "
      "the same YouTuber posted a second one. In 2013 it spread to Reddit.")


def silent(_):
    pass


def entry(**over):
    base = {
        "url": URL, "title": "Doge", "category": "meme",
        "parser_version": "1.6.0",
        "external_references": [
            {"index": 3, "text": "Tumblr post", "url": REF_3},
            {"index": 9, "text": "unrelated", "url": "https://example.org/9"}],
        "sections": [
            {"kind": "about", "heading": "About", "text": ["Doge is a meme."]},
            {"kind": "origin", "heading": "Origin", "text": [P0, P1],
             "links": [{"text": "Tumblr", "url": KYM_LINK, "paragraph": 0,
                        "offset": P0.index("Tumblr")}],
             "images": [{"src": IMG, "caption": "Kabosu", "after_paragraph": 0}],
             "embeds": [{"url": TIKTOK, "platform": "tiktok",
                         "after_paragraph": 1}]},
        ],
    }
    base.update(over)
    return base


def unit(**over):
    return ev.section_unit(entry(**over), "origin")


def schema():
    item, _sha = ev.load_schema(SCHEMA_PATH)
    return item


def validator(u=None):
    return ev.make_validator(ev.item_checker(schema()), u or unit())


def event(**over):
    """A model reply row. Since 2.1.0 it carries the date WORDS only — the
    pipeline parses them, so there is no date or precision to send."""
    row = {"sentences": [1], "date_text": "February 23rd, 2010",
           "location": "Tumblr", "location_type": "platform",
           "actors": ["Atsuko Sato"], "certainty": "confirmed"}
    row.update(over)
    return row


def run(*rows, u=None):
    return validator(u)(json.dumps({"events": list(rows)}))


def routes(handler):
    route = by_model({MINISTRAL: handler})
    return {("POST", CCDD, owc.CHAT_PATH): route, ("POST", UI, owc.CHAT_PATH): route}


def reply(rows):
    return routes(lambda body: Resp(
        200, {"message": {"content": json.dumps({"events": rows})}}))


def recorder(rows):
    bodies = []

    def handler(body):
        bodies.append(body)
        return Resp(200, {"message": {"content": json.dumps({"events": rows})}})

    return routes(handler), bodies


class SchemaTests(unittest.TestCase):
    def test_the_model_grammar_has_no_derived_and_no_free_text_fields(self):
        item = ev.request_format(schema())["properties"]["events"]["items"]
        for derived in ("frame_url", "source_section", "source_text",
                        "links", "images", "embeds", "date", "date_precision",
                        "date_basis", "date_anchor"):
            self.assertNotIn(derived, item["properties"], derived)
            self.assertNotIn(derived, item["required"], derived)
        self.assertNotIn("summary", item["properties"])      # removed in 2.0.0
        self.assertIn("sentences", item["required"])
        self.assertIn("date_text", item["properties"])   # the words, only

    def test_nothing_is_capped(self):
        events = ev.request_format(schema())["properties"]["events"]
        self.assertNotIn("maxItems", events)

    def test_the_grammar_is_itself_a_valid_schema(self):
        Draft202012Validator.check_schema(ev.request_format(schema()))

    def test_the_tracked_schema_still_describes_the_whole_stored_row(self):
        props = schema()["properties"]
        for key in ("source_text", "links", "images", "embeds", "actors"):
            self.assertIn(key, props)
        self.assertTrue(props["source_text"]["x-derived"])


class SentenceTests(unittest.TestCase):
    def split(self, text):
        return [text[a:b] for a, b in ev.split_sentences(text)]

    def test_plain_sentences(self):
        self.assertEqual(self.split("One. Two! Three?"), ["One.", "Two!", "Three?"])

    def test_abbreviations_and_initials_do_not_end_a_sentence(self):
        self.assertEqual(self.split("Mr. Smith met J. K. Rowling in the U.S. on "
                                    "Jan. 3rd. Then he left."),
                         ["Mr. Smith met J. K. Rowling in the U.S. on Jan. 3rd.",
                          "Then he left."])

    def test_a_citation_marker_stays_with_the_sentence_it_follows(self):
        self.assertEqual(self.split("It went viral. [4] The next day it died."),
                         ["It went viral. [4]", "The next day it died."])

    def test_spans_are_verbatim_slices_of_the_paragraph(self):
        for a, b in ev.split_sentences(P0):
            self.assertIn(P0[a:b], P0)

    def test_the_model_reads_numbered_sentences_without_markers(self):
        text = ev.numbered_text(unit())
        self.assertTrue(text.startswith("1: The original photo"))
        self.assertIn("\n\n3: On May 7th", text)       # paragraph break kept
        self.assertNotIn("[3]", text)


class SectionUnitTests(unittest.TestCase):
    def test_the_whole_section_is_the_unit_never_truncated(self):
        long = "A sentence about the meme. " * 400      # ~11k characters
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [long]}])
        self.assertEqual(u["source_chars"], len(long))
        self.assertEqual(len(u["sentences"]), 400)
        self.assertNotIn("truncated", u)

    def test_only_citations_the_section_actually_marks_are_resolved(self):
        self.assertEqual(unit()["citations"], {"3": REF_3})

    def test_the_stamp_moves_when_the_media_move_not_only_the_text(self):
        base = unit()["source_sha256"]
        moved = entry()
        moved["sections"][1]["images"][0]["after_paragraph"] = 1
        self.assertNotEqual(ev.section_unit(moved, "origin")["source_sha256"], base)

    def test_the_infobox_origin_field_is_not_the_origin_section(self):
        u = unit(origin="Tumblr")
        self.assertIn("Kabosu", u["paragraphs"][0])

    def test_a_missing_or_empty_section_is_no_unit(self):
        self.assertIsNone(ev.section_unit(entry(), "spread"))
        self.assertIsNone(ev.section_unit({"title": "no url"}, "origin"))


class ExtractiveTests(unittest.TestCase):
    """Every addition seen in review, fed back in. None may survive."""

    def test_the_evidence_is_copied_from_the_page_not_written_by_the_model(self):
        [row] = run(event())
        first_sentence = P0[:P0.index(" It was")]
        self.assertEqual(row["source_text"], first_sentence)   # [3] included
        self.assertIn("[3]", row["source_text"])

    def test_a_span_across_paragraphs_is_joined_like_the_frame_text(self):
        [row] = run(event(sentences=[2, 3], date_text=None))
        self.assertIn("\n\n", row["source_text"])
        for part in row["source_text"].split("\n\n"):
            self.assertIn(part, P0 + "\n\n" + P1)

    def test_an_added_qualifier_grounds_to_the_pages_own_words(self):
        """2.2.0: a near miss is resolved, not thrown away. The aside is
        the model's; "Tumblr" is the page's, so "Tumblr" is what is kept."""
        out = run(event(location="Tumblr (the blogging site)"))
        self.assertEqual(out[0]["location"], "Tumblr")
        self.assertEqual(out[0]["location_type"], "platform")

    def test_an_actor_that_names_nobody_is_not_an_actor(self):
        """"people", "users", "they" are on the page and ground perfectly
        well; as mk:eventActor literals they are noise nothing can join
        on. qwen3.8:27b returned exactly these on retroslop."""
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "In May 2013 people posted it. Later, users and Atsuko Sato did too."]}])
        out = run(event(sentences=[1], date_text="In May 2013", location=None,
                        actors=["people"]),
                  event(sentences=[2], date_text=None, location=None,
                        actors=["users", "Atsuko Sato"]), u=u)
        self.assertEqual(out[0]["actors"], [])
        self.assertEqual(out[1]["actors"], ["Atsuko Sato"])

    def test_an_embellished_actor_grounds_and_an_invented_one_does_not(self):
        out = run(event(actors=["Atsuko Sato", "Atsuko Sato (blogger)",
                                "Kabosu's vet"]))
        # The first two are one person as the page names her; the vet is
        # nobody the page names, and "Kabosu" alone does not stand in for
        # them — grounding needs the HEAD of what the model wrote.
        self.assertEqual(out[0]["actors"], ["Atsuko Sato"])

    def test_an_actor_named_elsewhere_in_the_section_is_kept(self):
        # Coreference: "the same YouTuber" in sentence 4, named in sentence 3.
        out = run(event(sentences=[4], date_text="That same day", location=None,
                        actors=["KwandaoRen66"]))
        self.assertEqual(out[0]["actors"], ["KwandaoRen66"])

    def test_a_year_the_page_omits_no_longer_costs_the_date(self):
        """The regression this version exists for. KYM states the year once
        and then omits it ("On May 7th, ..."); the model writes it back in.
        2.1 required the phrase verbatim and lost 34 of 421 pilot dates —
        and every "that same day" after one of them was then anchored to
        the wrong event. The words are now grounded to the page's, and the
        year comes from earlier in the section, as it always did."""
        [row] = run(event(sentences=[3], date_text="May 7th, 2010",
                          location=None, actors=[]))
        self.assertEqual(row["date_text"], "May 7th")       # the page's words
        self.assertEqual((row["date"], row["date_basis"]), ("2010-05-07", "stated"))

    def test_date_words_the_sentences_contradict_ground_to_nothing(self):
        """Grounding resolves a WORDING difference, never a different date:
        "June 2nd" where the sentence says May 7th is not a near miss."""
        [row] = run(event(sentences=[3], date_text="June 2nd, 2010",
                          location=None, actors=[]))
        self.assertIsNone(row["date_text"])
        self.assertIsNone(row["date"])

    def test_date_words_from_another_sentence_ground_to_nothing(self):
        out = run(event(date_text="May 7th"))                  # sentence 3's words
        self.assertIsNone(out[0]["date_text"])

    def test_typography_and_markers_are_not_additions(self):
        out = run(event(sentences=[2], date_text=None, location="4chan", actors=[],
                        certainty="unconfirmed"))
        self.assertEqual(out[0]["location"], "4chan")
        out = run(event(location='"Tumblr"'))
        self.assertEqual(out[0]["location"], '"Tumblr"')

    def test_an_event_pointing_at_no_sentence_fails_the_reply(self):
        """Not silently dropped: an event with no evidence is a broken
        REPLY, so the call is retried and then dead-lettered — a section
        that is visibly missing, never a row that quietly disappeared."""
        self.assertRaises(ValueError, validator(),
                          json.dumps({"events": [event(sentences=[99])]}))

    def test_a_sentence_ending_in_X_is_a_sentence(self):
        """The splitter read "... on X." as an initial and merged the two
        sentences; the model, reading the prose, numbered them its own way
        and pointed past the end of the section — which cost a whole
        section of the pilot. A name's initial still does not end one."""
        splits = ev.split_sentences
        text = "It went viral on X. For example, @foo posted it."
        self.assertEqual([text[a:b] for a, b in splits(text)],
                         ["It went viral on X.", "For example, @foo posted it."])
        for name in ("J. K. Rowling wrote it.", "By George R. R. Martin now."):
            whole = name + " It sold well."
            self.assertEqual([whole[a:b] for a, b in splits(whole)],
                             [name, "It sold well."])

    def test_a_citation_marker_ending_a_paragraph_is_not_a_sentence(self):
        """KYM ends paragraphs with "[2]" constantly. Splitting it off made
        an EMPTY numbered line for the model and moved the citation to a
        sentence nothing narrates — 687 orphaned citations across a
        2,909-section scan, 19% of sections affected."""
        for text in ('A character named Mr. Armstrong first appeared. [2]',
                     'The film premiered in the United States. [1]',
                     'It spread to Reddit. [3] [4]'):
            self.assertEqual([text[a:b] for a, b in ev.split_sentences(text)],
                             [text], text)
        both = 'He posted it on Twitter. [6] Then it spread. [7]'
        self.assertEqual([both[a:b] for a, b in ev.split_sentences(both)],
                         ['He posted it on Twitter. [6]', 'Then it spread. [7]'])
        # and every sentence still carries words once markers are hidden
        u = unit(sections=[{"kind": "origin", "heading": "Origin",
                            "text": ["It spread to Reddit. [3]"]}])
        self.assertTrue(all(line.split(": ", 1)[1].strip()
                            for line in ev.numbered_text(u).splitlines() if line))

    def test_valid_sentences_survive_an_invalid_one(self):
        out = run(event(sentences=[1, 99]))
        self.assertEqual(out[0]["sentences"], [1])

    def test_the_same_sentences_and_date_twice_is_one_event(self):
        """Merged, not dropped — the same event read twice is one event,
        and whatever the second reading saw is kept."""
        out = run(event(location=None, actors=["Atsuko Sato"]),
                  event(location="Tumblr", actors=["Kabosu"]))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["location"], "Tumblr")
        self.assertEqual(out[0]["actors"], ["Atsuko Sato", "Kabosu"])

    def test_an_empty_list_is_a_correct_answer(self):
        self.assertEqual(list(run()), [])

    def test_a_broken_reply_still_fails_the_unit(self):
        self.assertRaises(ValueError, validator(), "Here are the events:")
        self.assertRaises(KeyError, validator(), json.dumps({"rows": []}))
        self.assertRaises(TypeError, validator(), json.dumps({"events": {}}))

    def test_unknown_keys_never_reach_the_row(self):
        [row] = run(event(summary="A made-up summary", confidence=0.9))
        self.assertNotIn("summary", row)
        self.assertNotIn("confidence", row)


class DateParsingTests(unittest.TestCase):
    """2.1.0: the pipeline parses the date words; the model never dates
    anything. On the v2 pilot the model read "On June 4th, 2014" as
    2014-06-03 — a disagreement that cannot exist now."""

    def test_phrases_parse_to_their_own_precision(self):
        for phrase, expected in (
                ("February 23rd, 2010", ("2010-02-23", "day")),
                ("on Feb 23, 2010", ("2010-02-23", "day")),
                ("the 23rd of February, 2010", ("2010-02-23", "day")),
                ("May 2013", ("2013-05", "month")),
                ("early 2013", ("2013", "year")),
                ("In 2013", ("2013", "year"))):
            with self.subTest(phrase=phrase):
                self.assertEqual(ev.parse_date_phrase(phrase), expected)

    def test_a_missing_year_comes_from_the_section_never_from_nowhere(self):
        self.assertEqual(ev.parse_date_phrase("On May 7th", 2010),
                         ("2010-05-07", "day"))
        self.assertEqual(ev.parse_date_phrase("On May 7th"), (None, "none"))

    def test_an_impossible_or_absent_date_is_none(self):
        for phrase in ("February 31st, 2010", "In their post", "", None,
                       "shortly afterwards"):
            with self.subTest(phrase=phrase):
                self.assertEqual(ev.parse_date_phrase(phrase), (None, "none"))

    def test_relative_phrases_are_recognised_and_others_are_not(self):
        for phrase, days in (("That same day", 0), ("Later that day", 0),
                             ("on the same day", 0), ("An hour later", 0),
                             ("a few minutes later", 0), ("The following day", 1),
                             ("a day later", 1), ("three days later", 3),
                             ("2 days later", 2)):
            with self.subTest(phrase=phrase):
                self.assertEqual(ev.relative_offset(phrase), days)
        for phrase in ("shortly after", "the following week", "later that year",
                       "In 2013", "eventually"):
            with self.subTest(phrase=phrase):
                self.assertIsNone(ev.relative_offset(phrase))


class RelativeDateTests(unittest.TestCase):
    """The fix asked for on 2026-09-21: "that same day" is dated by the
    pipeline from the event before it, not guessed by the model."""

    def test_a_relative_event_is_dated_from_the_one_before_it(self):
        # Sentence 3 is "On May 7th, ..." (its year, 2010, comes from
        # sentence 1); sentence 4 is "That same day, ...".
        out = run(event(sentences=[3], date_text="On May 7th", location=None,
                        actors=[]),
                  event(sentences=[4], date_text="That same day", location=None,
                        actors=[]))
        self.assertEqual([e["date"] for e in out], ["2010-05-07", "2010-05-07"])
        self.assertEqual([e["date_basis"] for e in out], ["stated", "relative"])
        self.assertEqual(out[1]["date_anchor"], out[0]["event_id"])

    def test_the_offset_is_applied(self):
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "On February 23rd, 2010 it was posted. The following day it spread. "
            "Three days later it peaked."]}])
        out = run(event(sentences=[1], date_text="On February 23rd, 2010",
                        location=None, actors=[]),
                  event(sentences=[2], date_text="The following day",
                        location=None, actors=[]),
                  event(sentences=[3], date_text="Three days later",
                        location=None, actors=[]), u=u)
        self.assertEqual([e["date"] for e in out],
                         ["2010-02-23", "2010-02-24", "2010-02-27"])

    def test_a_relative_phrase_with_nothing_before_it_stays_undated(self):
        out = run(event(sentences=[4], date_text="That same day",
                        location=None, actors=[]))
        self.assertIsNone(out[0]["date"])
        self.assertEqual(out[0]["date_text"], "That same day")   # words kept

    def test_a_missing_year_comes_from_a_date_not_from_any_number(self):
        """A year token is not a year context. The pilot took years out of
        quoted captions ("return to 2010"), festival names ("Stagecoach
        2025") and asides ("it didn't establish the year 2026") — 10 of 339
        dated events, every one of them wrong."""
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "On March 13th, 2025, it appeared. "
            "The caption read, \"Reject modern memes, return to 2010.\" "
            "On April 24th, it spread."]}])
        out = run(event(sentences=[1], date_text="March 13th, 2025",
                        location=None, actors=[]),
                  event(sentences=[3], date_text="April 24th",
                        location=None, actors=[]), u=u)
        self.assertEqual([e["date"] for e in out], ["2025-03-13", "2025-04-24"])

    def test_a_year_quoted_later_in_the_event_cannot_supply_it(self):
        """The context stops where the event's own date words begin, so a
        date quoted after them ("We will return to January 1st, 2016") is
        not the year, while one earlier in the same sentences still is."""
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "On September 23rd, 2025, it started. "
            "On October 1st, a video said, \"We will return to January 1st, 2016.\""]}])
        out = run(event(sentences=[1], date_text="September 23rd, 2025",
                        location=None, actors=[]),
                  event(sentences=[2], date_text="October 1st",
                        location=None, actors=[]), u=u)
        self.assertEqual([e["date"] for e in out], ["2025-09-23", "2025-10-01"])

    def test_a_year_the_section_names_once_dates_what_precedes_it(self):
        """KYM often states the year AFTER the first event of a section
        ("On April 13th, ... On May 2nd, 2019, ..."), which left 30 of
        5,612 sampled events undated on a day the page gives. One year
        only: two years say nothing about which April is meant."""
        one = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "On April 13th, SethEverman posted a video. "
            "On May 2nd, 2019, it was reposted."]}])
        out = run(event(sentences=[1], date_text="April 13th", location=None,
                        actors=[]),
                  event(sentences=[2], date_text="May 2nd, 2019", location=None,
                        actors=[]), u=one)
        self.assertEqual([e["date"] for e in out], ["2019-04-13", "2019-05-02"])

        two = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "On May 25th, it was posted. It spread in June 2019. "
            "By March 2020, it was everywhere."]}])
        [first, *_] = run(event(sentences=[1], date_text="May 25th",
                                location=None, actors=[]),
                          event(sentences=[2], date_text="June 2019",
                                location=None, actors=[]),
                          event(sentences=[3], date_text="March 2020",
                                location=None, actors=[]), u=two)
        self.assertIsNone(first["date"])          # 2019 or 2020? neither

    def test_a_decade_dates_nothing(self):
        """"the first half of the 2010s" was dating events to 2010-01-01.
        date_precision has no "decade", and one year out of ten is a
        guess. "2016's election" is still the year 2016."""
        for phrase in ("During the first half of the 2010s", "the mid-2000s",
                       "the late 1990s", "the 2010s"):
            self.assertEqual(ev.parse_date_phrase(phrase, None), (None, "none"),
                             phrase)
        self.assertEqual(ev.parse_date_phrase("2016's election", None),
                         ("2016", "year"))

    def test_the_anchor_is_the_event_not_whatever_shares_its_sentence(self):
        """Two events in one sentence is ordinary KYM ("X posted; that same
        day Y reposted"), and 74 of 5,612 sampled events start on a
        sentence another event also starts on. Keying the anchor by
        sentence number let an undated event in between steal the link."""
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "On May 4th, 2013 it appeared. It was popular. "
            "That same day it reached Reddit."]}])
        # A and B both start at sentence 1; B is undated and sits between A
        # and the event that must anchor to A.
        out = run(event(sentences=[1], date_text="May 4th, 2013",
                        location=None, actors=[]),
                  event(sentences=[1, 2], date_text=None, location=None,
                        actors=[]),
                  event(sentences=[3], date_text="That same day",
                        location="Reddit", location_type="platform",
                        actors=[]), u=u)
        by_id = {e["event_id"]: e for e in out}
        [anchored] = [e for e in out if e["date_basis"] == "relative"]
        self.assertEqual(anchored["date"], "2013-05-04")
        self.assertEqual(by_id[anchored["date_anchor"]]["sentences"], [1])
        self.assertEqual(by_id[anchored["date_anchor"]]["date"], "2013-05-04")

    def test_a_year_the_phrase_states_beats_the_context(self):
        """"Around June 7th or June 8th, 2026" attaches its year to the
        SECOND day; the first was taking a year from elsewhere entirely."""
        self.assertEqual(
            ev.parse_date_phrase("Around June 7th or June 8th, 2026", 2017),
            ("2026-06-07", "day"))

    def test_a_relative_phrase_cannot_shift_a_month_precision_date(self):
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "In May 2013 it appeared. The following day it spread."]}])
        out = run(event(sentences=[1], date_text="In May 2013", location=None,
                        actors=[]),
                  event(sentences=[2], date_text="The following day",
                        location=None, actors=[]), u=u)
        self.assertEqual(out[0]["date"], "2013-05")
        self.assertIsNone(out[1]["date"])        # "the next day" of a month?

    def test_same_day_inherits_a_coarse_precision_unchanged(self):
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "In May 2013 it appeared. That same day it spread."]}])
        out = run(event(sentences=[1], date_text="In May 2013", location=None,
                        actors=[]),
                  event(sentences=[2], date_text="That same day", location=None,
                        actors=[]), u=u)
        self.assertEqual([(e["date"], e["date_precision"]) for e in out],
                         [("2013-05", "month"), ("2013-05", "month")])

    def test_a_chain_follows_the_page(self):
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "On May 1st, 2020 it began. The following day it grew. "
            "That same day it peaked."]}])
        out = run(event(sentences=[1], date_text="On May 1st, 2020",
                        location=None, actors=[]),
                  event(sentences=[2], date_text="The following day",
                        location=None, actors=[]),
                  event(sentences=[3], date_text="That same day",
                        location=None, actors=[]), u=u)
        self.assertEqual([e["date"] for e in out],
                         ["2020-05-01", "2020-05-02", "2020-05-02"])
        self.assertEqual(out[2]["date_anchor"], out[1]["event_id"])

    def test_a_relative_phrase_in_the_sentences_counts_even_if_unquoted(self):
        # The model returned no date words; sentence 4 begins "That same day".
        out = run(event(sentences=[3], date_text="On May 7th", location=None,
                        actors=[]),
                  event(sentences=[4], date_text=None, location=None, actors=[]))
        by_sentence = {e["sentences"][0]: e for e in out}
        self.assertEqual(by_sentence[4]["date"], "2010-05-07")
        self.assertEqual(by_sentence[4]["date_basis"], "relative")

    def test_an_absolute_date_in_the_sentences_is_NOT_taken_that_way(self):
        """A sentence often carries a date belonging to what it reports.
        Taking it would date the event by guesswork, so an event whose
        words are missing stays undated."""
        u = unit(sections=[{"kind": "origin", "heading": "Origin", "text": [
            "The screenshot shows that on June 3rd, 2014, a user posted it."]}])
        [row] = run(event(sentences=[1], date_text=None, location=None,
                          actors=[]), u=u)
        self.assertIsNone(row["date"])

    def test_resolution_follows_the_PAGE_order_not_the_replys(self):
        """The model may list events in any order; "that same day" means the
        day of the sentence before it on the page, not the row before it in
        the reply."""
        out = run(event(sentences=[4], date_text="That same day", location=None,
                        actors=[]),                      # listed FIRST
                  event(sentences=[3], date_text="On May 7th", location=None,
                        actors=[]))                      # but comes after it
        by_sentence = {e["sentences"][0]: e for e in out}
        self.assertEqual(by_sentence[3]["date"], "2010-05-07")
        self.assertEqual(by_sentence[4]["date"], "2010-05-07")
        self.assertEqual(by_sentence[4]["date_basis"], "relative")

    def test_a_later_event_never_dates_an_earlier_one(self):
        # The anchor must PRECEDE: a date further down the page is not
        # evidence for something narrated before it.
        out = run(event(sentences=[2], date_text="That same day", location=None,
                        actors=[]), event())
        by_sentence = {e["sentences"][0]: e for e in out}
        self.assertIsNone(by_sentence[2]["date"])
        self.assertEqual(by_sentence[1]["date"], "2010-02-23")


class AttachmentTests(unittest.TestCase):
    """By position, never by the model."""

    def test_a_link_inside_the_events_sentence_is_attached(self):
        [row] = run(event())
        self.assertIn({"url": KYM_LINK, "text": "Tumblr", "kind": "link"}, row["links"])

    def test_the_reference_a_marker_cites_is_attached_as_a_citation(self):
        [row] = run(event())
        self.assertIn({"url": REF_3, "text": "[3]", "kind": "citation"}, row["links"])

    def test_a_link_in_another_sentence_is_not_attached(self):
        [row] = run(event(sentences=[2], date_text=None, location="4chan", actors=[]))
        self.assertEqual(row["links"], [])

    def test_media_shown_after_the_paragraph_go_with_its_events(self):
        [p0] = run(event())
        [p1] = run(event(sentences=[5], date_text="In 2013", location="Reddit", actors=[]))
        self.assertEqual([i["src"] for i in p0["images"]], [IMG])
        self.assertEqual(p0["embeds"], [])
        self.assertEqual(p1["embeds"], [{"url": TIKTOK, "platform": "tiktok"}])
        self.assertEqual(p1["images"], [])

    def test_media_before_the_first_paragraph_go_with_the_first(self):
        e = entry()
        e["sections"][1]["images"][0]["after_paragraph"] = -1
        [row] = run(event(), u=ev.section_unit(e, "origin"))
        self.assertEqual([i["src"] for i in row["images"]], [IMG])


class AuditTests(unittest.TestCase):
    def record(self, rows):
        return {"events": list(run(*rows))}

    def test_a_validated_record_is_clean(self):
        self.assertEqual(ev.audit(self.record([event()]), unit()), [])

    def test_a_relative_date_found_in_the_sentences_passes_the_audit(self):
        """2.1.1 read the phrase from the sentences while the audit looked
        only at the date words, so extract() refused perfectly good
        records — it failed 2 of 3 chunks on the real run."""
        out = run(event(sentences=[3], date_text="On May 7th", location=None,
                        actors=[]),
                  event(sentences=[4], date_text=None, location=None, actors=[]))
        record = {"events": list(out)}
        self.assertEqual(record["events"][1]["date_basis"], "relative")
        # 2.2.0 also gives it the page's own wording for that date.
        self.assertEqual(record["events"][1]["date_text"], "That same day")
        self.assertEqual(ev.audit(record, unit()), [])

    def test_the_audit_catches_what_a_broken_validator_would_let_through(self):
        for tamper, expect in (
                ({"source_text": "The original photo of a CAT was posted."}, "verbatim"),
                ({"actors": ["Atsuko Sato (blogger)"]}, "actor"),
                ({"location": "Tumblr (site)"}, "location"),
                ({"date": "2010-03-23"}, "parses to"),
                ({"date_basis": None}, "no basis"),
                ({"links": [{"url": "https://evil.example", "kind": "link"}]}, "not on the page")):
            with self.subTest(tamper=tamper):
                rec = self.record([event()])
                rec["events"][0].update(tamper)
                problems = ev.audit(rec, unit())
                self.assertTrue(any(expect in p for p in problems), problems)


class IdentityTests(unittest.TestCase):
    def test_frame_key_agrees_with_the_store_id(self):
        from modules.mongo_base import url_doc_id
        self.assertEqual(ev.frame_key(URL), url_doc_id(URL))

    def test_event_id_is_the_evidence_and_the_date(self):
        a = ev.event_id(URL, "origin", [1], "2010-02-23", "day")
        self.assertEqual(a, ev.event_id(URL, "origin", [1], "2010-02-23", "day"))
        for other in (ev.event_id(URL, "origin", [2], "2010-02-23", "day"),
                      ev.event_id(URL, "spread", [1], "2010-02-23", "day"),
                      ev.event_id(URL, "origin", [1], None, "none")):
            self.assertNotEqual(a, other)
        self.assertIn("-", a)
        self.assertRaises(ValueError, int, a)


class PolicyTests(unittest.TestCase):
    """The policy is criteria, not a name (the 2026-09-18 review)."""

    def test_the_request_always_excludes_reasoning_models(self):
        self.assertEqual(ev.model_request({}).exclude_capabilities, {"thinking"})
        self.assertEqual(ev.model_request({"KG_EVENTS_MODEL": "x"}).model, "x")

    def test_the_default_is_chosen_on_the_real_inventory(self):
        c, _ = client(StubSession())
        chosen = ev.choose_model(c, ev.model_request({}))
        self.assertEqual((chosen.name, owc.host_name(chosen.host)),
                         (MINISTRAL, "ollama-ccdd"))

    def test_naming_a_reasoning_model_is_refused_not_rerouted(self):
        c, _ = client(StubSession())
        with self.assertRaises(owc.ModelUnavailableError):
            ev.choose_model(c, ev.model_request({"KG_EVENTS_MODEL": "qwen3.8:27b"}))

    def test_no_fallback_is_ever_a_reasoning_model(self):
        c, _ = client(StubSession())
        cands = owc.resolve_candidates(c.inventory(), ev.model_request({}),
                                       c.cfg.host_order)
        self.assertTrue(cands)
        self.assertFalse([m.name for m in cands if "thinking" in m.capabilities])


class Tmp(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def path(self, name="events.jsonl"):
        return os.path.join(self.dir, name)

    def lines(self, name="events.jsonl"):
        with open(self.path(name), encoding="utf-8") as fh:
            return fh.read().splitlines()


class ArtifactTests(Tmp):
    def test_a_line_is_appended_and_never_rewritten(self):
        out = self.path()
        ev.append_jsonl(out, {"frame_url": "a", "source_section": "origin"})
        first = self.lines()[0]
        for i in range(5):
            ev.append_jsonl(out, {"frame_url": f"u{i}", "source_section": "spread"})
        self.assertEqual(len(self.lines()), 6)
        self.assertEqual(self.lines()[0], first)

    def test_a_torn_final_line_is_skipped_not_fatal(self):
        out = self.path()
        ev.append_jsonl(out, {"frame_url": "a", "source_section": "origin"})
        with open(out, "a") as fh:
            fh.write('{"frame_url": "b", "source_sec')
        self.assertEqual(len(list(ev.iter_jsonl(out))), 1)


class ExtractTests(Tmp):
    def extract(self, session, units_=None, **kw):
        c, _ = client(session)
        return ev.extract(c, units_ or [unit()], self.path(), REQ,
                          schema_path=SCHEMA_PATH, progress=silent, **kw)

    def test_a_unit_is_extracted_audited_stamped_and_written(self):
        summary = self.extract(StubSession(reply([event()])))
        self.assertEqual((summary["extracted"], summary["events"]), (1, 1))
        record = json.loads(self.lines()[0])
        self.assertEqual(record["extraction_version"], ev.EXTRACTION_VERSION)
        self.assertEqual(record["model"], MINISTRAL)
        self.assertNotIn("discarded", record)          # 2.2.0: no such field
        self.assertNotIn("truncated", record)
        self.assertEqual(record["events"][0]["source_text"][:18], "The original photo")

    def test_an_addition_never_reaches_the_record(self):
        summary = self.extract(StubSession(reply(
            [event(actors=["Kabosu (dog)", "Kabosu's vet"])])))
        self.assertEqual(summary["events"], 1)
        record = json.loads(self.lines()[0])
        # "(dog)" is the model's aside and goes; the vet is nobody the page
        # names and is not stored at all. Neither is counted anywhere: the
        # record says what the page supports, not what the model tried.
        self.assertEqual(record["events"][0]["actors"], ["Kabosu"])
        self.assertNotIn("discarded_count", record)

    def test_a_reply_the_grammar_cannot_finish_is_retried_without_it(self):
        """Ollama's constrained sampler dies mid-string on non-Latin text:
        a Russian entry stopped dead two characters into "СтоЛичный
        Она-Нас" at every num_predict, with format=schema and with
        format=json, and came back complete with no format at all. 1.5% of
        sections carry a non-Latin script, so dead-lettering them loses the
        international memes specifically, not a random slice."""
        def handler(body):
            if "format" in body:
                return Resp(200, {"message": {"content":
                                  '{"events": [{"actors": ["\u0421\u0442'}})
            return Resp(200, {"message": {"content":
                "```json\n" + json.dumps({"events": [event()]}) + "\n```"}})

        summary = self.extract(StubSession(routes(handler)))
        self.assertEqual((summary["extracted"], summary["failed_count"]), (1, 0))
        self.assertEqual(summary["events"], 1)

    def test_a_grammarless_reply_still_has_to_be_the_schemas_shape(self):
        """The fence is tolerated; nothing else is."""
        self.assertEqual(ev._loads('```json\n{"events": []}\n```'), {"events": []})
        self.assertEqual(ev._loads('  {"events": []} '), {"events": []})
        self.assertRaises(ValueError, ev._loads, "```json\nnot json\n```")

    def test_the_request_is_numbered_sentences_and_the_extractive_grammar(self):
        chat, bodies = recorder([])
        self.extract(StubSession(chat))
        [body] = bodies
        user = body["messages"][1]["content"]
        self.assertIn("Sentences:\n1: The original photo", user)
        self.assertNotIn("[3]", user)
        self.assertIn("ORIGIN section", user)
        self.assertEqual(body["format"], ev.request_format(schema()))
        self.assertEqual(body["options"]["temperature"], 0)
        self.assertIs(body["think"], False)
        self.assertEqual(body["model"], MINISTRAL)

    def test_a_record_failing_its_audit_is_never_written(self):
        original = ev._attach
        ev._attach = lambda row, u: {**original(row, u),
                                     "actors": ["Somebody Invented"]}
        try:
            with self.assertRaises(AssertionError):
                self.extract(StubSession(reply([event()])))
        finally:
            ev._attach = original
        self.assertFalse(os.path.exists(self.path()))

    def test_a_resumed_run_asks_for_nothing(self):
        session = StubSession(reply([event()]))
        self.extract(session)
        before = len(session.posts())
        summary = self.extract(session)
        self.assertEqual(len(session.posts()), before)
        self.assertEqual(summary["attempted"], 0)

    def test_garbage_output_lands_in_failures_as_data(self):
        summary = self.extract(StubSession(routes(Resp(200, {"message": {
            "content": "Sure! Here are the events:"}}))))
        self.assertEqual((summary["extracted"], summary["failed_count"]), (0, 1))
        self.assertEqual(summary["failed"][0]["error_kind"], "invalid")
        self.assertFalse(os.path.exists(self.path()))

    def test_on_record_fires_after_the_line_is_on_disk(self):
        seen = []
        self.extract(StubSession(reply([event()])),
                     on_record=lambda rec: seen.append(len(self.lines())))
        self.assertEqual(seen, [1])


if __name__ == "__main__":       # pragma: no cover
    unittest.main(verbosity=2)
