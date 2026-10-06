"""lib/highlight.py, the event highlighter (pure; no Streamlit). Pinned: a value
is found the way the event audit finds it (case, quote and dash styles,
whitespace and [12] citation markers do not matter); offsets point into the
ORIGINAL text, so the page shows it unchanged; a value named only outside the
quote is reported, not dropped; overlapping values do not nest marks;
everything else is escaped."""
import pytest

from lib import highlight as H

CITED = "It was posted to Reddit [3] by u/Some—User on “Doge Day”."


@pytest.mark.parametrize("text, value, found", [
    ("On  January 24th, 2025, Kai Cenat's fans flooded the chat.", "on january 24th, 2025", "On  January 24th, 2025"),
    (CITED, "u/some-user", "u/Some—User"), (CITED, '"Doge Day"', "Doge Day"),
    (CITED, "Reddit by u/Some-User", "Reddit [3] by u/Some—User"),          # across the citation marker
])
def test_offsets_are_into_the_original_text(text, value, found):
    (s, e), = H.find(text, value)
    assert text[s:e] == found


def test_whole_words_first_every_occurrence_and_no_false_hits():
    text = "On February 7th, 2015, Randle created the @irvinrandle Instagram feed."
    (s, e), = H.find(text, "Randle")
    assert (s, text[s:e]) == (text.index("Randle"), "Randle")
    assert len(H.find("the @irvinrandle feed", "Randle")) == 1     # only inside a longer word: still shown
    text = "X posted it; later X deleted it."
    assert (len(H.find(text, "X")), H.find(text, "Twitter"), H.find(text, "")) == (2, [], [])


@pytest.mark.parametrize("text, event, spans, missing", [
    ("On January 24th, 2025, the clip was posted on X by @@scubaryan_.",
     {"date_text": "On January 24th, 2025", "locations": ["X"], "actors": ["@@scubaryan_", "Caiden Butler"]},
     [("when", "On January 24th, 2025"), ("where", "X"), ("who", "@@scubaryan_")],
     [("who", "Caiden Butler")]),                                            # named earlier: reported
    ("Seen on Reddit.", {"location": "Reddit"}, [("where", "Reddit")], []),  # the old single-location shape
    ("Fans flooded Kai Cenat's livestream.", {"locations": ["Kai Cenat's livestream"], "actors": ["Kai Cenat"]},
     [("where", "Kai Cenat's livestream")], []),                             # overlaps: the earlier longer span
])
def test_marks_and_missing(text, event, spans, missing):
    got, got_missing = H.mark_event(text, event)
    assert [(s.kind, text[s.start:s.end]) for s in got] == [(s.kind, s.value) for s in got] == spans
    assert got_missing == missing


def test_render_escapes_and_labels():
    text = "A <b> tag & X"
    out = H.render(text, H.mark_event(text, {"locations": ["X"]})[0])
    assert "A &lt;b&gt; tag &amp; " in out and '<mark class="kym-hl kym-hl-where" title="where">X</mark>' in out
    assert H.render("a\n\nb", []) == "a<br><br>b"
    assert all(f"kym-hl-{kind}" in H.legend() for kind in H.KINDS)
