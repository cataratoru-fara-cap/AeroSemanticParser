"""Tests for lib/highlight.py — the event highlighter (pure; no Streamlit).

What these pin:
  * a value is found the way the event audit finds it: case, quote and dash
    styles, whitespace and [12] citation markers do not matter;
  * offsets point into the ORIGINAL text, so the page shows it unchanged;
  * a value named only outside the quote is reported, not dropped;
  * overlapping values do not nest marks; everything else is escaped.
"""
import unittest

from lib import highlight as H


class FindTests(unittest.TestCase):
    def test_offsets_are_into_the_original_text(self):
        text = "On  January 24th, 2025, Kai Cenat's fans flooded the chat."
        (s, e), = H.find(text, "on january 24th, 2025")
        self.assertEqual(text[s:e], "On  January 24th, 2025")

    def test_typography_and_citations_do_not_matter(self):
        text = "It was posted to Reddit [3] by u/Some—User on “Doge Day”."
        (s, e), = H.find(text, "u/some-user")
        self.assertEqual(text[s:e], "u/Some—User")
        (s, e), = H.find(text, '"Doge Day"')
        self.assertEqual(text[s:e], "Doge Day")
        (s, e), = H.find(text, "Reddit by u/Some-User")          # across the marker
        self.assertEqual(text[s:e], "Reddit [3] by u/Some—User")

    def test_whole_words_first(self):
        text = "On February 7th, 2015, Randle created the @irvinrandle Instagram feed."
        (s, e), = H.find(text, "Randle")
        self.assertEqual((s, text[s:e]), (text.index("Randle"), "Randle"))
        # only inside a longer word: still shown, so the reader sees its source
        self.assertEqual(len(H.find("the @irvinrandle feed", "Randle")), 1)

    def test_every_occurrence_and_no_false_hits(self):
        text = "X posted it; later X deleted it."
        self.assertEqual(len(H.find(text, "X")), 2)
        self.assertEqual(H.find(text, "Twitter"), [])
        self.assertEqual(H.find(text, ""), [])


class MarkTests(unittest.TestCase):
    EVENT = {"date_text": "On January 24th, 2025", "locations": ["X"],
             "actors": ["@@scubaryan_", "Caiden Butler"]}
    TEXT = "On January 24th, 2025, the clip was posted on X by @@scubaryan_."

    def test_marks_and_missing(self):
        spans, missing = H.mark_event(self.TEXT, self.EVENT)
        self.assertEqual([(s.kind, self.TEXT[s.start:s.end]) for s in spans],
                         [("when", "On January 24th, 2025"), ("where", "X"),
                          ("who", "@@scubaryan_")])
        self.assertEqual(missing, [("who", "Caiden Butler")])   # named earlier

    def test_old_single_location_shape(self):
        spans, missing = H.mark_event("Seen on Reddit.", {"location": "Reddit"})
        self.assertEqual([(s.kind, s.value) for s in spans], [("where", "Reddit")])
        self.assertEqual(missing, [])

    def test_overlaps_keep_the_earlier_longer_span(self):
        ev = {"locations": ["Kai Cenat's livestream"], "actors": ["Kai Cenat"]}
        spans, missing = H.mark_event("Fans flooded Kai Cenat's livestream.", ev)
        self.assertEqual([(s.kind, s.value) for s in spans],
                         [("where", "Kai Cenat's livestream")])
        self.assertEqual(missing, [])

    def test_render_escapes_and_labels(self):
        text = "A <b> tag & X"
        spans, _ = H.mark_event(text, {"locations": ["X"]})
        out = H.render(text, spans)
        self.assertIn("A &lt;b&gt; tag &amp; ", out)
        self.assertIn('<mark class="kym-hl kym-hl-where" title="where">X</mark>', out)
        self.assertEqual(H.render("a\n\nb", []), "a<br><br>b")
        for kind in H.KINDS:
            self.assertIn(f"kym-hl-{kind}", H.legend())


if __name__ == "__main__":
    unittest.main()
