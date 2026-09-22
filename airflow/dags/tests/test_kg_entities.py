"""Tests for kg/entities.py — title, tags and About -> Wikidata entities.

Runs the real spaCy model (en_core_web_sm, pinned in requirements.txt)
against a lexicon built from the synthetic dump in wikidata_fixture.py, so
recognition, lookup and scoring are the production code end to end. The
tests needing the model skip, loudly, where it is not installed.

What these pin, beyond "it works":

  * **Grounding.** Every mention's text is the slice of the page its
    offsets name, and ``audit()`` catches each way a record can lie.
  * **The certain link wins.** The item whose KYM slug is this page is the
    title's entity at 1.0, and wins every other span that could name it —
    over the more-linked Venetian doge.
  * **Context separates senses; popularity alone does not.** "Mercury"
    on a planet page and on a chemistry page are two different items.
  * **Clarity.** A lone exact match is believed even when obscure-ish
    ("4chan"); a close race between two senses with nothing to break it
    is not (a "doge" tag on an unrelated page).
  * **NER labels only ever help.** A label that disagrees with the item's
    class costs nothing — en_core_web_sm calls Reddit a GPE.
  * **Offsets index the graph's own literal**: a unit's About is exactly
    kg/build.py's m4s:about.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_entities.py -v
"""
import os
import tempfile
import unittest

from modules.kg import build, entities as E, wikidata as wd
from modules.mongo_base import url_doc_id
from wikidata_fixture import write_dump

URL = "https://knowyourmeme.com/memes/doge"
ABOUT = ("Doge is a slang term for dog that is primarily associated with "
         "pictures of Shiba Inus, a breed of dog from Japan. It spread on "
         "Reddit and 4chan, where Zorblax Quentin posted a nimbus of hair.")


def entry(url=URL, title="Doge", tags=("doge", "shiba inu", "dogs", "eddie_now"),
          about=(ABOUT,), **over):
    doc = {"url": url, "title": title, "tags": list(tags), "parser_version": "1.6.1",
           "sections": [{"kind": "about", "heading": "About", "text": list(about)},
                        {"kind": "origin", "heading": "Origin", "text": ["Not linked."]}]}
    doc.update(over)
    return doc


def _model_available() -> bool:
    try:
        E.load_nlp()
        return True
    except Exception:          # spaCy or the model missing
        return False


MODEL = _model_available()


class FrameUnitTests(unittest.TestCase):
    """No model needed: the unit is plain data."""

    def test_the_unit_id_is_the_entry_id(self):
        u = E.frame_unit(entry())
        self.assertEqual(u["unit_id"], url_doc_id(URL))
        self.assertEqual(E.frame_key(URL), url_doc_id(URL))

    def test_about_is_exactly_the_graphs_m4s_about(self):
        doc = entry(about=("First paragraph.", "", "Second one."))
        nodes, _ = build.build_nodes_and_edges(doc)
        frame = next(n for n in nodes if n["kind"] == "frame")
        self.assertEqual(E.frame_unit(doc)["about"], frame["about"])

    def test_only_title_tags_and_about_are_read(self):
        u = E.frame_unit(entry())
        self.assertNotIn("Not linked", u["about"])

    def test_tags_are_stripped_deduplicated_and_kept_in_order(self):
        u = E.frame_unit(entry(tags=[" doge ", "shiba", "doge", "", None]))
        self.assertEqual(u["tags"], ["doge", "shiba"])

    def test_any_source_edit_moves_the_staleness_hash(self):
        base = E.frame_unit(entry())["source_sha256"]
        for over in ({"title": "Doge 2"}, {"tags": ["doge"]},
                     {"sections": [{"kind": "about", "text": ["Other."]}]}):
            self.assertNotEqual(E.frame_unit(entry(**over))["source_sha256"], base, over)
        # ...and a section the linker never reads does not.
        other = entry()
        other["sections"][1]["text"] = ["Edited origin."]
        self.assertEqual(E.frame_unit(other)["source_sha256"], base)

    def test_no_url_no_unit(self):
        self.assertIsNone(E.frame_unit({"title": "x"}))


@unittest.skipUnless(MODEL, "spaCy / en_core_web_sm not installed")
class LinkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        dump = write_dump(os.path.join(cls._tmp.name, "dump.json.gz"))
        path = os.path.join(cls._tmp.name, "lexicon.sqlite")
        wd.build_lexicon(dump, path, workers=0, progress=lambda _l: None)
        cls.lexicon = wd.Lexicon(path)
        cls.linker = E.Linker(cls.lexicon, E.load_nlp())
        cls.unit = E.frame_unit(entry())
        cls.record = cls.linker.link(cls.unit)

    @classmethod
    def tearDownClass(cls):
        cls.lexicon.close()
        cls._tmp.cleanup()

    def link(self, **over):
        return self.linker.link(E.frame_unit(entry(**over)))

    def by(self, field, record=None):
        return [(m["text"], m["qid"]) for m in (record or self.record)["mentions"]
                if m["field"] == field]

    # -- the certain link ------------------------------------------------------

    def test_the_title_is_the_item_whose_kym_slug_is_this_page(self):
        (m,) = [m for m in self.record["mentions"] if m["field"] == "title"]
        self.assertEqual((m["qid"], m["score"], m["method"]), ("Q15894956", 1.0, "kym_id"))
        self.assertEqual(self.record["self_qid"], "Q15894956")

    def test_the_self_item_wins_over_a_more_linked_namesake(self):
        # Q219, the Venetian doge, has twice the Wikipedias.
        self.assertIn(("doge", "Q15894956"), self.by("tag"))
        self.assertIn(("Doge", "Q15894956"), self.by("about"))

    def test_without_a_self_item_the_title_is_linked_like_any_text(self):
        r = self.link(url="https://knowyourmeme.com/memes/japan-stuff", title="Japan",
                      tags=[], about=["Japan."])
        self.assertIsNone(r["self_qid"])
        self.assertEqual(self.by("title", r), [("Japan", "Q17")])

    # -- recognition -------------------------------------------------------------

    def test_named_entities_concepts_and_tags_are_all_linked(self):
        about = dict(self.by("about"))
        for text, qid in (("Shiba Inus", "Q39315"), ("Japan", "Q17"),
                          ("Reddit", "Q1136"), ("4chan", "Q531"),
                          ("dog", "Q144"), ("hair", "Q28472")):
            self.assertEqual(about.get(text), qid, text)
        self.assertEqual(dict(self.by("tag")),
                         {"doge": "Q15894956", "shiba inu": "Q39315", "dogs": "Q144"})

    def test_a_username_tag_links_nothing(self):
        self.assertNotIn("eddie_now", dict(self.by("tag")))

    def test_a_named_entity_with_no_item_is_recorded_as_nil(self):
        self.assertIn("Zorblax Quentin", [n["text"] for n in self.record["nil"]])

    def test_a_weak_alias_match_is_rejected_not_linked(self):
        self.assertNotIn("nimbus", dict(self.by("about")))
        self.assertGreaterEqual(self.record["rejected_count"], 1)

    def test_a_linked_span_claims_its_characters(self):
        spans = [(m["start"], m["end"]) for m in self.record["mentions"]
                 if m["field"] == "about"]
        for i, (a, b) in enumerate(spans):
            for c, d in spans[i + 1:]:
                self.assertTrue(b <= c or d <= a, (a, b, c, d))
        self.assertNotIn("Shiba", dict(self.by("about")))

    def test_every_mention_is_the_pages_own_words(self):
        for m in self.record["mentions"]:
            hay = E.field_text(self.unit, m["field"], m.get("tag_index"))
            self.assertEqual(hay[m["start"]:m["end"]], m["text"])

    # -- disambiguation ------------------------------------------------------------

    def test_context_separates_two_senses(self):
        planet = self.link(url="https://knowyourmeme.com/memes/a", title="A", tags=[],
                           about=["Mercury is the smallest planet in the Solar System."])
        metal = self.link(url="https://knowyourmeme.com/memes/b", title="B", tags=[],
                          about=["The thermometer held mercury, a toxic liquid metal."])
        self.assertEqual(dict(self.by("about", planet))["Mercury"], "Q308")
        self.assertEqual(dict(self.by("about", metal))["mercury"], "Q925")

    def test_a_close_race_with_nothing_to_break_it_is_not_linked(self):
        r = self.link(url="https://knowyourmeme.com/memes/c", title="C", tags=["doge"],
                      about=["An unrelated page."])
        self.assertEqual(self.by("tag", r), [])

    def test_an_ner_label_that_disagrees_costs_nothing(self):
        cands = self.lexicon.candidates("reddit")
        as_gpe = self.linker.score("Reddit", cands, ner_label="GPE", context=set(),
                                   self_qid=None)
        unlabelled = self.linker.score("Reddit", cands, ner_label=None, context=set(),
                                       self_qid=None)
        self.assertEqual(as_gpe[0][0], unlabelled[0][0])

    def test_an_ner_label_that_agrees_adds(self):
        cands = self.lexicon.candidates("japan")
        agree = self.linker.score("Japan", cands, ner_label="GPE", context=set(),
                                  self_qid=None)
        self.assertEqual(agree[0][2]["type"], 1.0)       # Q6256 -> Q56061
        none = self.linker.score("Japan", cands, ner_label=None, context=set(),
                                 self_qid=None)
        self.assertGreater(agree[0][0], none[0][0])

    # -- the record ------------------------------------------------------------------

    def test_the_record_carries_the_three_stamps(self):
        self.assertEqual(self.record["linker_version"], E.LINKER_VERSION)
        self.assertEqual(self.record["lexicon_version"], self.lexicon.version)
        self.assertEqual(self.record["nlp_model"], E.model_stamp())

    def test_linking_is_deterministic(self):
        again = self.linker.link(self.unit)
        self.assertEqual(again["mentions"], self.record["mentions"])
        self.assertEqual(again["nil"], self.record["nil"])

    def test_features_are_kept_for_curation(self):
        for m in self.record["mentions"]:
            self.assertEqual(set(m["features"]),
                             {"prior", "context", "exact", "type", "kym", "clarity"})
            self.assertIn("candidates", m)
            self.assertIn("margin", m)

    def test_the_record_passes_its_audit(self):
        self.assertEqual(E.audit(self.record, self.unit), [])

    # -- link_units -----------------------------------------------------------------

    def test_link_units_batches_and_counts(self):
        units = [self.unit, E.frame_unit(entry(url="https://knowyourmeme.com/memes/e",
                                               title="E", tags=[], about=[]))]
        seen = []
        summary = E.link_units(self.linker, units, on_record=seen.append, batch_size=1)
        self.assertEqual(len(seen), 2)
        self.assertEqual(summary["units"], 2)
        self.assertEqual(summary["frames_with_links"], 1)
        self.assertEqual(summary["self_links"], 1)
        self.assertEqual(summary["mentions"], self.record["mention_count"])
        self.assertEqual(seen[0]["mentions"], self.record["mentions"])

    def test_a_record_that_fails_its_audit_is_raised_not_written(self):
        seen = []
        broken = dict(self.record, mention_count=999)
        original = self.linker.link
        self.linker.link = lambda unit, **_kw: broken
        try:
            with self.assertRaisesRegex(AssertionError, "failed its audit"):
                E.link_units(self.linker, [self.unit], on_record=seen.append)
        finally:
            self.linker.link = original
        self.assertEqual(seen, [])


class AuditTests(unittest.TestCase):
    """Each way a record can lie about the page. No model needed."""

    UNIT = E.frame_unit(entry())

    def record(self, **mention_over):
        m = {"field": "about", "text": "Japan", "start": ABOUT.index("Japan"),
             "end": ABOUT.index("Japan") + 5, "qid": "Q17", "score": 0.66,
             "method": "ner"}
        m.update(mention_over)
        return {"mentions": [m], "mention_count": 1, "entity_count": 1,
                "source_sha256": self.UNIT["source_sha256"], "linker_version": "1",
                "lexicon_version": "v", "nlp_model": "m"}

    def test_a_faithful_record_is_clean(self):
        self.assertEqual(E.audit(self.record(), self.UNIT), [])

    def test_text_that_is_not_the_pages(self):
        self.assertTrue(E.audit(self.record(text="JAPAN"), self.UNIT))

    def test_offsets_outside_the_field(self):
        self.assertTrue(E.audit(self.record(start=5000, end=5005), self.UNIT))

    def test_a_tag_mention_needs_its_index(self):
        self.assertTrue(E.audit(self.record(field="tag", text="doge", start=0, end=4),
                                self.UNIT))
        self.assertEqual(E.audit(self.record(field="tag", text="doge", start=0, end=4,
                                             tag_index=0), self.UNIT), [])

    def test_not_a_qid(self):
        self.assertTrue(E.audit(self.record(qid="Japan"), self.UNIT))

    def test_a_score_under_the_threshold_or_outside_0_1(self):
        self.assertTrue(E.audit(self.record(score=0.1), self.UNIT))
        self.assertTrue(E.audit(self.record(score=1.5), self.UNIT))

    def test_overlapping_mentions(self):
        r = self.record()
        r["mentions"].append(dict(r["mentions"][0]))
        r["mention_count"] = 2
        self.assertTrue(any("overlaps" in p for p in E.audit(r, self.UNIT)))

    def test_counts_and_stamps(self):
        self.assertTrue(E.audit(dict(self.record(), mention_count=2), self.UNIT))
        self.assertTrue(E.audit(dict(self.record(), source_sha256="x"), self.UNIT))
        self.assertTrue(E.audit(dict(self.record(), nlp_model=None), self.UNIT))

    def test_an_unknown_field_or_method(self):
        self.assertTrue(E.audit(self.record(field="spread"), self.UNIT))
        self.assertTrue(E.audit(self.record(method="guess"), self.UNIT))


if __name__ == "__main__":
    unittest.main(verbosity=2)
