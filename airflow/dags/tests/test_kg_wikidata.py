"""Tests for kg/wikidata.py — a Wikidata dump -> the local entity lexicon.

The fixture (wikidata_fixture.py) is a dump in the real JSON-lines layout,
so the builder under test is the one that reads the 156 GB file: same
decompression, same byte-level prefilter, same parser.

What these pin, beyond "it works":

  * **The filter.** A disambiguation page must never be a candidate (it
    is the page that lists every Doge, not a Doge); an item no Wikipedia
    has is kept when it has a KYM slug (a meme is often on Wikidata and KYM
    and nowhere else); a ``mul``-only label counts as English.
  * **The KYM join goes through P13484, not P6760.** P6760 is KYM's
    internal number, which the page never shows and the parser never
    extracts; P13484 is the URL's last segment. IMKG joined on P6760.
  * **P279 survives the filter.** A dropped class still carries the
    subclass edge the NER-type walk needs.
  * **The version is a function of the dump and the filters** — the stamp
    every frame is re-linked on.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_wikidata.py -v
"""
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout

from modules.kg import wikidata as wd
from wikidata_fixture import KEPT, dump_lines, item, write_dump


def build(tmp: str, name: str = "lexicon.sqlite", items=None, **kw) -> tuple[str, dict]:
    dump = write_dump(os.path.join(tmp, "dump.json.gz"), items)
    out = os.path.join(tmp, name)
    summary = wd.build_lexicon(dump, out, workers=0, progress=lambda _l: None, **kw)
    return out, summary


class NormTests(unittest.TestCase):
    def test_case_typography_and_dashes_fold(self):
        self.assertEqual(wd.norm("Etch-a-Sketch"), "etch a sketch")
        self.assertEqual(wd.norm("  Shiba–Inu "), "shiba inu")
        self.assertEqual(wd.norm("Lowe’s"), "lowe's")
        self.assertEqual(wd.norm("“Doge”."), "doge")

    def test_nothing_is_empty(self):
        self.assertEqual(wd.norm(None), "")
        self.assertEqual(wd.norm(" .. "), "")

    def test_kym_slug_is_the_urls_last_segment(self):
        self.assertEqual(wd.kym_slug("https://knowyourmeme.com/memes/sites/reddit"), "reddit")
        self.assertEqual(wd.kym_slug("https://knowyourmeme.com/memes/doge/"), "doge")
        self.assertEqual(wd.kym_slug("diet-coke-and-mentos"), "diet-coke-and-mentos")
        self.assertEqual(wd.kym_slug("https://knowyourmeme.com/memes/doge?x=1#y"), "doge")


class ParseEntityTests(unittest.TestCase):
    def test_a_property_and_the_array_brackets_are_not_items(self):
        lines = dump_lines()
        self.assertIsNone(wd.parse_entity(lines[0]))       # "["
        self.assertIsNone(wd.parse_entity(lines[-1]))      # "]"
        self.assertIsNone(wd.parse_entity(lines[-2]))      # the property

    def test_a_line_with_nothing_to_keep_is_dropped_before_parsing(self):
        line = json.dumps(item("Q1", "x"), separators=(",", ":"))
        self.assertIsNone(wd.parse_entity(line))

    def test_the_trailing_comma_is_tolerated(self):
        line = json.dumps(item("Q144", "dog", sitelinks=3), separators=(",", ":")) + ","
        self.assertEqual(wd.parse_entity(line)["label"], "dog")

    def test_only_wikipedias_count_as_sitelinks(self):
        row = wd.parse_entity(json.dumps(item("Q144", "dog", sitelinks=4),
                                         separators=(",", ":")))
        self.assertEqual(row["sitelinks"], 4)      # commonswiki not counted


class BuildTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.path, cls.summary = build(cls._tmp.name)
        cls.lex = wd.Lexicon(cls.path)

    @classmethod
    def tearDownClass(cls):
        cls.lex.close()
        cls._tmp.cleanup()

    def ids(self, text):
        return [c.id for c in self.lex.candidates(wd.norm(text))]

    def test_exactly_the_expected_items_are_kept(self):
        kept = {wd.qid_str(q) for (q,) in self.lex._db.execute("SELECT qid FROM entity")}
        self.assertEqual(kept, KEPT)
        self.assertEqual(self.summary["entities"], len(KEPT))

    def test_labels_and_aliases_are_looked_up_normalised(self):
        self.assertEqual(self.ids("Shiba Inu"), ["Q39315"])
        self.assertEqual(self.ids("shiba"), ["Q39315"])              # an alias
        self.assertEqual(self.ids("SHIBA-INU"), ["Q39315"])          # dash-folded, deduped

    def test_candidates_come_most_linked_first(self):
        self.assertEqual(self.ids("doge"), ["Q219", "Q15894956"])

    def test_a_disambiguation_page_is_never_a_candidate(self):
        self.assertNotIn("Q9999901", self.ids("Doge"))

    def test_a_kym_slug_keeps_an_item_no_wikipedia_has(self):
        self.assertEqual(self.ids("Kabosu"), ["Q9999903"])

    def test_a_mul_label_counts_as_english(self):
        (c,) = self.lex.candidates("atsuko sato")
        self.assertEqual((c.id, c.label, c.is_label), ("Q9999905", "Atsuko Sato", True))

    def test_an_item_without_an_english_or_mul_label_is_dropped(self):
        self.assertEqual(self.ids("Chien de garde"), [])

    def test_a_candidate_says_how_it_matched(self):
        (c,) = self.lex.candidates("shiba")
        self.assertEqual((c.surface, c.is_label, c.label), ("Shiba", False, "Shiba Inu"))
        self.assertEqual(c.sitelinks, 47)

    def test_frames_join_on_the_kym_slug(self):
        self.assertEqual(self.lex.by_kym("https://knowyourmeme.com/memes/doge").id,
                         "Q15894956")
        self.assertEqual(self.lex.by_kym("https://knowyourmeme.com/memes/sites/reddit").id,
                         "Q1136")
        self.assertIsNone(self.lex.by_kym("https://knowyourmeme.com/memes/cheems"))

    def test_the_numeric_kym_id_is_kept_for_later(self):
        self.assertEqual(self.lex.by_kym_id("13564").id, "Q15894956")
        self.assertIsNone(self.lex.by_kym("13564"))      # a number is not a slug

    def test_p279_survives_for_a_dropped_class(self):
        self.assertIsNone(self.lex.entity("Q9999906"))
        self.assertIn(215627, self.lex.ancestors(9999906))

    def test_ancestors_walk_p279_transitively(self):
        self.assertTrue({5, 215627} <= self.lex.ancestors(5))
        self.assertIn(56061, self.lex.families([6256]))

    def test_meta_records_what_the_lexicon_was_built_from(self):
        meta = self.lex.meta
        self.assertEqual(meta["dump"], "dump.json.gz")
        self.assertEqual(meta["builder_version"], wd.LEXICON_BUILDER_VERSION)
        self.assertEqual(meta["dump_newest_modified"], "2026-09-14T04:37:01Z")
        self.assertEqual(self.lex.version, self.summary["version"])


class VersionTests(unittest.TestCase):
    def test_the_same_dump_and_filters_give_the_same_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, a = build(tmp, "a.sqlite")
            _, b = build(tmp, "b.sqlite")
            _, c = build(tmp, "c.sqlite", min_sitelinks=2)
        self.assertEqual(a["version"], b["version"])
        self.assertNotEqual(a["version"], c["version"])

    def test_a_stricter_filter_drops_the_less_linked(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = build(tmp, min_sitelinks=2)
            with wd.Lexicon(path) as lex:
                self.assertIsNone(lex.entity("Q9999907"))       # 1 Wikipedia
                self.assertIsNotNone(lex.entity("Q9999903"))    # 0, but a KYM slug


class RobustnessTests(unittest.TestCase):
    def test_a_truncated_dump_keeps_what_was_read(self):
        # A head sample or a partial download ends in a torn gzip member.
        with tempfile.TemporaryDirectory() as tmp:
            dump = write_dump(os.path.join(tmp, "dump.json.gz"),
                              [item(f"Q{n}", f"thing {n}", sitelinks=2)
                               for n in range(1, 3000)])
            with open(dump, "rb") as fh:
                data = fh.read()
            with open(dump, "wb") as fh:
                fh.write(data[: len(data) // 2])
            out = os.path.join(tmp, "lex.sqlite")
            summary = wd.build_lexicon(dump, out, workers=0, progress=lambda _l: None)
        self.assertGreater(summary["entities"], 100)
        self.assertLess(summary["entities"], 2999)

    def test_a_failed_build_leaves_the_previous_lexicon_untouched(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "lex.sqlite")
            with open(out, "w") as fh:
                fh.write("previous")
            with self.assertRaises(OSError):
                wd.build_lexicon(os.path.join(tmp, "missing.json.gz"), out,
                                 workers=0, progress=lambda _l: None)
            with open(out) as fh:
                self.assertEqual(fh.read(), "previous")

    def test_a_missing_lexicon_says_how_to_build_one(self):
        with self.assertRaisesRegex(FileNotFoundError, "modules.kg.wikidata build"):
            wd.Lexicon("/nonexistent/lexicon.sqlite")

    def test_the_process_pool_gives_the_same_lexicon(self):
        with tempfile.TemporaryDirectory() as tmp:
            dump = write_dump(os.path.join(tmp, "dump.json.gz"))
            a = wd.build_lexicon(dump, os.path.join(tmp, "a.sqlite"), workers=0,
                                 progress=lambda _l: None)
            b = wd.build_lexicon(dump, os.path.join(tmp, "b.sqlite"), workers=2,
                                 progress=lambda _l: None)
        self.assertEqual({k: a[k] for k in ("entities", "aliases", "subclass_edges",
                                            "kym_ids", "version")},
                         {k: b[k] for k in ("entities", "aliases", "subclass_edges",
                                            "kym_ids", "version")})


class PriorTests(unittest.TestCase):
    def test_monotonic_and_saturating(self):
        self.assertEqual(wd.prior(0), 0.0)
        self.assertLess(wd.prior(3), wd.prior(30))
        self.assertEqual(wd.prior(150), 1.0)
        self.assertEqual(wd.prior(400), 1.0)


class CliTests(unittest.TestCase):
    def test_lookup_and_info_print_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = build(tmp)
            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertEqual(wd.main(["lookup", "--lexicon", path, "Shiba Inu"]), 0)
            self.assertEqual(json.loads(buf.getvalue())["candidates"][0]["qid"], "Q39315")
            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertEqual(wd.main(["info", "--lexicon", path]), 0)
            self.assertIn("version", json.loads(buf.getvalue()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
