"""Tests for kg/ntdiff.py — set difference over two N-Triples files.

The gate this module powers exists because the in-process and RML
derivations of the graph drifted 14,563 ``mk:relatesToMeme`` triples apart
and nothing compared them for two months.

``test_order_does_not_matter`` and ``test_repeats_are_not_differences`` are
the load-bearing ones: RDF is a set, so a serializer emitting the same
triples in a different order, or emitting one twice, is not a divergence.
A line-oriented diff would report both as differences and the gate would be
abandoned as noisy within a week.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_ntdiff.py -v
"""
import tempfile
import unittest
from pathlib import Path

from modules.kg import ntdiff

S = "<https://knowyourmeme.com/memes/doge>"
P = "<https://meme4.science/atlas/hasTag>"
TRIPLES = [
    f'{S} {P} "doge" .',
    f'{S} {P} "shiba" .',
    f'{S} <https://meme4.science/atlas/partOfSeries> '
    f'<https://knowyourmeme.com/memes/shiba-inu> .',
]


class NormalizeTests(unittest.TestCase):
    def test_blank_and_comment_lines_carry_no_triple(self):
        self.assertIsNone(ntdiff.normalize_line("   "))
        self.assertIsNone(ntdiff.normalize_line("# a comment"))

    def test_whitespace_is_collapsed(self):
        a = ntdiff.normalize_line(f'{S}   {P}    "doge" .')
        b = ntdiff.normalize_line(f'{S} {P} "doge" .')
        self.assertEqual(a, b)

    def test_explicit_xsd_string_is_equivalent_to_a_plain_literal(self):
        typed = (f'{S} {P} "doge"'
                 '^^<http://www.w3.org/2001/XMLSchema#string> .')
        self.assertEqual(ntdiff.normalize_line(typed),
                         ntdiff.normalize_line(f'{S} {P} "doge" .'))

    def test_trailing_dot_is_normalised_not_doubled(self):
        self.assertTrue(ntdiff.normalize_line(f'{S} {P} "x" .').endswith(' .'))
        self.assertFalse(ntdiff.normalize_line(f'{S} {P} "x" .').endswith('. .'))

    def test_whitespace_inside_a_literal_is_preserved(self):
        # Regression: collapsing runs of spaces inside literals merged two
        # distinct triples into one on the real corpus. Between terms it is
        # formatting; inside a literal it is the value.
        one = ntdiff.normalize_line(f'{S} {P} "doge  meme" .')
        two = ntdiff.normalize_line(f'{S} {P} "doge meme" .')
        self.assertNotEqual(one, two)
        self.assertIn('"doge  meme"', one)

    def test_tab_inside_a_literal_is_preserved(self):
        got = ntdiff.normalize_line(f'{S} {P} "a\\tb" .')
        self.assertIn('"a\\tb"', got)

    def test_language_tag_and_datatype_survive(self):
        tagged = ntdiff.normalize_line(f'{S} {P} "doge"@en .')
        self.assertIn('"doge"@en', tagged)
        typed = ntdiff.normalize_line(
            f'{S} {P} "3"^^<http://www.w3.org/2001/XMLSchema#integer> .')
        self.assertIn("XMLSchema#integer", typed)

    def test_literal_containing_a_space_is_one_term(self):
        got = ntdiff.normalize_line(f'{S} {P} "two words" .')
        self.assertEqual(got, f'{S} {P} "two words" .')


class PredicateTests(unittest.TestCase):
    def test_local_name_after_slash(self):
        self.assertEqual(ntdiff.predicate_of(TRIPLES[0]), "hasTag")

    def test_local_name_after_hash(self):
        line = ('<a> <http://www.w3.org/2004/02/skos/core#broader> <b> .')
        self.assertEqual(ntdiff.predicate_of(line), "broader")

    def test_unparsed_line_is_labelled_not_crashed(self):
        self.assertEqual(ntdiff.predicate_of("nonsense"), "(unparsed)")


class DigestTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def write(self, name, lines):
        path = self.tmp / name
        path.write_text("\n".join(lines) + "\n")
        return str(path)

    def test_order_does_not_matter(self):
        a = ntdiff.digest(self.write("a.nt", TRIPLES))
        b = ntdiff.digest(self.write("b.nt", list(reversed(TRIPLES))))
        self.assertEqual(a[0], b[0])
        self.assertEqual(a[1], b[1])

    def test_repeats_are_not_differences(self):
        a = ntdiff.digest(self.write("a.nt", TRIPLES))
        b = ntdiff.digest(self.write("b.nt", TRIPLES + [TRIPLES[0]]))
        self.assertEqual(a[0], b[0])
        self.assertEqual(a[1], b[1])   # counted once

    def test_a_different_set_digests_differently(self):
        a = ntdiff.digest(self.write("a.nt", TRIPLES))
        b = ntdiff.digest(self.write("b.nt", TRIPLES[:2]))
        self.assertNotEqual(a[0], b[0])

    def test_per_predicate_counts(self):
        _, count, preds = ntdiff.digest(self.write("a.nt", TRIPLES))
        self.assertEqual(count, 3)
        self.assertEqual(preds, {"hasTag": 2, "partOfSeries": 1})


class DiffTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def write(self, name, lines):
        path = self.tmp / name
        path.write_text("\n".join(lines) + "\n")
        return str(path)

    def test_identical_files_are_equal(self):
        a = self.write("a.nt", TRIPLES)
        b = self.write("b.nt", list(reversed(TRIPLES)))
        report = ntdiff.diff(a, b)
        self.assertTrue(report["equal"])
        self.assertEqual(report["in-process"]["triples"], 3)

    def test_divergence_is_reported_per_predicate(self):
        a = self.write("a.nt", TRIPLES)
        b = self.write("b.nt", TRIPLES[:1] + [
            f'{S} {P} "different" .',
            f'{S} <https://meme4.science/atlas/relatesToMeme> '
            f'<https://knowyourmeme.com/memes/cheems> .'])
        report = ntdiff.diff(a, b, buckets=8)
        self.assertFalse(report["equal"])
        self.assertEqual(report["by_predicate"]["hasTag"]["only_in_in-process"], 1)
        self.assertEqual(report["by_predicate"]["hasTag"]["only_in_rml"], 1)
        self.assertEqual(
            report["by_predicate"]["partOfSeries"]["only_in_in-process"], 1)
        self.assertEqual(
            report["by_predicate"]["relatesToMeme"]["only_in_rml"], 1)

    def test_samples_are_captured_and_capped(self):
        a = self.write("a.nt", [f'{S} {P} "t{i}" .' for i in range(40)])
        b = self.write("b.nt", [f'{S} {P} "t0" .'])
        report = ntdiff.diff(a, b, buckets=8, samples=5)
        self.assertEqual(len(report["only_in_in-process"]), 5)
        self.assertEqual(report["first_divergent_predicate"], "hasTag")

    def test_bucket_count_does_not_change_the_answer(self):
        a = self.write("a.nt", [f'{S} {P} "t{i}" .' for i in range(50)])
        b = self.write("b.nt", [f'{S} {P} "t{i}" .' for i in range(25)])
        few = ntdiff.diff(a, b, buckets=2)
        many = ntdiff.diff(a, b, buckets=64)
        self.assertEqual(few["by_predicate"]["hasTag"]["only_in_in-process"],
                         many["by_predicate"]["hasTag"]["only_in_in-process"])
        self.assertEqual(few["by_predicate"]["hasTag"]["only_in_in-process"], 25)

    def test_blank_nodes_are_refused_rather_than_mishandled(self):
        a = self.write("a.nt", ["_:b0 <http://p> <http://o> ."])
        b = self.write("b.nt", TRIPLES)
        with self.assertRaises(ValueError) as ctx:
            ntdiff.diff(a, b)
        self.assertIn("blank node", str(ctx.exception))

    def test_report_renders(self):
        a = self.write("a.nt", TRIPLES)
        b = self.write("b.nt", TRIPLES[:1])
        text = ntdiff.format_report(ntdiff.diff(a, b, buckets=4))
        self.assertIn("DIVERGENT", text)
        self.assertIn("hasTag", text)

    def test_equal_report_renders(self):
        a = self.write("a.nt", TRIPLES)
        text = ntdiff.format_report(ntdiff.diff(a, a))
        self.assertIn("identical", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
