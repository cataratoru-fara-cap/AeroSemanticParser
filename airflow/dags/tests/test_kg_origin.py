"""Tests for kg/origin.py -- canonicalizing `origin` (frame.from) into
origin_concept, NOT a "platform" concept (see the module docstring: the
real corpus mixes platforms, countries, franchises, companies and people
in this field). Mirrors test_kg_taxonomy.py's layout: the real curated
file is the migration's proof, synthetic YAML pins edge-case behaviour.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_origin.py -v
"""
import textwrap
import unittest
from pathlib import Path

from modules.kg import origin, taxonomy

CURATED = (Path(__file__).resolve().parents[1] / "kg_config"
          / "origin_taxonomy.yaml")


def _write(tmpdir: str, body: str) -> str:
    path = Path(tmpdir) / "origin.yaml"
    path.write_text(textwrap.dedent(body))
    return str(path)


class LoadCuratedFileTests(unittest.TestCase):
    """Against the real file."""

    @classmethod
    def setUpClass(cls):
        cls.tax = origin.load(str(CURATED))

    def test_loads_without_error(self):
        self.assertTrue(self.tax.aliases)
        self.assertTrue(self.tax.hierarchy.edges)

    def test_every_hierarchy_narrower_is_an_alias_target(self):
        canonical = set(self.tax.aliases.values())
        for e in self.tax.hierarchy.edges:
            self.assertIn(e.narrower, canonical, e.pair)

    def test_no_country_franchise_or_company_has_a_hierarchy_parent(self):
        # Spot-check a few real non-platform canonical slugs from the
        # curated aliases: none of them should appear as a narrower slug
        # in the platform-only hierarchy.
        narrower_slugs = {e.narrower for e in self.tax.hierarchy.edges}
        for slug in ("united-states", "the-simpsons", "donald-trump",
                    "elden-ring", "nintendo"):
            self.assertIn(slug, self.tax.aliases.values())
            self.assertNotIn(slug, narrower_slugs)

    def test_version_is_a_sha256_of_the_file(self):
        self.assertEqual(len(self.tax.version), 64)


class ParseFromDictTests(unittest.TestCase):
    """The alias/bucket split, without touching a real file."""

    def _load(self, tmp_path, body):
        return origin.load(_write(tmp_path, body))

    def test_aliases_and_hierarchy_both_parse(self, ):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tax = self._load(tmp, """
                aliases:
                  Twitter: twitter
                  X: twitter
                broader_confirmed:
                  - broader: social-network
                    narrower: twitter
                    rationale: "test"
            """)
        self.assertEqual(tax.aliases, {"Twitter": "twitter", "X": "twitter"})
        self.assertEqual(len(tax.hierarchy.edges), 1)

    def test_aliases_key_is_optional(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tax = self._load(tmp, "broader_confirmed: []\n")
        self.assertEqual(tax.aliases, {})

    def test_unknown_bucket_key_still_raises_via_taxonomy_parse(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(taxonomy.TaxonomyError):
                self._load(tmp, "aliases: {}\nnot_a_real_bucket: []\n")


class ConsistencyGuardTests(unittest.TestCase):
    def test_hierarchy_narrower_not_aliased_anywhere_is_rejected(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(taxonomy.TaxonomyError):
                self._load = origin.load(_write(tmp, """
                    aliases:
                      Twitter: twitter
                    broader_confirmed:
                      - broader: social-network
                        narrower: 4chan
                        rationale: "typo -- 4chan is never aliased here"
                """))

    def test_broader_umbrella_needs_no_alias(self):
        # The one deliberate asymmetry: social-network is a synthetic
        # parent, never itself an alias target -- must NOT raise.
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tax = origin.load(_write(tmp, """
                aliases:
                  Twitter: twitter
                broader_confirmed:
                  - broader: social-network
                    narrower: twitter
                    rationale: "ok"
            """))
        self.assertEqual(len(tax.hierarchy.edges), 1)


class ConceptEdgesTests(unittest.TestCase):
    def _tax(self, tmp):
        return origin.load(_write(tmp, """
            aliases:
              Twitter: twitter
              4chan: 4chan
            broader_confirmed:
              - broader: social-network
                narrower: twitter
                rationale: "ok"
              - broader: imageboard
                narrower: 4chan
                rationale: "ok"
        """))

    def test_concept_edges_use_the_origin_prefix(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tax = self._tax(tmp)
        edges = origin.concept_edges(tax)
        self.assertIn({"src": "origin:twitter", "dst": "origin:social-network",
                       "type": "subTypeOf"}, edges)

    def test_encodable_edges_filters_by_narrower_census_presence_only(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tax = self._tax(tmp)
        census = {"value_counts": {"Twitter": 100}}   # only twitter seen
        edges = origin.encodable_edges(tax, census)
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["src"], "origin:twitter")

    def test_encodable_edges_empty_when_narrower_never_seen(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tax = self._tax(tmp)
        census = {"value_counts": {"SomethingElseEntirely": 5}}
        self.assertEqual(origin.encodable_edges(tax, census), [])

    def test_validate_report_shape(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tax = self._tax(tmp)
        census = {"value_counts": {"Twitter": 100}}
        report = origin.validate(tax, census)
        self.assertEqual(report["edges_declared"], 2)
        self.assertEqual(report["edges_encoded"], 1)
        self.assertEqual(report["slugs_missing_from_census"], ["4chan"])
        self.assertEqual(report["aliases_declared"], 2)


class AliasResolutionTests(unittest.TestCase):
    ALIASES = {"Twitter": "twitter", "X / Twitter": "twitter"}

    def test_exact_alias_match(self):
        self.assertEqual(origin.resolve("Twitter", self.ALIASES), "twitter")

    def test_case_insensitive_alias_match(self):
        self.assertEqual(origin.resolve("twitter", self.ALIASES), "twitter")
        self.assertEqual(origin.resolve("TWITTER", self.ALIASES), "twitter")

    def test_unknown_value_falls_through_to_a_slug(self):
        self.assertEqual(origin.resolve("Some New Platform", self.ALIASES),
                         "some-new-platform")

    def test_unknown_value_never_raises_or_returns_empty(self):
        for raw in ("", "   ", "!!!", "日本語"):
            slug = origin.resolve(raw, self.ALIASES)
            self.assertTrue(slug)

    def test_punctuation_only_value_falls_back_to_a_hash_slug(self):
        slug = origin.resolve("!!!", {})
        self.assertTrue(slug.startswith("origin-"))


class CanonicalCensusTests(unittest.TestCase):
    def test_raw_values_are_summed_through_aliases(self):
        raw_census = {"value_counts": {"Twitter": 10, "X / Twitter": 5, "4chan": 3}}
        aliases = {"Twitter": "twitter", "X / Twitter": "twitter"}
        canonical = origin.canonical_census(raw_census, aliases)
        self.assertEqual(canonical["value_counts"]["twitter"], 15)
        self.assertEqual(canonical["value_counts"]["4chan"], 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
