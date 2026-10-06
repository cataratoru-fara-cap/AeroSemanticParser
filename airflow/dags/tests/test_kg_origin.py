"""kg/origin.py: `origin` (frame.from) -> origin_concept, NOT a "platform"
concept (the corpus mixes platforms, countries, franchises, companies and
people in this field). As in test_kg_taxonomy.py, the real curated file is the
migration's proof and synthetic YAML pins the edge cases."""
import textwrap

import pytest

from helpers import KG_CONFIG
from modules.kg import origin, taxonomy


@pytest.fixture
def load(tmp_path):
    def write_and_load(body):
        path = tmp_path / "origin.yaml"
        path.write_text(textwrap.dedent(body))
        return origin.load(str(path))
    return write_and_load


def test_the_curated_file():
    tax = origin.load(str(KG_CONFIG / "origin_taxonomy.yaml"))
    assert tax.aliases and tax.hierarchy.edges and len(tax.version) == 64      # a sha256 of the file
    canonical, narrower = set(tax.aliases.values()), {e.narrower for e in tax.hierarchy.edges}
    assert narrower <= canonical
    # a country, a franchise, a person, a game, a company: aliased, never under the platform hierarchy
    for slug in ("united-states", "the-simpsons", "donald-trump", "elden-ring", "nintendo"):
        assert slug in canonical and slug not in narrower


TWO = """
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
"""


def test_parsing(load):
    tax = load("""
        aliases:
          Twitter: twitter
          X: twitter
        broader_confirmed:
          - broader: social-network
            narrower: twitter
            rationale: "test"
    """)
    # social-network, a synthetic umbrella, is no alias target: the one deliberate asymmetry
    assert tax.aliases == {"Twitter": "twitter", "X": "twitter"} and len(tax.hierarchy.edges) == 1
    assert load("broader_confirmed: []\n").aliases == {}                       # aliases is optional


@pytest.mark.parametrize("body", [
    "aliases: {}\nnot_a_real_bucket: []\n",                                     # via taxonomy's parse
    """
    aliases:
      Twitter: twitter
    broader_confirmed:
      - broader: social-network
        narrower: 4chan
        rationale: "typo -- 4chan is never aliased here"
    """,
])
def test_refused(load, body):
    with pytest.raises(taxonomy.TaxonomyError):
        load(body)


def test_edges_are_encoded_for_narrowers_the_census_has_seen(load):
    tax = load(TWO)
    assert {"src": "origin:twitter", "dst": "origin:social-network", "type": "subTypeOf"} in origin.concept_edges(tax)
    [edge] = origin.encodable_edges(tax, {"value_counts": {"Twitter": 100}})
    assert edge["src"] == "origin:twitter"
    assert origin.encodable_edges(tax, {"value_counts": {"SomethingElseEntirely": 5}}) == []
    report = origin.validate(tax, {"value_counts": {"Twitter": 100}})
    assert (report["edges_declared"], report["edges_encoded"], report["slugs_missing_from_census"],
            report["aliases_declared"]) == (2, 1, ["4chan"], 2)


ALIASES = {"Twitter": "twitter", "X / Twitter": "twitter"}


@pytest.mark.parametrize("raw, slug", [("Twitter", "twitter"), ("twitter", "twitter"), ("TWITTER", "twitter"),
                                       ("Some New Platform", "some-new-platform")])
def test_resolve(raw, slug):
    assert origin.resolve(raw, ALIASES) == slug


@pytest.mark.parametrize("raw", ["", "   ", "!!!", "日本語"])
def test_resolve_never_raises_or_returns_empty(raw):
    assert origin.resolve(raw, ALIASES)


def test_a_punctuation_only_value_gets_a_hash_slug_and_the_census_sums_through_aliases():
    assert origin.resolve("!!!", {}).startswith("origin-")
    canonical = origin.canonical_census({"value_counts": {"Twitter": 10, "X / Twitter": 5, "4chan": 3}}, ALIASES)
    assert (canonical["value_counts"]["twitter"], canonical["value_counts"]["4chan"]) == (15, 3)
