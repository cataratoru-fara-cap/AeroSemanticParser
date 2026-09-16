"""The vocabulary invariant: one edge type set, two representations.

kg_config/kg_mapping.yarrrml.yml states, in a comment, that the RDF predicate
local names are deliberately identical to kg/build.py's edge ``type``
strings — "one vocabulary, two exports, no separate name to keep in sync by
hand". That was true when written and enforced by nothing.

This test makes it a checked invariant. If someone adds an edge type in
build.py without adding the matching predicate to the mapping (or renames
one on either side), the build fails here rather than producing a property
graph and an RDF graph that quietly describe different things.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_vocabulary.py -v
"""
import unittest
from pathlib import Path

from modules.kg import build, taxonomy

# .yarrrml.yml, not .yarrrml: yatter refuses any extension but .yml/.yaml,
# and this is also the name the file's own header gives it.
MAPPING = Path(__file__).resolve().parents[1] / "kg_config" / "kg_mapping.yarrrml.yml"

# Predicates that describe a node rather than relate two nodes. These are
# attributes and class membership, not graph edges, so they are outside the
# edge-type vocabulary by design.
ATTRIBUTE_PREDICATES = {
    "a",            # rdf:type
    "label",        # rdfs:label
    "category",     # mk:category
    "status",       # mk:status
    "inScheme",     # skos:inScheme
    "prefLabel",    # skos:prefLabel
}


def mapping_predicate_local_names() -> set[str]:
    """Every predicate in every `po:` block, reduced to its local name."""
    import yaml

    doc = yaml.safe_load(MAPPING.read_text())
    locals_: set[str] = set()
    for rule in (doc.get("mappings") or {}).values():
        for po in rule.get("po") or []:
            predicate = po[0] if isinstance(po, list) else po
            locals_.add(str(predicate).split(":")[-1])
    return locals_


class VocabularyInvariantTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mapped = mapping_predicate_local_names()
        cls.ours = set(build.EDGE_TYPES) | set(taxonomy.CONCEPT_EDGE_TYPES)

    def test_mapping_file_is_readable(self):
        # It is source, tracked in dags/kg_config/. It used to live under the
        # gitignored data/, where it existed only on one machine.
        self.assertTrue(MAPPING.is_file(), f"{MAPPING} is missing")
        self.assertTrue(self.mapped)

    def test_every_edge_type_has_a_predicate_in_the_mapping(self):
        missing = self.ours - self.mapped
        self.assertEqual(missing, set(),
                         f"edge types with no RDF predicate: {sorted(missing)}")

    def test_every_relating_predicate_is_a_known_edge_type(self):
        extra = self.mapped - self.ours - ATTRIBUTE_PREDICATES
        self.assertEqual(extra, set(),
                         f"RDF predicates no edge type produces: {sorted(extra)}")

    def test_the_two_sets_are_exactly_equal(self):
        self.assertEqual(self.mapped - ATTRIBUTE_PREDICATES, self.ours)

    def test_skos_broader_local_name_is_broader(self):
        # The concept edge type string must be the SKOS local name, or the
        # mapping and the property graph disagree on one relation.
        self.assertEqual(taxonomy.CONCEPT_EDGE_TYPES, ("broader",))
        self.assertIn("broader", self.mapped)

    def test_edge_types_and_concept_types_do_not_overlap(self):
        self.assertEqual(
            set(build.EDGE_TYPES) & set(taxonomy.CONCEPT_EDGE_TYPES), set())


if __name__ == "__main__":
    unittest.main(verbosity=2)
