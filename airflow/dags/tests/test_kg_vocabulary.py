"""The vocabulary invariant: one graph, two derivations, one declared vocabulary.

MemeAtlas's RDF is produced by kg/rdf.py and independently re-derived by
kg_config/kg_mapping.yarrrml.yml (via morph-kgc, in kym_kg_validate). The
diff gate compares their OUTPUT, but only for the data a build happens to
contain. These tests compare the two derivations' DEFINITIONS, so a
predicate added on one side and forgotten on the other fails here, before a
build, even if no entry in the corpus exercises it yet.

Checked, at the level of full IRIs (local names collide across m4s:/mk:/
skos:, so comparing local names — as this test once did — is not enough):

  * the predicates the mapping uses == the predicates rdf.py can emit
  * the constant classes the mapping uses == rdf.py's constant classes,
    and the data-driven class templates are the same namespaces
  * the CSVs the mapping reads == the CSVs serialize.py writes, and every
    ``$(column)`` it references is in that CSV's header
  * every mk: term rdf.py can emit is declared in kg_config/memeatlas.ttl,
    and every term declared there is one rdf.py can emit (no dead terms)
  * IMKG's own terms are used, not re-declared under mk:

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_vocabulary.py -v
"""
import re
import unittest
from pathlib import Path

from modules.kg import build, rdf, serialize, taxonomy

CONFIG = Path(__file__).resolve().parents[1] / "kg_config"
# .yarrrml.yml, not .yarrrml: yatter refuses any extension but .yml/.yaml.
MAPPING = CONFIG / "kg_mapping.yarrrml.yml"
ONTOLOGY = CONFIG / "memeatlas.ttl"

_TEMPLATE = re.compile(r"\$\(([^)]+)\)")


def load_mapping() -> dict:
    import yaml
    return yaml.safe_load(MAPPING.read_text(encoding="utf-8"))


def expand(term: str, prefixes: dict[str, str]) -> str:
    term = term.split("~", 1)[0]
    if term.startswith(("http://", "https://")):
        return term
    prefix, _, local = term.partition(":")
    return prefixes[prefix] + local


def source_file(rule: dict) -> str:
    (source,) = rule["sources"]
    return Path(source[0].split("~", 1)[0]).name


def declared_mk_terms() -> set[str]:
    """Subjects of declarations in memeatlas.ttl: ``mk:Term a ...`` lines."""
    mk = rdf.PREFIXES["mk"]
    return {mk + m.group(1) for m in re.finditer(
        r"^mk:([A-Za-z][A-Za-z0-9]*)\s+a\s", ONTOLOGY.read_text(encoding="utf-8"),
        re.MULTILINE)}


class MappingCrosswalkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.doc = load_mapping()
        cls.prefixes = {**rdf.PREFIXES, **cls.doc["prefixes"]}
        cls.predicates: set[str] = set()
        cls.constant_classes: set[str] = set()
        cls.class_templates: set[str] = set()
        for rule in cls.doc["mappings"].values():
            for po in rule.get("po") or []:
                pred, obj = po[0], po[1]
                if pred == "a":
                    if "$(" in obj:
                        cls.class_templates.add(obj.split("$(", 1)[0])
                    else:
                        cls.constant_classes.add(expand(obj, cls.prefixes))
                else:
                    cls.predicates.add(expand(pred, cls.prefixes))

    def test_mapping_prefixes_agree_with_rdf_py(self):
        for prefix, iri in self.doc["prefixes"].items():
            self.assertEqual(rdf.PREFIXES.get(prefix), iri, prefix)

    def test_predicates_are_exactly_what_rdf_py_emits(self):
        ours = rdf.all_predicates(include_provenance=False)
        self.assertEqual(self.predicates - ours, set(),
                         "mapping predicates rdf.py never emits")
        self.assertEqual(ours - self.predicates, set(),
                         "rdf.py predicates the mapping never emits")

    def test_constant_classes_are_exactly_rdf_pys(self):
        # mk:ExternalReference / mk:AdditionalReference come from a template
        # in the mapping (ref_class) and are constants in rdf.py.
        ref_classes = {rdf.PREFIXES["mk"] + c for c in rdf.REFERENCE_CLASSES}
        self.assertEqual(self.constant_classes, rdf.constant_classes() - ref_classes)

    def test_class_templates_are_the_imkg_and_atlas_namespaces(self):
        self.assertEqual(self.class_templates,
                         {rdf.KYM_CLASS_BASE, rdf.TYPES_BASE, rdf.PREFIXES["mk"]})

    def test_sources_are_exactly_the_files_serialize_writes(self):
        read = {source_file(rule) for rule in self.doc["mappings"].values()}
        self.assertEqual(read, serialize.all_rml_files())

    def test_every_referenced_column_exists_in_its_csv(self):
        headers = {name: tuple(c for c, _ in cols)
                   for name, (_, cols) in serialize.RML_NODE_FILES.items()}
        headers.update({name: ("url", col)
                        for name, (_, col) in serialize.RML_LIST_FILES.items()})
        headers.update(serialize.RML_CONCEPT_FILES)
        headers.update({name: header for name, header
                        in serialize.EDGE_TYPE_TO_RML_FILE.values()})
        for rule_name, rule in self.doc["mappings"].items():
            header = headers[source_file(rule)]
            texts = [rule["subjects"]] + [str(x) for po in rule.get("po") or []
                                          for x in po]
            for text in texts:
                for column in _TEMPLATE.findall(text):
                    self.assertIn(column, header, f"{rule_name}: $({column})")

    def test_no_csv_column_collides_with_morph_kgc_internals(self):
        columns = {c for _, cols in serialize.RML_NODE_FILES.values() for c, _ in cols}
        columns |= {c for _, c in serialize.RML_LIST_FILES.values()}
        columns |= {c for cols in serialize.RML_CONCEPT_FILES.values() for c in cols}
        columns |= {c for _, cols in serialize.EDGE_TYPE_TO_RML_FILE.values() for c in cols}
        self.assertEqual(columns & serialize.RESERVED_COLUMNS, set())

    def test_every_edge_type_has_an_rml_file_and_a_predicate(self):
        ours = set(build.EDGE_TYPES) | set(taxonomy.CONCEPT_EDGE_TYPES)
        self.assertEqual(set(serialize.EDGE_TYPE_TO_RML_FILE), ours)
        self.assertEqual(set(rdf.EDGE_PREDICATES), ours)

    def test_edge_types_and_concept_types_do_not_overlap(self):
        self.assertEqual(
            set(build.EDGE_TYPES) & set(taxonomy.CONCEPT_EDGE_TYPES), set())


class ImkgAlignmentTests(unittest.TestCase):
    """Where IMKG has a term, MemeAtlas uses it."""

    M4S = rdf.PREFIXES["m4s"]

    def test_imkg_terms_are_used(self):
        preds = rdf.all_predicates()
        for local in ("title", "status", "year", "from", "about", "added",
                      "last_update_source", "tag"):
            self.assertIn(self.M4S + local, preds, local)
        self.assertIn(self.M4S + "MediaFrame", rdf.constant_classes())

    def test_series_is_skos_broader_and_taxonomy_is_not(self):
        skos = rdf.PREFIXES["skos"]
        self.assertEqual(rdf.EDGE_PREDICATES["partOfSeries"],
                         (skos + "broader", False, skos + "narrower"))
        self.assertNotIn("skos", rdf.EDGE_PREDICATES["subTypeOf"][0])

    def test_no_extension_term_shadows_an_imkg_one(self):
        mk_locals = {t[len(rdf.PREFIXES["mk"]):] for t in declared_mk_terms()}
        imkg = {"MediaFrame", "title", "status", "year", "from", "about",
                "added", "last_update_source", "tag", "origin", "spread"}
        self.assertEqual(mk_locals & imkg, set())


class OntologyDeclarationTests(unittest.TestCase):
    MK = rdf.PREFIXES["mk"]

    @classmethod
    def setUpClass(cls):
        cls.declared = declared_mk_terms()
        emitted = rdf.all_predicates(include_provenance=True) | rdf.constant_classes()
        cls.emitted_mk = {t for t in emitted if t.startswith(cls.MK)}

    def test_ontology_file_is_readable(self):
        self.assertTrue(ONTOLOGY.is_file(), f"{ONTOLOGY} is missing")
        self.assertTrue(self.declared)

    def test_every_emitted_mk_term_is_declared(self):
        self.assertEqual(self.emitted_mk - self.declared, set())

    def test_every_declared_term_is_emitted_or_structural(self):
        # mk:Reference is the superclass of the two reference classes;
        # the scheme is emitted as a resource, not a predicate or class.
        structural = {self.MK + "Reference", rdf.SCHEME_IRI}
        self.assertEqual(self.declared - self.emitted_mk - structural, set())

    def test_ontology_parses_as_turtle(self):
        try:
            import rdflib
        except ImportError:
            self.skipTest("rdflib not installed; the Fuseki load checks it live")
        g = rdflib.Graph()
        g.parse(str(ONTOLOGY), format="turtle")
        self.assertGreater(len(g), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
