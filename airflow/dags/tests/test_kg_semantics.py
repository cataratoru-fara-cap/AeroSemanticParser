"""Tests for kg/semantics.py — describe / embed / analyze on the real client,
over a stubbed HTTP session (no model server is ever contacted).

What these pin, beyond "it works":

  * ``manual_overrides`` survive every write and are never regenerated, even
    with --force. The previous writer dropped the list on each save.
  * a resumed run is ONE model: it pins to the model recorded in the file
    (by digest, or by name for files that predate digests) and stops rather
    than finishing the job with a different model.
  * embeddings are reused only when provably computed from today's text.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_kg_semantics.py -v
"""
import json
import os
import tempfile
import unittest

from modules import openwebui_client as owc
from modules.kg import semantics as sem
from test_openwebui_client import CCDD, UI, Resp, StubSession, by_model, client

MISTRAL = "mistral-small3.2:24b"
MISTRAL_DIGEST = "5a408ab55df5"          # prefix; the fixture carries the full hash
CHAT_REQ = owc.ModelRequest(model=MISTRAL)
EMBED_REQ = owc.ModelRequest(model="qwen3-embedding:0.6b", kind="embedding")

DEFINITION = ("A meme format in which a still image is captioned with bold text to deliver "
              "a joke, typically reused by many users with new captions on the same base image.")


def definition_reply(body):
    slug = body["messages"][1]["content"].split('"')[1]
    return Resp(200, {"message": {"content": json.dumps(
        {"definition": f"{slug}: {DEFINITION}"})}})


def silent(_):
    pass


class Tmp(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def path(self, name):
        return os.path.join(self.dir, name)

    def write(self, name, obj):
        with open(self.path(name), "w") as fh:
            json.dump(obj, fh)
        return self.path(name)

    def read(self, name):
        with open(self.path(name)) as fh:
            return json.load(fh)


class ParseDefinitionTests(unittest.TestCase):
    def test_accepts_a_bounded_definition(self):
        self.assertEqual(sem.parse_definition(json.dumps({"definition": f" {DEFINITION} "})),
                         DEFINITION)

    def test_rejects_prose_wrong_shape_and_wild_lengths(self):
        for bad in ("not json", json.dumps({"def": DEFINITION}), json.dumps({"definition": 3}),
                    json.dumps({"definition": "too short"}),
                    json.dumps({"definition": "word " * 200})):
            with self.assertRaises((ValueError, KeyError, TypeError)):
                sem.parse_definition(bad)


class DescribeTests(Tmp):
    def session(self, ui=None):
        return StubSession({("POST", UI, owc.CHAT_PATH): ui or by_model({MISTRAL: definition_reply}),
                            ("POST", CCDD, owc.CHAT_PATH): by_model({})})

    def test_fresh_run_records_model_digest_and_host(self):
        c, _ = client(self.session())
        out = self.path("defs.json")
        summary = sem.describe(c, ["meme", "exploitable"], out, CHAT_REQ, progress=silent)
        doc = self.read("defs.json")
        self.assertEqual(set(doc["definitions"]), {"meme", "exploitable"})
        self.assertEqual(doc["model"], MISTRAL)
        self.assertTrue(doc["digest"].startswith(MISTRAL_DIGEST))
        self.assertEqual(doc["hosts"], [UI])
        self.assertEqual((doc["format"], doc["prompt_version"]), (2, sem.PROMPT_VERSION))
        self.assertEqual((summary["generated"], summary["failed"]), (2, []))
        self.assertFalse(os.path.exists(out + ".tmp"))

    def test_manual_overrides_survive_and_are_never_regenerated(self):
        out = self.write("defs.json", {
            "model": MISTRAL, "prompt_version": sem.PROMPT_VERSION,
            "definitions": {"song": "HAND WRITTEN", "meme": "generated earlier"},
            "manual_overrides": ["song"]})
        s = self.session()
        c, _ = client(s)
        sem.describe(c, ["song", "meme"], out, CHAT_REQ, force=True, progress=silent)
        doc = self.read("defs.json")
        self.assertEqual(doc["manual_overrides"], ["song"])
        self.assertEqual(doc["definitions"]["song"], "HAND WRITTEN")
        self.assertNotEqual(doc["definitions"]["meme"], "generated earlier")
        asked = [json.dumps(c) for c in s.calls if c[0] == "POST"]
        self.assertEqual(len(asked), 1)

    def test_a_legacy_file_resumes_pinned_to_its_model_by_name(self):
        out = self.write("defs.json", {"model": MISTRAL, "prompt_version": sem.PROMPT_VERSION,
                                       "definitions": {"meme": "cached"}})
        c, _ = client(self.session())
        sem.describe(c, ["meme", "exploitable"], out, CHAT_REQ, progress=silent)
        doc = self.read("defs.json")
        self.assertEqual(doc["definitions"]["meme"], "cached")
        self.assertIn("exploitable", doc["definitions"])
        self.assertTrue(doc["digest"].startswith(MISTRAL_DIGEST))   # now recorded
        self.assertEqual(c.pins[sem.DESCRIBE_PURPOSE].name, MISTRAL)

    def test_resume_refuses_to_finish_with_a_different_model(self):
        out = self.write("defs.json", {"model": MISTRAL, "digest": "f" * 64,
                                       "prompt_version": sem.PROMPT_VERSION,
                                       "definitions": {"meme": "cached"}})
        # Same-tier qwen3.8 is healthy on ccdd, but the file is mistral's.
        s = StubSession({("POST", UI, owc.CHAT_PATH): by_model({}),
                         ("POST", CCDD, owc.CHAT_PATH): by_model({"qwen3.8:27b": definition_reply})})
        c, _ = client(s)
        with self.assertRaises(owc.ModelUnavailableError):
            sem.describe(c, ["meme", "exploitable"], out, CHAT_REQ, progress=silent)
        self.assertNotIn("exploitable", self.read("defs.json")["definitions"])

    def test_force_may_fall_back_and_records_what_it_used(self):
        out = self.write("defs.json", {"model": MISTRAL, "prompt_version": sem.PROMPT_VERSION,
                                       "definitions": {"meme": "cached"}})
        s = StubSession({("POST", UI, owc.CHAT_PATH): by_model({}),
                         ("POST", CCDD, owc.CHAT_PATH): by_model({"qwen3.8:27b": definition_reply})})
        c, _ = client(s)
        sem.describe(c, ["meme"], out, CHAT_REQ, force=True, progress=silent)
        self.assertEqual(self.read("defs.json")["model"], "qwen3.8:27b")

    def test_a_stale_prompt_version_is_refused_not_silently_regenerated(self):
        # data/kg_type_definitions.json really is v1 under a v2 prompt. A
        # resume must neither regenerate 110 definitions on its own (it did,
        # in the first live run of this rewrite) nor mix v1 and v2.
        out = self.write("defs.json", {"model": MISTRAL, "prompt_version": "1",
                                       "definitions": {"meme": "v1 text"}})
        s = self.session()
        c, _ = client(s)
        with self.assertRaises(sem.PromptVersionMismatch):
            sem.describe(c, ["meme", "exploitable"], out, CHAT_REQ, progress=silent)
        self.assertEqual(s.calls, [])
        self.assertEqual(self.read("defs.json")["definitions"], {"meme": "v1 text"})

    def test_a_stale_but_complete_file_only_warns(self):
        out = self.write("defs.json", {"model": MISTRAL, "prompt_version": "1",
                                       "definitions": {"meme": "v1 text"}})
        said = []
        c, _ = client(StubSession(tags_for=()))
        summary = sem.describe(c, ["meme"], out, CHAT_REQ, progress=said.append)
        self.assertFalse(summary["prompt_is_current"])
        self.assertTrue(any(line.startswith("WARNING") for line in said))

    def test_force_regenerates_a_stale_file(self):
        out = self.write("defs.json", {"model": MISTRAL, "prompt_version": "1",
                                       "definitions": {"meme": "v1 text"}})
        c, _ = client(self.session())
        sem.describe(c, ["meme"], out, CHAT_REQ, force=True, progress=silent)
        doc = self.read("defs.json")
        self.assertNotEqual(doc["definitions"]["meme"], "v1 text")
        self.assertEqual(doc["prompt_version"], sem.PROMPT_VERSION)

    def test_a_finished_file_needs_no_model_server(self):
        out = self.write("defs.json", {"model": MISTRAL, "prompt_version": sem.PROMPT_VERSION,
                                       "definitions": {"meme": "cached"}})
        s = StubSession(tags_for=())
        c, _ = client(s)
        summary = sem.describe(c, ["meme"], out, CHAT_REQ, progress=silent)
        self.assertEqual((s.calls, summary["missing"]), ([], []))

    def test_unusable_output_is_recorded_and_the_rest_continue(self):
        def reply(body):
            if '"bad"' in body["messages"][1]["content"]:
                return Resp(200, {"message": {"content": "I think a bad meme is..."}})
            return definition_reply(body)
        c, _ = client(self.session(ui=by_model({MISTRAL: reply})))
        summary = sem.describe(c, ["bad", "meme"], self.path("d.json"), CHAT_REQ, progress=silent)
        self.assertEqual([f["slug"] for f in summary["failed"]], ["bad"])
        self.assertEqual(summary["missing"], ["bad"])
        self.assertIn("meme", self.read("d.json")["definitions"])


class EmbedTests(Tmp):
    def definitions(self, **defs):
        return self.write("defs.json", {"model": MISTRAL, "digest": "d" * 64,
                                        "prompt_version": sem.PROMPT_VERSION,
                                        "definitions": defs, "manual_overrides": []})

    def session(self, calls):
        def reply(body):
            calls.append(list(body["input"]))
            return Resp(200, {"embeddings": [[float(len(t)), 1.0, 0.0] for t in body["input"]]})
        return StubSession({("POST", UI, owc.EMBED_PATH): by_model({"qwen3-embedding:0.6b": reply})})

    def test_writes_a_self_describing_normalised_file(self):
        defs = self.definitions(meme="a", song="bb")
        calls = []
        c, _ = client(self.session(calls))
        sem.embed(c, defs, self.path("emb.json"), EMBED_REQ, progress=silent)
        doc = sem.load_embeddings(self.path("emb.json"))
        self.assertEqual((doc["model"], doc["dim"], doc["normalized"]),
                         ("qwen3-embedding:0.6b", 3, True))
        self.assertTrue(doc["digest"])
        self.assertEqual(set(doc["text_sha256"]), {"meme", "song"})
        for vec in doc["vectors"].values():
            self.assertAlmostEqual(sum(x * x for x in vec), 1.0)

    def test_only_vectors_of_unchanged_text_are_reused(self):
        calls = []
        c, _ = client(self.session(calls))
        sem.embed(c, self.definitions(meme="a", song="bb"), self.path("emb.json"),
                  EMBED_REQ, progress=silent)
        calls.clear()
        c2, _ = client(self.session(calls))
        sem.embed(c2, self.definitions(meme="a", song="changed", film="new"),
                  self.path("emb.json"), EMBED_REQ, batch_size=8, progress=silent)
        self.assertEqual(calls, [["film: new", "song: changed"]])
        self.assertEqual(c2.pins[sem.EMBED_PURPOSE].dim, 3)

    def test_legacy_file_without_text_hashes_is_re_embedded(self):
        self.write("emb.json", {"model": "qwen3-embedding:0.6b",
                                "vectors": {"meme": [1.0, 0.0, 0.0]}})
        calls = []
        c, _ = client(self.session(calls))
        sem.embed(c, self.definitions(meme="a"), self.path("emb.json"), EMBED_REQ, progress=silent)
        self.assertEqual(calls, [["meme: a"]])

    def test_removed_definitions_drop_their_vectors(self):
        calls = []
        c, _ = client(self.session(calls))
        sem.embed(c, self.definitions(meme="a", song="bb"), self.path("emb.json"),
                  EMBED_REQ, progress=silent)
        c2, _ = client(self.session(calls))
        summary = sem.embed(c2, self.definitions(meme="a"), self.path("emb.json"),
                            EMBED_REQ, progress=silent)
        self.assertEqual(summary["dropped"], ["song"])
        self.assertEqual(set(self.read("emb.json")["vectors"]), {"meme"})

    def test_a_file_mixing_dimensions_is_refused(self):
        self.write("emb.json", {"model": "x", "vectors": {"a": [1.0, 0.0], "b": [1.0, 0.0, 0.0]}})
        with self.assertRaises(owc.EmbeddingMixError):
            sem.load_embeddings(self.path("emb.json"))
        self.write("emb2.json", {"model": "x", "dim": 3, "vectors": {"a": [1.0, 0.0]}})
        with self.assertRaises(owc.EmbeddingMixError):
            sem.load_embeddings(self.path("emb2.json"))


class AnalyzeTests(Tmp):
    def setUp(self):
        super().setUp()
        try:
            import numpy  # noqa: F401
            import scipy  # noqa: F401
        except ImportError:
            self.skipTest("numpy/scipy not installed")

    def test_runs_on_a_legacy_census_and_reports_coverage_instead_of_keyerror(self):
        self.write("emb.json", {"model": "qwen3-embedding:0.6b", "vectors": {
            "a": [1.0, 0.0, 0.0], "b": [0.96, 0.28, 0.0], "c": [0.0, 1.0, 0.0],
            "d": [0.0, 0.0, 1.0]}})
        self.write("census.json", {                       # the pre-unification key names
            "entries_with_entry_type": 1000, "type_counts": {"a": 100, "b": 90, "c": 50, "z": 5},
            "pair_cooccurrence": [{"a": "a", "b": "c", "count": 40}]})
        report = sem.analyze(self.path("emb.json"), self.path("census.json"),
                             self.path("report.json"), coarse_k=2, fine_k=3, progress=silent)
        self.assertEqual(report["coverage"], {"without_vector": ["z"],
                                              "without_census_count": ["d"]})
        self.assertEqual(report["neighbors"]["a"][0]["type"], "b")
        self.assertEqual([(r["a"], r["b"]) for r in report["substitution_candidates"]], [("a", "b")])
        self.assertEqual([(r["a"], r["b"]) for r in report["complementary_pairs"]], [("a", "c")])
        self.assertEqual(report["embeddings"]["model"], "qwen3-embedding:0.6b")


class ImportTests(unittest.TestCase):
    def test_importing_reads_no_configuration(self):
        # The old module raised SystemExit at import without a key.
        import importlib
        env = {k: v for k, v in os.environ.items() if not k.startswith("OPENWEBUI")}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            importlib.reload(sem)


import unittest.mock  # noqa: E402

if __name__ == "__main__":
    unittest.main(verbosity=2)
