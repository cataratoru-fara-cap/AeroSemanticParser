"""kg/semantics.py: describe / embed / analyze on the real client over a stubbed
HTTP session. Pinned beyond "it works": manual_overrides survive every write and
are never regenerated, even with --force (the previous writer dropped them); a
resumed run is ONE model, pinned to the one the file records (by digest, or by
name for older files), and stops rather than finish with another; embeddings
are reused only when provably computed from today's text."""
import importlib
import json
import os
from unittest import mock

import pytest

from modules import openwebui_client as owc
from modules.kg import semantics as sem
from openwebui_stub import CCDD, UI, Resp, StubSession, by_model, client

MISTRAL = "mistral-small3.2:24b"
MISTRAL_DIGEST = "5a408ab55df5"          # a prefix; the fixture carries the full hash
CHAT_REQ = owc.ModelRequest(model=MISTRAL)
EMBED_REQ = owc.ModelRequest(model="qwen3-embedding:0.6b", kind="embedding")
DEFINITION = ("A meme format in which a still image is captioned with bold text to deliver a joke, typically "
              "reused by many users with new captions on the same base image.")


def silent(_):
    pass


def definition_reply(body):
    slug = body["messages"][1]["content"].split('"')[1]
    return Resp(200, {"message": {"content": json.dumps({"definition": f"{slug}: {DEFINITION}"})}})


@pytest.fixture
def files(tmp_path):
    class Files:
        def path(self, name):
            return str(tmp_path / name)

        def write(self, name, obj):
            (tmp_path / name).write_text(json.dumps(obj))
            return self.path(name)

        def read(self, name):
            return json.loads((tmp_path / name).read_text())
    return Files()


def test_parse_definition():
    assert sem.parse_definition(json.dumps({"definition": f" {DEFINITION} "})) == DEFINITION
    for bad in ("not json", json.dumps({"def": DEFINITION}), json.dumps({"definition": 3}),
                json.dumps({"definition": "too short"}), json.dumps({"definition": "word " * 200})):
        with pytest.raises((ValueError, KeyError, TypeError)):
            sem.parse_definition(bad)


# -- describe ---------------------------------------------------------------------------

def session(ui=None):
    return StubSession({("POST", UI, owc.CHAT_PATH): ui or by_model({MISTRAL: definition_reply}),
                        ("POST", CCDD, owc.CHAT_PATH): by_model({})})


QWEN_ONLY = {("POST", UI, owc.CHAT_PATH): by_model({}),
             ("POST", CCDD, owc.CHAT_PATH): by_model({"qwen3.8:27b": definition_reply})}


def defs_file(files, **over):
    return files.write("defs.json", {"model": MISTRAL, "prompt_version": sem.PROMPT_VERSION,
                                     "definitions": {"meme": "cached"}, **over})


def test_a_fresh_run_records_model_digest_and_host(files):
    out = files.path("defs.json")
    summary = sem.describe(client(session())[0], ["meme", "exploitable"], out, CHAT_REQ, progress=silent)
    doc = files.read("defs.json")
    assert set(doc["definitions"]) == {"meme", "exploitable"} and doc["model"] == MISTRAL
    assert doc["digest"].startswith(MISTRAL_DIGEST) and doc["hosts"] == [UI]
    assert (doc["format"], doc["prompt_version"], summary["generated"], summary["failed"]) == \
        (2, sem.PROMPT_VERSION, 2, [])
    assert not os.path.exists(out + ".tmp")


def test_manual_overrides_survive_and_are_never_regenerated(files):
    out = defs_file(files, definitions={"song": "HAND WRITTEN", "meme": "generated earlier"}, manual_overrides=["song"])
    s = session()
    sem.describe(client(s)[0], ["song", "meme"], out, CHAT_REQ, force=True, progress=silent)
    doc = files.read("defs.json")
    assert (doc["manual_overrides"], doc["definitions"]["song"]) == (["song"], "HAND WRITTEN")
    assert doc["definitions"]["meme"] != "generated earlier"
    assert len([c for c in s.calls if c[0] == "POST"]) == 1


def test_a_legacy_file_resumes_pinned_to_its_model_by_name(files):
    out = defs_file(files)
    c, _ = client(session())
    sem.describe(c, ["meme", "exploitable"], out, CHAT_REQ, progress=silent)
    doc = files.read("defs.json")
    assert doc["definitions"]["meme"] == "cached" and "exploitable" in doc["definitions"]
    assert doc["digest"].startswith(MISTRAL_DIGEST) and c.pins[sem.DESCRIBE_PURPOSE].name == MISTRAL


def test_resume_refuses_to_finish_with_a_different_model_and_force_records_it(files):
    out = defs_file(files, digest="f" * 64)     # qwen3.8 is healthy and same tier, but the file is mistral's
    with pytest.raises(owc.ModelUnavailableError):
        sem.describe(client(StubSession(QWEN_ONLY))[0], ["meme", "exploitable"], out, CHAT_REQ, progress=silent)
    assert "exploitable" not in files.read("defs.json")["definitions"]
    out = defs_file(files)
    sem.describe(client(StubSession(QWEN_ONLY))[0], ["meme"], out, CHAT_REQ, force=True, progress=silent)
    assert files.read("defs.json")["model"] == "qwen3.8:27b"


def test_a_stale_prompt_version_is_refused_warned_or_forced(files):
    # the live file really was v1 under a v2 prompt: a resume must neither
    # regenerate everything on its own (the rewrite's first run did) nor mix them
    out = defs_file(files, prompt_version="1", definitions={"meme": "v1 text"})
    s = session()
    with pytest.raises(sem.PromptVersionMismatch):
        sem.describe(client(s)[0], ["meme", "exploitable"], out, CHAT_REQ, progress=silent)
    assert s.calls == [] and files.read("defs.json")["definitions"] == {"meme": "v1 text"}
    said = []                                                     # complete but stale: only a warning
    summary = sem.describe(client(StubSession(tags_for=()))[0], ["meme"], out, CHAT_REQ, progress=said.append)
    assert not summary["prompt_is_current"] and any(line.startswith("WARNING") for line in said)
    sem.describe(client(session())[0], ["meme"], out, CHAT_REQ, force=True, progress=silent)
    doc = files.read("defs.json")
    assert doc["definitions"]["meme"] != "v1 text" and doc["prompt_version"] == sem.PROMPT_VERSION


def test_a_finished_file_needs_no_model_server(files):
    s = StubSession(tags_for=())
    summary = sem.describe(client(s)[0], ["meme"], defs_file(files), CHAT_REQ, progress=silent)
    assert (s.calls, summary["missing"]) == ([], [])


def test_unusable_output_is_recorded_and_the_rest_continue(files):
    def reply(body):
        if '"bad"' in body["messages"][1]["content"]:
            return Resp(200, {"message": {"content": "I think a bad meme is..."}})
        return definition_reply(body)
    summary = sem.describe(client(session(ui=by_model({MISTRAL: reply})))[0], ["bad", "meme"], files.path("d.json"),
                           CHAT_REQ, progress=silent)
    assert [f["slug"] for f in summary["failed"]] == ["bad"] == summary["missing"]
    assert "meme" in files.read("d.json")["definitions"]


# -- embed ------------------------------------------------------------------------------------

def definitions(files, **defs):
    return files.write("defs.json", {"model": MISTRAL, "digest": "d" * 64, "prompt_version": sem.PROMPT_VERSION,
                                     "definitions": defs, "manual_overrides": []})


def embedder(calls):
    def reply(body):
        calls.append(list(body["input"]))
        return Resp(200, {"embeddings": [[float(len(t)), 1.0, 0.0] for t in body["input"]]})
    return client(StubSession({("POST", UI, owc.EMBED_PATH): by_model({"qwen3-embedding:0.6b": reply})}))[0]


def test_embed_writes_a_self_describing_normalised_file(files):
    sem.embed(embedder([]), definitions(files, meme="a", song="bb"), files.path("emb.json"), EMBED_REQ, progress=silent)
    doc = sem.load_embeddings(files.path("emb.json"))
    assert (doc["model"], doc["dim"], doc["normalized"]) == ("qwen3-embedding:0.6b", 3, True) and doc["digest"]
    assert set(doc["text_sha256"]) == {"meme", "song"}
    assert all(sum(x * x for x in v) == pytest.approx(1.0) for v in doc["vectors"].values())


def test_only_vectors_of_unchanged_text_are_reused_and_removed_ones_drop(files):
    sem.embed(embedder([]), definitions(files, meme="a", song="bb"), files.path("emb.json"), EMBED_REQ, progress=silent)
    calls = []
    c = embedder(calls)
    sem.embed(c, definitions(files, meme="a", song="changed", film="new"), files.path("emb.json"), EMBED_REQ,
              batch_size=8, progress=silent)
    assert calls == [["film: new", "song: changed"]] and c.pins[sem.EMBED_PURPOSE].dim == 3
    summary = sem.embed(embedder([]), definitions(files, meme="a"), files.path("emb.json"), EMBED_REQ, progress=silent)
    assert summary["dropped"] == ["film", "song"] and set(files.read("emb.json")["vectors"]) == {"meme"}


def test_a_run_with_nothing_to_embed_leaves_the_file_untouched(files):
    defs = definitions(files, meme="a")
    sem.embed(embedder([]), defs, files.path("emb.json"), EMBED_REQ, progress=silent)
    before = files.read("emb.json")
    assert before["produced_at"]
    calls = []
    summary = sem.embed(embedder(calls), defs, files.path("emb.json"), EMBED_REQ, progress=silent)
    assert (calls, summary["embedded"]) == ([], 0) and files.read("emb.json") == before    # produced_at kept


def test_a_legacy_file_without_text_hashes_is_re_embedded(files):
    files.write("emb.json", {"model": "qwen3-embedding:0.6b", "vectors": {"meme": [1.0, 0.0, 0.0]}})
    calls = []
    sem.embed(embedder(calls), definitions(files, meme="a"), files.path("emb.json"), EMBED_REQ, progress=silent)
    assert calls == [["meme: a"]]


@pytest.mark.parametrize("doc", [{"model": "x", "vectors": {"a": [1.0, 0.0], "b": [1.0, 0.0, 0.0]}},
                                 {"model": "x", "dim": 3, "vectors": {"a": [1.0, 0.0]}}])
def test_a_file_mixing_dimensions_is_refused(files, doc):
    with pytest.raises(owc.EmbeddingMixError):
        sem.load_embeddings(files.write("emb.json", doc))


# -- analyze and import ----------------------------------------------------------------------

def test_analyze_runs_on_a_legacy_census_and_reports_coverage(files):
    pytest.importorskip("numpy")
    pytest.importorskip("scipy")
    files.write("emb.json", {"model": "qwen3-embedding:0.6b", "vectors": {
        "a": [1.0, 0.0, 0.0], "b": [0.96, 0.28, 0.0], "c": [0.0, 1.0, 0.0], "d": [0.0, 0.0, 1.0]}})
    files.write("census.json", {"entries_with_entry_type": 1000,              # the pre-unification key names
                                "type_counts": {"a": 100, "b": 90, "c": 50, "z": 5},
                                "pair_cooccurrence": [{"a": "a", "b": "c", "count": 40}]})
    report = sem.analyze(files.path("emb.json"), files.path("census.json"), files.path("report.json"),
                         coarse_k=2, fine_k=3, progress=silent)
    assert report["coverage"] == {"without_vector": ["z"], "without_census_count": ["d"]}   # not a KeyError
    assert report["neighbors"]["a"][0]["type"] == "b"
    assert [(r["a"], r["b"]) for r in report["substitution_candidates"]] == [("a", "b")]
    assert [(r["a"], r["b"]) for r in report["complementary_pairs"]] == [("a", "c")]
    assert report["embeddings"]["model"] == "qwen3-embedding:0.6b"


def test_importing_reads_no_configuration():
    # the old module raised SystemExit at import without a key
    env = {k: v for k, v in os.environ.items() if not k.startswith("OPENWEBUI")}
    with mock.patch.dict(os.environ, env, clear=True):
        importlib.reload(sem)
