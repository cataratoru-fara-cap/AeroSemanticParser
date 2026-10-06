"""openwebui_client.py against stubbed HTTP, selection on the lab's REAL
inventories (openwebui_stub). Two tests pin facts learned on the live servers
and are not redundant: a 403 "Model not found" is not an auth failure (Open
WebUI's answer for a model the host does not serve), and each key works only
on its own host."""
import json

import pytest
import requests

from modules import openwebui_client as owc
from openwebui_stub import CCDD, HOSTS, MODEL_NOT_FOUND, UI, Resp, StubSession, by_model, chat_ok, client, inventory

MSGS = [{"role": "user", "content": "x"}]
GPT = owc.ModelRequest(model="gpt-oss:120b")
EMBED = owc.ModelRequest(model="qwen3-embedding:0.6b", kind="embedding")


def chat(routes, request=GPT, **kw):
    s = StubSession(routes)
    c, sleeps = client(s)
    return c.chat(MSGS, request, **kw), s, c, sleeps


# -- inventory facts ------------------------------------------------------------

@pytest.mark.parametrize("text, want", [("24.0B", 24.0), ("595.78M", pytest.approx(0.59578)),
                                        ("1.2T", 1200.0), ("", None), ("big", None)])
def test_parameter_sizes(text, want):
    assert owc.parse_parameter_size(text) == want


def test_tier_bands():
    assert [owc.tier_for(x) for x in (0.6, 6.99, 7.0, 23.9, 24.0, 69.9, 70.0)] == \
        ["sm", "sm", "md", "md", "lg", "lg", "xl"]
    assert owc.tier_for(None) is None


def test_real_models_classify_by_what_they_are_optimised_for():
    spec = {(owc.host_name(m.host), m.name): m.specialization for ms in inventory().values() for m in ms}
    assert {k: spec[k] for k in [("ollama-ccdd", "qwen3-vl:32b"), ("ollama-ui", "glm-ocr:latest"),
                                 ("ollama-ccdd", "devstral-2:123b"), ("ollama-ui", "qwen3-embedding:0.6b"),
                                 ("ollama-ui", "mistral-small3.2:24b"), ("ollama-ccdd", "qwen3.8:27b")]} == {
        ("ollama-ccdd", "qwen3-vl:32b"): "vision", ("ollama-ui", "glm-ocr:latest"): "vision",
        ("ollama-ccdd", "devstral-2:123b"): "coding", ("ollama-ui", "qwen3-embedding:0.6b"): "embedding",
        # vision CAPABILITY is not vision SPECIALISATION
        ("ollama-ui", "mistral-small3.2:24b"): "general", ("ollama-ccdd", "qwen3.8:27b"): "general"}
    assert owc.classify_specialization("mixtral:8x7b", "llama", ["completion"], {"mixtral:8x7b": "coding"}) == "coding"


def test_tier_comes_from_the_reported_size_and_cloud_has_none():
    gemma = next(m for m in inventory()[UI] if m.name == "gemma4:e4b")
    assert (gemma.params_b, gemma.tier) == (8.0, "md")              # "e4b" is not 4B
    cloud = next(m for m in inventory()[CCDD] if m.name == "minimax-m2.7:cloud")
    assert cloud.is_cloud and cloud.tier is None


# -- configuration --------------------------------------------------------------

def test_config_from_env():
    cfg = owc.LLMConfig.from_env({"OPENWEBUI_API_KEY": "k"})
    assert cfg.host_order == owc.DEFAULT_HOSTS and owc.host_name(cfg.host_order[0]) == "ollama-ccdd"
    cfg = owc.LLMConfig.from_env({"OPENWEBUI_API_KEY": "ui-key", "OPENWEBUI_API_KEY_OLLAMA_CCDD": "ccdd-key"})
    assert {h.name: h.api_key for h in cfg.hosts} == {"ollama-ccdd": "ccdd-key", "ollama-ui": "ui-key"}
    assert [h.name for h in owc.LLMConfig.from_env({"OPENWEBUI_API_KEY_OLLAMA_UI": "k"}).hosts] == ["ollama-ui"]
    with pytest.raises(owc.OpenWebUIAuthError):
        owc.LLMConfig.from_env({"OPENWEBUI_API_KEY": ""})
    cfg = owc.LLMConfig.from_env({"OPENWEBUI_HOSTS": f" {UI}/ , {CCDD}", "OPENWEBUI_API_KEY": "k",
                                  "OPENWEBUI_SPECIALIZATION_OVERRIDES": "mixtral:8x7b=coding",
                                  "OPENWEBUI_ALLOW_CLOUD": "true"})
    assert (cfg.host_order, cfg.specialization_overrides, cfg.allow_cloud) == \
        ((UI, CCDD), (("mixtral:8x7b", "coding"),), True)
    assert "sk-very-secret" not in repr(owc.LLMConfig.from_env({"OPENWEBUI_API_KEY": "sk-very-secret"}))


def test_rotated_host_order():
    cfg = client(StubSession())[0].cfg
    assert [cfg.rotated(i).host_order for i in range(3)] == [(CCDD, UI), (UI, CCDD), (CCDD, UI)]


@pytest.mark.parametrize("kw", [{}, {"tier": "huge"}, {"model": "x", "specialization": "poetry"},
                                {"model": "x", "capabilities": {"thinking"}, "exclude_capabilities": {"thinking"}}])
def test_an_invalid_request_is_refused(kw):
    with pytest.raises(ValueError):
        owc.ModelRequest(**kw)


def test_a_request_from_env():
    req = owc.ModelRequest.from_env("KG_CHAT", default_model="m",
                                    env={"KG_CHAT_TIER": "lg", "KG_CHAT_SPECIALIZATION": ""})
    assert (req.model, req.tier, req.specialization) == ("m", "lg", None)


# -- selection (pure) -------------------------------------------------------------

def resolve(pin=None, allow_cloud=False, **kw):
    return [(owc.host_name(m.host), m.name)
            for m in owc.resolve_candidates(inventory(), owc.ModelRequest(**kw), HOSTS, pin=pin,
                                            allow_cloud=allow_cloud)]


def test_the_requested_model_first_then_the_nearest_general_of_its_tier():
    got = resolve(model="mistral-small3.2:24b")
    # nearest size (27.3B), ccdd first: ccdd has priority
    assert got[:3] == [("ollama-ui", "mistral-small3.2:24b"), ("ollama-ccdd", "qwen3.8:27b"), ("ollama-ui", "qwen3.8:27b")]
    assert not {("ollama-ccdd", "qwen3-vl:32b"), ("ollama-ccdd", "minimax-m2.7:cloud")} & set(got)
    assert {m for _, m in got} <= {"mistral-small3.2:24b", "qwen3.8:27b", "glm-4.7-flash:bf16",
                                   "nemotron-3.5-lightning:30b", "qwen3.6:35b", "mixtral:8x7b"}
    assert resolve(model="gpt-oss:120b")[:2] == [("ollama-ccdd", "gpt-oss:120b"), ("ollama-ui", "gpt-oss:120b")]
    assert resolve(model="qwen3:8b") == resolve(model="qwen3:8b")    # deterministic


@pytest.mark.parametrize("kw, want", [
    ({"tier": "lg", "specialization": "vision"}, [("ollama-ccdd", "qwen3-vl:32b")]),
    ({"tier": "xl", "specialization": "coding"}, [("ollama-ccdd", "devstral-2:123b")]),
    ({"model": "qwen3-embedding:0.6b", "kind": "embedding"}, [("ollama-ui", "qwen3-embedding:0.6b")]),  # never a chat model
    ({"model": "minimax-m2.7:cloud"}, []),                                        # cloud needs an opt-in
    ({"model": "minimax-m2.7:cloud", "allow_cloud": True}, [("ollama-ccdd", "minimax-m2.7:cloud")]),
    ({"model": "mistral-small3.2:24b", "allow_fallback": False}, [("ollama-ui", "mistral-small3.2:24b")]),
    ({"model": "llama9:900b"}, []),                                               # unknown, no tier
    # an excluded model is not served even by name
    ({"model": "qwen3.8:27b", "exclude_capabilities": {"thinking"}, "allow_fallback": False}, []),
])
def test_candidates(kw, want):
    assert resolve(**kw) == want


def test_specialization_capabilities_and_any():
    assert {m for _, m in resolve(tier="md", specialization="vision")} == {"qwen3-vl:8b", "qwen3-vl:8b-thinking"}
    assert ("ollama-ccdd", "qwen3-vl:32b") in resolve(model="mistral-small3.2:24b", specialization="any")
    assert all(m.startswith("qwen3-vl") for _, m in resolve(model="qwen3-vl:8b"))   # vision falls back to vision
    assert {m for _, m in resolve(model="mistral-small3.2:24b", capabilities=frozenset({"vision"}))} == \
        {"mistral-small3.2:24b", "qwen3.8:27b", "qwen3.6:35b"}
    assert resolve(model="llama9:900b", tier="lg")


def test_an_exclusion_filters_the_request_and_every_fallback():
    # 2026-09-18: asked for a non-reasoning model, the fallback ranked four reasoning models next
    assert [m for _, m in resolve(model="mistral-small3.2:24b")][1] == "qwen3.8:27b"
    assert [m for _, m in resolve(model="mistral-small3.2:24b", exclude_capabilities={"thinking"})] == \
        ["mistral-small3.2:24b", "mixtral:8x7b"]


def test_a_pin_admits_only_the_pinned_weights_including_aliases():
    digest = next(m.digest for m in inventory()[UI] if m.name == "gemma4:e4b")
    assert resolve(model="mistral-small3.2:24b", pin=owc.ModelPin("gemma4:e4b", digest)) == \
        [("ollama-ui", "gemma4:e4b"), ("ollama-ui", "gemma4:latest")]


# -- the client: hosts --------------------------------------------------------------

def test_each_key_only_works_on_its_own_host():
    s = StubSession()
    client(s)[0].inventory()
    assert {h: hdr["Authorization"] for m, h, p, _, hdr in s.calls if m == "GET"} == \
        {"ollama-ccdd": "Bearer key-ccdd", "ollama-ui": "Bearer key-ui"}


def test_403_model_not_found_is_not_an_auth_failure():
    res, _, c, _ = chat({("POST", CCDD, owc.CHAT_PATH): MODEL_NOT_FOUND, ("POST", UI, owc.CHAT_PATH): chat_ok("hi")},
                        owc.ModelRequest(model="qwen3.8:27b"))
    assert res.ok and owc.host_name(res.host) == "ollama-ui"
    assert not c.host_status()["ollama-ccdd"]["auth_failed"]


def test_an_auth_failure_fails_over_and_everywhere_raises():
    res, _, c, _ = chat({("GET", CCDD, owc.TAGS_PATH): Resp(401, {"detail": "Not authenticated"}),
                         ("POST", UI, owc.CHAT_PATH): chat_ok()})
    assert res.ok and owc.host_name(res.host) == "ollama-ui" and c.host_status()["ollama-ccdd"]["auth_failed"]
    bad = Resp(401, {"detail": "token invalid"})
    with pytest.raises(owc.OpenWebUIAuthError):
        chat({("GET", CCDD, owc.TAGS_PATH): bad, ("GET", UI, owc.TAGS_PATH): bad})


@pytest.mark.parametrize("error", [
    requests.ConnectionError("HTTPSConnectionPool(host='ollama-ccdd...', port=443): Max retries exceeded (Caused by "
                             "NewConnectionError('Failed to establish a new connection: [Errno 111] Connection refused'))"),
    requests.ConnectTimeout("slow dns"),
])
def test_an_unreachable_host_fails_over_to_the_same_model_without_retrying(error):
    res, s, _, sleeps = chat({("POST", CCDD, owc.CHAT_PATH): error, ("POST", UI, owc.CHAT_PATH): chat_ok()})
    assert (owc.host_name(res.host), res.model, sleeps, res.fell_back) == ("ollama-ui", "gpt-oss:120b", [], False)
    assert s.posts() == [("ollama-ccdd", "gpt-oss:120b"), ("ollama-ui", "gpt-oss:120b")]


def test_a_connection_dropped_mid_request_is_retried_on_the_same_host():
    # seen live: the only host serving the embedding model reset once during a cold load
    dropped = requests.ConnectionError("('Connection aborted.', RemoteDisconnected('Remote end closed "
                                       "connection without response'))")
    s = StubSession({("POST", UI, owc.EMBED_PATH): [dropped, Resp(200, {"embeddings": [[1, 0]]})]})
    c, sleeps = client(s)
    res = c.embed(["a"], EMBED)
    assert res.ok and (res.attempts, len(sleeps)) == (2, 1)
    assert c.host_status()["ollama-ui"]["cooling_down_s"] == 0


def test_5xx_is_retried_with_backoff_then_the_host_is_left():
    res, s, c, sleeps = chat({("POST", CCDD, owc.CHAT_PATH): Resp(502, text="bad gateway"),
                              ("POST", UI, owc.CHAT_PATH): chat_ok()})
    assert res.ok and res.attempts == 4 and s.posts().count(("ollama-ccdd", "gpt-oss:120b")) == 3
    assert sleeps == [2.0, 4.0]                       # capped exponential, jitter 1
    assert c.host_status()["ollama-ccdd"]["cooling_down_s"] > 0


def test_every_host_down_raises_no_healthy_host():
    down = requests.ConnectionError("Failed to establish a new connection")
    with pytest.raises(owc.NoHealthyHostError):
        chat({("GET", CCDD, owc.TAGS_PATH): down, ("GET", UI, owc.TAGS_PATH): down})


# -- the client: fallback and pinning --------------------------------------------------

def test_a_model_missing_everywhere_falls_back_within_its_tier_and_says_so():
    res, _, c, _ = chat({("POST", UI, owc.CHAT_PATH): by_model({}),
                         ("POST", CCDD, owc.CHAT_PATH): by_model({"qwen3.8:27b": chat_ok()})},
                        owc.ModelRequest(model="mistral-small3.2:24b"))
    assert (owc.host_name(res.host), res.model, res.fell_back, c.pins["chat"].name) == \
        ("ollama-ccdd", "qwen3.8:27b", True, "qwen3.8:27b")
    s = StubSession({("POST", UI, owc.CHAT_PATH): by_model({}),
                     ("POST", CCDD, owc.CHAT_PATH): by_model({"qwen3.8:27b": chat_ok()})})
    images = client(s)[0].chat([{"role": "user", "content": "x", "images": ["aGk="]}],
                               owc.ModelRequest(model="mistral-small3.2:24b"))
    assert images.model == "qwen3.8:27b"                  # images need vision; glm-4.7 has none


def test_a_pinned_purpose_never_switches_models_but_may_change_host():
    answers = {"qwen3.8:27b": chat_ok()}
    s = StubSession({("POST", UI, owc.CHAT_PATH): by_model({"mistral-small3.2:24b": chat_ok()}),
                     ("POST", CCDD, owc.CHAT_PATH): by_model(answers)})
    c, _ = client(s)
    req = owc.ModelRequest(model="qwen3.8:27b")
    assert c.chat(MSGS, req).ok
    answers.clear()                                       # gone from both hosts; mistral is healthy
    with pytest.raises(owc.ModelUnavailableError, match="refusing to switch models mid-run"):
        c.chat(MSGS, req)
    assert ("ollama-ui", "mistral-small3.2:24b") not in s.posts()
    s = StubSession({("POST", CCDD, owc.CHAT_PATH): [chat_ok(), requests.ConnectTimeout()],
                     ("POST", UI, owc.CHAT_PATH): chat_ok()})
    c, _ = client(s)
    first, second = c.chat(MSGS, req), c.chat(MSGS, req)
    assert (owc.host_name(first.host), owc.host_name(second.host)) == ("ollama-ccdd", "ollama-ui")
    assert first.digest == second.digest


def test_purposes_pin_independently_and_a_pin_is_never_replaced():
    c, _ = client(StubSession({("POST", CCDD, owc.CHAT_PATH): chat_ok(), ("POST", UI, owc.CHAT_PATH): chat_ok()}))
    c.chat(MSGS, owc.ModelRequest(model="qwen3.8:27b"), purpose="a")
    c.chat(MSGS, GPT, purpose="b")
    assert {p: pin.name for p, pin in c.pins.items()} == {"a": "qwen3.8:27b", "b": "gpt-oss:120b"}
    c, _ = client(StubSession())
    c.pin("p", "gpt-oss:120b", "a951a23b46a1")             # a resumed run's explicit pin
    with pytest.raises(owc.EmbeddingMixError):
        c.pin("p", "qwen3.8:27b", "22130167c4c2")


# -- the client: output and protocol ---------------------------------------------------

def test_a_rejected_output_sleeps_between_attempts_and_is_returned_as_data():
    res, s, _, sleeps = chat({("POST", CCDD, owc.CHAT_PATH): chat_ok("prose, not JSON")}, validate=json.loads)
    assert (res.ok, res.error_kind, len(sleeps)) == (False, "invalid", 2)   # the old code never slept
    assert s.posts() == [("ollama-ccdd", "gpt-oss:120b")] * 3              # no failover
    res, *_ = chat({("POST", CCDD, owc.CHAT_PATH): chat_ok('{"definition": "d"}')},
                   validate=lambda t: json.loads(t)["definition"])
    assert (res.content, res.parsed) == ('{"definition": "d"}', "d")


def warm(route):
    return chat({("POST", CCDD, owc.CHAT_PATH): route}, options={"temperature": 0}, validate=json.loads,
                retry_temperature=0.4)


def test_a_deterministic_rejection_is_retried_once_warmer():
    seen = []
    res, _, _, sleeps = warm(lambda body: seen.append(body["options"]["temperature"])
                             or chat_ok("prose" if len(seen) == 1 else '{"a": 1}'))
    assert (res.ok, res.parsed, res.attempts, seen, sleeps) == (True, {"a": 1}, 2, [0, 0.4], [])
    res, s, _, sleeps = warm(chat_ok("prose"))         # rejected twice: give up after the warm retry
    assert (res.ok, res.error_kind, res.attempts, len(s.posts()), sleeps) == (False, "invalid", 2, 2, [])


def test_a_generation_the_host_aborts_is_the_answers_fault():
    aborted = Resp(500, {"error": "prediction aborted, token repeat limit reached"})
    seen = []
    res, _, _, sleeps = warm(lambda body: seen.append(body["options"]["temperature"])
                             or (aborted if len(seen) == 1 else chat_ok('{"a": 1}')))
    assert (res.ok, res.parsed, seen, sleeps) == (True, {"a": 1}, [0, 0.4], [])
    res, s, _, _ = warm(aborted)                        # every time: a failed ANSWER, no other host
    assert (res.ok, res.error_kind, len(s.posts())) == (False, "invalid", 2)
    res, s, _, _ = chat({("POST", CCDD, owc.CHAT_PATH): aborted})
    assert (res.ok, res.error_kind) == (False, "invalid")
    assert s.posts() == [("ollama-ccdd", "gpt-oss:120b")] * 3


@pytest.mark.parametrize("think, fmt, ok", [
    (False, {"type": "object"}, True),
    (None, {"type": "object"}, False),     # thinking allowed: not the answer
    (False, None, False),                  # no grammar: prose
])
def test_a_grammar_answer_filed_as_thinking_is_the_answer(think, fmt, ok):
    # Ollama 0.34.1 + qwen3-vl:32b with think=false and a format grammar
    filed = Resp(200, {"message": {"role": "assistant", "content": "", "thinking": '{"a": 1}'}, "done": True})
    res, *_ = chat({("POST", CCDD, owc.CHAT_PATH): filed}, format=fmt, think=think, validate=json.loads)
    assert res.ok == ok and (not ok or res.parsed == {"a": 1})


def test_a_bad_request_is_permanent_and_a_malformed_body_is_an_attempt():
    res, s, _, sleeps = chat({("POST", CCDD, owc.CHAT_PATH): Resp(400, {"detail": "bad"})})
    assert (res.ok, res.error_kind, len(s.posts()), sleeps) == (False, "permanent", 1, [])
    res, _, _, sleeps = chat({("POST", CCDD, owc.CHAT_PATH): [Resp(200, text="<html>"), chat_ok()]})
    assert res.ok and (res.attempts, len(sleeps)) == (2, 1)


# -- embeddings ----------------------------------------------------------------------

def embed(route, texts=("a", "b"), **kw):
    c, _ = client(StubSession({("POST", UI, owc.EMBED_PATH): route}))
    return c, c.embed(list(texts), EMBED, **kw)


def test_vectors_in_order_normalised_and_dim_pinned():
    c, res = embed(Resp(200, {"embeddings": [[3, 4], [0, 2]]}), normalize=True)
    assert (res.vectors, res.dim, c.pins["embed"].dim) == ([[0.6, 0.8], [0.0, 1.0]], 2, 2)


@pytest.mark.parametrize("vectors", [[[1, 0]], [[0, 0], [1, 0]], [[1, 0], [1, 0, 0]]],
                         ids=["count mismatch (not a zip)", "a zero vector", "ragged"])
def test_a_bad_embedding_reply_is_a_protocol_error(vectors):
    _, res = embed(Resp(200, {"embeddings": vectors}))
    assert (res.ok, res.error_kind) == (False, "protocol")


def test_a_dimension_change_mid_run_raises_and_a_chat_model_never_embeds():
    c, _ = embed([Resp(200, {"embeddings": [[1, 0]]}), Resp(200, {"embeddings": [[1, 0, 0]]})], texts=["a"])
    with pytest.raises(owc.EmbeddingMixError):
        c.embed(["b"], EMBED)
    with pytest.raises(owc.ModelUnavailableError):
        embed(MODEL_NOT_FOUND)


def test_empty_input_needs_no_server():
    s = StubSession()
    c, _ = client(s)
    assert c.embed([], EMBED).vectors == [] and s.calls == []
