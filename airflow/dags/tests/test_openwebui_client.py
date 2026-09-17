"""Tests for openwebui_client.py — against stubbed HTTP, never a real server.

Selection is tested on the lab's REAL inventories
(fixtures/openwebui_tags_*.json, captured 2026-09-17), so "what would the
client pick" is answered for the models that actually exist rather than for
invented ones.

Two tests pin facts learned from the live servers and should not be deleted
as redundant:

  * ``test_403_model_not_found_is_not_an_auth_failure`` — Open WebUI answers
    a request for a model the host does not serve with
    ``403 {"detail": "Model not found"}``, not 404. Treating every 403 as a
    bad key would have marked ollama-ccdd unauthenticated the first time it
    was asked for mistral-small3.2, which only ollama-ui has.
  * ``test_each_key_only_works_on_its_own_host`` — keys are per host; the
    ollama-ui key is rejected by ollama-ccdd with 401 and vice versa.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m pytest /opt/airflow/dags/tests/test_openwebui_client.py -v
"""
import json
import os
import unittest

import requests

from modules import openwebui_client as owc

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
CCDD = "https://ollama-ccdd.pagoda.liris.cnrs.fr"
UI = "https://ollama-ui.pagoda.liris.cnrs.fr"
HOSTS = (CCDD, UI)


def tags(host: str) -> dict:
    name = "ollama_ccdd" if host == CCDD else "ollama_ui"
    with open(os.path.join(FIXTURES, f"openwebui_tags_{name}.json")) as fh:
        return json.load(fh)


def inventory() -> dict:
    return {h: [owc.ModelInfo.from_tags(h, m) for m in tags(h)["models"]] for h in HOSTS}


def names(candidates):
    return [(owc.host_name(m.host), m.name) for m in candidates]


# --- HTTP stubs -----------------------------------------------------------------

class Resp:
    def __init__(self, status=200, payload=None, text=""):
        self.status_code, self._payload, self.text = status, payload, text

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON")
        return self._payload


def chat_ok(content="ok"):
    return Resp(200, {"message": {"role": "assistant", "content": content}, "done": True})


MODEL_NOT_FOUND = Resp(403, {"detail": "Model not found"})


class StubSession:
    """Routes by (method, host, path). A route is a Resp, an exception, a
    list consumed in order (last one repeats), or a callable(json) -> Resp."""

    def __init__(self, routes=None, *, tags_for=HOSTS):
        self.routes = dict(routes or {})
        for h in tags_for:
            self.routes.setdefault(("GET", h, owc.TAGS_PATH), Resp(200, tags(h)))
        self.calls = []

    def _serve(self, method, url, json_body=None, headers=None):
        host = next(h for h in (CCDD, UI) if url.startswith(h))
        path = url[len(host):]
        self.calls.append((method, owc.host_name(host), path,
                           (json_body or {}).get("model"), headers))
        route = self.routes.get((method, host, path))
        if route is None:
            return Resp(404, {"detail": "no route in stub"})
        if isinstance(route, list):
            route = route.pop(0) if len(route) > 1 else route[0]
        if isinstance(route, Exception):
            raise route
        if callable(route) and not isinstance(route, Resp):
            return route(json_body or {})
        return route

    def get(self, url, headers=None, timeout=None):
        return self._serve("GET", url, headers=headers)

    def post(self, url, json=None, headers=None, timeout=None):
        return self._serve("POST", url, json, headers)

    def posts(self):
        return [(h, model) for m, h, p, model, _ in self.calls if m == "POST"]


def config(**over):
    base = dict(hosts=(owc.HostConfig(CCDD, "key-ccdd"), owc.HostConfig(UI, "key-ui")),
                max_attempts=3)
    base.update(over)
    return owc.LLMConfig(**base)


def client(session, **cfg):
    sleeps = []
    c = owc.OpenWebUIClient(config(**cfg), session, sleep=sleeps.append,
                            clock=lambda: 1000.0, jitter=lambda: 1.0)
    return c, sleeps


def by_model(handlers):
    """A POST route that answers per requested model name."""
    def route(body):
        handler = handlers.get(body.get("model"), MODEL_NOT_FOUND)
        return handler(body) if callable(handler) and not isinstance(handler, Resp) else handler
    return route


# --- parsing and classification ---------------------------------------------------

class InventoryFactsTests(unittest.TestCase):
    def test_parameter_sizes(self):
        self.assertEqual(owc.parse_parameter_size("24.0B"), 24.0)
        self.assertAlmostEqual(owc.parse_parameter_size("595.78M"), 0.59578)
        self.assertEqual(owc.parse_parameter_size("1.2T"), 1200.0)
        self.assertIsNone(owc.parse_parameter_size(""))
        self.assertIsNone(owc.parse_parameter_size("big"))

    def test_tier_bands(self):
        self.assertEqual([owc.tier_for(x) for x in (0.6, 6.99, 7.0, 23.9, 24.0, 69.9, 70.0)],
                         ["sm", "sm", "md", "md", "lg", "lg", "xl"])
        self.assertIsNone(owc.tier_for(None))

    def test_real_models_classify_by_what_they_are_optimised_for(self):
        spec = {(owc.host_name(m.host), m.name): m.specialization
                for ms in inventory().values() for m in ms}
        self.assertEqual(spec[("ollama-ccdd", "qwen3-vl:32b")], "vision")
        self.assertEqual(spec[("ollama-ui", "glm-ocr:latest")], "vision")
        self.assertEqual(spec[("ollama-ccdd", "devstral-2:123b")], "coding")
        self.assertEqual(spec[("ollama-ui", "qwen3-embedding:0.6b")], "embedding")
        # vision CAPABILITY is not vision SPECIALISATION
        self.assertEqual(spec[("ollama-ui", "mistral-small3.2:24b")], "general")
        self.assertEqual(spec[("ollama-ccdd", "qwen3.8:27b")], "general")

    def test_tier_comes_from_reported_size_not_the_name(self):
        gemma = next(m for m in inventory()[UI] if m.name == "gemma4:e4b")
        self.assertEqual((gemma.params_b, gemma.tier), (8.0, "md"))   # "e4b" is not 4B

    def test_cloud_model_is_flagged_and_has_no_tier(self):
        cloud = next(m for m in inventory()[CCDD] if m.name == "minimax-m2.7:cloud")
        self.assertTrue(cloud.is_cloud)
        self.assertIsNone(cloud.tier)

    def test_overrides_win(self):
        self.assertEqual(owc.classify_specialization("mixtral:8x7b", "llama", ["completion"],
                                                     {"mixtral:8x7b": "coding"}), "coding")


class ConfigTests(unittest.TestCase):
    def test_default_host_order_puts_ccdd_first(self):
        cfg = owc.LLMConfig.from_env({"OPENWEBUI_API_KEY": "k"})
        self.assertEqual(cfg.host_order, owc.DEFAULT_HOSTS)
        self.assertEqual(owc.host_name(cfg.host_order[0]), "ollama-ccdd")

    def test_per_host_key_then_default(self):
        cfg = owc.LLMConfig.from_env({"OPENWEBUI_API_KEY": "ui-key",
                                      "OPENWEBUI_API_KEY_OLLAMA_CCDD": "ccdd-key"})
        keys = {h.name: h.api_key for h in cfg.hosts}
        self.assertEqual(keys, {"ollama-ccdd": "ccdd-key", "ollama-ui": "ui-key"})

    def test_keyless_host_is_skipped_and_no_key_at_all_raises(self):
        cfg = owc.LLMConfig.from_env({"OPENWEBUI_API_KEY_OLLAMA_UI": "k"})
        self.assertEqual([h.name for h in cfg.hosts], ["ollama-ui"])
        with self.assertRaises(owc.OpenWebUIAuthError):
            owc.LLMConfig.from_env({"OPENWEBUI_API_KEY": ""})

    def test_host_list_and_overrides_from_env(self):
        cfg = owc.LLMConfig.from_env({
            "OPENWEBUI_HOSTS": f" {UI}/ , {CCDD}", "OPENWEBUI_API_KEY": "k",
            "OPENWEBUI_SPECIALIZATION_OVERRIDES": "mixtral:8x7b=coding",
            "OPENWEBUI_ALLOW_CLOUD": "true"})
        self.assertEqual(cfg.host_order, (UI, CCDD))
        self.assertEqual(cfg.specialization_overrides, (("mixtral:8x7b", "coding"),))
        self.assertTrue(cfg.allow_cloud)

    def test_keys_never_appear_in_a_repr(self):
        cfg = owc.LLMConfig.from_env({"OPENWEBUI_API_KEY": "sk-very-secret"})
        self.assertNotIn("sk-very-secret", repr(cfg))

    def test_request_validation(self):
        with self.assertRaises(ValueError):
            owc.ModelRequest()
        with self.assertRaises(ValueError):
            owc.ModelRequest(tier="huge")
        with self.assertRaises(ValueError):
            owc.ModelRequest(model="x", specialization="poetry")
        req = owc.ModelRequest.from_env("KG_CHAT", default_model="m",
                                        env={"KG_CHAT_TIER": "lg", "KG_CHAT_SPECIALIZATION": ""})
        self.assertEqual((req.model, req.tier, req.specialization), ("m", "lg", None))


# --- selection (pure) -------------------------------------------------------------

class ResolveCandidatesTests(unittest.TestCase):
    def resolve(self, **kw):
        pin = kw.pop("pin", None)
        allow_cloud = kw.pop("allow_cloud", False)
        return names(owc.resolve_candidates(inventory(), owc.ModelRequest(**kw), HOSTS,
                                            pin=pin, allow_cloud=allow_cloud))

    def test_requested_model_first_then_nearest_general_same_tier(self):
        got = self.resolve(model="mistral-small3.2:24b")
        self.assertEqual(got[0], ("ollama-ui", "mistral-small3.2:24b"))
        # nearest size (27.3B), on ccdd first because ccdd has priority
        self.assertEqual(got[1:3], [("ollama-ccdd", "qwen3.8:27b"), ("ollama-ui", "qwen3.8:27b")])
        self.assertNotIn(("ollama-ccdd", "qwen3-vl:32b"), got)      # vision-optimised
        self.assertNotIn(("ollama-ccdd", "minimax-m2.7:cloud"), got)
        self.assertTrue(all(m in ("mistral-small3.2:24b", "qwen3.8:27b", "glm-4.7-flash:bf16",
                                  "nemotron-3.5-lightning:30b", "qwen3.6:35b", "mixtral:8x7b")
                            for _, m in got))

    def test_same_model_on_the_other_host_comes_before_any_other_model(self):
        got = self.resolve(model="gpt-oss:120b")
        self.assertEqual(got[:2], [("ollama-ccdd", "gpt-oss:120b"), ("ollama-ui", "gpt-oss:120b")])

    def test_opting_into_a_specialization(self):
        self.assertEqual(self.resolve(tier="lg", specialization="vision"),
                         [("ollama-ccdd", "qwen3-vl:32b")])
        md_vision = self.resolve(tier="md", specialization="vision")
        self.assertEqual({m for _, m in md_vision}, {"qwen3-vl:8b", "qwen3-vl:8b-thinking"})
        self.assertEqual(self.resolve(tier="xl", specialization="coding"),
                         [("ollama-ccdd", "devstral-2:123b")])

    def test_any_ignores_the_grouping(self):
        self.assertIn(("ollama-ccdd", "qwen3-vl:32b"),
                      self.resolve(model="mistral-small3.2:24b", specialization="any"))

    def test_a_vision_model_falls_back_to_vision_models(self):
        got = self.resolve(model="qwen3-vl:8b")
        self.assertTrue(all(m.startswith("qwen3-vl") for _, m in got))

    def test_required_capabilities_filter_fallbacks(self):
        got = {m for _, m in self.resolve(model="mistral-small3.2:24b",
                                          capabilities=frozenset({"vision"}))}
        self.assertEqual(got, {"mistral-small3.2:24b", "qwen3.8:27b", "qwen3.6:35b"})

    def test_embedding_never_falls_back_to_a_chat_model(self):
        self.assertEqual(self.resolve(model="qwen3-embedding:0.6b", kind="embedding"),
                         [("ollama-ui", "qwen3-embedding:0.6b")])

    def test_cloud_models_need_an_explicit_opt_in(self):
        self.assertEqual(self.resolve(model="minimax-m2.7:cloud"), [])
        self.assertEqual(self.resolve(model="minimax-m2.7:cloud", allow_cloud=True),
                         [("ollama-ccdd", "minimax-m2.7:cloud")])

    def test_no_fallback_when_disallowed(self):
        self.assertEqual(self.resolve(model="mistral-small3.2:24b", allow_fallback=False),
                         [("ollama-ui", "mistral-small3.2:24b")])

    def test_unknown_model_without_a_tier_has_no_candidates(self):
        self.assertEqual(self.resolve(model="llama9:900b"), [])
        self.assertTrue(self.resolve(model="llama9:900b", tier="lg"))

    def test_a_pin_admits_only_the_pinned_weights_including_aliases(self):
        digest = next(m.digest for m in inventory()[UI] if m.name == "gemma4:e4b")
        got = self.resolve(model="mistral-small3.2:24b",
                           pin=owc.ModelPin("gemma4:e4b", digest))
        self.assertEqual(got, [("ollama-ui", "gemma4:e4b"), ("ollama-ui", "gemma4:latest")])

    def test_deterministic(self):
        self.assertEqual(self.resolve(model="qwen3:8b"), self.resolve(model="qwen3:8b"))


# --- the client: failover, pinning, errors ------------------------------------------

class HostFailoverTests(unittest.TestCase):
    def test_each_key_only_works_on_its_own_host(self):
        s = StubSession()
        c, _ = client(s)
        c.inventory()
        sent = {h: hdr["Authorization"] for m, h, p, _, hdr in s.calls if m == "GET"}
        self.assertEqual(sent, {"ollama-ccdd": "Bearer key-ccdd", "ollama-ui": "Bearer key-ui"})

    def test_403_model_not_found_is_not_an_auth_failure(self):
        # Pin qwen3.8 first so the call targets ccdd, then make ccdd say
        # "Model not found": the client must move to ollama-ui and keep ccdd.
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): MODEL_NOT_FOUND,
                         ("POST", UI, owc.CHAT_PATH): chat_ok("hi")})
        c, _ = client(s)
        res = c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="qwen3.8:27b"))
        self.assertTrue(res.ok)
        self.assertEqual(owc.host_name(res.host), "ollama-ui")
        self.assertFalse(c.host_status()["ollama-ccdd"]["auth_failed"])

    def test_auth_failure_on_one_host_fails_over(self):
        s = StubSession({("GET", CCDD, owc.TAGS_PATH): Resp(401, {"detail": "Not authenticated"}),
                         ("POST", UI, owc.CHAT_PATH): chat_ok()})
        c, _ = client(s)
        res = c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"))
        self.assertTrue(res.ok)
        self.assertEqual(owc.host_name(res.host), "ollama-ui")
        self.assertTrue(c.host_status()["ollama-ccdd"]["auth_failed"])

    def test_auth_failure_everywhere_raises(self):
        bad = Resp(401, {"detail": "token invalid"})
        s = StubSession({("GET", CCDD, owc.TAGS_PATH): bad, ("GET", UI, owc.TAGS_PATH): bad})
        c, _ = client(s)
        with self.assertRaises(owc.OpenWebUIAuthError):
            c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"))

    def test_unreachable_host_fails_over_to_the_same_model_without_retrying(self):
        refused = requests.ConnectionError(
            "HTTPSConnectionPool(host='ollama-ccdd...', port=443): Max retries exceeded "
            "(Caused by NewConnectionError('Failed to establish a new connection: "
            "[Errno 111] Connection refused'))")
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): refused,
                         ("POST", UI, owc.CHAT_PATH): chat_ok()})
        c, sleeps = client(s)
        res = c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"))
        self.assertEqual((owc.host_name(res.host), res.model), ("ollama-ui", "gpt-oss:120b"))
        self.assertEqual(s.posts(), [("ollama-ccdd", "gpt-oss:120b"), ("ollama-ui", "gpt-oss:120b")])
        self.assertEqual(sleeps, [])
        self.assertFalse(res.fell_back)

    def test_a_connection_dropped_mid_request_is_retried_on_the_same_host(self):
        # Seen live: the only host serving the embedding model reset the
        # connection once during a cold load. That is not "host down".
        dropped = requests.ConnectionError(
            "('Connection aborted.', RemoteDisconnected('Remote end closed "
            "connection without response'))")
        s = StubSession({("POST", UI, owc.EMBED_PATH): [dropped, Resp(200, {"embeddings": [[1, 0]]})]})
        c, sleeps = client(s)
        res = c.embed(["a"], owc.ModelRequest(model="qwen3-embedding:0.6b", kind="embedding"))
        self.assertTrue(res.ok)
        self.assertEqual((res.attempts, len(sleeps)), (2, 1))
        self.assertEqual(c.host_status()["ollama-ui"]["cooling_down_s"], 0)

    def test_connect_timeout_counts_as_host_down(self):
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): requests.ConnectTimeout("slow dns"),
                         ("POST", UI, owc.CHAT_PATH): chat_ok()})
        c, sleeps = client(s)
        res = c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"))
        self.assertEqual((owc.host_name(res.host), sleeps), ("ollama-ui", []))

    def test_5xx_is_retried_with_backoff_then_the_host_is_left(self):
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): Resp(502, text="bad gateway"),
                         ("POST", UI, owc.CHAT_PATH): chat_ok()})
        c, sleeps = client(s)
        res = c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"))
        self.assertTrue(res.ok)
        self.assertEqual(s.posts().count(("ollama-ccdd", "gpt-oss:120b")), 3)
        self.assertEqual(sleeps, [2.0, 4.0])                 # capped exponential, jitter=1
        self.assertGreater(c.host_status()["ollama-ccdd"]["cooling_down_s"], 0)
        self.assertEqual(res.attempts, 4)

    def test_every_host_down_raises_no_healthy_host(self):
        down = requests.ConnectionError("Failed to establish a new connection")
        s = StubSession({("GET", CCDD, owc.TAGS_PATH): down, ("GET", UI, owc.TAGS_PATH): down})
        c, _ = client(s)
        with self.assertRaises(owc.NoHealthyHostError):
            c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"))


class ModelFallbackAndPinningTests(unittest.TestCase):
    def test_model_missing_everywhere_falls_back_within_tier_and_flags_it(self):
        s = StubSession({("POST", UI, owc.CHAT_PATH): by_model({}),        # mistral gone
                         ("POST", CCDD, owc.CHAT_PATH): by_model({"qwen3.8:27b": chat_ok()})})
        c, _ = client(s)
        res = c.chat([{"role": "user", "content": "x"}],
                     owc.ModelRequest(model="mistral-small3.2:24b"))
        self.assertEqual((owc.host_name(res.host), res.model), ("ollama-ccdd", "qwen3.8:27b"))
        self.assertTrue(res.fell_back)
        self.assertEqual(c.pins["chat"].name, "qwen3.8:27b")

    def test_a_pinned_purpose_never_switches_models(self):
        answers = {"qwen3.8:27b": chat_ok()}
        s = StubSession({("POST", UI, owc.CHAT_PATH): by_model({"mistral-small3.2:24b": chat_ok()}),
                         ("POST", CCDD, owc.CHAT_PATH): by_model(answers)})
        c, _ = client(s)
        msgs = [{"role": "user", "content": "x"}]
        req = owc.ModelRequest(model="qwen3.8:27b")
        self.assertTrue(c.chat(msgs, req).ok)
        # qwen3.8 disappears from BOTH hosts; mistral is healthy and same tier.
        answers.clear()
        s.routes[("POST", UI, owc.CHAT_PATH)] = by_model({"mistral-small3.2:24b": chat_ok()})
        with self.assertRaises(owc.ModelUnavailableError) as ctx:
            c.chat(msgs, req)
        self.assertIn("refusing to switch models mid-run", str(ctx.exception))
        self.assertNotIn(("ollama-ui", "mistral-small3.2:24b"), s.posts())

    def test_a_pinned_purpose_may_still_change_host(self):
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): [chat_ok(), requests.ConnectTimeout()],
                         ("POST", UI, owc.CHAT_PATH): chat_ok()})
        c, _ = client(s)
        msgs, req = [{"role": "user", "content": "x"}], owc.ModelRequest(model="qwen3.8:27b")
        first, second = c.chat(msgs, req), c.chat(msgs, req)
        self.assertEqual(owc.host_name(first.host), "ollama-ccdd")
        self.assertEqual(owc.host_name(second.host), "ollama-ui")
        self.assertEqual(first.digest, second.digest)

    def test_purposes_pin_independently(self):
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): chat_ok(),
                         ("POST", UI, owc.CHAT_PATH): chat_ok()})
        c, _ = client(s)
        msgs = [{"role": "user", "content": "x"}]
        c.chat(msgs, owc.ModelRequest(model="qwen3.8:27b"), purpose="a")
        c.chat(msgs, owc.ModelRequest(model="gpt-oss:120b"), purpose="b")
        self.assertEqual({p: pin.name for p, pin in c.pins.items()},
                         {"a": "qwen3.8:27b", "b": "gpt-oss:120b"})

    def test_explicit_pin_for_a_resumed_run_and_no_repinning(self):
        c, _ = client(StubSession())
        c.pin("p", "gpt-oss:120b", "a951a23b46a1")
        with self.assertRaises(owc.EmbeddingMixError):
            c.pin("p", "qwen3.8:27b", "22130167c4c2")

    def test_images_require_vision(self):
        s = StubSession({("POST", UI, owc.CHAT_PATH): by_model({}),
                         ("POST", CCDD, owc.CHAT_PATH): by_model({"qwen3.8:27b": chat_ok()})})
        c, _ = client(s)
        res = c.chat([{"role": "user", "content": "x", "images": ["aGk="]}],
                     owc.ModelRequest(model="mistral-small3.2:24b"))
        self.assertEqual(res.model, "qwen3.8:27b")        # vision-capable; glm-4.7 is not


class OutputAndProtocolTests(unittest.TestCase):
    def test_rejected_output_sleeps_between_attempts_and_is_returned_as_data(self):
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): chat_ok("prose, not JSON")})
        c, sleeps = client(s)
        res = c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"),
                     validate=json.loads)
        self.assertFalse(res.ok)
        self.assertEqual(res.error_kind, "invalid")
        self.assertEqual(len(sleeps), 2)                    # the old code never slept
        self.assertEqual(s.posts(), [("ollama-ccdd", "gpt-oss:120b")] * 3)   # no failover

    def test_validated_value_is_returned(self):
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): chat_ok('{"definition": "d"}')})
        c, _ = client(s)
        res = c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"),
                     validate=lambda t: json.loads(t)["definition"])
        self.assertEqual((res.content, res.parsed), ('{"definition": "d"}', "d"))

    def test_bad_request_is_permanent_and_not_retried(self):
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): Resp(400, {"detail": "bad"})})
        c, sleeps = client(s)
        res = c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"))
        self.assertEqual((res.ok, res.error_kind), (False, "permanent"))
        self.assertEqual((len(s.posts()), sleeps), (1, []))

    def test_malformed_body_counts_as_an_attempt_and_sleeps(self):
        s = StubSession({("POST", CCDD, owc.CHAT_PATH): [Resp(200, text="<html>"), chat_ok()]})
        c, sleeps = client(s)
        res = c.chat([{"role": "user", "content": "x"}], owc.ModelRequest(model="gpt-oss:120b"))
        self.assertTrue(res.ok)
        self.assertEqual((res.attempts, len(sleeps)), (2, 1))


class EmbeddingTests(unittest.TestCase):
    REQ = owc.ModelRequest(model="qwen3-embedding:0.6b", kind="embedding")

    def run_embed(self, route, texts=("a", "b"), **kw):
        s = StubSession({("POST", UI, owc.EMBED_PATH): route})
        c, _ = client(s)
        return c, c.embed(list(texts), self.REQ, **kw)

    def test_vectors_in_order_normalised_and_dim_pinned(self):
        c, res = self.run_embed(Resp(200, {"embeddings": [[3, 4], [0, 2]]}), normalize=True)
        self.assertEqual(res.vectors, [[0.6, 0.8], [0.0, 1.0]])
        self.assertEqual((res.dim, c.pins["embed"].dim), (2, 2))

    def test_count_mismatch_is_a_protocol_error_not_a_zip(self):
        _, res = self.run_embed(Resp(200, {"embeddings": [[1, 0]]}))
        self.assertEqual((res.ok, res.error_kind), (False, "protocol"))

    def test_zero_or_ragged_vectors_are_protocol_errors(self):
        _, zero = self.run_embed(Resp(200, {"embeddings": [[0, 0], [1, 0]]}))
        _, ragged = self.run_embed(Resp(200, {"embeddings": [[1, 0], [1, 0, 0]]}))
        self.assertEqual((zero.error_kind, ragged.error_kind), ("protocol", "protocol"))

    def test_dimension_change_mid_run_raises(self):
        s = StubSession({("POST", UI, owc.EMBED_PATH): [
            Resp(200, {"embeddings": [[1, 0]]}), Resp(200, {"embeddings": [[1, 0, 0]]})]})
        c, _ = client(s)
        c.embed(["a"], self.REQ)
        with self.assertRaises(owc.EmbeddingMixError):
            c.embed(["b"], self.REQ)

    def test_embedding_request_is_never_served_by_a_chat_model(self):
        s = StubSession({("POST", UI, owc.EMBED_PATH): MODEL_NOT_FOUND})
        c, _ = client(s)
        with self.assertRaises(owc.ModelUnavailableError):
            c.embed(["a"], self.REQ)

    def test_empty_input_needs_no_server(self):
        s = StubSession()
        c, _ = client(s)
        self.assertEqual(c.embed([], self.REQ).vectors, [])
        self.assertEqual(s.calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
