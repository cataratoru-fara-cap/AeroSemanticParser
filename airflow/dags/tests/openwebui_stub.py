"""A stubbed Open WebUI over the lab's REAL inventories
(fixtures/openwebui_tags_*.json, captured 2026-09-17), for every test that
drives modules/openwebui_client.py — no server is ever contacted."""
from __future__ import annotations

import json

from helpers import FIXTURES
from modules import openwebui_client as owc

CCDD = "https://ollama-ccdd.pagoda.liris.cnrs.fr"
UI = "https://ollama-ui.pagoda.liris.cnrs.fr"
HOSTS = (CCDD, UI)


def tags(host: str) -> dict:
    name = "ollama_ccdd" if host == CCDD else "ollama_ui"
    return json.loads((FIXTURES / f"openwebui_tags_{name}.json").read_text())


def inventory() -> dict:
    return {h: [owc.ModelInfo.from_tags(h, m) for m in tags(h)["models"]] for h in HOSTS}


class Resp:
    def __init__(self, status=200, payload=None, text=""):
        self.status_code, self._payload, self.text = status, payload, text

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON")
        return self._payload


def chat_ok(content="ok"):
    return Resp(200, {"message": {"role": "assistant", "content": content}, "done": True})


# Open WebUI's answer for a model the host does not serve: 403, not 404
MODEL_NOT_FOUND = Resp(403, {"detail": "Model not found"})


class StubSession:
    """Routes by (method, host, path). A route is a Resp, an exception, a list
    consumed in order (the last one repeats), or a callable(json) -> Resp."""

    def __init__(self, routes=None, *, tags_for=HOSTS):
        self.routes = dict(routes or {})
        for h in tags_for:
            self.routes.setdefault(("GET", h, owc.TAGS_PATH), Resp(200, tags(h)))
        self.calls = []

    def _serve(self, method, url, json_body=None, headers=None):
        host = next(h for h in HOSTS if url.startswith(h))
        path = url[len(host):]
        self.calls.append((method, owc.host_name(host), path, (json_body or {}).get("model"), headers))
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
    return owc.LLMConfig(**{"hosts": (owc.HostConfig(CCDD, "key-ccdd"), owc.HostConfig(UI, "key-ui")),
                            "max_attempts": 3, **over})


def client(session, **cfg):
    """(client, the sleeps it asked for): no real sleeping, a fixed clock, no jitter."""
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
