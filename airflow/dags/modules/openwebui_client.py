"""
openwebui_client.py — resilient client for the lab's Open WebUI / Ollama hosts
================================================================================
Pure HTTP library + thin CLI, in the scrapingant_client mould: no Airflow, no
Mongo, no module-level environment reads or mutable state. Every LLM or
embedding call in this repository goes through here, so the reliability rules
below live in one place instead of being re-invented per script.

The hosts
---------
The lab runs Open WebUI in front of Ollama on more than one machine, and they
are not interchangeable:

  * each host has its OWN API key — a key minted on one is rejected by the
    other with 401 (verified 2026-09-17);
  * each host serves a DIFFERENT inventory — the chat default
    ``mistral-small3.2:24b`` and the only embedding model
    ``qwen3-embedding:0.6b`` exist on ollama-ui alone, while ollama-ccdd has
    the vision and coding models;
  * hosts go down independently, which is why this module exists.

``OPENWEBUI_HOSTS`` lists them in priority order (default: ollama-ccdd, then
ollama-ui). A host's key is ``OPENWEBUI_API_KEY_<SLUG>``, SLUG being the first
DNS label upper-cased with non-alphanumerics as ``_`` (``ollama-ccdd`` ->
``OPENWEBUI_API_KEY_OLLAMA_CCDD``), falling back to ``OPENWEBUI_API_KEY``.

How a model is chosen
---------------------
Inventories are discovered at runtime from ``GET /ollama/api/tags`` — never
hard-coded — and every model is described by facts the server reports:

  identity        its DIGEST (the weights hash). Two names with one digest are
                  one model (``gemma4:e4b`` == ``gemma4:latest``); one name
                  with two digests on two hosts is two models. "The same
                  model on another host" always means the same digest.
  kind            ``embedding`` if the server lists that capability, else
                  ``generation``.
  capabilities    as reported: completion, vision, tools, thinking, embedding.
  tier            from the reported parameter size, never parsed from the
                  name (``gemma4:e4b`` reads as 4B and is 8.0B):
                      xl >= 70B   lg >= 24B   md >= 7B   sm < 7B
                  A model without a reported size has no tier and is never
                  selected as a fallback.
  specialization  what the model is OPTIMISED for: ``general``, ``vision``,
                  ``coding`` or ``embedding``. Servers do not report this —
                  ``vision`` capability alone does not make a model
                  vision-optimised (``mistral-small3.2`` has it and is a
                  general model) — so it comes from a short curated pattern
                  table below, overridable per model with
                  ``OPENWEBUI_SPECIALIZATION_OVERRIDES=name=vision,...``.

A call names what it wants with a ``ModelRequest``: a model, and/or a tier,
plus optionally a specialization and required capabilities. The candidates
are tried in this order (``resolve_candidates``, a pure function):

  1. the requested model — its digest — on every host that serves it, in
     host priority order. Host failover never changes the model.
  2. only then, other models of the SAME tier, the SAME kind, the SAME
     specialization, and with every required capability: nearest parameter
     count to the requested model first, then name, each on its hosts in
     priority order. Specialization defaults to the requested model's own
     (a general model falls back to general models); pass one explicitly to
     opt into a group ("any vision-optimised lg model"), or ``"any"`` to
     ignore the grouping.

Cloud-proxied models (``minimax-m2.7:cloud``: a 375-byte pointer to a remote
provider) send prompts outside the lab and are never selected unless
``allow_cloud`` is set.

Pinning: one model per purpose per run
--------------------------------------
Once a call for a given ``purpose`` succeeds, that purpose is PINNED to the
model's digest for the life of the client. Later calls for the purpose may
fail over to another host serving the identical digest, but never to a
different model — if no host serves it any more, the call raises
``ModelUnavailableError`` instead of silently mixing outputs of two models
into one dataset. This holds for chat, vision and embeddings alike; for
embeddings the vector dimension is pinned too. A resumed run re-establishes
its pin from the artifact it is extending (``client.pin(...)``), so a run
that stops and restarts is still one model.

Error taxonomy
--------------
    transport   DNS / refused / reset       -> host unhealthy for a cooldown,
                                               fail over, no retry there
    auth        401, or 403 that is not     -> host marked unauthenticated,
                "model not found"              fail over; OpenWebUIAuthError
                                               only when EVERY host rejected
                                               its key. (scrapingant_client
                                               raises on the first 401; with
                                               per-host keys a 401 is a
                                               routing fact, not a verdict.)
    missing     403 "Model not found" —     -> that (host, model) excluded,
                what Open WebUI actually       re-select
                returns — or Ollama's 404
    permanent   400/405/413/422 and other   -> returned as data, no failover:
                4xx                            the request is wrong everywhere
    retryable   408/409/425/429/5xx,        -> retried on the same host with
                timeouts, malformed body       capped full-jitter backoff,
                                               then host unhealthy + failover
    invalid     the caller's ``validate``   -> counts as an attempt AND
                rejected the model output      sleeps (retrying instantly just
                                               burns the budget), then
                                               returned as data
    protocol    an embedding response with  -> returned as data, never zipped
                the wrong count, ragged or     blindly against the inputs
                zero vectors

Timeouts are generous on purpose: a cold load of an 8B model took 47 s on
ollama-ccdd, and a 120B model takes far longer. But note the ceiling the
hosts impose: Open WebUI's proxy closes any inference request that has not
answered within 50 s (``RemoteDisconnected`` after exactly 50.0 s, observed
2026-09-17 while ollama-ui was contended), so on these hosts the effective
per-request budget is 50 s whatever ``read_timeout_s`` says. Such a drop is
retried like a timeout. When a host is busy, prefer smaller requests (e.g.
fewer texts per embedding batch) and more attempts
(``OPENWEBUI_MAX_ATTEMPTS``) over a longer timeout.

Config (environment; empty means unset):
    OPENWEBUI_HOSTS                  comma-separated base URLs, priority order
    OPENWEBUI_API_KEY                default key
    OPENWEBUI_API_KEY_<SLUG>         per-host key
    OPENWEBUI_CONNECT_TIMEOUT_S      default 10
    OPENWEBUI_READ_TIMEOUT_S         default 600
    OPENWEBUI_MAX_ATTEMPTS           per host, retryable errors; default 3
    OPENWEBUI_UNHEALTHY_COOLDOWN_S   default 120
    OPENWEBUI_ALLOW_CLOUD            default false
    OPENWEBUI_SPECIALIZATION_OVERRIDES   name=specialization,...

CLI:
    python -m modules.openwebui_client probe
    python -m modules.openwebui_client probe --model mistral-small3.2:24b
    python -m modules.openwebui_client probe --tier lg --specialization vision
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urlparse

import requests

log = logging.getLogger("openwebui_client")

DEFAULT_HOSTS: tuple[str, ...] = (
    "https://ollama-ccdd.pagoda.liris.cnrs.fr",
    "https://ollama-ui.pagoda.liris.cnrs.fr",
)

TAGS_PATH = "/ollama/api/tags"
CHAT_PATH = "/ollama/api/chat"
EMBED_PATH = "/ollama/api/embed"

# Ascending. Thresholds in billions of parameters, largest first.
TIERS: tuple[str, ...] = ("sm", "md", "lg", "xl")
_TIER_THRESHOLDS_B: tuple[tuple[str, float], ...] = (("xl", 70.0), ("lg", 24.0),
                                                     ("md", 7.0))

KINDS: tuple[str, ...] = ("generation", "embedding")
SPECIALIZATIONS: tuple[str, ...] = ("general", "vision", "coding", "embedding")
ANY_SPECIALIZATION = "any"

# What a model is optimised for, which no server reports. Matched against the
# model name first, then its family; first hit wins, "general" otherwise.
# Keep this short and literal: an entry here changes which models may stand
# in for each other.
_NAME_SPECIALIZATIONS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("coding", re.compile(
        r"devstral|codestral|coder|codellama|starcoder|codegemma|codeqwen"
        r"|granite-code", re.I)),
    ("vision", re.compile(
        r"(?:^|[-_.:/])vl(?:$|[-_.:\d])|llava|moondream|minicpm-v|bakllava"
        r"|ocr|-vision", re.I)),
)
_FAMILY_SPECIALIZATIONS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("vision", re.compile(r"vl$|ocr|llava|mllama", re.I)),
)

_AUTH_STATUSES = {401, 403}
_PERMANENT_STATUSES = {400, 405, 413, 422}
_RETRYABLE_STATUSES = {408, 409, 425, 429}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class OpenWebUIError(RuntimeError):
    """Base class. Only raised for conditions a caller cannot route around."""


class OpenWebUIAuthError(OpenWebUIError):
    """No usable key: every configured host rejected its key, or none is set."""


class NoHealthyHostError(OpenWebUIError):
    """Every host is unreachable or cooling down after failures."""


class ModelUnavailableError(OpenWebUIError):
    """No reachable host serves any acceptable model — including the case of
    a pinned purpose whose model is gone, where switching is refused."""


class EmbeddingMixError(OpenWebUIError):
    """A run would mix vectors from different models or dimensions."""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def host_slug(base_url: str) -> str:
    """``https://ollama-ccdd.pagoda...`` -> ``OLLAMA_CCDD``."""
    label = (urlparse(base_url).hostname or base_url).split(".")[0]
    return re.sub(r"[^A-Za-z0-9]", "_", label).upper()


def host_name(base_url: str) -> str:
    """``https://ollama-ccdd.pagoda...`` -> ``ollama-ccdd``, for logs."""
    return (urlparse(base_url).hostname or base_url).split(".")[0]


@dataclass(frozen=True)
class HostConfig:
    base_url: str
    api_key: str = field(repr=False)     # never in a repr, log or traceback

    @property
    def name(self) -> str:
        return host_name(self.base_url)


def _env(env: Mapping[str, str], key: str, default: str | None = None) -> str | None:
    value = (env.get(key) or "").strip()
    return value or default


@dataclass(frozen=True)
class LLMConfig:
    """Immutable per-run configuration; build once, pass explicitly."""

    hosts: tuple[HostConfig, ...]             # priority order
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 600.0
    max_attempts: int = 3
    backoff_base_s: float = 2.0
    backoff_max_s: float = 30.0
    unhealthy_cooldown_s: float = 120.0
    allow_cloud: bool = False
    specialization_overrides: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.hosts:
            raise OpenWebUIAuthError("no Open WebUI host has an API key")
        for _, spec in self.specialization_overrides:
            if spec not in SPECIALIZATIONS:
                raise ValueError(f"unknown specialization override {spec!r}; "
                                 f"expected one of {SPECIALIZATIONS}")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "LLMConfig":
        env = os.environ if env is None else env
        urls = [u.strip().rstrip("/") for u in
                (_env(env, "OPENWEBUI_HOSTS") or ",".join(DEFAULT_HOSTS)).split(",")
                if u.strip()]
        default_key = _env(env, "OPENWEBUI_API_KEY")
        hosts, keyless = [], []
        for url in urls:
            key = _env(env, f"OPENWEBUI_API_KEY_{host_slug(url)}") or default_key
            if key:
                hosts.append(HostConfig(url, key))
            else:
                keyless.append(url)
        if keyless:
            log.warning("No API key for %s (set OPENWEBUI_API_KEY_%s); host skipped",
                        ", ".join(keyless), host_slug(keyless[0]))
        if not hosts:
            raise OpenWebUIAuthError(
                "No Open WebUI API key is set. Create one in Open WebUI "
                "(Settings -> Account -> API keys) on each host and set "
                "OPENWEBUI_API_KEY_<HOST> — e.g. OPENWEBUI_API_KEY_OLLAMA_CCDD.")
        overrides = []
        for item in (_env(env, "OPENWEBUI_SPECIALIZATION_OVERRIDES") or "").split(","):
            if "=" in item:
                name, spec = item.split("=", 1)
                overrides.append((name.strip(), spec.strip()))
        return cls(
            hosts=tuple(hosts),
            connect_timeout_s=float(_env(env, "OPENWEBUI_CONNECT_TIMEOUT_S", "10")),
            read_timeout_s=float(_env(env, "OPENWEBUI_READ_TIMEOUT_S", "600")),
            max_attempts=int(_env(env, "OPENWEBUI_MAX_ATTEMPTS", "3")),
            unhealthy_cooldown_s=float(_env(env, "OPENWEBUI_UNHEALTHY_COOLDOWN_S", "120")),
            allow_cloud=(_env(env, "OPENWEBUI_ALLOW_CLOUD", "false").lower()
                         in ("1", "true", "yes")),
            specialization_overrides=tuple(overrides),
        )

    @property
    def host_order(self) -> tuple[str, ...]:
        return tuple(h.base_url for h in self.hosts)


# ---------------------------------------------------------------------------
# Inventory: what the servers say about their models
# ---------------------------------------------------------------------------

_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([KMBT])?\s*$", re.I)
_SIZE_SCALE_B = {"K": 1e-6, "M": 1e-3, "B": 1.0, "T": 1e3, None: 1e-9}


def parse_parameter_size(value: str | None) -> float | None:
    """``"24.0B"`` -> 24.0, ``"595.78M"`` -> 0.59578 (billions), ``""`` -> None."""
    if not value:
        return None
    match = _SIZE_RE.match(str(value))
    if not match:
        return None
    unit = match.group(2).upper() if match.group(2) else None
    return float(match.group(1)) * _SIZE_SCALE_B[unit]


def tier_for(params_b: float | None) -> str | None:
    if params_b is None:
        return None
    for tier, threshold in _TIER_THRESHOLDS_B:
        if params_b >= threshold:
            return tier
    return "sm"


def classify_specialization(name: str, family: str | None,
                            capabilities: Iterable[str],
                            overrides: Mapping[str, str] | None = None) -> str:
    if overrides and name in overrides:
        return overrides[name]
    if "embedding" in set(capabilities):
        return "embedding"
    for spec, pattern in _NAME_SPECIALIZATIONS:
        if pattern.search(name):
            return spec
    for spec, pattern in _FAMILY_SPECIALIZATIONS:
        if family and pattern.search(family):
            return spec
    return "general"


def _is_cloud(raw: Mapping[str, Any]) -> bool:
    tag = str(raw.get("name", "")).rsplit(":", 1)[-1]
    details = raw.get("details") or {}
    return (tag.endswith("cloud") or bool(raw.get("remote_host"))
            or bool(details.get("remote_host")))


@dataclass(frozen=True)
class ModelInfo:
    host: str                       # base URL
    name: str
    digest: str
    family: str | None
    parameter_size: str | None
    params_b: float | None
    tier: str | None
    capabilities: frozenset[str]
    specialization: str
    is_cloud: bool

    @property
    def kind(self) -> str:
        return "embedding" if "embedding" in self.capabilities else "generation"

    @classmethod
    def from_tags(cls, host: str, raw: Mapping[str, Any],
                  overrides: Mapping[str, str] | None = None) -> "ModelInfo":
        details = raw.get("details") or {}
        name = str(raw.get("name") or raw.get("model"))
        caps = frozenset(raw.get("capabilities") or ())
        params = parse_parameter_size(details.get("parameter_size"))
        return cls(
            host=host, name=name,
            # A server that omits the digest still gets a stable identity,
            # but a host-scoped one: without the weights hash, "the same
            # model elsewhere" cannot be proven, so it is never assumed.
            digest=str(raw.get("digest") or f"{host}#{name}"),
            family=details.get("family"),
            parameter_size=details.get("parameter_size") or None,
            params_b=params, tier=tier_for(params), capabilities=caps,
            specialization=classify_specialization(
                name, details.get("family"), caps, overrides),
            is_cloud=_is_cloud(raw),
        )


# ---------------------------------------------------------------------------
# Requests, pins, results
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ModelRequest:
    """What a call wants. At least one of ``model`` / ``tier``.

    ``specialization`` None means "the requested model's own"; a value opts
    into that group; ``"any"`` ignores the grouping. ``capabilities`` are
    hard requirements (e.g. ``{"vision"}`` when sending images).
    """

    model: str | None = None
    tier: str | None = None
    specialization: str | None = None
    capabilities: frozenset[str] = frozenset()
    kind: str = "generation"
    allow_fallback: bool = True

    def __post_init__(self) -> None:
        if not self.model and not self.tier:
            raise ValueError("a ModelRequest needs a model, a tier, or both")
        if self.tier is not None and self.tier not in TIERS:
            raise ValueError(f"tier {self.tier!r} not in {TIERS}")
        if (self.specialization is not None
                and self.specialization not in SPECIALIZATIONS + (ANY_SPECIALIZATION,)):
            raise ValueError(f"specialization {self.specialization!r} not in "
                             f"{SPECIALIZATIONS + (ANY_SPECIALIZATION,)}")
        if self.kind not in KINDS:
            raise ValueError(f"kind {self.kind!r} not in {KINDS}")
        object.__setattr__(self, "capabilities", frozenset(self.capabilities))

    @classmethod
    def from_env(cls, prefix: str, *, kind: str = "generation",
                 default_model: str | None = None,
                 env: Mapping[str, str] | None = None) -> "ModelRequest":
        """``KG_CHAT`` -> KG_CHAT_MODEL / _TIER / _SPECIALIZATION."""
        env = os.environ if env is None else env
        return cls(model=_env(env, f"{prefix}_MODEL", default_model),
                   tier=_env(env, f"{prefix}_TIER"),
                   specialization=_env(env, f"{prefix}_SPECIALIZATION"),
                   kind=kind)


@dataclass(frozen=True)
class ModelPin:
    """The model a purpose is bound to for the rest of a run."""

    name: str
    digest: str
    dim: int | None = None          # embeddings only


@dataclass
class LLMResult:
    """Outcome of one call, whichever hosts and models it went through."""

    ok: bool
    purpose: str
    model: str | None = None
    digest: str | None = None
    host: str | None = None
    content: str | None = None                  # chat: raw message content
    parsed: Any = None                          # chat: validate()'s return
    vectors: list[list[float]] | None = None    # embed
    dim: int | None = None
    fell_back: bool = False                     # not the requested model
    error: str | None = None
    error_kind: str | None = None               # permanent | invalid | protocol
    attempts: int = 0
    elapsed_s: float = 0.0
    trail: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Selection (pure)
# ---------------------------------------------------------------------------

def resolve_candidates(inventory: Mapping[str, Sequence[ModelInfo]],
                       request: ModelRequest, host_order: Sequence[str], *,
                       pin: ModelPin | None = None,
                       allow_cloud: bool = False) -> list[ModelInfo]:
    """Every acceptable (host, model), best first. See the module docstring.

    Deterministic for a given inventory, so parallel workers choose alike.
    """
    ordered = [m for h in host_order for m in inventory.get(h, ())]
    host_rank = {h: i for i, h in enumerate(host_order)}

    required = set(request.capabilities)
    required.add("embedding" if request.kind == "embedding" else "completion")

    def usable(m: ModelInfo) -> bool:
        return (m.kind == request.kind and required <= m.capabilities
                and (allow_cloud or not m.is_cloud))

    def on_hosts(models: Iterable[ModelInfo], prefer_name: str | None) -> list[ModelInfo]:
        return sorted(models, key=lambda m: (host_rank.get(m.host, len(host_rank)),
                                             m.name != prefer_name, m.name))

    if pin is not None:
        # Mid-run: the pinned weights on any host, and nothing else — ever.
        return on_hosts((m for m in ordered if m.digest == pin.digest and usable(m)),
                        pin.name)

    anchor = next((m for m in ordered if m.name == request.model), None) \
        if request.model else None
    primary = on_hosts((m for m in ordered if anchor and m.digest == anchor.digest
                        and usable(m)), request.model) if anchor else []
    if not request.allow_fallback:
        return primary

    tier = request.tier or (anchor.tier if anchor else None)
    if tier is None:
        return primary
    if request.kind == "embedding":
        spec = "embedding"
    else:
        spec = request.specialization or (anchor.specialization if anchor else "general")

    by_digest: dict[str, list[ModelInfo]] = {}
    for m in ordered:
        if (usable(m) and m.tier == tier and (anchor is None or m.digest != anchor.digest)
                and (spec == ANY_SPECIALIZATION or m.specialization == spec)):
            by_digest.setdefault(m.digest, []).append(m)

    def rank(digest: str) -> tuple:
        models = by_digest[digest]
        params = models[0].params_b
        canonical = min(m.name for m in models)
        if anchor and anchor.params_b and params:
            return (abs(math.log(params / anchor.params_b)), canonical)
        return (-(params or 0.0), canonical)

    fallback: list[ModelInfo] = []
    for digest in sorted(by_digest, key=rank):
        fallback.extend(on_hosts(by_digest[digest], None))
    return primary + fallback


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    """Plain pooled session; retries are explicit (see the taxonomy)."""
    return requests.Session()


@dataclass
class _HostState:
    unhealthy_until: float = 0.0
    auth_failed: bool = False
    last_error: str | None = None


@dataclass
class _Attempt:
    outcome: str        # ok | auth | missing | permanent | exhausted | transport | invalid | protocol
    attempts: int
    body: Any = None
    parsed: Any = None
    error: str | None = None


class _InvalidOutput(ValueError):
    pass


class _ProtocolError(ValueError):
    pass


class OpenWebUIClient:
    """One run's view of the hosts: health, inventory, and purpose pins."""

    def __init__(self, cfg: LLMConfig, session: requests.Session | None = None, *,
                 sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic,
                 jitter: Callable[[], float] = random.random) -> None:
        self.cfg = cfg
        self.session = session or make_session()
        self._sleep, self._clock, self._jitter = sleep, clock, jitter
        self._hosts = {h.base_url: h for h in cfg.hosts}
        self._state = {h.base_url: _HostState() for h in cfg.hosts}
        self._inventory: dict[str, list[ModelInfo]] = {}
        self._pins: dict[str, ModelPin] = {}

    # -- hosts --------------------------------------------------------------

    def _available(self, base: str) -> bool:
        st = self._state[base]
        return not st.auth_failed and self._clock() >= st.unhealthy_until

    def _mark_unhealthy(self, base: str, reason: str) -> None:
        st = self._state[base]
        st.unhealthy_until = self._clock() + self.cfg.unhealthy_cooldown_s
        st.last_error = reason
        log.warning("%s unhealthy for %.0fs: %s", host_name(base),
                    self.cfg.unhealthy_cooldown_s, reason)

    def _mark_auth_failed(self, base: str, reason: str) -> None:
        st = self._state[base]
        st.auth_failed, st.last_error = True, reason
        log.error("%s rejected its API key (%s); set OPENWEBUI_API_KEY_%s",
                  host_name(base), reason, host_slug(base))
        if all(s.auth_failed for s in self._state.values()):
            raise OpenWebUIAuthError(
                "every Open WebUI host rejected its API key: " + "; ".join(
                    f"{host_name(b)}: {s.last_error}" for b, s in self._state.items()))

    def _headers(self, base: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._hosts[base].api_key}",
                "Content-Type": "application/json"}

    @property
    def _timeout(self) -> tuple[float, float]:
        return (self.cfg.connect_timeout_s, self.cfg.read_timeout_s)

    def host_status(self) -> dict[str, dict[str, Any]]:
        now = self._clock()
        return {host_name(b): {"url": b, "auth_failed": s.auth_failed,
                               "cooling_down_s": max(0.0, round(s.unhealthy_until - now, 1)),
                               "models": len(self._inventory.get(b, [])),
                               "last_error": s.last_error}
                for b, s in self._state.items()}

    # -- inventory ------------------------------------------------------------

    def inventory(self, refresh: bool = False) -> dict[str, list[ModelInfo]]:
        """Models per reachable host. Fetched lazily; ``refresh`` re-reads
        every host that is not known-bad (and retries cooled-down ones)."""
        overrides = dict(self.cfg.specialization_overrides)
        for base in self.cfg.host_order:
            if not self._available(base):
                self._inventory.pop(base, None)
                continue
            if base in self._inventory and not refresh:
                continue
            try:
                resp = self.session.get(base + TAGS_PATH, headers=self._headers(base),
                                        timeout=(self.cfg.connect_timeout_s, 60))
            except requests.RequestException as exc:
                self._mark_unhealthy(base, f"tags: {exc.__class__.__name__}")
                self._inventory.pop(base, None)
                continue
            if resp.status_code in _AUTH_STATUSES:
                self._inventory.pop(base, None)
                self._mark_auth_failed(base, f"tags: {resp.status_code} {_detail(resp)}")
                continue
            if resp.status_code != 200:
                self._mark_unhealthy(base, f"tags: {resp.status_code} {_detail(resp)}")
                self._inventory.pop(base, None)
                continue
            try:
                raw_models = resp.json().get("models") or []
            except ValueError:
                self._mark_unhealthy(base, "tags: malformed JSON")
                self._inventory.pop(base, None)
                continue
            self._inventory[base] = [ModelInfo.from_tags(base, m, overrides)
                                     for m in raw_models]
        return {b: list(ms) for b, ms in self._inventory.items()}

    # -- pins -------------------------------------------------------------------

    @property
    def pins(self) -> dict[str, ModelPin]:
        return dict(self._pins)

    def pin(self, purpose: str, name: str, digest: str, dim: int | None = None) -> ModelPin:
        """Bind a purpose to a model before its first call — how a resumed
        run stays on the model its existing output came from."""
        new = ModelPin(name, digest, dim)
        current = self._pins.get(purpose)
        if current is not None and (current.digest != digest
                                    or (dim and current.dim and dim != current.dim)):
            raise EmbeddingMixError(
                f"purpose {purpose!r} is already pinned to {current.name}@"
                f"{current.digest[:12]}; refusing to re-pin to {name}@{digest[:12]}")
        self._pins[purpose] = replace(current, dim=current.dim or dim) if current else new
        return self._pins[purpose]

    def find_digest(self, name: str) -> str | None:
        """The digest ``name`` has on the highest-priority host serving it."""
        inv = self.inventory()
        for base in self.cfg.host_order:
            for m in inv.get(base, ()):
                if m.name == name:
                    return m.digest
        return None

    # -- calls ------------------------------------------------------------------

    def chat(self, messages: Sequence[Mapping[str, Any]], request: ModelRequest, *,
             purpose: str | None = None, format: Any = None,
             options: Mapping[str, Any] | None = None, think: bool | None = None,
             validate: Callable[[str], Any] | None = None) -> LLMResult:
        """One non-streaming chat completion.

        ``validate(content)`` returns the parsed value or raises ValueError;
        a rejection counts as an attempt and sleeps before the next one.
        Messages carrying ``images`` add the ``vision`` requirement.
        """
        if request.kind != "generation":
            raise ValueError("chat() needs a generation request")
        if any(m.get("images") for m in messages):
            request = replace(request, capabilities=request.capabilities | {"vision"})

        def payload(model: ModelInfo) -> dict[str, Any]:
            body: dict[str, Any] = {"model": model.name, "messages": list(messages),
                                    "stream": False}
            if format is not None:
                body["format"] = format
            if options:
                body["options"] = dict(options)
            if think is not None:
                body["think"] = think
            return body

        def parse(body: Any) -> tuple[str, Any]:
            try:
                content = body["message"]["content"]
            except (KeyError, TypeError):
                raise _ProtocolError("chat response has no message.content") from None
            if not isinstance(content, str):
                raise _ProtocolError("chat message.content is not a string")
            if validate is None:
                return content, content
            try:
                return content, validate(content)
            except (ValueError, KeyError, TypeError) as exc:
                raise _InvalidOutput(f"{exc.__class__.__name__}: {exc}") from None

        return self._call(CHAT_PATH, payload, parse, request,
                          purpose or "chat")

    def embed(self, texts: Sequence[str], request: ModelRequest, *,
              purpose: str | None = None, normalize: bool = False) -> LLMResult:
        """Embed ``texts`` in one request. Vectors come back in input order,
        or not at all: a response that does not match the input one-to-one
        is a protocol error, never zipped best-effort."""
        if request.kind != "embedding":
            raise ValueError("embed() needs an embedding request")
        purpose = purpose or "embed"
        texts = list(texts)
        if not texts:
            return LLMResult(ok=True, purpose=purpose, vectors=[])

        def payload(model: ModelInfo) -> dict[str, Any]:
            return {"model": model.name, "input": texts}

        def parse(body: Any) -> tuple[None, list[list[float]]]:
            vectors = body.get("embeddings") if isinstance(body, dict) else None
            if not isinstance(vectors, list) or len(vectors) != len(texts):
                got = len(vectors) if isinstance(vectors, list) else "no"
                raise _ProtocolError(f"{got} embeddings for {len(texts)} inputs")
            dims = {len(v) if isinstance(v, list) else -1 for v in vectors}
            if len(dims) != 1 or dims == {0} or dims == {-1}:
                raise _ProtocolError(f"ragged or empty embedding dimensions {sorted(dims)}")
            out = []
            for vec in vectors:
                norm = math.sqrt(sum(float(x) * float(x) for x in vec))
                if not norm or math.isnan(norm):
                    raise _ProtocolError("zero or NaN embedding vector")
                out.append([float(x) / norm for x in vec] if normalize
                           else [float(x) for x in vec])
            return None, out

        result = self._call(EMBED_PATH, payload, parse, request, purpose)
        if result.ok:
            result.vectors, result.parsed = result.parsed, None
            result.dim = len(result.vectors[0])
            pin = self._pins[purpose]
            if pin.dim is None:
                self._pins[purpose] = replace(pin, dim=result.dim)
            elif pin.dim != result.dim:
                raise EmbeddingMixError(
                    f"{purpose!r}: {result.model} returned {result.dim}-dim vectors "
                    f"but this run is pinned to {pin.dim}")
        return result

    # -- the failover loop ----------------------------------------------------

    def _call(self, path: str, payload: Callable[[ModelInfo], dict],
              parse: Callable[[Any], tuple[Any, Any]], request: ModelRequest,
              purpose: str) -> LLMResult:
        started = self._clock()
        attempts, trail = 0, []
        excluded: set[tuple[str, str]] = set()
        refreshed = False

        while True:
            pin = self._pins.get(purpose)
            candidates = [m for m in resolve_candidates(
                              self.inventory(), request, self.cfg.host_order,
                              pin=pin, allow_cloud=self.cfg.allow_cloud)
                          if (m.host, m.digest) not in excluded and self._available(m.host)]
            if not candidates:
                if not refreshed:
                    refreshed = True
                    self.inventory(refresh=True)
                    continue
                raise self._exhausted(request, purpose, pin, trail)

            model = candidates[0]
            outcome = self._attempt(model, path, payload(model), parse)
            attempts += outcome.attempts
            where = f"{host_name(model.host)}/{model.name}"

            if outcome.outcome == "ok":
                if pin is None:
                    self._pins[purpose] = ModelPin(model.name, model.digest)
                    log.info("%s pinned to %s@%s", purpose, model.name, model.digest[:12])
                fell_back = bool(request.model) and self._pins[purpose].digest != (
                    self.find_digest(request.model) if request.model else None)
                content, parsed = outcome.parsed
                if trail:
                    log.warning("%s served by %s after: %s", purpose, where, "; ".join(trail))
                return LLMResult(ok=True, purpose=purpose, model=model.name,
                                 digest=model.digest, host=model.host, content=content,
                                 parsed=parsed, fell_back=fell_back, attempts=attempts,
                                 elapsed_s=self._clock() - started, trail=trail)

            trail.append(f"{where}: {outcome.outcome} ({outcome.error})")
            if outcome.outcome in ("auth", "transport", "exhausted"):
                if outcome.outcome == "auth":
                    self._mark_auth_failed(model.host, outcome.error or "")
                else:
                    self._mark_unhealthy(model.host, outcome.error or "")
                continue
            if outcome.outcome == "missing":
                excluded.add((model.host, model.digest))
                self._inventory[model.host] = [m for m in self._inventory.get(model.host, [])
                                               if m.digest != model.digest]
                continue
            # permanent / invalid / protocol: a property of this request, not
            # of the host — returned as data rather than retried elsewhere.
            return LLMResult(ok=False, purpose=purpose, model=model.name,
                             digest=model.digest, host=model.host,
                             error=outcome.error, error_kind=outcome.outcome,
                             attempts=attempts, elapsed_s=self._clock() - started,
                             trail=trail)

    def _exhausted(self, request: ModelRequest, purpose: str, pin: ModelPin | None,
                   trail: list[str]) -> OpenWebUIError:
        why = ("; tried: " + "; ".join(trail)) if trail else ""
        if not any(self._available(b) for b in self.cfg.host_order):
            return NoHealthyHostError("no Open WebUI host is reachable: " + "; ".join(
                f"{host_name(b)}: {s.last_error}" for b, s in self._state.items()) + why)
        if pin is not None:
            return ModelUnavailableError(
                f"{purpose!r} is pinned to {pin.name}@{pin.digest[:12]} for this run and "
                f"no reachable host serves it; refusing to switch models mid-run{why}")
        return ModelUnavailableError(
            f"no reachable host serves an acceptable model for {request}{why}")

    def _log_retry(self, model: ModelInfo, attempt: int, reason: str) -> None:
        # A read timeout is 10 minutes by default; without this line a stuck
        # first attempt looks like the client hanging.
        log.warning("[%d/%d] %s/%s: %s", attempt, self.cfg.max_attempts,
                    host_name(model.host), model.name, reason)

    def _backoff(self, attempt: int) -> None:
        cap = min(self.cfg.backoff_max_s, self.cfg.backoff_base_s * (2 ** (attempt - 1)))
        self._sleep(cap * self._jitter())

    def _attempt(self, model: ModelInfo, path: str, body: dict,
                 parse: Callable[[Any], tuple[Any, Any]]) -> _Attempt:
        """Up to max_attempts on ONE host for ONE model."""
        last = "unknown"
        for attempt in range(1, self.cfg.max_attempts + 1):
            try:
                resp = self.session.post(model.host + path, json=body,
                                         headers=self._headers(model.host),
                                         timeout=self._timeout)
            except requests.RequestException as exc:
                if _never_connected(exc):
                    # DNS failure, refused, connect timeout: the host is down.
                    return _Attempt("transport", attempt, error=_describe(exc))
                # Read timeouts and connections dropped mid-request. requests
                # reports the latter as ConnectionError too, and treating it
                # as "host down" gave up on the only embedding host after one
                # reset during a cold model load (seen live, 2026-09-17).
                last = _describe(exc)
                self._log_retry(model, attempt, last)
                if attempt < self.cfg.max_attempts:
                    self._backoff(attempt)
                continue

            status = resp.status_code
            if status == 200:
                try:
                    data = resp.json()
                except ValueError:
                    last = "200 with a malformed JSON body"
                    self._log_retry(model, attempt, last)
                    if attempt < self.cfg.max_attempts:
                        self._backoff(attempt)
                    continue
                try:
                    return _Attempt("ok", attempt, body=data, parsed=parse(data))
                except _InvalidOutput as exc:
                    last = f"output rejected: {exc}"
                    self._log_retry(model, attempt, last)
                    if attempt < self.cfg.max_attempts:
                        self._backoff(attempt)
                    continue
                except _ProtocolError as exc:
                    return _Attempt("protocol", attempt, error=str(exc))

            detail = _detail(resp)
            if status == 404 or (status == 403 and "model not found" in detail.lower()):
                return _Attempt("missing", attempt, error=f"{status} {detail}")
            if status in _AUTH_STATUSES:
                return _Attempt("auth", attempt, error=f"{status} {detail}")
            if status in _RETRYABLE_STATUSES or status >= 500:
                last = f"{status} {detail}"
                self._log_retry(model, attempt, last)
                if attempt < self.cfg.max_attempts:
                    self._backoff(attempt)
                continue
            return _Attempt("permanent", attempt, error=f"{status} {detail}")

        kind = "invalid" if last.startswith("output rejected") else "exhausted"
        return _Attempt(kind, self.cfg.max_attempts, error=last)


_CONNECT_FAILURE_MARKERS = ("NewConnectionError", "NameResolutionError",
                            "Failed to establish a new connection",
                            "Name or service not known", "nodename nor servname",
                            "Connection refused", "No route to host")


def _never_connected(exc: requests.RequestException) -> bool:
    """True when the request never reached the server."""
    if isinstance(exc, requests.ConnectTimeout):
        return True
    if not isinstance(exc, requests.ConnectionError):
        return False
    text = repr(exc.args) + repr(getattr(exc, "__context__", None))
    return any(marker in text for marker in _CONNECT_FAILURE_MARKERS)


def _describe(exc: BaseException) -> str:
    """Exception class plus a short message — URLs only, never headers."""
    return f"{exc.__class__.__name__}: {str(exc)[:160]}"


def _detail(resp: requests.Response) -> str:
    """Open WebUI errors are {"detail": ...}, Ollama's {"error": ...}."""
    try:
        data = resp.json()
        if isinstance(data, dict):
            return str(data.get("detail") or data.get("error") or data)[:300]
    except ValueError:
        pass
    return (resp.text or "")[:300]


# ---------------------------------------------------------------------------
# Thin CLI
# ---------------------------------------------------------------------------

def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inspect the Open WebUI hosts and model selection.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    probe = sub.add_parser("probe", help="host health, inventories, and "
                                         "(optionally) the candidate order for a request")
    probe.add_argument("--model")
    probe.add_argument("--tier", choices=TIERS)
    probe.add_argument("--specialization",
                       choices=SPECIALIZATIONS + (ANY_SPECIALIZATION,))
    probe.add_argument("--kind", choices=KINDS, default="generation")
    probe.add_argument("--capability", action="append", default=[])
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    cfg = LLMConfig.from_env()
    client = OpenWebUIClient(cfg)
    try:
        inventory = client.inventory()
    except OpenWebUIAuthError as exc:
        print(f"AUTH: {exc}")
        return 1

    for name, st in client.host_status().items():
        state = ("AUTH FAILED" if st["auth_failed"] else
                 "DOWN" if st["cooling_down_s"] else "ok")
        print(f"{name:14s} {state:11s} {st['models']:3d} models  {st['url']}"
              + (f"  ({st['last_error']})" if st["last_error"] else ""))
    print()
    print(f"{'host':12s} {'model':30s} {'size':>8s} tier {'specialization':14s} "
          f"{'digest':12s} capabilities")
    for base in cfg.host_order:
        for m in sorted(inventory.get(base, ()), key=lambda m: m.name):
            print(f"{host_name(base):12s} {m.name:30s} {m.parameter_size or '-':>8s} "
                  f"{m.tier or '-':4s} {m.specialization:14s} {m.digest[:12]:12s} "
                  f"{','.join(sorted(m.capabilities))}{'  [cloud]' if m.is_cloud else ''}")

    if args.model or args.tier:
        request = ModelRequest(model=args.model, tier=args.tier,
                               specialization=args.specialization, kind=args.kind,
                               capabilities=frozenset(args.capability))
        print(f"\ncandidate order for {request}:")
        for i, m in enumerate(resolve_candidates(inventory, request, cfg.host_order,
                                                 allow_cloud=cfg.allow_cloud), 1):
            print(f"  {i:2d}. {host_name(m.host):12s} {m.name:30s} "
                  f"{m.parameter_size or '-':>8s} {m.specialization}")
    return 0 if inventory else 1


if __name__ == "__main__":
    sys.exit(_cli())
