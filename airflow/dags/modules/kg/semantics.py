"""
kg/semantics.py — approach 1: definition-embedding analysis of entry types
=================================================================================
Three subcommands forming a pipeline (each stage caches, so all are resumable
and rerunnable without recomputation):

  describe   entry-type slugs -> LLM-written dictionary definitions
  embed      definitions -> unit vectors
  analyze    vectors + co-occurrence census -> report:
             nearest neighbors, clusters, and the two disagreement lists
             (substitution candidates / complementary facets)

No Mongo, no Airflow. Input is the census JSON; everything else is files.
Every model call goes through modules/openwebui_client.py, which owns host
failover (ollama-ccdd first, then ollama-ui), same-tier model fallback, and
per-host API keys — see that module's docstring for the rules and the env.

    python -m modules.kg.semantics describe \
        --census kg_census_entry_type.json --out kg_type_definitions.json
    python -m modules.kg.semantics embed \
        --definitions kg_type_definitions.json --out kg_type_embeddings.json
    python -m modules.kg.semantics analyze \
        --embeddings kg_type_embeddings.json --census kg_census_entry_type.json \
        --out kg_type_semantics_report.json

Which model (env; all optional):
    KG_CHAT_MODEL  / KG_CHAT_TIER  / KG_CHAT_SPECIALIZATION    default mistral-small3.2:24b
    KG_EMBED_MODEL / KG_EMBED_TIER                             default qwen3-embedding:0.6b

One model per artifact
----------------------
A definitions file or an embeddings file is only meaningful if ONE model
produced all of it: 119 definitions from two different LLMs are not one
dataset, and cosine similarity between vectors from two embedding models is
noise. So:

  * each artifact records the model's name, DIGEST (the weights hash) and,
    for embeddings, the vector dimension;
  * a resumed run pins its client to that digest before the first call, so
    host failover is allowed but model fallback is not — if the recorded
    model is gone, the run stops with an explanation instead of mixing;
  * ``--force`` is the one way to start over, and a forced run may fall back
    to another model of the same tier, which the new artifact then records.

Files written by the previous version of this module (no digest) are read.
Their model is identified by NAME, resolved to the digest that name has on
the hosts today — the best evidence available — and the log says so.

What the previous version got wrong, fixed here
-----------------------------------------------
  * ``manual_overrides`` — the 9 hand-written definitions — were dropped from
    the file on every describe write, and ``--force`` regenerated them.
    They are now preserved and never regenerated.
  * a file written under an older PROMPT_VERSION was silently extended and
    then relabelled with the new version on the next write. Now a stale file
    is reported, adding to it is refused, and ``--force`` regenerates it
    (manual overrides excepted). This is not hypothetical:
    data/kg_type_definitions.json is prompt v1 while the prompt is v2.
  * unparseable model output was retried with no pause, burning the whole
    attempt budget in milliseconds against a model returning prose.
  * embeddings were zipped blindly against the response and normalised
    without a zero-norm guard; the whole file was written only at the end.
  * a census slug with no vector raised KeyError deep inside analyze; it is
    now reported.

`analyze` needs numpy + scipy (requirements.txt).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from modules.openwebui_client import (
    EmbeddingMixError,
    LLMConfig,
    ModelRequest,
    ModelUnavailableError,
    OpenWebUIClient,
)

log = logging.getLogger("kg.semantics")


class PromptVersionMismatch(RuntimeError):
    """The definitions file was written with a different prompt version."""

PROMPT_VERSION = "2"
DEFINITIONS_FORMAT = 2
EMBEDDINGS_FORMAT = 2

DEFAULT_CHAT_MODEL = "mistral-small3.2:24b"
DEFAULT_EMBED_MODEL = "qwen3-embedding:0.6b"

# Pin names on the client: one model per purpose per run.
DESCRIBE_PURPOSE = "kg.semantics.describe"
EMBED_PURPOSE = "kg.semantics.embed"

MIN_WORDS, MAX_WORDS = 15, 70
EMBED_BATCH = 32

SYSTEM_PROMPT = (
    "You are helping build a knowledge graph of internet memes based on "
    "knowyourmeme.com (KYM). KYM assigns entries controlled 'entry type' labels. "
    "You write short dictionary-style definitions of these labels AS USED ON KYM, "
    "not their general English meaning. Example: on KYM, 'exploitable' means a "
    "base image or template deliberately edited and re-captioned by many users, "
    "NOT a security vulnerability. Do not define a word by repeating its own "
    "words. Do NOT use generic filler like 'gained significant popularity/"
    "recognition within internet culture' or 'often spawning memes and "
    "trends' — nearly every KYM entry could truthfully claim that, so it "
    "carries no distinguishing information and different entries end up with "
    "near-identical definitions. Instead, name the SPECIFIC mechanism in the "
    "first clause: is it a media format (song/video/image), a real-world "
    "event category, a person's occupation, a rhetorical/textual device, a "
    "platform or tool, or something else? Lead with that distinction. "
    "Respond ONLY with JSON: {\"definition\": \"...\"}"
)

USER_TMPL = (
    "Define the KYM entry type \"{slug}\" in 25-50 words, dictionary style, "
    "describing what kind of meme entry receives this label on knowyourmeme.com."
)


# ---------------------------------------------------------------- files ----

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(path: str, obj: Any, **kw: Any) -> None:
    """Temp file then rename: a crash mid-write leaves the previous file."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, **kw)
    os.replace(tmp, path)


def _text_sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_definitions(path: str) -> dict[str, Any]:
    """Read a definitions file in either format, normalised to format 2."""
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    return {
        "format": DEFINITIONS_FORMAT,
        "prompt_version": str(raw.get("prompt_version") or ""),
        "model": raw.get("model"),
        "digest": raw.get("digest"),             # None in format-1 files
        "hosts": list(raw.get("hosts") or []),
        "produced_at": raw.get("produced_at"),
        "definitions": dict(raw.get("definitions") or {}),
        "manual_overrides": sorted(set(raw.get("manual_overrides") or [])),
    }


def load_embeddings(path: str) -> dict[str, Any]:
    """Read an embeddings file, refusing one whose vectors disagree with
    each other or with the dimension it declares."""
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    vectors = raw.get("vectors") or {}
    dims = {len(v) for v in vectors.values()}
    declared = raw.get("dim")
    if len(dims) > 1 or (declared is not None and dims and dims != {declared}):
        raise EmbeddingMixError(
            f"{path}: vectors of dimension {sorted(dims)} but the file declares "
            f"{declared!r} — it mixes models and cannot be analysed")
    return {
        "format": EMBEDDINGS_FORMAT,
        "model": raw.get("model"),
        "digest": raw.get("digest"),
        "dim": declared if declared is not None else (dims.pop() if dims else None),
        "normalized": raw.get("normalized", True),   # format 1 always normalised
        "prompt_version": raw.get("prompt_version"),
        "hosts": list(raw.get("hosts") or []),
        "produced_at": raw.get("produced_at"),
        "text_sha256": dict(raw.get("text_sha256") or {}),
        "vectors": vectors,
    }


# --------------------------------------------------------------- pinning ----

def _pin_to_artifact(client: OpenWebUIClient, purpose: str, doc: dict[str, Any],
                     what: str, dim: int | None = None) -> None:
    """Bind ``purpose`` to the model that produced ``doc`` before any call."""
    digest = doc.get("digest")
    if not digest:
        digest = client.find_digest(doc["model"])
        if digest is None:
            raise ModelUnavailableError(
                f"the existing {what} were produced by {doc['model']!r}, which no "
                f"reachable host serves. Resuming with another model would mix "
                f"two models in one file: re-run later, or pass --force to "
                f"regenerate everything with what is available.")
        log.warning("%s predate digest tracking; assuming %s is still the weights "
                    "now served as %s", what, doc["model"], digest[:12])
        doc["digest"] = digest
    client.pin(purpose, doc["model"], digest, dim=dim)


def _note_host(doc: dict[str, Any], host: str | None) -> None:
    if host and host not in doc["hosts"]:
        doc["hosts"].append(host)


# ---------------------------------------------------------------- describe ----

def parse_definition(content: str) -> str:
    """The model's JSON reply -> the definition, or ValueError."""
    definition = json.loads(content)["definition"]
    if not isinstance(definition, str):
        raise ValueError("definition is not a string")
    definition = definition.strip()
    words = len(definition.split())
    if not MIN_WORDS <= words <= MAX_WORDS:      # soft bounds; only wild output fails
        raise ValueError(f"{words} words, outside {MIN_WORDS}-{MAX_WORDS}")
    return definition


def describe(client: OpenWebUIClient, slugs: Iterable[str], out_path: str,
             request: ModelRequest, *, force: bool = False,
             progress: Callable[[str], None] = print) -> dict[str, Any]:
    slugs = list(slugs)
    doc = (load_definitions(out_path) if os.path.exists(out_path) else
           {"format": DEFINITIONS_FORMAT, "prompt_version": PROMPT_VERSION,
            "model": None, "digest": None, "hosts": [], "produced_at": None,
            "definitions": {}, "manual_overrides": []})
    manual = set(doc["manual_overrides"])
    defs = doc["definitions"]

    generated = {s: d for s, d in defs.items() if s not in manual}
    stale = bool(generated) and doc["prompt_version"] != PROMPT_VERSION
    if force:
        if generated:
            progress(f"--force: regenerating {len(generated)} definitions; "
                     f"{len(manual)} manual overrides kept")
        for slug in generated:
            del defs[slug]
        doc.update(model=None, digest=None, hosts=[], prompt_version=PROMPT_VERSION)
        generated, stale = {}, False
    todo = [s for s in slugs if s not in manual and s not in defs]
    if stale:
        # Refused, not auto-regenerated: regenerating is ~20 s of shared lab
        # GPU per definition and changes every downstream embedding and
        # report, which is the operator's call to make — and finishing the
        # file under the new prompt would mix two prompts in one dataset.
        message = (f"{out_path}: {len(generated)} definitions were written with "
                   f"prompt v{doc['prompt_version'] or '?'}, the current prompt is "
                   f"v{PROMPT_VERSION}. Pass --force to regenerate them "
                   f"({len(manual)} manual overrides are kept).")
        if todo:
            raise PromptVersionMismatch(message + f" Refusing to add {len(todo)} "
                                        f"v{PROMPT_VERSION} definitions to that file.")
        progress("WARNING: " + message)
    if todo and generated:
        # Only when there is work: a finished file needs no model server.
        _pin_to_artifact(client, DESCRIBE_PURPOSE, doc, "definitions")
        progress(f"Resuming: {len(generated)} cached, {len(todo)} to go, "
                 f"pinned to {doc['model']}")
    failed: list[dict[str, Any]] = []
    for i, slug in enumerate(todo, 1):
        result = client.chat(
            [{"role": "system", "content": SYSTEM_PROMPT},
             {"role": "user", "content": USER_TMPL.format(slug=slug)}],
            request, purpose=DESCRIBE_PURPOSE, format="json",
            options={"temperature": 0.2}, validate=parse_definition)
        if not result.ok:
            failed.append({"slug": slug, "error_kind": result.error_kind,
                           "error": result.error})
            progress(f"  !! {slug}: {result.error_kind}: {result.error}")
            continue
        defs[slug] = result.parsed
        doc["model"], doc["digest"] = doc["model"] or result.model, result.digest
        _note_host(doc, result.host)
        doc["produced_at"] = _now()
        _write_json(out_path, doc, indent=2)     # after EVERY slug -> resumable
        progress(f"  [{i}/{len(todo)}] {slug}: {result.parsed[:60]}...")

    if not todo and force:
        _write_json(out_path, doc, indent=2)
    summary = {"slugs": len(slugs), "generated": len(todo) - len(failed),
               "prompt_version": doc["prompt_version"], "prompt_is_current": not stale,
               "manual_overrides": len(manual), "failed": failed,
               "missing": sorted(set(slugs) - set(defs)),
               "model": doc["model"], "digest": doc["digest"]}
    progress(f"Done: {len(set(slugs) & set(defs))}/{len(slugs)} definitions in "
             f"{out_path} ({doc['model']})")
    return summary


# ------------------------------------------------------------------- embed ----

def embed(client: OpenWebUIClient, definitions_path: str, out_path: str,
          request: ModelRequest, *, force: bool = False, batch_size: int = EMBED_BATCH,
          progress: Callable[[str], None] = print) -> dict[str, Any]:
    defs_doc = load_definitions(definitions_path)
    texts = {s: f"{s}: {d}" for s, d in sorted(defs_doc["definitions"].items())}
    shas = {s: _text_sha(t) for s, t in texts.items()}

    doc: dict[str, Any] = {
        "format": EMBEDDINGS_FORMAT, "model": None, "digest": None, "dim": None,
        "normalized": True, "prompt_version": defs_doc["prompt_version"],
        "definitions_model": defs_doc["model"], "hosts": [], "produced_at": None,
        "text_sha256": {}, "vectors": {}}
    dropped: list[str] = []
    if os.path.exists(out_path) and not force:
        old = load_embeddings(out_path)
        dropped = sorted(set(old["vectors"]) - set(texts))   # definition removed since
        # A vector is reusable only if we can prove it embeds today's text.
        # Format-1 files carry no text hashes, so nothing in them qualifies.
        keep = {s for s, v in old["vectors"].items()
                if s in shas and old["text_sha256"].get(s) == shas[s]}
        if keep:
            doc.update({k: old[k] for k in ("model", "digest", "dim", "hosts")})
            doc["vectors"] = {s: old["vectors"][s] for s in keep}
            doc["text_sha256"] = {s: shas[s] for s in keep}
            progress(f"Resuming: {len(keep)} vectors reusable ({doc['model']})")
        elif old["vectors"]:
            progress(f"Re-embedding all: none of the {len(old['vectors'])} existing "
                     f"vectors can be shown to match the current definitions")

    todo = [s for s in texts if s not in doc["vectors"]]
    if todo and doc["vectors"]:
        _pin_to_artifact(client, EMBED_PURPOSE, doc, "embeddings", dim=doc["dim"])
    for start in range(0, len(todo), batch_size):
        batch = todo[start:start + batch_size]
        result = client.embed([texts[s] for s in batch], request,
                              purpose=EMBED_PURPOSE, normalize=True)
        if not result.ok:
            raise RuntimeError(f"embedding batch at {start} failed "
                               f"({result.error_kind}): {result.error}. "
                               f"{len(doc['vectors'])} vectors are saved; re-run to resume.")
        for slug, vec in zip(batch, result.vectors or []):   # lengths checked by the client
            doc["vectors"][slug] = vec
            doc["text_sha256"][slug] = shas[slug]
        doc["model"] = doc["model"] or result.model
        doc["digest"], doc["dim"] = result.digest, result.dim
        _note_host(doc, result.host)
        doc["produced_at"] = _now()
        _write_json(out_path, doc)                     # after EVERY batch
        progress(f"  embedded {len(doc['vectors'])}/{len(texts)}")

    if dropped or not todo:
        _write_json(out_path, doc)
    progress(f"Done: {len(doc['vectors'])} vectors ({doc['dim']} dims, "
             f"{doc['model']}) in {out_path}")
    return {"vectors": len(doc["vectors"]), "embedded": len(todo), "dropped": dropped,
            "model": doc["model"], "digest": doc["digest"], "dim": doc["dim"]}


# ----------------------------------------------------------------- analyze ----

def analyze(embeddings_path: str, census_path: str, out_path: str, *,
            top_k: int = 5, coarse_k: int = 8, fine_k: int = 25,
            sim_threshold: float = 0.6,
            progress: Callable[[str], None] = print) -> dict[str, Any]:
    import numpy as np
    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import squareform

    from modules.kg.census import load_census

    emb = load_embeddings(embeddings_path)
    census = load_census(census_path)
    counts: dict[str, int] = census.get("value_counts") or {}
    n_entries = census.get("entries_with_value") or 0
    vec_map = emb["vectors"]

    slugs = sorted(vec_map)
    coverage = {"without_vector": sorted(set(counts) - set(vec_map)),
                "without_census_count": sorted(set(vec_map) - set(counts))}
    if coverage["without_vector"] or coverage["without_census_count"]:
        progress(f"Coverage gaps: {len(coverage['without_vector'])} census types have "
                 f"no vector, {len(coverage['without_census_count'])} vectors have no "
                 f"census count (they get neighbours and clusters, not co-occurrence)")

    idx = {s: i for i, s in enumerate(slugs)}
    M = np.array([vec_map[s] for s in slugs])
    sim = M @ M.T                                    # unit vectors: cosine == dot

    cooc = {tuple(sorted((p["a"], p["b"]))): p["count"]
            for p in census.get("pair_cooccurrence") or []}

    # -- nearest neighbors per type ------------------------------------------
    neighbors = {}
    for s in slugs:
        order = np.argsort(-sim[idx[s]])
        neighbors[s] = [{"type": slugs[j], "cos": round(float(sim[idx[s], j]), 3)}
                        for j in order[1:top_k + 1]]

    # -- hierarchical clustering at two granularities ------------------------
    dist = squareform(np.clip(1.0 - sim, 0.0, None), checks=False)
    Z = linkage(dist, method="average")
    clusters = {}
    for label, k in (("coarse", coarse_k), ("fine", fine_k)):
        assignment = fcluster(Z, t=k, criterion="maxclust")
        groups: dict[int, list[str]] = {}
        for s, c in zip(slugs, assignment):
            groups.setdefault(int(c), []).append(s)
        clusters[label] = sorted(groups.values(), key=len, reverse=True)

    # -- disagreement quadrants ----------------------------------------------
    substitution, complementary = [], []
    for i in range(len(slugs)):
        for j in range(i + 1, len(slugs)):
            a, b = slugs[i], slugs[j]
            ca, cb = counts.get(a, 0), counts.get(b, 0)
            if not (ca and cb and n_entries):
                continue
            s_ij = float(sim[i, j])
            observed = cooc.get((a, b), 0)
            expected = ca * cb / n_entries
            if s_ij >= sim_threshold and expected >= 3 and observed < expected / 3:
                substitution.append({"a": a, "b": b, "cos": round(s_ij, 3),
                                     "observed": observed,
                                     "expected": round(expected, 1)})
            if observed:
                pmi = float(np.log2(observed * n_entries / (ca * cb)))
                if pmi >= 3.0 and s_ij < 0.45:
                    complementary.append({"a": a, "b": b, "cos": round(s_ij, 3),
                                          "cooccur": observed,
                                          "pmi_bits": round(pmi, 2)})

    substitution.sort(key=lambda r: -r["cos"])
    complementary.sort(key=lambda r: -r["pmi_bits"])

    report = {"neighbors": neighbors, "clusters": clusters,
              "substitution_candidates": substitution,
              "complementary_pairs": complementary,
              "coverage": coverage,
              "embeddings": {k: emb[k] for k in ("model", "digest", "dim",
                                                 "prompt_version", "produced_at")},
              "params": {"sim_threshold": sim_threshold,
                         "coarse_k": coarse_k, "fine_k": fine_k}}
    _write_json(out_path, report, indent=2)

    progress("\n=== substitution candidates (similar meaning, avoid co-occurring) ===")
    for r in substitution[:15]:
        progress(f"  {r['a']:24s} ~ {r['b']:24s} cos={r['cos']}  "
                 f"observed={r['observed']} expected={r['expected']}")
    progress("\n=== complementary pairs (co-used, different meaning) ===")
    for r in complementary[:15]:
        progress(f"  {r['a']:24s} + {r['b']:24s} cos={r['cos']}  pmi={r['pmi_bits']}")
    progress(f"\nFull report: {out_path}")
    return report


# ---------------------------------------------------------------------- CLI ----

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("describe")
    d.add_argument("--census", required=True)
    d.add_argument("--out", required=True)
    d.add_argument("--force", action="store_true",
                   help="Regenerate every generated definition (manual overrides are kept)")

    e = sub.add_parser("embed")
    e.add_argument("--definitions", required=True)
    e.add_argument("--out", required=True)
    e.add_argument("--force", action="store_true",
                   help="Re-embed everything, even vectors that could be reused")
    e.add_argument("--batch-size", type=int, default=EMBED_BATCH,
                   help="Texts per request. Smaller is steadier on a busy host: "
                        "Open WebUI cuts any request at 50 s, and the file is "
                        "saved after every batch, so a failure costs one batch.")

    a = sub.add_parser("analyze")
    a.add_argument("--embeddings", required=True)
    a.add_argument("--census", required=True)
    a.add_argument("--out", required=True)
    a.add_argument("--top-k", type=int, default=5)
    a.add_argument("--coarse-k", type=int, default=8)
    a.add_argument("--fine-k", type=int, default=25)
    a.add_argument("--sim-threshold", type=float, default=0.6)

    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    if args.cmd == "analyze":
        analyze(args.embeddings, args.census, args.out, top_k=args.top_k,
                coarse_k=args.coarse_k, fine_k=args.fine_k,
                sim_threshold=args.sim_threshold)
        return 0

    # Configuration is read here, where it is used — never at import time.
    client = OpenWebUIClient(LLMConfig.from_env())
    if args.cmd == "describe":
        from modules.kg.census import load_census
        slugs = list(load_census(args.census).get("value_counts") or {})
        request = ModelRequest.from_env("KG_CHAT", default_model=DEFAULT_CHAT_MODEL)
        summary = describe(client, slugs, args.out, request, force=args.force)
        return 1 if summary["failed"] or summary["missing"] else 0

    request = ModelRequest.from_env("KG_EMBED", kind="embedding",
                                    default_model=DEFAULT_EMBED_MODEL)
    embed(client, args.definitions, args.out, request, force=args.force,
          batch_size=args.batch_size)
    return 0


if __name__ == "__main__":
    sys.exit(main())
