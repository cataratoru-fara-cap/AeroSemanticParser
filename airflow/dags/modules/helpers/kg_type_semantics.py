"""
kg_type_semantics.py — approach 1: definition-embedding analysis of entry types
=================================================================================
Three subcommands forming a pipeline (each stage caches, so all are resumable
and rerunnable without recomputation):

  describe   119 slugs -> LLM-written dictionary definitions   (mistral on pagoda)
  embed      definitions -> unit vectors                        (qwen3-embedding)
  analyze    vectors + co-occurrence census -> report:
             nearest neighbors, clusters, and the two disagreement lists
             (substitution candidates / complementary facets)

No Mongo, no Airflow. Input is the census JSON; everything else is files.

    export OPENWEBUI_API_KEY=sk-...   # from Open WebUI Settings -> Account -> API keys

    python -m modules.helpers.kg_type_semantics describe \
        --census kg_census_entry_type.json --out kg_type_definitions.json
    python -m modules.helpers.kg_type_semantics embed \
        --definitions kg_type_definitions.json --out kg_type_embeddings.json
    python -m modules.helpers.kg_type_semantics analyze \
        --embeddings kg_type_embeddings.json --census kg_census_entry_type.json \
        --out kg_type_semantics_report.json

Config (env):
    OPENWEBUI_BASE_URL  default https://pagoda.liris.cnrs.fr (no port — this
                        is Open WebUI in front of Ollama, not raw Ollama)
    OPENWEBUI_API_KEY   REQUIRED. Generate one in Open WebUI:
                        Settings -> Account -> API keys -> Create new key.
    KG_CHAT_MODEL       default mistral-small3.2:24b
    KG_EMBED_MODEL      default qwen3-embedding:0.6b

Endpoints used (from the instance's own OpenAPI spec):
    POST /ollama/api/chat   -- forwards as-is to the Ollama backend
    POST /ollama/api/embed  -- forwards as-is to the Ollama backend
Both require `Authorization: Bearer <OPENWEBUI_API_KEY>`.

`analyze` needs numpy + scipy:  pip install numpy scipy
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

BASE_URL = os.getenv("OPENWEBUI_BASE_URL", "https://pagoda.liris.cnrs.fr").rstrip("/")
API_KEY = os.getenv("OPENWEBUI_API_KEY")
CHAT_MODEL = os.getenv("KG_CHAT_MODEL", "mistral-small3.2:24b")
EMBED_MODEL = os.getenv("KG_EMBED_MODEL", "qwen3-embedding:0.6b")
PROMPT_VERSION = "1"

if not API_KEY:
    raise SystemExit(
        "OPENWEBUI_API_KEY is not set. Generate one in Open WebUI: "
        "Settings -> Account -> API keys -> Create new key, then "
        "export OPENWEBUI_API_KEY=... (or pass -e to docker compose exec)."
    )

SYSTEM_PROMPT = (
    "You are helping build a knowledge graph of internet memes based on "
    "knowyourmeme.com (KYM). KYM assigns entries controlled 'entry type' labels. "
    "You write short dictionary-style definitions of these labels AS USED ON KYM, "
    "not their general English meaning. Example: on KYM, 'exploitable' means a "
    "base image or template deliberately edited and re-captioned by many users, "
    "NOT a security vulnerability. Do not define a word by repeating its own "
    "words. Respond ONLY with JSON: {\"definition\": \"...\"}"
)

USER_TMPL = (
    "Define the KYM entry type \"{slug}\" in 25-50 words, dictionary style, "
    "describing what kind of meme entry receives this label on knowyourmeme.com."
)


def _post(path: str, payload: dict) -> dict:
    req = urllib.request.Request(
        BASE_URL + path,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"{e.code} from {path}: {body[:500]}") from None


# ---------------------------------------------------------------- describe ----

def cmd_describe(args):
    with open(args.census) as f:
        slugs = list(json.load(f)["type_counts"].keys())

    cache: dict = {}
    if os.path.exists(args.out):
        with open(args.out) as f:
            cache = json.load(f).get("definitions", {})
        print(f"Resuming: {len(cache)} definitions already cached")

    for i, slug in enumerate(slugs):
        if slug in cache and not args.force:
            continue
        definition = None
        for attempt in range(2):
            r = _post("/ollama/api/chat", {
                "model": CHAT_MODEL,
                "stream": False,
                "format": "json",
                "options": {"temperature": 0.2},
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": USER_TMPL.format(slug=slug)},
                ],
            })
            try:
                candidate = json.loads(r["message"]["content"])["definition"].strip()
            except (KeyError, json.JSONDecodeError):
                continue
            n_words = len(candidate.split())
            if 15 <= n_words <= 70:      # soft bounds; hard-fail only wild output
                definition = candidate
                break
        if definition is None:
            print(f"  !! {slug}: no usable definition after 2 attempts, skipping")
            continue
        cache[slug] = definition
        with open(args.out, "w") as f:   # write after EVERY slug -> resumable
            json.dump({"model": CHAT_MODEL, "prompt_version": PROMPT_VERSION,
                       "definitions": cache}, f, indent=2)
        print(f"  [{i + 1}/{len(slugs)}] {slug}: {definition[:60]}...")

    print(f"Done: {len(cache)}/{len(slugs)} definitions in {args.out}")


# ------------------------------------------------------------------- embed ----

def cmd_embed(args):
    with open(args.definitions) as f:
        defs = json.load(f)["definitions"]

    slugs = sorted(defs)
    texts = [f"{s}: {defs[s]}" for s in slugs]

    vectors: dict[str, list[float]] = {}
    for start in range(0, len(texts), 32):
        batch_slugs = slugs[start:start + 32]
        r = _post("/ollama/api/embed", {"model": EMBED_MODEL,
                                  "input": texts[start:start + 32]})
        for s, vec in zip(batch_slugs, r["embeddings"]):
            norm = sum(x * x for x in vec) ** 0.5
            vectors[s] = [x / norm for x in vec]   # unit length: cosine == dot
        print(f"  embedded {min(start + 32, len(texts))}/{len(texts)}")

    with open(args.out, "w") as f:
        json.dump({"model": EMBED_MODEL, "vectors": vectors}, f)
    print(f"Done: {len(vectors)} vectors ({len(next(iter(vectors.values())))} dims) in {args.out}")


# ----------------------------------------------------------------- analyze ----

def cmd_analyze(args):
    import numpy as np
    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import squareform

    with open(args.embeddings) as f:
        vec_map = json.load(f)["vectors"]
    with open(args.census) as f:
        census = json.load(f)

    slugs = sorted(vec_map)
    idx = {s: i for i, s in enumerate(slugs)}
    M = np.array([vec_map[s] for s in slugs])
    sim = M @ M.T                                    # cosine similarity matrix

    counts = census["type_counts"]
    n_entries = census["entries_with_entry_type"]
    cooc = {}
    for p in census["pair_cooccurrence"]:
        cooc[tuple(sorted((p["a"], p["b"])))] = p["count"]

    # -- nearest neighbors per type ------------------------------------------
    neighbors = {}
    for s in slugs:
        order = np.argsort(-sim[idx[s]])
        neighbors[s] = [{"type": slugs[j], "cos": round(float(sim[idx[s], j]), 3)}
                        for j in order[1:args.top_k + 1]]

    # -- hierarchical clustering at two granularities ------------------------
    dist = squareform(1.0 - sim, checks=False)
    Z = linkage(dist, method="average")
    clusters = {}
    for label, k in (("coarse", args.coarse_k), ("fine", args.fine_k)):
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
            s_ij = float(sim[i, j])
            observed = cooc.get((a, b), 0)
            expected = counts[a] * counts[b] / n_entries
            if s_ij >= args.sim_threshold and expected >= 3 and observed < expected / 3:
                substitution.append({"a": a, "b": b, "cos": round(s_ij, 3),
                                      "observed": observed,
                                      "expected": round(expected, 1)})
            pmi = (np.log2(observed * n_entries / (counts[a] * counts[b]))
                   if observed else None)
            if pmi is not None and pmi >= 3.0 and s_ij < 0.45:
                complementary.append({"a": a, "b": b, "cos": round(s_ij, 3),
                                       "cooccur": observed,
                                       "pmi_bits": round(float(pmi), 2)})

    substitution.sort(key=lambda r: -r["cos"])
    complementary.sort(key=lambda r: -r["pmi_bits"])

    report = {"neighbors": neighbors, "clusters": clusters,
              "substitution_candidates": substitution,
              "complementary_pairs": complementary,
              "params": {"sim_threshold": args.sim_threshold,
                          "coarse_k": args.coarse_k, "fine_k": args.fine_k}}
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n=== substitution candidates (similar meaning, avoid co-occurring) ===")
    for r in substitution[:15]:
        print(f"  {r['a']:24s} ~ {r['b']:24s} cos={r['cos']}  "
              f"observed={r['observed']} expected={r['expected']}")
    print(f"\n=== complementary pairs (co-used, different meaning) ===")
    for r in complementary[:15]:
        print(f"  {r['a']:24s} + {r['b']:24s} cos={r['cos']}  pmi={r['pmi_bits']}")
    print(f"\nFull report: {args.out}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("describe")
    d.add_argument("--census", required=True)
    d.add_argument("--out", required=True)
    d.add_argument("--force", action="store_true",
                    help="Regenerate even if cached")
    d.set_defaults(fn=cmd_describe)

    e = sub.add_parser("embed")
    e.add_argument("--definitions", required=True)
    e.add_argument("--out", required=True)
    e.set_defaults(fn=cmd_embed)

    a = sub.add_parser("analyze")
    a.add_argument("--embeddings", required=True)
    a.add_argument("--census", required=True)
    a.add_argument("--out", required=True)
    a.add_argument("--top-k", type=int, default=5)
    a.add_argument("--coarse-k", type=int, default=8)
    a.add_argument("--fine-k", type=int, default=25)
    a.add_argument("--sim-threshold", type=float, default=0.6)
    a.set_defaults(fn=cmd_analyze)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()