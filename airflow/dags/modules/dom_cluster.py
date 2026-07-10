"""
dom_cluster.py — structural clustering of scraped KYM DOMs
===========================================================
Pure feature-extraction + clustering library with a thin CLI, mirroring
the kym_discover / scrapingant_client philosophy: no Airflow imports, no
Mongo imports, no module-level mutable state. The CLI (and later the
DAG) glues this to cluster_store; this file only knows how to turn HTML
into structure tokens and token counts into clusters.

Why cluster DOM structure
-------------------------
1. Layout-variance analytics — how many genuinely different page
   templates does KYM actually serve across the scraped corpus, and how
   are they distributed per namespace?
2. Scraper routing — the goal is a SMALL number of extraction scripts,
   one per DOM cluster, instead of one per page. HDBSCAN additionally
   labels pages that fit no cluster as -1 ("unknown template"): exactly
   the signal that a NEW template has appeared and a new script must be
   generated.

How a page becomes a vector
---------------------------
The extractor throws away all text and content attributes and keeps
only the rendering skeleton. For every element it emits

    unigram   tag.classA.classB            e.g.  section.bodycopy
    bigram    parent-token>child-token     e.g.  div.entry>section.bodycopy

Class lists are sorted and capped, and classes containing digits are
dropped (ad slots, entry ids, build hashes — content noise, not
layout). Two pages built from the same template but hosting different
memes therefore produce near-identical token distributions; an entry
page and a photo gallery do not. Think: clustering houses by their
blueprints, ignoring the furniture.

    counts -> TF-IDF -> L2 -> TruncatedSVD -> HDBSCAN (default) | KMeans

On L2-normalised vectors euclidean distance is a monotone function of
cosine similarity, so clustering happens "by skeleton shape", not by
page size. HDBSCAN picks the number of clusters itself and emits
outliers; KMeans (--algorithm kmeans --k N) is kept for controlled
variance analysis with a fixed cluster count.

Heavy deps (lxml / numpy / scipy / scikit-learn) are imported lazily so
importing this module — e.g. at DAG parse time — stays free.

CLI (local smoke tests against the docker-compose Mongo):

    # 1. tokenise every ok DOM without fresh features (idempotent)
    python -m modules.dom_cluster extract --limit 500 --workers 4

    # 2. cluster all features -> run + labels in Mongo + report on disk
    python -m modules.dom_cluster cluster --report /tmp/dom_clusters.json

    # 3. corpus counters
    python -m modules.dom_cluster stats

Dependencies to add to airflow/requirements.txt:
    lxml  scikit-learn>=1.3   (numpy / scipy / joblib come with sklearn)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any, Iterable, Iterator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dom_cluster")

# Bump whenever tokenisation changes: cluster_store treats every cached
# dom_features row with an older version as stale and re-extracts it.
EXTRACTOR_VERSION = 1


# ---------------------------------------------------------------------------
# Phase 1 — HTML -> structure tokens
# ---------------------------------------------------------------------------

# Tags that carry no layout information (or explode node counts: svg).
_SKIP_TAGS = frozenset({
    "script", "style", "noscript", "template", "iframe",
    "svg", "path", "g", "defs", "use", "symbol",
    "circle", "rect", "line", "polygon", "polyline", "ellipse",
    "br", "wbr",
})

_DIGIT_RE = re.compile(r"\d")


@dataclass(frozen=True)
class ExtractConfig:
    """Bounds for one HTML -> tokens pass (frozen, like CrawlConfig)."""
    max_nodes: int = 30_000        # hard stop for pathological pages
    max_depth: int = 60            # ignore children below this depth
    max_classes_per_node: int = 3  # longest class list kept in a token


DEFAULT_EXTRACT = ExtractConfig()


def _node_token(el, cfg: ExtractConfig) -> str:
    """Element -> 'tag.classA.classB' with digit-bearing classes dropped.

    Dropping any class containing a digit also loses the odd legitimate
    grid class (col-2 …), but on KYM the digit-bearing classes are
    overwhelmingly generated (entry_1234, ad slots) — a cheap, high-
    precision noise filter.
    """
    classes = el.get("class") or ""
    kept = sorted({c for c in classes.split() if c and not _DIGIT_RE.search(c)})
    kept = kept[: cfg.max_classes_per_node]
    tag = el.tag.lower()
    return tag + ("." + ".".join(kept) if kept else "")


def extract_structure_tokens(
    html: str, cfg: ExtractConfig = DEFAULT_EXTRACT
) -> dict[str, Any]:
    """
    HTML -> {'tokens': {token: count}, 'n_nodes': int, 'max_depth': int}.

    Emits a unigram per element and a parent>child bigram per edge; all
    text, ids and non-class attributes are ignored, so two pages from
    the same template with different content produce (near-)identical
    token multisets.
    """
    if not html or not html.strip():
        raise ValueError("empty HTML")

    from lxml import html as lhtml  # lazy: keep module import light

    root = lhtml.fromstring(html)
    tokens: Counter[str] = Counter()
    n_nodes = 0
    max_depth = 0

    stack: list[tuple[Any, int, str | None]] = [(root, 0, None)]
    while stack:
        el, depth, parent_tok = stack.pop()
        if not isinstance(el.tag, str):        # comments / processing instr.
            continue
        tag = el.tag.lower()
        if tag in _SKIP_TAGS:
            continue
        if n_nodes >= cfg.max_nodes:
            log.debug("max_nodes=%d hit — truncating extraction", cfg.max_nodes)
            break
        n_nodes += 1
        max_depth = max(max_depth, depth)

        tok = _node_token(el, cfg)
        tokens[tok] += 1
        if parent_tok is not None:
            tokens[parent_tok + ">" + tok] += 1

        if depth < cfg.max_depth:
            for child in el:
                stack.append((child, depth + 1, tok))

    return {"tokens": dict(tokens), "n_nodes": n_nodes, "max_depth": max_depth}


def _extract_worker(
    item: tuple[str, str | None, str], cfg: ExtractConfig = DEFAULT_EXTRACT
) -> dict[str, Any]:
    """(url, content_sha256, html) -> feature doc, or an error doc.

    Top-level function so it pickles into a ProcessPoolExecutor. Never
    raises: one broken page must not kill a 23k-page run.
    """
    url, sha, html = item
    try:
        feats = extract_structure_tokens(html, cfg)
        return {
            "url": url,
            "content_sha256": sha,
            "extractor_version": EXTRACTOR_VERSION,
            **feats,
        }
    except Exception as exc:  # noqa: BLE001 — classified by the caller
        return {"url": url, "error": f"{type(exc).__name__}: {exc}"}


def iter_extract(
    items: Iterable[dict[str, Any]],
    workers: int = 1,
    cfg: ExtractConfig = DEFAULT_EXTRACT,
) -> Iterator[dict[str, Any]]:
    """
    Stream {'url','content_sha256','html'} dicts through the extractor.

    Yields feature docs ready for cluster_store.save_features(); failed
    pages become warnings, not exceptions. workers > 1 fans out over a
    ProcessPoolExecutor (parsing is the CPU bottleneck at corpus scale).
    """
    done = failed = 0

    def _finish(doc: dict[str, Any]) -> dict[str, Any] | None:
        nonlocal done, failed
        if "error" in doc:
            failed += 1
            log.warning("extraction failed for %s: %s", doc["url"], doc["error"])
            return None
        done += 1
        if done % 250 == 0:
            log.info("  extracted %d pages (%d failed)", done, failed)
        return doc

    def _prep(it: dict[str, Any]) -> tuple[str, str | None, str]:
        return (it["url"], it.get("content_sha256"), it["html"])

    if workers <= 1:
        for it in items:
            doc = _finish(_extract_worker(_prep(it), cfg))
            if doc:
                yield doc
    else:
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(max_workers=workers) as pool:
            fn = partial(_extract_worker, cfg=cfg)
            for doc in pool.map(fn, map(_prep, items), chunksize=8):
                doc = _finish(doc)
                if doc:
                    yield doc

    log.info("extraction finished — %d ok, %d failed", done, failed)


# ---------------------------------------------------------------------------
# Phase 2 — token counts -> clusters
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClusterConfig:
    """One clustering run's knobs (frozen; asdict() goes in the run doc)."""
    algorithm: str = "hdbscan"      # 'hdbscan' | 'kmeans'
    min_cluster_size: int = 25      # hdbscan: smallest template family we care about
    min_samples: int | None = None  # hdbscan conservativeness (None = library default)
    k: int = 8                      # kmeans only
    svd_components: int = 100
    min_df: int = 2                 # drop tokens appearing in fewer docs
    top_tokens: int = 12            # distinguishing tokens kept per cluster
    sample_urls: int = 5            # example pages kept per cluster
    silhouette_sample: int = 4000
    random_state: int = 42


DEFAULT_CLUSTER = ClusterConfig()


@dataclass
class ClusterResult:
    """JSON-native result (safe for XCom once this becomes a DAG).

    labels          per-document cluster id, -1 = outlier / unknown template
    probabilities   hdbscan membership strength per doc (None for kmeans)
    metrics         corpus-level numbers for the variance analysis
    clusters        per-cluster summaries with *indices* into the input
                    order; the caller resolves indices to URLs
    """
    labels: list[int]
    probabilities: list[float] | None
    metrics: dict[str, Any]
    clusters: list[dict[str, Any]]


def build_count_matrix(token_maps: list[dict[str, int]]):
    """token-count dicts -> (csr count matrix, term list). Pure."""
    import numpy as np
    from scipy.sparse import csr_matrix

    vocab: dict[str, int] = {}
    indptr = [0]
    indices: list[int] = []
    data: list[float] = []
    for tm in token_maps:
        for tok, cnt in tm.items():
            indices.append(vocab.setdefault(tok, len(vocab)))
            data.append(float(cnt))
        indptr.append(len(indices))

    X = csr_matrix(
        (np.asarray(data), np.asarray(indices), np.asarray(indptr)),
        shape=(len(token_maps), len(vocab)),
    )
    terms: list[str] = [""] * len(vocab)
    for tok, i in vocab.items():
        terms[i] = tok
    return X, terms


def cluster_token_maps(
    token_maps: list[dict[str, int]],
    cfg: ClusterConfig = DEFAULT_CLUSTER,
    return_models: bool = False,
):
    """
    Cluster documents by structural-token distribution.

    Returns a ClusterResult, or (ClusterResult, models) when
    return_models=True — models holds the fitted tfidf/svd/normalizer,
    the term list and per-cluster medoid vectors, i.e. everything needed
    later to assign a NEW page to its nearest cluster (or flag it
    unknown) without re-clustering the corpus.
    """
    import numpy as np
    from sklearn.decomposition import TruncatedSVD
    from sklearn.feature_extraction.text import TfidfTransformer
    from sklearn.preprocessing import Normalizer

    n_docs = len(token_maps)
    floor = cfg.k if cfg.algorithm == "kmeans" else cfg.min_cluster_size
    if n_docs < max(floor, 10):
        raise ValueError(
            f"only {n_docs} documents — need at least {max(floor, 10)} "
            f"for a meaningful {cfg.algorithm} run (extract more DOMs first)"
        )

    # counts -> tf-idf (L2-normalised)
    X, terms = build_count_matrix(token_maps)
    df = np.asarray((X > 0).sum(axis=0)).ravel()
    keep = np.where(df >= cfg.min_df)[0]
    if keep.size == 0:
        raise ValueError(f"no tokens survive min_df={cfg.min_df}")
    X = X[:, keep]
    terms = [terms[i] for i in keep]

    tfidf_tr = TfidfTransformer(sublinear_tf=True)  # norm='l2' by default
    T = tfidf_tr.fit_transform(X)

    # LSA: SVD + re-normalise, so euclidean ~ cosine in the reduced space
    n_comp = max(2, min(cfg.svd_components, T.shape[1] - 1, T.shape[0] - 1))
    svd = TruncatedSVD(n_components=n_comp, random_state=cfg.random_state)
    normalizer = Normalizer(copy=False)
    Z = normalizer.fit_transform(svd.fit_transform(T))

    if cfg.algorithm == "hdbscan":
        try:
            from sklearn.cluster import HDBSCAN
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "sklearn.cluster.HDBSCAN needs scikit-learn>=1.3 — "
                "bump it in airflow/requirements.txt"
            ) from exc
        model = HDBSCAN(min_cluster_size=cfg.min_cluster_size,
                        min_samples=cfg.min_samples)
        model.fit(Z)
        labels = model.labels_.astype(int)
        probs = getattr(model, "probabilities_", None)
    elif cfg.algorithm == "kmeans":
        from sklearn.cluster import KMeans
        model = KMeans(n_clusters=cfg.k, n_init=10, random_state=cfg.random_state)
        labels = model.fit_predict(Z).astype(int)
        probs = None
    else:
        raise ValueError(f"unknown algorithm {cfg.algorithm!r}")

    cluster_ids = sorted({int(l) for l in labels} - {-1})
    n_noise = int((labels == -1).sum())

    silhouette = None
    if len(cluster_ids) >= 2:
        from sklearn.metrics import silhouette_score
        mask = labels != -1
        pts = int(mask.sum())
        if pts > len(cluster_ids) + 1:
            silhouette = float(silhouette_score(
                Z[mask], labels[mask],
                sample_size=min(cfg.silhouette_sample, pts),
                random_state=cfg.random_state,
            ))

    metrics: dict[str, Any] = {
        "algorithm": cfg.algorithm,
        "n_docs": n_docs,
        "n_features": len(terms),
        "svd_components": n_comp,
        "svd_explained_variance": round(float(svd.explained_variance_ratio_.sum()), 4),
        "n_clusters": len(cluster_ids),
        "n_noise": n_noise,
        "noise_frac": round(n_noise / n_docs, 4),
        "silhouette": None if silhouette is None else round(silhouette, 4),
        "cluster_sizes": {str(c): int((labels == c).sum()) for c in cluster_ids},
    }

    # per-cluster summaries: top distinguishing tokens, medoid, samples
    gmean = np.asarray(T.mean(axis=0)).ravel()
    clusters: list[dict[str, Any]] = []
    medoids: dict[int, list[float]] = {}
    summary_ids = cluster_ids + ([-1] if n_noise else [])
    rng = np.random.default_rng(cfg.random_state)

    for cid in summary_ids:
        idxs = np.where(labels == cid)[0]
        cmean = np.asarray(T[idxs].mean(axis=0)).ravel()
        contrast = cmean - gmean
        order = np.argsort(contrast)[::-1][: cfg.top_tokens]
        top = [[terms[i], round(float(contrast[i]), 5)]
               for i in order if contrast[i] > 0]

        medoid_idx: int | None = None
        if cid != -1:
            centroid = Z[idxs].mean(axis=0)
            local = int(np.argmin(((Z[idxs] - centroid) ** 2).sum(axis=1)))
            medoid_idx = int(idxs[local])
            medoids[cid] = Z[medoid_idx].tolist()

        pool = [int(i) for i in idxs if int(i) != medoid_idx]
        n_pick = min(cfg.sample_urls - (medoid_idx is not None), len(pool))
        picked = ([] if medoid_idx is None else [medoid_idx])
        if n_pick > 0:
            picked += [int(i) for i in rng.choice(pool, size=n_pick, replace=False)]

        clusters.append({
            "cluster_id": int(cid),
            "size": int(idxs.size),
            "top_tokens": top,
            "medoid_index": medoid_idx,
            "sample_indices": picked,
        })

    result = ClusterResult(
        labels=[int(l) for l in labels],
        probabilities=None if probs is None else [round(float(p), 4) for p in probs],
        metrics=metrics,
        clusters=clusters,
    )
    if not return_models:
        return result

    models = {
        "extractor_version": EXTRACTOR_VERSION,
        "config": asdict(cfg),
        "terms": terms,
        "tfidf_transformer": tfidf_tr,
        "svd": svd,
        "normalizer": normalizer,
        "medoids": medoids,
    }
    return result, models


# ---------------------------------------------------------------------------
# Entry point — thin CLI over the library (same calls the future DAG makes)
# ---------------------------------------------------------------------------

def _import_store():
    """Sibling import that works from the dags dir and from inside modules/."""
    try:
        from modules import cluster_store  # type: ignore
    except ImportError:
        import cluster_store  # type: ignore
    return cluster_store


def _cmd_extract(args: argparse.Namespace) -> None:
    store = _import_store().get_store()
    try:
        pending = store.select_pending_extraction(
            EXTRACTOR_VERSION, limit=args.limit, force=args.force)
        log.info("%d DOM(s) pending feature extraction", len(pending))
        if not pending:
            return
        docs = iter_extract(store.iter_ok_html(pending), workers=args.workers)
        tallies = store.save_features(docs)  # streams: durable per page
        log.info("EXTRACT COMPLETE — %s", tallies)
    finally:
        store.close()


def _cmd_cluster(args: argparse.Namespace) -> None:
    store = _import_store().get_store()
    started_at = datetime.now(timezone.utc)
    try:
        urls, token_maps = store.load_features(EXTRACTOR_VERSION)
        log.info("Loaded %d feature rows (extractor v%d)", len(urls), EXTRACTOR_VERSION)

        cfg = ClusterConfig(
            algorithm=args.algorithm,
            min_cluster_size=args.min_cluster_size,
            min_samples=args.min_samples,
            k=args.k,
            svd_components=args.svd_components,
        )
        out = cluster_token_maps(token_maps, cfg, return_models=bool(args.model_out))
        result, models = out if args.model_out else (out, None)

        run_id = started_at.strftime("run_%Y%m%dT%H%M%SZ")

        # resolve indices -> URLs and enrich with the namespace breakdown
        ns_map = store.namespace_map(urls)
        members: dict[int, list[str]] = {}
        for url, label in zip(urls, result.labels):
            members.setdefault(label, []).append(url)

        cluster_docs = []
        for c in result.clusters:
            ns_counts = Counter(ns_map.get(u) or "unknown"
                                for u in members.get(c["cluster_id"], []))
            cluster_docs.append({
                "cluster_id": c["cluster_id"],
                "size": c["size"],
                "top_tokens": c["top_tokens"],
                "medoid_url": (urls[c["medoid_index"]]
                               if c["medoid_index"] is not None else None),
                "sample_urls": [urls[i] for i in c["sample_indices"]],
                "namespaces": dict(ns_counts.most_common()),
            })
        cluster_docs.sort(key=lambda d: (d["cluster_id"] == -1, -d["size"]))

        store.save_assignments(run_id, urls, result.labels, result.probabilities)
        store.save_clusters(run_id, cluster_docs)
        store.save_run(run_id, params=asdict(cfg), metrics=result.metrics,
                       n_docs=len(urls), started_at=started_at)
        log.info("Run %s persisted — %s", run_id, result.metrics)

        if args.write_template_type:
            mapping = {u: ("unknown" if l == -1 else f"dom_c{l}")
                       for u, l in zip(urls, result.labels)}
            n = store.write_template_types(mapping)
            log.info("page_template_type written back on %d url rows "
                     "(NOTE: labels are per-run — pin a reference run before "
                     "downstream stages rely on these)", n)

        report = {
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "params": asdict(cfg),
            "metrics": result.metrics,
            "clusters": cluster_docs,
        }
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                               encoding="utf-8")
        log.info("Report → %s", report_path)

        if models is not None:
            import joblib  # ships with scikit-learn
            models["run_id"] = run_id
            model_path = Path(args.model_out)
            model_path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(models, model_path)
            log.info("Model artefacts (for future assign-new-page) → %s", model_path)
    finally:
        store.close()


def _cmd_stats(_args: argparse.Namespace) -> None:
    store = _import_store().get_store()
    try:
        print(json.dumps(store.stats(), indent=2, default=str))
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cluster scraped KYM DOMs by page structure")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ex = sub.add_parser("extract",
                          help="tokenise ok DOMs into dom_features (idempotent)")
    p_ex.add_argument("--limit", type=int, default=0,
                      help="max pages this run (0 = all pending)")
    p_ex.add_argument("--workers", type=int,
                      default=max(1, (os.cpu_count() or 2) - 1),
                      help="parallel parser processes")
    p_ex.add_argument("--force", action="store_true",
                      help="re-extract even rows whose features look fresh")
    p_ex.set_defaults(func=_cmd_extract)

    p_cl = sub.add_parser("cluster",
                          help="cluster all extracted features and persist a run")
    p_cl.add_argument("--algorithm", choices=["hdbscan", "kmeans"],
                      default=DEFAULT_CLUSTER.algorithm)
    p_cl.add_argument("--min-cluster-size", type=int,
                      default=DEFAULT_CLUSTER.min_cluster_size,
                      help="hdbscan: smallest template family worth a script")
    p_cl.add_argument("--min-samples", type=int, default=None,
                      help="hdbscan conservativeness (default: library default)")
    p_cl.add_argument("--k", type=int, default=DEFAULT_CLUSTER.k,
                      help="kmeans: number of clusters")
    p_cl.add_argument("--svd-components", type=int,
                      default=DEFAULT_CLUSTER.svd_components)
    p_cl.add_argument("--report", default="dom_cluster_report.json",
                      help="human-readable JSON report path")
    p_cl.add_argument("--model-out", default=None,
                      help="joblib path for the fitted tfidf/svd/medoids "
                           "(enables assigning new pages later)")
    p_cl.add_argument("--write-template-type", action="store_true",
                      help="write page_template_type back onto the urls "
                           "collection (dom_cN / unknown)")
    p_cl.set_defaults(func=_cmd_cluster)

    p_st = sub.add_parser("stats", help="corpus / feature / run counters")
    p_st.set_defaults(func=_cmd_stats)

    args = parser.parse_args()
    try:
        args.func(args)
    except (ValueError, RuntimeError) as exc:
        log.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()