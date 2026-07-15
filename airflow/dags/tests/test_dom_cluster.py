"""
Unit tests for modules/dom_cluster.py — pure logic only, no Mongo.

Run from the repo:  cd airflow && python -m pytest tests/test_dom_cluster.py
(or from airflow/dags: python -m pytest ../tests/test_dom_cluster.py)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make `modules` importable whether tests/ sits next to dags/ or inside it.
_here = Path(__file__).resolve()
for _cand in (_here.parents[1] / "dags", _here.parents[1]):
    if (_cand / "modules").is_dir():
        sys.path.insert(0, str(_cand))
        break

lxml = pytest.importorskip("lxml")

from modules.dom_cluster import (  # noqa: E402
    ClusterConfig,
    ExtractConfig,
    _fit,
    _prepare,
    _stratified_silhouette,
    build_count_matrix,
    cluster_token_maps,
    extract_structure_tokens,
    iter_extract,
    sweep_token_maps,
    _format_sweep_table,
)


# ---------------------------------------------------------------------------
# Synthetic page templates: same skeleton, varying content
# ---------------------------------------------------------------------------

def _entry_page(title: str, n_paras: int, n_tags: int) -> str:
    paras = "".join(f"<p>{title} paragraph {i}</p>" for i in range(n_paras))
    tags = "".join(f'<a class="tag" href="/t/{i}">tag{i}</a>' for i in range(n_tags))
    return f"""
    <html><body>
      <header class="site-header"><nav class="main-nav"><ul>
        <li><a href="/">home</a></li><li><a href="/memes">memes</a></li>
      </ul></nav></header>
      <article class="entry wide">
        <h1 class="entry-title">{title}</h1>
        <aside class="infobox"><dl><dt>Status</dt><dd>confirmed</dd></dl></aside>
        <section class="bodycopy about">{paras}</section>
        <section class="bodycopy origin"><p>origin of {title}</p></section>
        <footer class="entry-tags">{tags}</footer>
      </article>
    </body></html>"""


def _gallery_page(n_cards: int) -> str:
    cards = "".join(
        f'<figure class="photo-card"><img src="/img/{i}.jpg">'
        f"<figcaption>image {i}</figcaption></figure>"
        for i in range(n_cards)
    )
    return f"""
    <html><body>
      <header class="site-header"><nav class="main-nav"></nav></header>
      <main class="gallery grid"><div class="grid-inner">{cards}</div></main>
      <div class="pagination"><a class="next">next</a></div>
    </body></html>"""


def _forum_page(n_posts: int) -> str:
    posts = "".join(
        f'<article class="post"><span class="author">u{i}</span>'
        f'<div class="post-body"><p>reply {i}</p></div></article>'
        for i in range(n_posts)
    )
    return f"""
    <html><body>
      <div class="forum-wrap"><h2 class="thread-title">a thread</h2>
        <ol class="post-list">{posts}</ol>
      </div>
    </body></html>"""


def _alien_page() -> str:
    """A layout unlike any family above (old-school table soup)."""
    rows = "".join(
        f"<tr><td><b>k{i}</b></td><td><i>v{i}</i></td></tr>" for i in range(20)
    )
    return f"""
    <html><body bgcolor="white">
      <center><font size="4">legacy page</font></center>
      <table border="1"><tbody>{rows}</tbody></table>
      <hr><marquee>totally different skeleton</marquee>
    </body></html>"""


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def test_tokens_ignore_text_content():
    a = extract_structure_tokens(_entry_page("Doge", 3, 4))
    b = extract_structure_tokens(_entry_page("Loss", 3, 4))
    assert a["tokens"] == b["tokens"]  # same skeleton, different memes


def test_digit_classes_and_skip_tags_dropped():
    html = """<html><body>
        <div class="entry_5432 body"><script>evil()</script>
        <p class="ad-slot-12"></p></div></body></html>"""
    tokens = extract_structure_tokens(html)["tokens"]
    assert "div.body" in tokens          # digit class gone, real class kept
    assert not any("script" in t for t in tokens)
    assert "p" in tokens                 # ad-slot-12 dropped -> bare tag


def test_bigrams_encode_parent_child():
    tokens = extract_structure_tokens(_forum_page(2))["tokens"]
    assert tokens["ol.post-list>article.post"] == 2


def test_empty_html_raises():
    with pytest.raises(ValueError):
        extract_structure_tokens("   ")


def test_max_nodes_truncates():
    html = "<html><body>" + "<p></p>" * 500 + "</body></html>"
    out = extract_structure_tokens(html, ExtractConfig(max_nodes=50))
    assert out["n_nodes"] == 50


def test_iter_extract_survives_bad_pages():
    items = [
        {"url": "u1", "content_sha256": "x", "html": _entry_page("a", 2, 2)},
        {"url": "u2", "content_sha256": "y", "html": ""},  # broken
        {"url": "u3", "content_sha256": "z", "html": _gallery_page(3)},
    ]
    docs = list(iter_extract(items, workers=1))
    assert [d["url"] for d in docs] == ["u1", "u3"]
    assert all("tokens" in d and "error" not in d for d in docs)


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------

def _corpus(n_per_family: int = 15):
    maps, truth = [], []
    for i in range(n_per_family):
        maps.append(extract_structure_tokens(
            _entry_page(f"meme{i}", 2 + i % 5, 3 + i % 4))["tokens"])
        truth.append("entry")
    for i in range(n_per_family):
        maps.append(extract_structure_tokens(_gallery_page(4 + i % 7))["tokens"])
        truth.append("gallery")
    for i in range(n_per_family):
        maps.append(extract_structure_tokens(_forum_page(3 + i % 6))["tokens"])
        truth.append("forum")
    return maps, truth


def test_build_count_matrix_shape_and_counts():
    maps = [{"a": 2, "b": 1}, {"b": 3, "c": 1}]
    X, terms = build_count_matrix(maps)
    assert X.shape == (2, 3)
    dense = X.toarray()
    assert dense[0][terms.index("a")] == 2
    assert dense[1][terms.index("b")] == 3


def test_kmeans_separates_the_three_templates():
    pytest.importorskip("sklearn")
    maps, truth = _corpus()
    cfg = ClusterConfig(algorithm="kmeans", k=3, min_df=1, svd_components=20)
    result = cluster_token_maps(maps, cfg)

    from sklearn.metrics import adjusted_rand_score
    assert adjusted_rand_score(truth, result.labels) == pytest.approx(1.0)
    assert result.metrics["n_clusters"] == 3
    assert sum(result.metrics["cluster_sizes"].values()) == len(maps)


def test_hdbscan_flags_unknown_template_as_noise():
    pytest.importorskip("sklearn")
    maps, _ = _corpus()
    maps.append(extract_structure_tokens(_alien_page())["tokens"])  # the intruder
    cfg = ClusterConfig(algorithm="hdbscan", min_cluster_size=5,
                        min_samples=3, min_df=1, svd_components=20)
    result = cluster_token_maps(maps, cfg)

    assert result.metrics["n_clusters"] == 3
    assert result.labels[-1] == -1              # new template -> outlier
    assert result.probabilities is not None

    # the -1 summary is the "needs a new scraping script" queue
    noise = [c for c in result.clusters if c["cluster_id"] == -1]
    assert noise and noise[0]["size"] == 1
    assert noise[0]["medoid_index"] is None


def test_cluster_summaries_are_consistent():
    pytest.importorskip("sklearn")
    maps, _ = _corpus()
    cfg = ClusterConfig(algorithm="kmeans", k=3, min_df=1,
                        svd_components=20, sample_urls=4)
    result, models = cluster_token_maps(maps, cfg, return_models=True)

    for c in result.clusters:
        assert c["size"] > 0
        assert c["medoid_index"] in c["sample_indices"]
        assert len(c["sample_indices"]) <= cfg.sample_urls
        assert all(result.labels[i] == c["cluster_id"]
                   for i in c["sample_indices"])
        assert c["top_tokens"] and all(w > 0 for _, w in c["top_tokens"])

    # model artefacts carry what a future assign-new-page step needs
    assert set(models) >= {"terms", "tfidf_transformer", "svd",
                           "normalizer", "medoids", "extractor_version"}
    assert set(models["medoids"]) == {0, 1, 2}


def test_too_few_documents_raises():
    pytest.importorskip("sklearn")
    with pytest.raises(ValueError):
        cluster_token_maps([{"a": 1}] * 3)


# ---------------------------------------------------------------------------
# Binary features, stratified silhouette, sweep
# ---------------------------------------------------------------------------

def test_binary_features_ignore_article_length():
    """Same skeleton, wildly different paragraph counts -> identical
    vectors under binary features (counts leak length; presence keeps
    layout only)."""
    pytest.importorskip("sklearn")
    np = pytest.importorskip("numpy")
    maps = [
        extract_structure_tokens(_entry_page("short", 2, 3))["tokens"],
        extract_structure_tokens(_entry_page("long", 40, 3))["tokens"],
        extract_structure_tokens(_gallery_page(5))["tokens"],   # anchor doc
    ]
    X, _ = _prepare(maps, min_df=1)

    z_bin = _fit(X, ClusterConfig(algorithm="kmeans", k=2, binary=True, svd_components=2))["Z"]
    assert np.allclose(z_bin[0], z_bin[1])

    z_cnt = _fit(X, ClusterConfig(algorithm="kmeans", k=2, binary=False, svd_components=2))["Z"]
    assert not np.allclose(z_cnt[0], z_cnt[1])


def test_counts_mode_still_separates_templates():
    pytest.importorskip("sklearn")
    maps, truth = _corpus()
    cfg = ClusterConfig(algorithm="kmeans", k=3, binary=False,
                        min_df=1, svd_components=20)
    result = cluster_token_maps(maps, cfg)

    from sklearn.metrics import adjusted_rand_score
    assert adjusted_rand_score(truth, result.labels) == pytest.approx(1.0)


def test_metrics_carry_the_new_fields():
    pytest.importorskip("sklearn")
    maps, _ = _corpus()
    cfg = ClusterConfig(algorithm="kmeans", k=3, min_df=1, svd_components=20)
    m = cluster_token_maps(maps, cfg).metrics

    assert m["binary_features"] is True
    assert m["davies_bouldin"] is not None and m["davies_bouldin"] >= 0
    assert m["largest_cluster_frac"] == pytest.approx(15 / 45, abs=1e-3)
    assert m["silhouette"] is not None


def test_stratified_silhouette_sees_tiny_clusters():
    """The 17,186/14 failure mode: a plain random subsample of a huge
    blob + a tiny satellite scores the split as excellent while barely
    sampling the satellite. Stratification must include the satellite
    wholesale and still return a finite, computable score."""
    np = pytest.importorskip("numpy")
    pytest.importorskip("sklearn")
    rng = np.random.default_rng(0)
    big = rng.normal(0.0, 1.0, size=(2000, 5))
    tiny = rng.normal(8.0, 0.1, size=(6, 5))
    Z = np.vstack([big, tiny])
    labels = np.array([0] * 2000 + [1] * 6)

    cfg = ClusterConfig(silhouette_sample=200, silhouette_floor=5)
    score = _stratified_silhouette(Z, labels, cfg)
    assert score is not None and 0.0 < score <= 1.0

    # single cluster -> undefined
    assert _stratified_silhouette(Z, np.zeros(len(Z), dtype=int), cfg) is None


def test_sweep_runs_grid_and_persists_nothing():
    pytest.importorskip("sklearn")
    maps, _ = _corpus()
    configs = [
        ClusterConfig(algorithm="kmeans", k=2, min_df=1, svd_components=10),
        ClusterConfig(algorithm="kmeans", k=3, min_df=1, svd_components=10),
        ClusterConfig(algorithm="hdbscan", min_cluster_size=5, min_samples=3,
                      min_df=1, svd_components=10, binary=False),
    ]
    rows = sweep_token_maps(maps, configs)

    assert len(rows) == 3
    assert {r["algorithm"] for r in rows} == {"kmeans", "hdbscan"}
    assert all("silhouette" in r["metrics"] and "fit_seconds" in r for r in rows)
    assert rows[2]["features"] == "counts"

    table = _format_sweep_table(rows)
    assert "kmeans" in table and "hdbscan" in table and "noise%" in table
    # k=3 matches the three synthetic families -> should sort to the top
    assert "k=3" in table.splitlines()[2]