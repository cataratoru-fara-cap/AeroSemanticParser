"""
kg/metrics.py — IMKG-comparable graph statistics + data-quality checks
=======================================================================
Replicates the Section 5 analysis of Tommasini, Ilievski & Wijesiriwardene,
"IMKG: The Internet Meme Knowledge Graph" (ESWC 2023), over the MemeAtlas
graph, and adds the integrity checks that paper did not report.

Two blocks of output:

  REPLICATION  — the Table 2 columns (#nodes, #edges, #rels, avg degree,
                 #frames), relation-type frequency, and PageRank centrality.
                 Printed side-by-side with IMKG's published KYM-source row
                 so the delta is readable without cross-referencing the PDF.

  INTEGRITY    — coverage and consistency checks: unresolved series parents,
                 frames with no semantic attachment, isolated nodes,
                 connected components, series-chain depth and cycles,
                 duplicate/self edges, label completeness.

Reads one build's kg_nodes/kg_edges from Mongo (the published build unless
--build names another; same env vars as kg_store.py), or the exported CSVs
with --from-csv for a laptop run with no container.

Two scopes:

  core   the IMKG-comparable core: frames, their entry types, tags, series
         and links (CORE_NODE_KINDS / CORE_EDGE_TYPES) — the KYM row of
         IMKG's Table 2. kym_kg computes it after every publish.
  full   the whole graph, side by side with IMKG's full graph, plus LAYERS:
         what the 6.x layers add per frame (Wikidata entities from the text
         and from template images, imgflip templates, events). About three
         times the core's memory (~2 GB for 6.5.0), so it runs by hand, not
         in the worker.

Pure stdlib: no numpy, no networkx, no pymongo unless --from-csv is absent.
PageRank is power iteration by hand so this cannot fail on a missing dep
five minutes before a meeting.

Run inside the Airflow container:
    docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
        python -m modules.kg.metrics --out /opt/airflow/data/kg_metrics.json

The whole graph, from the host (the venv, MONGODB_URI pointing at Mongo):
    python -m modules.kg.metrics --scope full \
        --out ../data/kg/current/metrics_full.json

Or against exported CSVs:
    python kg_metrics.py --from-csv --nodes kg_view_nodes.csv \
        --edges kg_view_edges.csv --out kg_metrics.json

CAVEAT ON COMPARABILITY (read before quoting any number):
IMKG is RDF — every literal (about, origin, spread, year, status, image url)
is a triple, so its KYM subgraph reports 18 relation types and 914,941 edges
over 12,585 frames, i.e. ~73 edges per frame. MemeAtlas's kg_nodes/kg_edges
is property-graph shaped: literals live in node columns, not edges. A raw
edge-count comparison therefore understates MemeAtlas by roughly an order of
magnitude and means nothing. --triple-equivalent counts populated node
attributes as if they were outgoing edges, which is the only like-for-like
reading. Both are printed; the honest one is labelled.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter, defaultdict

# --------------------------------------------------------------------------
# Published IMKG figures — ESWC 2023, Table 2 and Section 5 prose.
# The KYM row is the directly comparable baseline: same source, frames only.
# --------------------------------------------------------------------------
IMKG_KYM = {
    "nodes": 167_662,
    "edges": 914_941,
    "rel_types": 18,
    "avg_degree": 10.91,
    "frames": 12_585,
}
IMKG_FULL = {
    "nodes": 4_850_636,
    "edges": 16_549_810,
    "rel_types": 836,
    "avg_degree": 6.82,
    "frames": 12_585,
}
# Node attributes treated as triples under --triple-equivalent.
ATTR_FIELDS = ("label", "category", "status")

# The IMKG-comparable core (the KYM row of IMKG's Table 2). kym_kg's
# compute_metrics measures this scope after every publish.
#
# sharesSameSeries (6.6.0) is left out of it. It is derived entirely from
# partOfSeries — two frames with one parent — so its ~680k edges would
# nearly double the core's edge count and average degree while connecting
# nothing the core does not already connect, and the core's figures would
# stop being comparable with those published for 5.0.1 and 6.5.0. The full
# scope counts it.
CORE_NODE_KINDS = ("frame", "frame_stub", "entry_type_concept", "tag_concept",
                   "external_ref")
CORE_EDGE_TYPES = ("hasEntryType", "hasTag", "partOfSeries", "relatesToMeme",
                   "citesExternal", "subTypeOf")
SCOPES = ("core", "full")

# 6.1.0: a frame's links to Wikidata items, one edge type per field.
ENTITY_FRAME_EDGES = ("fromTitle", "fromTags", "fromAbout")


# ---------------------------------------------------------------- loading ---

def in_scope(edge: dict, scope: str) -> bool:
    """subTypeOf is ONE edge type shared by entry_type's curated hierarchy
    (in the core) and origin's (5.0.0, not in it): origin's start at an
    ``origin:`` node, which the core never loads, so they would show up as
    dangling and inflate the core's edge and degree counts."""
    if scope == "full":
        return True
    return (edge["type"] in CORE_EDGE_TYPES
            and not (edge["type"] == "subTypeOf" and edge["src"].startswith("origin:")))


def restrict_to_scope(nodes: dict, edges: list, scope: str) -> dict:
    """In the core, a node other than a frame counts only if a core edge
    touches it. A KIND does not say which layer a node belongs to: since
    6.4.0 every template's imgflip page is an ``external_ref`` reached only
    by ``imgflipPage``, and counting those put 26,745 isolated nodes into
    6.5.0's core (5.0.1 had none). A frame always counts: a frame with no
    edges is a finding."""
    if scope == "full":
        return nodes
    touched = {e["src"] for e in edges} | {e["dst"] for e in edges}
    return {nid: n for nid, n in nodes.items()
            if n.get("kind") == "frame" or nid in touched}


def load_build(build_id: str, scope: str = "core") -> tuple[dict, list]:
    """One build's graph at a scope: only the fields metrics reads (section
    text alone would not fit) and no per-mention occurrence lists."""
    from modules import kg_store as store
    kinds = CORE_NODE_KINDS if scope == "core" else None
    types = CORE_EDGE_TYPES if scope == "core" else None
    nodes = {n["id"]: n for n in store.iter_nodes(build_id, kinds=kinds, fields=ATTR_FIELDS)}
    edges = [e for e in store.iter_edges(build_id, types=types, occurrences=False)
             if in_scope(e, scope)]
    return restrict_to_scope(nodes, edges, scope), edges


def load_from_csv(nodes_path: str, edges_path: str) -> tuple[dict, list]:
    nodes: dict[str, dict] = {}
    with open(nodes_path, newline="") as f:
        for row in csv.DictReader(f):
            nodes[row["id"]] = row
    edges: list[dict] = []
    with open(edges_path, newline="") as f:
        for row in csv.DictReader(f):
            edges.append({"src": row["source"], "dst": row["target"],
                          "type": row["type"]})
    return nodes, edges


# ------------------------------------------------------------- primitives ---

class _UnionFind:
    def __init__(self):
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:       # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def pagerank(out_adj: dict[str, list[str]], all_ids: list[str],
             damping: float = 0.85, iterations: int = 40,
             tol: float = 1e-8) -> dict[str, float]:
    """Power iteration with dangling-mass redistribution. Pure stdlib."""
    n = len(all_ids)
    if n == 0:
        return {}
    rank = {i: 1.0 / n for i in all_ids}
    dangling = [i for i in all_ids if not out_adj.get(i)]
    for _ in range(iterations):
        nxt = dict.fromkeys(all_ids, 0.0)
        leaked = sum(rank[i] for i in dangling) / n
        for src, dsts in out_adj.items():
            share = rank[src] / len(dsts)
            for d in dsts:
                nxt[d] += share
        base = (1.0 - damping) / n + damping * leaked
        nxt = {i: base + damping * v for i, v in nxt.items()}
        delta = sum(abs(nxt[i] - rank[i]) for i in all_ids)
        rank = nxt
        if delta < tol:
            break
    return rank


def _longest_chain(out_adj: dict[str, list[str]],
                   roots: list[str]) -> tuple[int, list[str]]:
    """Longest path length + one witness path, over a DAG. O(V + E).

    Memoised, not enumerated. The previous version pushed
    ``(child, path + [child], seen | {child})`` for every child of every
    partial path — a full simple-path enumeration, exponential in branching
    factor. It survived only because ``partOfSeries`` happens to be
    near-tree-shaped (19,158 edges, max depth 7). A single popular series
    parent with many children that themselves chain would have hung the
    whole report, and nothing in the data model prevents that.

    Acyclicity is the caller's guarantee: compute_metrics runs _find_cycles
    first and reports an undefined depth while cyclic. That is what lets
    this drop the per-path ``seen`` set and settle each node exactly once.
    """
    best_from: dict[str, tuple[int, list[str]]] = {}

    for root in roots:
        # Iterative post-order: a node is settled only once every child is.
        stack: list[tuple[str, bool]] = [(root, False)]
        while stack:
            node, expanded = stack.pop()
            if node in best_from:
                continue
            children = out_adj.get(node, ())
            if not expanded:
                pending = [c for c in children if c not in best_from]
                if pending:
                    stack.append((node, True))
                    stack.extend((c, False) for c in pending)
                    continue
            if children:
                length, tail = max((best_from[c] for c in children),
                                   key=lambda lt: lt[0])
                best_from[node] = (length + 1, [node] + tail)
            else:
                best_from[node] = (0, [node])

    if not best_from:
        return 0, []
    # Every settled node is reachable from a root, and a path starting at an
    # interior node is a suffix of one starting at a root — so the max over
    # all settled nodes equals the max over roots.
    return max(best_from.values(), key=lambda lt: lt[0])


def _find_cycles(out_adj: dict[str, list[str]], nodes: list[str],
                 limit: int = 10) -> list[list[str]]:
    """Iterative DFS colouring. Returns up to `limit` cycle witnesses."""
    WHITE, GREY, BLACK = 0, 1, 2
    colour = dict.fromkeys(nodes, WHITE)
    cycles: list[list[str]] = []
    for start in nodes:
        if colour[start] != WHITE:
            continue
        stack = [(start, iter(out_adj.get(start, [])), [start])]
        colour[start] = GREY
        while stack:
            node, it, path = stack[-1]
            advanced = False
            for child in it:
                if colour.get(child, WHITE) == GREY:
                    cycles.append(path[path.index(child):] + [child]
                                  if child in path else path + [child])
                    if len(cycles) >= limit:
                        return cycles
                elif colour.get(child, WHITE) == WHITE:
                    colour[child] = GREY
                    stack.append((child, iter(out_adj.get(child, [])),
                                  path + [child]))
                    advanced = True
                    break
            if not advanced:
                colour[node] = BLACK
                stack.pop()
    return cycles


# ---------------------------------------------------------------- metrics ---

def compute_metrics(nodes: dict, edges: list, triple_equivalent: bool = False,
                    top_k: int = 20) -> dict:
    """Pure transform: (nodes, edges) -> metrics dict. No I/O."""
    n_nodes = len(nodes)
    kind_counts = Counter(n.get("kind") or "(none)" for n in nodes.values())
    edge_type_counts = Counter(e["type"] for e in edges)

    # -- adjacency, duplicates, self-loops, dangling ------------------------
    out_adj: dict[str, list[str]] = defaultdict(list)
    undirected_degree: Counter[str] = Counter()
    seen_edges: set[tuple[str, str, str]] = set()
    duplicate_edges = self_loops = 0
    dangling_src = dangling_dst = 0

    for e in edges:
        src, dst, typ = e["src"], e["dst"], e["type"]
        key = (src, typ, dst)
        if key in seen_edges:
            duplicate_edges += 1
        seen_edges.add(key)
        if src == dst:
            self_loops += 1
        if src not in nodes:
            dangling_src += 1
        if dst not in nodes:
            dangling_dst += 1
        out_adj[src].append(dst)
        undirected_degree[src] += 1
        undirected_degree[dst] += 1

    n_edges = len(edges)

    # -- triple-equivalent view (see module docstring) ----------------------
    attr_triples = 0
    for n in nodes.values():
        for field in ATTR_FIELDS:
            v = n.get(field)
            if v not in (None, "", "None"):
                attr_triples += 1
    te_edges = n_edges + attr_triples
    te_rel_types = len(edge_type_counts) + len(ATTR_FIELDS)

    headline_edges = te_edges if triple_equivalent else n_edges
    headline_rels = te_rel_types if triple_equivalent else len(edge_type_counts)

    # IMKG computes degree as 2E/N (verified: 914941/167662*2 = 10.91)
    avg_degree = (2 * headline_edges / n_nodes) if n_nodes else 0.0

    # -- connected components (undirected) ---------------------------------
    uf = _UnionFind()
    for nid in nodes:
        uf.find(nid)
    for e in edges:
        if e["src"] in nodes and e["dst"] in nodes:
            uf.union(e["src"], e["dst"])
    comp_sizes = Counter(uf.find(nid) for nid in nodes)
    sizes = sorted(comp_sizes.values(), reverse=True)
    largest = sizes[0] if sizes else 0

    # -- components with concept nodes removed (the shatter test) ----------
    concept_kinds = {"tag_concept", "entry_type_concept"}
    frame_ids = {nid for nid, n in nodes.items()
                 if n.get("kind") not in concept_kinds}
    uf2 = _UnionFind()
    for nid in frame_ids:
        uf2.find(nid)
    for e in edges:
        if e["src"] in frame_ids and e["dst"] in frame_ids:
            uf2.union(e["src"], e["dst"])
    frame_comp_sizes = Counter(uf2.find(nid) for nid in frame_ids)
    frame_sizes = sorted(frame_comp_sizes.values(), reverse=True)

    # -- semantic attachment per frame -------------------------------------
    has_type: set[str] = set()
    has_tag: set[str] = set()
    tag_count: Counter[str] = Counter()
    type_count: Counter[str] = Counter()
    for e in edges:
        if e["type"] == "hasEntryType":
            has_type.add(e["src"])
            type_count[e["src"]] += 1
        elif e["type"] == "hasTag":
            has_tag.add(e["src"])
            tag_count[e["src"]] += 1

    real_frames = [nid for nid, n in nodes.items() if n.get("kind") == "frame"]
    n_frames = len(real_frames)
    no_type = [f for f in real_frames if f not in has_type]
    no_tag = [f for f in real_frames if f not in has_tag]
    no_either = [f for f in real_frames
                 if f not in has_type and f not in has_tag]

    stubs = [nid for nid, n in nodes.items() if n.get("kind") == "frame_stub"]
    isolated = [nid for nid in nodes if undirected_degree[nid] == 0]
    unlabelled = [nid for nid, n in nodes.items()
                  if n.get("kind") != "frame_stub"
                  and not (n.get("label") or "").strip()]

    # -- series chains ------------------------------------------------------
    # edge direction is child --partOfSeries--> parent
    series_adj: dict[str, list[str]] = defaultdict(list)
    series_children: set[str] = set()     # appear as src == have a parent
    series_parents: set[str] = set()      # appear as dst == are a parent
    for e in edges:
        if e["type"] == "partOfSeries":
            series_adj[e["src"]].append(e["dst"])
            series_children.add(e["src"])
            series_parents.add(e["dst"])
    series_nodes = series_children | series_parents
    # series tops: never a child of anything
    series_roots = [n for n in series_nodes if n not in series_children]
    # traversal starts at leaves (nothing points at them) and walks up.
    # No `or sorted(series_nodes)` fallback: that branch was unreachable.
    # An empty difference means every node is a parent, which in a finite
    # graph requires a cycle — and the cycle check below already diverts
    # those to an undefined depth before _longest_chain is ever called.
    leaves = sorted(series_nodes - series_parents)
    cycles = _find_cycles(series_adj, sorted(series_nodes))
    if cycles:
        chain_depth, chain_witness = -1, []      # undefined while cyclic
    else:
        chain_depth, chain_witness = _longest_chain(dict(series_adj), leaves)

    # -- centrality ---------------------------------------------------------
    # PageRank runs over the node set only. Both ends must be filtered, not
    # just the source: `nxt` is keyed by all_ids, so an edge pointing at a
    # target with no node raised KeyError and took the whole report down —
    # in a function that also reports `dangling_edge_targets`, so it could
    # only count dangling targets on a graph that had none. Live data hid it
    # because the old writer upserted a node for every edge endpoint.
    #
    # Sources left with no surviving target are dropped rather than kept
    # with an empty list: `share = rank[src] / len(dsts)` divides before the
    # inner loop. Dropping them is also correct — pagerank treats a source
    # absent from out_adj as dangling and redistributes its mass.
    all_ids = list(nodes)
    reachable = {src: [d for d in dsts if d in nodes]
                 for src, dsts in out_adj.items() if src in nodes}
    pr = pagerank({src: dsts for src, dsts in reachable.items() if dsts},
                  all_ids)

    def _label(nid: str) -> str:
        return (nodes.get(nid, {}).get("label") or nid)[:60]

    def _kind(nid: str) -> str:
        return nodes.get(nid, {}).get("kind") or "?"

    top_pagerank = [{"id": nid, "label": _label(nid), "kind": _kind(nid),
                     "score": round(pr[nid], 6)}
                    for nid, _ in sorted(pr.items(), key=lambda kv: -kv[1])[:top_k]]
    top_degree = [{"id": nid, "label": _label(nid), "kind": _kind(nid),
                   "degree": d}
                  for nid, d in undirected_degree.most_common(top_k)]
    # frames only — otherwise hub tags drown every frame out of the ranking
    top_frames_pr = [{"id": nid, "label": _label(nid),
                      "score": round(pr[nid], 6)}
                     for nid, _ in sorted(pr.items(), key=lambda kv: -kv[1])
                     if _kind(nid) == "frame"][:top_k]

    # -- tag degree distribution (for choosing --top-tags honestly) --------
    tag_degree = Counter()
    for e in edges:
        if e["type"] == "hasTag":
            tag_degree[e["dst"]] += 1
    tag_deg_values = sorted(tag_degree.values(), reverse=True)

    def _pct(p: float) -> int:
        if not tag_deg_values:
            return 0
        idx = int(len(tag_deg_values) * (1.0 - p))
        return tag_deg_values[min(len(tag_deg_values) - 1, max(0, idx))]

    return {
        "replication": {
            "counting_mode": "triple_equivalent" if triple_equivalent
                             else "property_graph",
            "nodes": n_nodes,
            "edges": headline_edges,
            "edges_property_graph": n_edges,
            "edges_triple_equivalent": te_edges,
            "attribute_triples": attr_triples,
            "rel_types": headline_rels,
            "avg_degree": round(avg_degree, 2),
            "frames": n_frames,
            "nodes_by_kind": dict(kind_counts),
            "edges_by_type": dict(edge_type_counts.most_common()),
            "imkg_kym_baseline": IMKG_KYM,
            "imkg_full_baseline": IMKG_FULL,
            "top_pagerank": top_pagerank,
            "top_pagerank_frames_only": top_frames_pr,
            "top_degree": top_degree,
        },
        "integrity": {
            "unresolved_series_parents": len(stubs),
            "unresolved_series_parent_pct": round(
                100 * len(stubs) / max(n_frames + len(stubs), 1), 2),
            "frames_without_entry_type": len(no_type),
            "frames_without_entry_type_pct": round(
                100 * len(no_type) / max(n_frames, 1), 2),
            "frames_without_tags": len(no_tag),
            "frames_without_tags_pct": round(
                100 * len(no_tag) / max(n_frames, 1), 2),
            "frames_without_either": len(no_either),
            "frames_without_either_sample": no_either[:10],
            "isolated_nodes": len(isolated),
            "isolated_nodes_sample": isolated[:10],
            "unlabelled_nodes": len(unlabelled),
            "unlabelled_nodes_sample": unlabelled[:10],
            "duplicate_edges": duplicate_edges,
            "self_loops": self_loops,
            "dangling_edge_sources": dangling_src,
            "dangling_edge_targets": dangling_dst,
            "connected_components": len(sizes),
            "largest_component": largest,
            "largest_component_pct": round(100 * largest / max(n_nodes, 1), 2),
            "components_without_concepts": len(frame_sizes),
            "largest_component_without_concepts": (frame_sizes[0]
                                                   if frame_sizes else 0),
            "series_chain_max_depth": chain_depth,
            "series_chain_witness": chain_witness[:12],
            "series_roots": len(series_roots),
            "series_cycles_found": len(cycles),
            "series_cycle_sample": cycles[:3],
            "entry_types_per_frame_mean": round(
                sum(type_count.values()) / max(n_frames, 1), 2),
            "tags_per_frame_mean": round(
                sum(tag_count.values()) / max(n_frames, 1), 2),
            "distinct_tags": len(tag_degree),
            "tag_degree_max": tag_deg_values[0] if tag_deg_values else 0,
            "tag_degree_p50": _pct(0.50),
            "tag_degree_p90": _pct(0.90),
            "tag_degree_p99": _pct(0.99),
            "tags_used_once": sum(1 for v in tag_deg_values if v == 1),
            "category_distribution": dict(Counter(
                (n.get("category") or "(none)") for n in nodes.values()
                if n.get("kind") == "frame").most_common()),
            "status_distribution": dict(Counter(
                (n.get("status") or "(none)") for n in nodes.values()
                if n.get("kind") == "frame").most_common()),
        },
    }


def layer_metrics(nodes: dict, edges: list, top_k: int = 20) -> dict:
    """What the layers beyond the IMKG core add, per frame (6.x): Wikidata
    items linked from the frame's text (6.1.0, curated in 6.5.0) and from its
    templates' images (6.4.0), imgflip templates, events (6.0.0). Pure."""
    kind = {nid: n.get("kind") for nid, n in nodes.items()}
    frames = {nid for nid, k in kind.items() if k == "frame"}
    templates = {nid for nid, k in kind.items() if k == "template"}
    entities = {nid for nid, k in kind.items() if k == "wikidata_entity"}

    linked_by_field: dict[str, set[str]] = {t: set() for t in ENTITY_FRAME_EDGES}
    entity_frames: dict[str, set[str]] = defaultdict(set)
    entity_templates: dict[str, set[str]] = defaultdict(set)
    frame_templates: dict[str, set[str]] = defaultdict(set)
    template_frames: dict[str, set[str]] = defaultdict(set)
    events_per_frame: Counter[str] = Counter()
    for e in edges:
        typ, src, dst = e["type"], e["src"], e["dst"]
        if typ in linked_by_field:
            linked_by_field[typ].add(src)
            entity_frames[dst].add(src)
        elif typ == "fromImage":
            entity_templates[dst].add(src)
        elif typ == "hasTemplate":
            frame_templates[src].add(dst)
            template_frames[dst].add(src)
        elif typ == "hasEvent":
            events_per_frame[src] += 1

    with_entity = set().union(*linked_by_field.values()) & frames
    with_template = set(frame_templates) & frames
    templates_reading = set().union(*entity_templates.values())  # templates showing an item
    with_template_entity = {f for f in with_template if frame_templates[f] & templates_reading}
    with_events = set(events_per_frame) & frames
    from_text, from_images = set(entity_frames), set(entity_templates)
    n_frames = max(len(frames), 1)

    def pct(k: int, n: int = n_frames) -> float:
        return round(100 * k / max(n, 1), 2)

    def label(nid: str) -> str:
        return (nodes.get(nid, {}).get("label") or nid)[:60]

    def top(by: dict[str, set[str]], key: str) -> list[dict]:
        ranked = sorted(by.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:top_k]
        return [{"id": nid, "label": label(nid), key: len(v)} for nid, v in ranked]

    counts = sorted(events_per_frame[f] for f in with_events)
    return {
        "frames": len(frames),
        "frames_with_entity_link": len(with_entity),
        "frames_with_entity_link_pct": pct(len(with_entity)),
        "frames_with_entity_link_by_field": {
            t: len(linked_by_field[t] & frames) for t in ENTITY_FRAME_EDGES},
        "frames_with_template": len(with_template),
        "frames_with_template_pct": pct(len(with_template)),
        "frames_with_template_entity": len(with_template_entity),
        "frames_with_template_entity_pct": pct(len(with_template_entity)),
        "frames_with_events": len(with_events),
        "frames_with_events_pct": pct(len(with_events)),
        "frames_with_all_three": len(with_entity & with_template & with_events),
        "frames_with_none": len(frames - with_entity - with_template - with_events),
        "events_per_frame_mean": round(sum(counts) / max(len(counts), 1), 2),
        "events_per_frame_median": counts[len(counts) // 2] if counts else 0,
        "templates_per_frame_mean": round(
            sum(len(frame_templates[f]) for f in with_template) / max(len(with_template), 1), 2),
        "wikidata_entities": len(entities),
        "entities_from_text_only": len(from_text - from_images),
        "entities_from_images_only": len(from_images - from_text),
        "entities_from_both": len(from_text & from_images),
        "templates": len(templates),
        "templates_with_entity": len(templates_reading & templates),
        "templates_with_entity_pct": pct(len(templates_reading & templates), len(templates)),
        "templates_shared_by_frames": sum(1 for t in templates if len(template_frames[t]) > 1),
        "top_entities_by_frames": top(entity_frames, "frames"),
        "top_entities_by_templates": top(entity_templates, "templates"),
        "top_templates_by_frames": top(template_frames, "frames"),
    }


# ----------------------------------------------------------------- report ---

def format_report(m: dict) -> str:
    r, g = m["replication"], m["integrity"]
    L: list[str] = []
    add = L.append

    add("=" * 74)
    add("REPLICATION — IMKG (ESWC 2023) Table 2 columns")
    add("=" * 74)
    if m.get("scope"):
        add(f"scope: {m['scope']}   build: {m.get('build_id') or '-'}")
    add(f"counting mode: {r['counting_mode']}")
    add("")
    add(f"{'':22} {'MemeAtlas':>14} {'IMKG (KYM)':>14} {'IMKG (full)':>14}")
    for key, label in [("nodes", "#nodes"), ("edges", "#edges"),
                       ("rel_types", "#rels"), ("avg_degree", "degree"),
                       ("frames", "#frames")]:
        add(f"{label:22} {r[key]:>14,} {IMKG_KYM[key]:>14,} "
            f"{IMKG_FULL[key]:>14,}" if not isinstance(r[key], float) else
            f"{label:22} {r[key]:>14.2f} {IMKG_KYM[key]:>14.2f} "
            f"{IMKG_FULL[key]:>14.2f}")
    add("")
    add(f"  property-graph edges : {r['edges_property_graph']:,}")
    add(f"  attribute triples    : {r['attribute_triples']:,}")
    add(f"  triple-equivalent    : {r['edges_triple_equivalent']:,}")
    add("")
    add("nodes by kind:")
    for k, v in sorted(r["nodes_by_kind"].items(), key=lambda kv: -kv[1]):
        add(f"  {k:24} {v:>12,}")
    add("edges by type:")
    for k, v in r["edges_by_type"].items():
        add(f"  {k:24} {v:>12,}")
    add("")
    add("top PageRank (all nodes):")
    for row in r["top_pagerank"][:10]:
        add(f"  {row['score']:.6f}  {row['kind']:20} {row['label']}")
    add("top PageRank (frames only):")
    for row in r["top_pagerank_frames_only"][:10]:
        add(f"  {row['score']:.6f}  {row['label']}")
    add("")

    add("=" * 74)
    add("INTEGRITY — checks IMKG did not report")
    add("=" * 74)
    rows = [
        ("unresolved series parents", f"{g['unresolved_series_parents']:,} "
                                      f"({g['unresolved_series_parent_pct']}%)"),
        ("frames w/o entry_type", f"{g['frames_without_entry_type']:,} "
                                  f"({g['frames_without_entry_type_pct']}%)"),
        ("frames w/o tags", f"{g['frames_without_tags']:,} "
                            f"({g['frames_without_tags_pct']}%)"),
        ("frames w/o either", f"{g['frames_without_either']:,}"),
        ("isolated nodes", f"{g['isolated_nodes']:,}"),
        ("unlabelled (excl. stubs)", f"{g['unlabelled_nodes']:,}"),
        ("duplicate edges", f"{g['duplicate_edges']:,}"),
        ("self loops", f"{g['self_loops']:,}"),
        ("dangling edge endpoints", f"{g['dangling_edge_sources']:,} src / "
                                    f"{g['dangling_edge_targets']:,} dst"),
        ("connected components", f"{g['connected_components']:,} "
                                 f"(largest {g['largest_component_pct']}%)"),
        ("components w/o concepts", f"{g['components_without_concepts']:,} "
                                    f"(largest "
                                    f"{g['largest_component_without_concepts']:,})"),
        ("series cycles", f"{g['series_cycles_found']:,}"),
        ("series max chain depth", f"{g['series_chain_max_depth']}"),
        ("series roots", f"{g['series_roots']:,}"),
        ("entry_types per frame", f"{g['entry_types_per_frame_mean']}"),
        ("tags per frame", f"{g['tags_per_frame_mean']}"),
        ("distinct tags", f"{g['distinct_tags']:,}"),
        ("tag degree max/p99/p90/p50", f"{g['tag_degree_max']} / "
                                       f"{g['tag_degree_p99']} / "
                                       f"{g['tag_degree_p90']} / "
                                       f"{g['tag_degree_p50']}"),
        ("tags used exactly once", f"{g['tags_used_once']:,}"),
    ]
    for label, val in rows:
        add(f"  {label:30} {val}")

    if g["series_cycles_found"]:
        add("")
        add("  !! partOfSeries cycles present — chain depth undefined.")
        for c in g["series_cycle_sample"]:
            add(f"     {' -> '.join(x[-40:] for x in c)}")

    add("")
    add("frame category distribution:")
    for k, v in list(g["category_distribution"].items())[:12]:
        add(f"  {k:24} {v:>10,}")
    add("frame status distribution:")
    for k, v in list(g["status_distribution"].items())[:12]:
        add(f"  {k:24} {v:>10,}")

    lay = m.get("layers")
    if lay:
        add("")
        add("=" * 74)
        add("LAYERS — what 6.x adds beyond the IMKG core, per frame")
        add("=" * 74)
        by_field = lay["frames_with_entity_link_by_field"]
        rows = [
            ("frames", f"{lay['frames']:,}"),
            ("with a Wikidata link", f"{lay['frames_with_entity_link']:,} "
                                     f"({lay['frames_with_entity_link_pct']}%)"),
            ("  by field (title/tags/About)", " / ".join(f"{by_field[t]:,}" for t in ENTITY_FRAME_EDGES)),
            ("with a template", f"{lay['frames_with_template']:,} ({lay['frames_with_template_pct']}%)"),
            ("  whose image shows an item", f"{lay['frames_with_template_entity']:,} "
                                            f"({lay['frames_with_template_entity_pct']}%)"),
            ("with events", f"{lay['frames_with_events']:,} ({lay['frames_with_events_pct']}%)"),
            ("with all three / none", f"{lay['frames_with_all_three']:,} / {lay['frames_with_none']:,}"),
            ("events per frame mean / median", f"{lay['events_per_frame_mean']} / "
                                               f"{lay['events_per_frame_median']}"),
            ("templates per frame (mean)", f"{lay['templates_per_frame_mean']}"),
            ("Wikidata items", f"{lay['wikidata_entities']:,}"),
            ("  from text only / images only / both", f"{lay['entities_from_text_only']:,} / "
                                                      f"{lay['entities_from_images_only']:,} / "
                                                      f"{lay['entities_from_both']:,}"),
            ("templates", f"{lay['templates']:,}"),
            ("  showing an item", f"{lay['templates_with_entity']:,} ({lay['templates_with_entity_pct']}%)"),
            ("  kept by more than one frame", f"{lay['templates_shared_by_frames']:,}"),
        ]
        for label, val in rows:
            add(f"  {label:38} {val}")
        add("top Wikidata items by frames linking them:")
        for row in lay["top_entities_by_frames"][:10]:
            add(f"  {row['frames']:>8,}  {row['label']}")
        add("top Wikidata items by templates showing them:")
        for row in lay["top_entities_by_templates"][:10]:
            add(f"  {row['templates']:>8,}  {row['label']}")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from-csv", action="store_true",
                    help="Read exported CSVs instead of Mongo.")
    ap.add_argument("--nodes", default="kg_view_nodes.csv")
    ap.add_argument("--edges", default="kg_view_edges.csv")
    ap.add_argument("--triple-equivalent", action="store_true",
                    help="Count populated node attributes as edges, for a "
                         "like-for-like comparison with IMKG's RDF counts.")
    ap.add_argument("--top-k", type=int, default=20)
    ap.add_argument("--build", default=None,
                    help="Build id to measure (default: the published one).")
    ap.add_argument("--scope", choices=SCOPES, default="core",
                    help="core = the IMKG-comparable core (what kym_kg measures); "
                         "full = the whole graph, plus the LAYERS block.")
    ap.add_argument("--out", default=None, help="Write full metrics JSON here.")
    args = ap.parse_args()

    build_id = None
    if args.from_csv:
        nodes, edges = load_from_csv(args.nodes, args.edges)
    else:
        from modules import kg_store
        build_id = args.build or kg_store.current_build_id()
        if not build_id:
            raise SystemExit("No published build and no --build given.")
        nodes, edges = load_build(build_id, args.scope)

    m = compute_metrics(nodes, edges, args.triple_equivalent, args.top_k)
    m["scope"], m["build_id"] = args.scope, build_id
    if args.scope == "full":
        m["layers"] = layer_metrics(nodes, edges, args.top_k)
    print(format_report(m))

    if args.out:
        with open(args.out, "w") as f:
            json.dump(m, f, indent=2)
        print(f"\nfull metrics -> {args.out}")


if __name__ == "__main__":
    main()