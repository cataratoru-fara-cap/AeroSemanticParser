"""
kym_kg_validate_dag.py — the RDF diff gate: re-derive the graph, compare
==========================================================================
Top-level orchestration ONLY. Re-derives a published KG build's RDF through
an INDEPENDENT path — the YARRRML mapping in kg_config/, compiled by yatter,
materialised by morph-kgc over the build's rml_data/*.csv — and diffs it
against the graph.nt that kg/rdf.py serialized in-process.

Why a second derivation is worth maintaining
--------------------------------------------
The two paths drifted 14,563 ``mk:relatesToMeme`` triples apart for two
months and nothing noticed, because nothing compared them. This DAG is the
comparison. In steady state the two outputs are equal modulo four
provenance predicates the RML path has no way to know about (buildId etc.),
and that whitelist is the ONLY tolerated difference: a single content
triple on either side is a failure.

Why its own DAG
---------------
morph-kgc + yatter pull ~60 MB including native duckdb/pyoxigraph wheels
and take about a minute on 800k triples. That does not belong inside every
build. It runs @weekly against the published build and can be triggered by
hand against any build id.

Verdict, not veto
-----------------
A failure marks the build's ``validation`` record and shows red on the
dashboard. It does NOT un-publish: the in-process serialization is the
product, and retroactively yanking a live graph is worse than flagging it.
Set ``require_rdf_diff=True`` on kym_kg if you want the gate to block
publication instead.

Pipeline:
    resolve_build     param build_id, else the published one
    compile_mapping   yatter: kg_mapping.yarrrml.yml -> <build>/kg_mapping.rml.ttl
    run_morph_kgc     cwd=<build> so the mapping's relative rml_data/ paths
                      resolve; na_values EMPTY (see below)
    diff              kg/ntdiff.py, memory-bounded set difference
    report            <build>/rdf_diff.json, kg_builds.validation, run_summaries

na_values is deliberately empty. The original morph-kgc config inherited
pandas' NA list, which treats the literal string "null" as missing — and two
real KYM tags ARE the string "null". A mapping toolchain must not apply CSV
heuristics to a curated corpus; that alone silently deleted two triples.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import timedelta

from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowSkipException

from modules import kg_store as store

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "gabi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

KG_DATA_DIR = os.getenv("KG_DATA_DIR", "/opt/airflow/data/kg")
KG_CONFIG_DIR = os.getenv("KG_CONFIG_DIR", "/opt/airflow/dags/kg_config")
MAPPING_SRC = os.path.join(KG_CONFIG_DIR, "kg_mapping.yarrrml.yml")

PROVENANCE_PREDICATES = frozenset(
    {"buildId", "snapshotAt", "kgBuildVersion", "taxonomyVersion"})

#
# number_of_processes is capped because morph-kgc defaults to one process
# per CPU (16 on this host), each holding its own slice of the data, and the
# worker container is capped (2 GiB then, 3 GiB now). Measured on the 6.0.0 full build
# (2026-09-18): the default peaked at 1.97 GiB — hitting the cgroup limit
# 10,163 times — and in the DAG it stalled for the whole 20-minute task
# timeout with 4 of 96 rules unfinished. 8 processes peaked at 1.80 GiB in
# 14 s; 4 peaked at 1.21 GiB in 17 s. The 5.1.0 lifting of the Origin/Spread
# deferral grew the graph ~17%, which is what tipped a run that had been
# sitting at the edge. Three seconds is a cheap price for the headroom.
MORPH_INI = """[CONFIGURATION]
output_file = rml_output.nt
output_format = N-TRIPLES
na_values =
number_of_processes = 4

[DataSource1]
mappings = kg_mapping.rml.ttl
"""


def _build_dir(build_id: str) -> str:
    return os.path.join(KG_DATA_DIR, "builds", build_id)


def _run(cmd: list[str], cwd: str, what: str) -> str:
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    tail = (proc.stdout + proc.stderr)[-4000:]
    if proc.returncode != 0:
        raise RuntimeError(f"{what} failed (exit {proc.returncode}):\n{tail}")
    return tail


@dag(
    dag_id="kym_kg_validate",
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["memeatlas", "kym", "kg", "validate"],
    params={
        "build_id": Param("", type="string",
                          description="Build to validate; empty = the published one"),
        "fail_on_divergence": Param(True, type="boolean",
                                    description="Fail the run (not the build) on content divergence"),
    },
)
def kym_kg_validate_dag():

    @task
    def resolve_build(params: dict | None = None) -> dict:
        bid = (params or {}).get("build_id") or ""
        if not bid:
            current = store.current_build()
            if not current:
                raise RuntimeError("no published KG build to validate")
            bid = current["build_id"]
        build_dir = _build_dir(bid)
        for required in ("graph.nt", "manifest.json", "rml_data"):
            if not os.path.exists(os.path.join(build_dir, required)):
                raise RuntimeError(f"build {bid} is missing {required} in {build_dir}")

        # The gate compares two derivations of ONE build, so the mapping and
        # the build have to be the same generation. Validating an older
        # build with today's mapping is meaningless, and fails deep inside
        # morph-kgc ("No such file: rml_data/events.csv" — the weekly run of
        # 2026-09-20, against a 5.0.1 build, once the event layer landed).
        # Skipped rather than failed: nothing is wrong with either the build
        # or the mapping, they are just from different versions.
        from modules.kg import build as kg_build
        from modules.kg import serialize
        manifest = serialize.load_manifest(build_dir)
        built_with = (manifest.get("stamps") or {}).get("kg_build_version")
        missing = sorted(name for name in serialize.all_rml_files()
                         if not os.path.exists(
                             os.path.join(build_dir, serialize.RML_DIR, name)))
        if missing:
            raise AirflowSkipException(
                f"build {bid} was written by KG {built_with or 'an older version'} "
                f"and the mapping now reads {len(missing)} source(s) it never "
                f"wrote ({', '.join(missing[:3])}...). Validate a build made by "
                f"KG {kg_build.KG_BUILD_VERSION}.")
        log.info("Validating build %s (KG %s) in %s", bid, built_with, build_dir)
        return {"build_id": bid, "build_dir": build_dir}

    @task(execution_timeout=timedelta(minutes=5))
    def compile_mapping(target: dict) -> dict:
        """yatter refuses any extension but .yml/.yaml, hence the file name."""
        out = os.path.join(target["build_dir"], "kg_mapping.rml.ttl")
        _run([sys.executable, "-m", "yatter", "-i", MAPPING_SRC, "-o", out],
             cwd=target["build_dir"], what="yatter")
        with open(out, encoding="utf-8") as fh:
            rules = sum(1 for line in fh if "TriplesMap" in line)
        log.info("Compiled %s -> %s (%d TriplesMaps)", MAPPING_SRC, out, rules)
        return {**target, "mapping_ttl": out, "triples_maps": rules}

    @task(execution_timeout=timedelta(minutes=20))
    def run_morph_kgc(compiled: dict) -> dict:
        ini = os.path.join(compiled["build_dir"], "morph_kgc.ini")
        with open(ini, "w", encoding="utf-8") as fh:
            fh.write(MORPH_INI)
        tail = _run([sys.executable, "-m", "morph_kgc", "morph_kgc.ini"],
                    cwd=compiled["build_dir"], what="morph-kgc")
        out = os.path.join(compiled["build_dir"], "rml_output.nt")
        if not os.path.exists(out):
            raise RuntimeError(f"morph-kgc produced no {out}\n{tail}")
        log.info("morph-kgc finished: %s", tail.strip().splitlines()[-1:])
        return {**compiled, "rml_nt": out}

    @task(execution_timeout=timedelta(minutes=15))
    def diff(materialised: dict, params: dict | None = None) -> dict:
        from modules.kg import ntdiff
        graph = os.path.join(materialised["build_dir"], "graph.nt")
        report = ntdiff.diff(graph, materialised["rml_nt"],
                             label_a="in-process", label_b="rml")
        divergent = set(report.get("divergent_predicates") or [])
        content = sorted(divergent - PROVENANCE_PREDICATES)
        verdict = {
            "build_id": materialised["build_id"],
            "equal_modulo_provenance": not content,
            "content_divergence": content,
            "provenance_only": sorted(divergent & PROVENANCE_PREDICATES),
            "in_process_triples": report["in-process"]["triples"],
            "rml_triples": report["rml"]["triples"],
            "by_predicate": {p: v for p, v in report["by_predicate"].items()
                             if p in divergent},
            "samples": {"only_in_in-process": report["only_in_in-process"],
                        "only_in_rml": report["only_in_rml"]},
        }
        log.info("\n%s", ntdiff.format_report(report, label_a="in-process",
                                                label_b="rml"))
        return verdict

    # min_one_success, not none_failed: "none_failed" also fires when the
    # upstream SKIPPED, so the day resolve_build first skipped (an older
    # published build against today's mapping) report ran with no verdict
    # and failed — turning a correct skip into a red run.
    @task(trigger_rule="none_failed_min_one_success")
    def report(verdict: dict, params: dict | None = None,
               run_id: str | None = None) -> str:
        from modules import summary_store
        out = os.path.join(_build_dir(verdict["build_id"]), "rdf_diff.json")
        with open(out + ".tmp", "w", encoding="utf-8") as fh:
            json.dump(verdict, fh, indent=2, default=str)
        os.replace(out + ".tmp", out)
        store.record_validation(verdict["build_id"], {
            "equal": verdict["equal_modulo_provenance"],
            "content_divergence": verdict["content_divergence"],
            "in_process_triples": verdict["in_process_triples"],
            "rml_triples": verdict["rml_triples"],
            "report": out,
        })
        doc_id = summary_store.save_summary(
            stage="kg_validate", dag_id="kym_kg_validate",
            run_id=run_id or "manual", summary=verdict)
        if verdict["content_divergence"] and (params or {}).get(
                "fail_on_divergence", True):
            raise RuntimeError(
                "RDF derivations disagree on content predicates "
                f"{verdict['content_divergence']} — see {out}")
        return doc_id

    target = resolve_build()
    compiled = compile_mapping(target)
    materialised = run_morph_kgc(compiled)
    verdict = diff(materialised)
    report(verdict)


kym_kg_validate_dag()
