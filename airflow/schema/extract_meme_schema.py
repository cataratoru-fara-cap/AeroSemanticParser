#!/usr/bin/env python3
"""
extract_meme_schema.py
Extract a minimum validating schema (ShEx / SHACL) for all Wikidata
instances of "internet meme" (wd:Q2927074) using sheXer.

Two modes:
  dump      (default) Download all truthy triples of the memes into a local
            .nt file with paginated SPARQL CONSTRUCT queries, then run
            sheXer on the file. Fast, repeatable, gentle on WDQS.
  endpoint  Let sheXer query the Wikidata SPARQL endpoint directly
            (one subgraph-building query per instance). Simpler but much
            slower; use --cap for a quick sanity check.

Usage examples:
  python extract_meme_schema.py                          # dump mode, ShEx out
  python extract_meme_schema.py --threshold 0.25 --shacl # also emit SHACL
  python extract_meme_schema.py --mode endpoint --cap 50 # quick smoke test
  python extract_meme_schema.py --skip-download          # reuse memes_truthy.nt

Requires: pip install shexer requests
Tested with shexer 2.7.3.1.
"""

import argparse
import sys
import time

import requests
from shexer.consts import NT, SHACL_TURTLE
from shexer.shaper import Shaper

# ---------------------------------------------------------------- constants

WDQS = "https://query.wikidata.org/sparql"
WD = "http://www.wikidata.org/entity/"
WDT = "http://www.wikidata.org/prop/direct/"

MEME_CLASS = WD + "Q2927074"  # internet meme
P31 = WDT + "P31"             # instance of

# WDQS policy requires a descriptive User-Agent with contact info.
# TODO: put your own contact info / repo URL here.
USER_AGENT = "MemeAtlasSchemaExtractor/0.1 (AeroSemanticParser; contact: cristian.vaireanu@insa-lyon.fr)"

DUMP_FILE = "memes_truthy.nt"
BATCH_SIZE = 200          # QIDs per CONSTRUCT query
SLEEP_BETWEEN_QUERIES = 1  # seconds; be polite to WDQS

NAMESPACES = {
    WD: "wd",
    WDT: "wdt",
    "http://www.w3.org/2001/XMLSchema#": "xsd",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf",
    "http://www.w3.org/2000/01/rdf-schema#": "rdfs",
    "http://weso.es/shapes/": "shapes",
}

# ----------------------------------------------------------------- download


def run_sparql(query: str, accept: str, max_retries: int = 5) -> requests.Response:
    """POST a query to WDQS with retry/backoff on 429 and 5xx."""
    headers = {"User-Agent": USER_AGENT, "Accept": accept}
    for attempt in range(max_retries):
        resp = requests.post(WDQS, data={"query": query}, headers=headers, timeout=120)
        if resp.status_code == 200:
            return resp
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = int(resp.headers.get("Retry-After", 2 ** (attempt + 1)))
            print(f"  WDQS returned {resp.status_code}, retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue
        resp.raise_for_status()
    raise RuntimeError(f"WDQS kept failing after {max_retries} retries")


def fetch_meme_qids() -> list[str]:
    """Return the full IRIs of every instance of internet meme."""
    query = """
    SELECT ?item WHERE { ?item wdt:P31 wd:Q2927074 . }
    """
    resp = run_sparql(query, accept="application/sparql-results+json")
    bindings = resp.json()["results"]["bindings"]
    qids = [b["item"]["value"] for b in bindings]
    print(f"Found {len(qids)} instances of internet meme (wd:Q2927074).")
    return qids


def download_truthy_triples(qids: list[str], out_path: str) -> None:
    """
    Download all *truthy* triples (predicates in wdt:) of the given entities
    into an n-triples file, in batches. Truthy triples are enough for a
    minimum schema and avoid dragging in labels in 300 languages, sitelinks,
    and statement/reference nodes.
    """
    with open(out_path, "w", encoding="utf-8") as f:
        for i in range(0, len(qids), BATCH_SIZE):
            batch = qids[i:i + BATCH_SIZE]
            values = " ".join(f"<{qid}>" for qid in batch)
            # STRSTARTS on ".../prop/direct/" keeps wdt: only; it excludes
            # prop/direct-normalized/ (wdtn:) because of the trailing slash.
            query = f"""
            CONSTRUCT {{ ?item ?p ?o }}
            WHERE {{
              VALUES ?item {{ {values} }}
              ?item ?p ?o .
              FILTER(STRSTARTS(STR(?p), "{WDT}"))
            }}
            """
            resp = run_sparql(query, accept="application/n-triples")
            f.write(resp.text)
            if not resp.text.endswith("\n"):
                f.write("\n")
            done = min(i + BATCH_SIZE, len(qids))
            print(f"  downloaded triples for {done}/{len(qids)} entities")
            time.sleep(SLEEP_BETWEEN_QUERIES)
    print(f"Dump written to {out_path}")

# --------------------------------------------------------------- extraction


def build_shaper(args: argparse.Namespace) -> Shaper:
    common = dict(
        target_classes=[MEME_CLASS],
        namespaces_dict=NAMESPACES,
        instantiation_property=P31,  # Wikidata uses wdt:P31, not rdf:type
        # False => constraints keep their real cardinality, so
        # acceptance_threshold can actually prune rare properties.
        # With the default (True) every rare property survives as
        # "wdt:Pxxx ? " and you never get a *minimum* schema.
        all_instances_are_compliant_mode=args.all_compliant,
        # Annotate P/Q ids with their English labels in comments
        # (needs internet; uses wLighter under the hood).
        wikidata_annotation=args.annotate,
    )
    if args.mode == "endpoint":
        return Shaper(
            url_endpoint=WDQS,
            depth_for_building_subgraph=1,
            instances_cap=args.cap,  # -1 = no cap
            **common,
        )
    return Shaper(graph_file_input=args.dump_file, input_format=NT, **common)


def _serialize(shaper, args, out_path, output_format=None):
    """
    Get the schema as a string and write it ourselves.

    We never pass output_file to sheXer, because with wikidata_annotation=True
    it forwards the path to wlighter, whose RawCommentsFormatter.set_up() calls
    open(path, "wa") -- an invalid mode -> ValueError. The string_output path
    leaves out_file=None and skips that open() call. (wlighter 1.0.2)
    """
    kwargs = dict(
        string_output=True,
        acceptance_threshold=args.threshold,
        verbose=args.verbose,
    )
    if output_format is not None:
        kwargs["output_format"] = output_format

    schema = shaper.shex_graph(**kwargs)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(schema)
    print(f"schema written to {out_path}")


def extract_schema(shaper: Shaper, args: argparse.Namespace) -> None:
    _serialize(shaper, args, args.out + ".shex")
    if args.shacl:
        _serialize(shaper, args, args.out + ".ttl", output_format=SHACL_TURTLE)

# --------------------------------------------------------------------- main


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["dump", "endpoint"], default="dump")
    p.add_argument("--threshold", type=float, default=0.25,
                   help="min ratio of instances a constraint must cover to be "
                        "kept (sheXer acceptance_threshold; default 0.25)")
    p.add_argument("--shacl", action="store_true",
                   help="also serialize the schema as SHACL turtle")
    p.add_argument("--annotate", action="store_true",
                   help="add English labels of P/Q ids as comments (extra "
                        "Wikidata requests)")
    p.add_argument("--all-compliant", action="store_true",
                   help="keep every property with relaxed cardinality instead "
                        "of pruning below the threshold")
    p.add_argument("--cap", type=int, default=-1,
                   help="endpoint mode: max instances to sample (-1 = all)")
    p.add_argument("--dump-file", default=DUMP_FILE)
    p.add_argument("--skip-download", action="store_true",
                   help="dump mode: reuse an existing dump file")
    p.add_argument("--out", default="meme_schema",
                   help="output file prefix (default: meme_schema)")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.mode == "dump" and not args.skip_download:
        qids = fetch_meme_qids()
        download_truthy_triples(qids, args.dump_file)

    shaper = build_shaper(args)
    extract_schema(shaper, args)


if __name__ == "__main__":
    main()