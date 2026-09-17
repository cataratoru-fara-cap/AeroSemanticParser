"""
kg/ntdiff.py — memory-bounded set difference over two N-Triples files
========================================================================
Pure: no Mongo, no Airflow, no external tools. Compares the in-process
serialization (kg/rdf.py) against the output of the RML/morph-kgc
validation path, and reports where they disagree.

Why this exists
---------------
The two derivations of this graph drifted 14,563 ``mk:relatesToMeme``
triples apart and nobody noticed for two months, because nothing ever
compared them. A second, independent derivation is only worth maintaining
if something checks it.

Why it is not `sort -u | comm -3`
---------------------------------
That works, and is faster, but it makes the gate depend on GNU coreutils
behaviour inside a container and gives a line-oriented answer where the
useful answer is per-predicate. This is O(n) rather than O(n log n), holds
a bounded amount of memory regardless of graph size, and reports in the
vocabulary the reader thinks in.

Two stages, cheap first
-----------------------
1. **Digest.** Stream each file once, normalising as it goes, and
   accumulate an order-independent combiner (XOR over per-line digests)
   plus per-predicate counts. Two sequential reads, O(1) memory. If the
   digests match, the files are equal as SETS and there is nothing to do.
2. **Bucket diff**, only on mismatch. Hash each normalised line into one of
   ``buckets`` spill files and compare bucket by bucket as in-memory sets.
   At 256 buckets and 800k triples that is ~3,100 lines (~500 KB) resident
   at a time.

Set semantics is the whole point: RDF is a set, so line order and repeats
are not differences. Both sides are normalised before anything is compared.

No blank nodes
--------------
``kg_mapping.yarrrml.yml`` states there are none anywhere, and kg/rdf.py emits
none. That is what makes a purely syntactic comparison sound — with blank
nodes this would need graph isomorphism, not a set difference. If blank
nodes are ever introduced, this module stops being correct and must be told
so rather than quietly producing nonsense; ``diff`` raises on encountering
one.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections import Counter
from typing import Any, Iterator

__all__ = ["normalize_line", "predicate_of", "digest", "diff", "format_report"]

_PREDICATE = re.compile(r">\s+<([^>]+)>\s")
_XSD_STRING = "^^<http://www.w3.org/2001/XMLSchema#string>"

# One N-Triples term: an IRI, or a quoted literal with its optional datatype
# or language tag, or (defensively) any non-space run. Terms are matched
# rather than split on whitespace because a literal may legally CONTAIN
# whitespace, and that whitespace is significant.
_TERM = re.compile(
    r'<[^>]*>'                                   # IRI
    r'|"(?:[^"\\]|\\.)*"(?:\^\^<[^>]*>|@[A-Za-z0-9-]+)?'   # literal
    r'|\S+'                                      # fallback
)


_LITERAL = re.compile(r'"((?:[^"\\]|\\.)*)"(.*)\Z', re.S)
_ESCAPE = re.compile(r'\\(?:u([0-9A-Fa-f]{4})|U([0-9A-Fa-f]{8})|(.))', re.S)
_ECHAR = {"t": "\t", "b": "\b", "n": "\n", "r": "\r", "f": "\f",
          '"': '"', "'": "'", "\\": "\\"}


def _unescape(body: str) -> str:
    if "\\" not in body:
        return body

    def repl(m: re.Match) -> str:
        if m.group(1):
            return chr(int(m.group(1), 16))
        if m.group(2):
            return chr(int(m.group(2), 16))
        return _ECHAR.get(m.group(3), "\\" + m.group(3))
    return _ESCAPE.sub(repl, body)


def canonical_literal(term: str) -> str:
    """One literal term, re-serialised in a single canonical escaping.

    N-Triples lets a serializer escape a character or write it raw — TAB
    may appear as ``\\t`` or as a literal tab, and any character may be a
    ``\\uXXXX`` escape — and those are the SAME RDF term. Serializers
    disagree: kg/rdf.py writes ``\\t``, morph-kgc writes a raw tab. Comparing
    lines as text would report every paragraph containing a tab as a
    divergence, so a literal's lexical value is decoded and re-encoded with
    only the four characters N-Triples requires escaped (backslash, quote,
    LF, CR). Datatype and language suffixes are kept as they are.
    """
    m = _LITERAL.match(term)
    if not m:
        return term
    body = _unescape(m.group(1))
    body = (body.replace("\\", "\\\\").replace('"', '\\"')
                .replace("\n", "\\n").replace("\r", "\\r"))
    return f'"{body}"{m.group(2)}'


def normalize_line(line: str) -> str | None:
    """Canonicalise one N-Triples line, or None if it carries no triple.

    Whitespace *between* terms and a trailing explicit ``xsd:string`` type
    are not semantic differences — a plain literal and one typed xsd:string
    denote the same RDF term, and serializers disagree about which to emit.
    Normalising those away stops the gate reporting a non-difference. The
    same goes for how a literal's characters are escaped; see
    ``canonical_literal``.

    Whitespace *inside* a quoted literal is a different matter: it is part
    of the value. An earlier version of this function did
    ``" ".join(line.split())``, which collapsed runs of spaces inside
    literals too — enough to merge two distinct triples into one on the real
    corpus. A gate that silently merges genuine differences is worse than no
    gate, so terms are tokenised instead of split.
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if line.endswith("."):
        line = line[:-1].rstrip()
    terms = []
    for term in _TERM.findall(line):
        if term.endswith(_XSD_STRING):
            term = term[:-len(_XSD_STRING)]
        if term.startswith('"'):
            term = canonical_literal(term)
        terms.append(term)
    if not terms:
        return None
    return " ".join(terms) + " ."


def predicate_of(triple: str) -> str:
    """The predicate's local name, for reporting."""
    match = _PREDICATE.search(triple)
    if not match:
        return "(unparsed)"
    iri = match.group(1)
    return iri.rsplit("/", 1)[-1].rsplit("#", 1)[-1]


def _iter_normalized(path: str) -> Iterator[str]:
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.lstrip().startswith("_:") or " _:" in line:
                raise ValueError(
                    f"{path} contains a blank node; ntdiff compares triples as "
                    "a set, which is only sound for a ground graph")
            triple = normalize_line(line)
            if triple is not None:
                yield triple


def digest(path: str) -> tuple[str, int, dict[str, int]]:
    """(set digest, distinct triples, per-predicate counts). O(1) memory.

    The combiner is an XOR over per-line sha256 digests, so it depends on
    the SET of lines and not their order. Duplicate lines XOR to nothing and
    are counted once, which is the correct reading for RDF.
    """
    combined = bytearray(32)
    seen_hashes: set[bytes] = set()
    counts: Counter[str] = Counter()
    for triple in _iter_normalized(path):
        h = hashlib.sha256(triple.encode("utf-8")).digest()
        if h in seen_hashes:
            continue            # a set, so a repeat is not a difference
        seen_hashes.add(h)
        for i, byte in enumerate(h):
            combined[i] ^= byte
        counts[predicate_of(triple)] += 1
    return bytes(combined).hex(), len(seen_hashes), dict(counts)


def _spill(path: str, workdir: str, tag: str, buckets: int) -> list[str]:
    paths = [os.path.join(workdir, f"{tag}.{i:03d}") for i in range(buckets)]
    handles = [open(p, "w", encoding="utf-8") for p in paths]
    try:
        for triple in _iter_normalized(path):
            index = hashlib.blake2b(triple.encode("utf-8"),
                                    digest_size=4).digest()[0] % buckets
            handles[index].write(triple + "\n")
    finally:
        for fh in handles:
            fh.close()
    return paths


def diff(path_a: str, path_b: str, *, label_a: str = "in-process",
         label_b: str = "rml", workdir: str | None = None,
         buckets: int = 256, samples: int = 10) -> dict[str, Any]:
    """Compare two N-Triples files as sets. Returns a report dict."""
    digest_a, count_a, preds_a = digest(path_a)
    digest_b, count_b, preds_b = digest(path_b)

    report: dict[str, Any] = {
        # Both conditions, not just the digest. The XOR combiner is
        # order-independent by construction, which is what we want, but that
        # also means it is not a cryptographic commitment to the set. The
        # count is free and closes the gap.
        "equal": digest_a == digest_b and count_a == count_b,
        label_a: {"path": path_a, "triples": count_a, "digest": digest_a},
        label_b: {"path": path_b, "triples": count_b, "digest": digest_b},
        "by_predicate": {},
        f"only_in_{label_a}": [],
        f"only_in_{label_b}": [],
        "first_divergent_predicate": None,
    }
    if report["equal"]:
        return report

    owned = workdir is None
    workdir = workdir or tempfile.mkdtemp(prefix="ntdiff.")
    try:
        bucket_a = _spill(path_a, workdir, "a", buckets)
        bucket_b = _spill(path_b, workdir, "b", buckets)

        only_a: Counter[str] = Counter()
        only_b: Counter[str] = Counter()
        sample_a: list[str] = []
        sample_b: list[str] = []
        for pa, pb in zip(bucket_a, bucket_b):
            with open(pa, encoding="utf-8") as fh:
                set_a = {line.rstrip("\n") for line in fh}
            with open(pb, encoding="utf-8") as fh:
                set_b = {line.rstrip("\n") for line in fh}
            for triple in set_a - set_b:
                only_a[predicate_of(triple)] += 1
                if len(sample_a) < samples:
                    sample_a.append(triple)
            for triple in set_b - set_a:
                only_b[predicate_of(triple)] += 1
                if len(sample_b) < samples:
                    sample_b.append(triple)
    finally:
        if owned:
            for name in os.listdir(workdir):
                os.unlink(os.path.join(workdir, name))
            os.rmdir(workdir)

    by_predicate = {}
    for predicate in sorted(set(preds_a) | set(preds_b)):
        by_predicate[predicate] = {
            label_a: preds_a.get(predicate, 0),
            label_b: preds_b.get(predicate, 0),
            "delta": preds_a.get(predicate, 0) - preds_b.get(predicate, 0),
            f"only_in_{label_a}": only_a.get(predicate, 0),
            f"only_in_{label_b}": only_b.get(predicate, 0),
        }

    divergent = [p for p, v in by_predicate.items()
                 if v[f"only_in_{label_a}"] or v[f"only_in_{label_b}"]]
    report["by_predicate"] = by_predicate
    report[f"only_in_{label_a}"] = sorted(sample_a)
    report[f"only_in_{label_b}"] = sorted(sample_b)
    report["first_divergent_predicate"] = divergent[0] if divergent else None
    report["divergent_predicates"] = divergent
    return report


def format_report(report: dict[str, Any], *, label_a: str = "in-process",
                  label_b: str = "rml") -> str:
    """Human-readable rendering, for a task log."""
    if report["equal"]:
        return (f"RDF diff: identical — {report[label_a]['triples']} triples, "
                f"digest {report[label_a]['digest'][:16]}…")

    lines = [
        f"RDF diff: DIVERGENT",
        f"  {label_a:<12} {report[label_a]['triples']:>9} triples",
        f"  {label_b:<12} {report[label_b]['triples']:>9} triples",
        "",
        f"  {'predicate':<20}{label_a:>12}{label_b:>10}{'only A':>9}{'only B':>9}",
    ]
    for predicate, v in report["by_predicate"].items():
        if not (v[f"only_in_{label_a}"] or v[f"only_in_{label_b}"]):
            continue
        lines.append(f"  {predicate:<20}{v[label_a]:>12}{v[label_b]:>10}"
                     f"{v[f'only_in_{label_a}']:>9}{v[f'only_in_{label_b}']:>9}")
    for label in (label_a, label_b):
        sample = report.get(f"only_in_{label}") or []
        if sample:
            lines.append(f"\n  only in {label}, {len(sample)} sample(s):")
            lines.extend(f"    {t[:150]}" for t in sample)
    return "\n".join(lines)
