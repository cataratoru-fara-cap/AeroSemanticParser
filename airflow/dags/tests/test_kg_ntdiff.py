"""kg/ntdiff.py: set difference over two N-Triples files, the gate that exists
because the in-process and RML derivations drifted 14,563 mk:relatesToMeme
triples apart unnoticed for two months. Load-bearing: order and repeats are not
differences (RDF is a set); a line diff would report both and be abandoned as
noisy within a week."""
import pytest

from modules.kg import ntdiff

S = "<https://knowyourmeme.com/memes/doge>"
P = "<https://meme4.science/atlas/hasTag>"
TRIPLES = [f'{S} {P} "doge" .', f'{S} {P} "shiba" .',
           f'{S} <https://meme4.science/atlas/partOfSeries> <https://knowyourmeme.com/memes/shiba-inu> .']
Q = f"<< {S} <https://meme4.science/atlas/citesExternal> <https://en.wikipedia.org/wiki/Doge> >>"
A = "<https://meme4.science/atlas/anchorText>"
XSD = "http://www.w3.org/2001/XMLSchema#"
norm = ntdiff.normalize_line


# -- normalisation ----------------------------------------------------------------------

@pytest.mark.parametrize("a, b", [
    (f'{S}   {P}    "doge" .', f'{S} {P} "doge" .'),                       # whitespace between terms
    (f'{S} {P} "doge"^^<{XSD}string> .', f'{S} {P} "doge" .'),             # explicit xsd:string = plain
    # morph-kgc writes TAB raw, kg/rdf.py writes \t: one term
    (f'{S} {P} "a\tb" .', f'{S} {P} "a\\tb" .'),
    (f'{S} {P} "\\u00E9" .', f'{S} {P} "é" .'),
    (f'{Q} {A} "a\\tb \\"c\\"" .', f'{Q}  {A}\t"a\tb \\"c\\""  .'),        # ours and morph-kgc's annotation
])
def test_spellings_of_one_triple_normalise_equal(a, b):
    assert norm(a) == norm(b)


@pytest.mark.parametrize("a, b", [
    # inside a literal whitespace is the value: collapsing it merged two real triples
    (f'{S} {P} "doge  meme" .', f'{S} {P} "doge meme" .'),
    (f'{Q} {A} "a  b" .', f'{Q} {A} "a b" .'),
])
def test_different_values_stay_different(a, b):
    assert norm(a) != norm(b)


@pytest.mark.parametrize("line, kept", [
    (f'{S} {P} "doge  meme" .', '"doge  meme"'), (f'{S} {P} "a\\tb" .', '"a\tb"'),   # the TAB survives decoded
    (f'{S} {P} "doge"@en .', '"doge"@en'), (f'{S} {P} "3"^^<{XSD}integer> .', "XMLSchema#integer"),
    (f'{S} {P} "q\\"uote back\\\\slash nl\\n" .', '"q\\"uote back\\\\slash nl\\n"'),   # required escapes canonical
])
def test_what_normalisation_keeps(line, kept):
    assert kept in norm(line)


def test_normalised_line_shapes():
    assert norm("   ") is None and norm("# a comment") is None
    assert norm(f'{S} {P} "x" .').endswith(" .") and not norm(f'{S} {P} "x" .').endswith(". .")
    assert norm(f'{S} {P} "a\\tb"^^<{XSD}integer> .').endswith(f"^^<{XSD}integer> .")
    assert norm(f'{S} {P} "two words" .') == f'{S} {P} "two words" .'          # one term


@pytest.mark.parametrize("line, pred", [
    (TRIPLES[0], "hasTag"), ("<a> <http://www.w3.org/2004/02/skos/core#broader> <b> .", "broader"),
    ("nonsense", "(unparsed)"), (norm(f'{Q} {A} "wiki" .'), "anchorText"), (norm(f"{S} {P} {S} ."), "hasTag"),
])
def test_the_predicate_local_name(line, pred):
    assert ntdiff.predicate_of(line) == pred


# -- digest and diff ----------------------------------------------------------------------------

@pytest.fixture
def nt(tmp_path):
    def write(name, lines):
        path = tmp_path / name
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return str(path)
    return write


def test_digest_is_a_set(nt):
    a = ntdiff.digest(nt("a.nt", TRIPLES))
    assert a[:2] == ntdiff.digest(nt("b.nt", list(reversed(TRIPLES))))[:2]           # order
    assert a[:2] == ntdiff.digest(nt("c.nt", TRIPLES + [TRIPLES[0]]))[:2]            # repeats, counted once
    assert a[0] != ntdiff.digest(nt("d.nt", TRIPLES[:2]))[0]
    assert a[1:] == (3, {"hasTag": 2, "partOfSeries": 1})
    # a quoted subject is not mistaken for a blank node
    assert ntdiff.digest(nt("q.nt", [f'{Q} {A} "wiki" .']))[1:] == (1, {"anchorText": 1})


def test_identical_and_divergent_files(nt):
    a = nt("a.nt", TRIPLES)
    report = ntdiff.diff(a, nt("b.nt", list(reversed(TRIPLES))))
    assert report["equal"] and report["in-process"]["triples"] == 3
    assert "identical" in ntdiff.format_report(ntdiff.diff(a, a))
    report = ntdiff.diff(a, nt("c.nt", TRIPLES[:1] + [
        f'{S} {P} "different" .', f'{S} <https://meme4.science/atlas/citesMediaFrame> <https://knowyourmeme.com/memes/cheems> .']),
        buckets=8)
    by = report["by_predicate"]
    assert not report["equal"]
    assert (by["hasTag"]["only_in_in-process"], by["hasTag"]["only_in_rml"], by["partOfSeries"]["only_in_in-process"],
            by["citesMediaFrame"]["only_in_rml"]) == (1, 1, 1, 1)
    text = ntdiff.format_report(ntdiff.diff(a, nt("d.nt", TRIPLES[:1]), buckets=4))
    assert "DIVERGENT" in text and "hasTag" in text


def test_samples_are_capped_and_buckets_do_not_change_the_answer(nt):
    report = ntdiff.diff(nt("a.nt", [f'{S} {P} "t{i}" .' for i in range(40)]), nt("b.nt", [f'{S} {P} "t0" .']),
                         buckets=8, samples=5)
    assert len(report["only_in_in-process"]) == 5 and report["first_divergent_predicate"] == "hasTag"
    a, b = nt("c.nt", [f'{S} {P} "t{i}" .' for i in range(50)]), nt("d.nt", [f'{S} {P} "t{i}" .' for i in range(25)])
    assert ntdiff.diff(a, b, buckets=2)["by_predicate"]["hasTag"]["only_in_in-process"] == \
        ntdiff.diff(a, b, buckets=64)["by_predicate"]["hasTag"]["only_in_in-process"] == 25


def test_blank_nodes_are_refused_rather_than_mishandled(nt):
    with pytest.raises(ValueError, match="blank node"):
        ntdiff.diff(nt("a.nt", ["_:b0 <http://p> <http://o> ."]), nt("b.nt", TRIPLES))
