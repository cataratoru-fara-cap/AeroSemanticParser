"""kg/wikidata.py: a Wikidata dump -> the local entity lexicon. The fixture
(wikidata_fixture.py) is a dump in the real JSON-lines layout, so the builder
under test is the one that reads the 156 GB file.

Pinned beyond "it works": a disambiguation page is never a candidate; an item
no Wikipedia has is kept for its KYM slug (a meme is often on Wikidata and KYM
and nowhere else); a `mul`-only label counts as English; the KYM join goes
through P13484 (the URL's last segment), not P6760 (KYM's internal number, the
one IMKG joined on, never on the page); P279 survives the filter for the
NER-type walk; the version is a function of the dump and the filters.
"""
import io
import json
from contextlib import redirect_stdout

import pytest

from modules.kg import wikidata as wd
from wikidata_fixture import KEPT, dump_lines, item, write_dump


def build(tmp, name="lexicon.sqlite", items=None, **kw):
    out = str(tmp / name)
    return out, wd.build_lexicon(write_dump(str(tmp / "dump.json.gz"), items), out, workers=0,
                                 progress=lambda _l: None, **kw)


@pytest.mark.parametrize("text, want", [("Etch-a-Sketch", "etch a sketch"), ("  Shiba–Inu ", "shiba inu"),
                                        ("Lowe’s", "lowe's"), ("“Doge”.", "doge"), (None, ""), (" .. ", "")])
def test_norm_folds_case_typography_and_dashes(text, want):
    assert wd.norm(text) == want


@pytest.mark.parametrize("url, slug", [("https://knowyourmeme.com/memes/sites/reddit", "reddit"),
                                       ("https://knowyourmeme.com/memes/doge/", "doge"),
                                       ("diet-coke-and-mentos", "diet-coke-and-mentos"),
                                       ("https://knowyourmeme.com/memes/doge?x=1#y", "doge")])
def test_the_kym_slug_is_the_urls_last_segment(url, slug):
    assert wd.kym_slug(url) == slug


def test_parse_entity():
    lines = dump_lines()
    assert [wd.parse_entity(x) for x in (lines[0], lines[-1], lines[-2])] == [None] * 3   # "[", "]", the property
    assert wd.parse_entity(json.dumps(item("Q1", "x"), separators=(",", ":"))) is None    # dropped before parsing
    trailing = json.dumps(item("Q144", "dog", sitelinks=3), separators=(",", ":")) + ","  # the comma is tolerated
    assert wd.parse_entity(trailing)["label"] == "dog"
    assert wd.parse_entity(json.dumps(item("Q144", "dog", sitelinks=4), separators=(",", ":")))["sitelinks"] == 4


# -- the built lexicon ----------------------------------------------------------------------

def ids(lex, text):
    return [c.id for c in lex.candidates(wd.norm(text))]


def test_exactly_the_expected_items_are_kept(fixture_lexicon, fixture_lexicon_build):
    assert {wd.qid_str(q) for (q,) in fixture_lexicon._db.execute("SELECT qid FROM entity")} == KEPT
    assert fixture_lexicon_build[1]["entities"] == len(KEPT)


@pytest.mark.parametrize("text, want", [
    ("Shiba Inu", ["Q39315"]), ("shiba", ["Q39315"]), ("SHIBA-INU", ["Q39315"]),   # alias; dash-folded, deduped
    ("doge", ["Q219", "Q15894956"]),                                               # most linked first
    ("Kabosu", ["Q9999903"]),                                                      # a KYM slug, no Wikipedia
    ("Chien de garde", []),                                                        # no English or mul label
])
def test_lookup(fixture_lexicon, text, want):
    assert ids(fixture_lexicon, text) == want


def test_candidates(fixture_lexicon):
    assert "Q9999901" not in ids(fixture_lexicon, "Doge")                        # a disambiguation page
    [c] = fixture_lexicon.candidates("atsuko sato")                               # a mul label is English
    assert (c.id, c.label, c.is_label) == ("Q9999905", "Atsuko Sato", True)
    [c] = fixture_lexicon.candidates("shiba")
    assert (c.surface, c.is_label, c.label, c.sitelinks) == ("Shiba", False, "Shiba Inu", 47)


def test_frames_join_on_the_kym_slug(fixture_lexicon):
    lex = fixture_lexicon
    assert lex.by_kym("https://knowyourmeme.com/memes/doge").id == "Q15894956"
    assert lex.by_kym("https://knowyourmeme.com/memes/sites/reddit").id == "Q1136"
    assert lex.by_kym("https://knowyourmeme.com/memes/cheems") is None
    assert lex.by_kym_id("13564").id == "Q15894956" and lex.by_kym("13564") is None   # a number is no slug


def test_the_class_tree(fixture_lexicon):
    lex = fixture_lexicon
    assert lex.entity("Q9999906") is None and 215627 in lex.ancestors(9999906)    # P279 of a dropped class
    assert {5, 215627} <= lex.ancestors(5) and 56061 in lex.families([6256])     # transitive


def test_meta_records_what_the_lexicon_was_built_from(fixture_lexicon, fixture_lexicon_build):
    meta = fixture_lexicon.meta
    assert (meta["dump"], meta["builder_version"], meta["dump_newest_modified"]) == \
        ("dump.json.gz", wd.LEXICON_BUILDER_VERSION, "2026-09-14T04:37:01Z")
    assert fixture_lexicon.version == fixture_lexicon_build[1]["version"]


# -- versions, robustness, the CLI ---------------------------------------------------------------

def test_the_version_follows_the_dump_and_the_filters(tmp_path):
    _, a = build(tmp_path, "a.sqlite")
    _, b = build(tmp_path, "b.sqlite")
    path, c = build(tmp_path, "c.sqlite", min_sitelinks=2)
    assert a["version"] == b["version"] != c["version"]
    with wd.Lexicon(path) as lex:                                                # a stricter filter
        assert lex.entity("Q9999907") is None and lex.entity("Q9999903") is not None   # 1 Wikipedia; 0 but a slug


def test_a_truncated_dump_keeps_what_was_read(tmp_path):
    # a head sample or a partial download ends in a torn gzip member
    dump = write_dump(str(tmp_path / "dump.json.gz"), [item(f"Q{n}", f"thing {n}", sitelinks=2) for n in range(1, 3000)])
    data = (tmp_path / "dump.json.gz").read_bytes()
    (tmp_path / "dump.json.gz").write_bytes(data[: len(data) // 2])
    summary = wd.build_lexicon(dump, str(tmp_path / "lex.sqlite"), workers=0, progress=lambda _l: None)
    assert 100 < summary["entities"] < 2999


def test_a_failed_build_leaves_the_previous_lexicon_and_a_missing_one_says_how(tmp_path):
    out = tmp_path / "lex.sqlite"
    out.write_text("previous")
    with pytest.raises(OSError):
        wd.build_lexicon(str(tmp_path / "missing.json.gz"), str(out), workers=0, progress=lambda _l: None)
    assert out.read_text() == "previous"
    with pytest.raises(FileNotFoundError, match="modules.kg.wikidata build"):
        wd.Lexicon("/nonexistent/lexicon.sqlite")


def test_the_process_pool_gives_the_same_lexicon(tmp_path):
    dump = write_dump(str(tmp_path / "dump.json.gz"))
    keys = ("entities", "aliases", "subclass_edges", "kym_ids", "version")
    a = wd.build_lexicon(dump, str(tmp_path / "a.sqlite"), workers=0, progress=lambda _l: None)
    b = wd.build_lexicon(dump, str(tmp_path / "b.sqlite"), workers=2, progress=lambda _l: None)
    assert {k: a[k] for k in keys} == {k: b[k] for k in keys}


def test_the_prior_is_monotonic_and_saturates():
    assert wd.prior(0) == 0.0 and wd.prior(3) < wd.prior(30) and wd.prior(150) == wd.prior(400) == 1.0


def test_the_cli_prints_json(fixture_lexicon_build):
    path = fixture_lexicon_build[0]
    for args, check in ((["lookup", "--lexicon", path, "Shiba Inu"], lambda d: d["candidates"][0]["qid"] == "Q39315"),
                        (["info", "--lexicon", path], lambda d: "version" in d)):
        buf = io.StringIO()
        with redirect_stdout(buf):
            assert wd.main(args) == 0
        assert check(json.loads(buf.getvalue()))
