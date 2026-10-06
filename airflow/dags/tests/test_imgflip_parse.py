"""imgflip_parse.py, against pages saved from imgflip on 2026-09-28
(fixtures/imgflip/). Pinned: the template id comes from the image key (base
36), so featured templates, whose URL carries no id, still get one; featured
(the URL has no id) and animated (imgflip's own label) are read, not guessed;
"no results" is not a parse failure, and a page that is not a search page IS
one (a block page must never read as "imgflip has nothing"); a template page
gives its alternate names, format and size, and loses the "Meme Template"
suffix."""
import pytest

from helpers import FIXTURES
from modules import imgflip_parse as ip


def page(name):
    return (FIXTURES / "imgflip" / name).read_text(encoding="utf-8")


def test_the_key_is_the_id_in_base_36():
    assert (ip.template_id_from_key("1ur9b0"), ip.key_from_template_id(112126428)) == (112126428, "1ur9b0")
    for n in (0, 35, 36, 8072285, 247375501):
        assert ip.template_id_from_key(ip.key_from_template_id(n)) == n


@pytest.mark.parametrize("url, key", [
    ("//i.imgflip.com/4/1ur9b0.jpg", ("1ur9b0", "jpg")), ("https://i.imgflip.com/2/3jpogl.jpg", ("3jpogl", "jpg")),
    ("https://i.imgflip.com/3jpogl.mp4", ("3jpogl", "mp4")),                   # every size directory, or none
    ("/s/meme/Distracted-Boyfriend.jpg", None), (None, None),
])
def test_image_key(url, key):
    assert ip.image_key(url) == key


def test_thumbnails_for_animated_templates_are_the_small_still():
    assert ip.thumb_urls("3jpogl", animated=True) == ["https://i.imgflip.com/2/3jpogl.jpg"]
    assert ip.thumb_urls("1ur9b0")[0] == "https://i.imgflip.com/4/1ur9b0.jpg"


@pytest.mark.parametrize("url, want", [
    # the links KYM uses
    ("https://imgflip.com/memegenerator/118783322/My-disappointment", ("memegenerator", 118783322, "My-disappointment")),
    ("https://imgflip.com/memegenerator/Distracted-Boyfriend", ("memegenerator", None, "Distracted-Boyfriend")),
    ("https://imgflip.com/meme/Distracted-Boyfriend", ("meme", None, "Distracted-Boyfriend")),
    ("https://imgflip.com/memetemplate/112126428", ("memetemplate", 112126428, None)),
    ("https://imgflip.com/gif-maker/214509333/This-is-fine", ("gif-maker", 214509333, "This-is-fine")),
    # instances, other pages, other sites
    ("https://imgflip.com/i/3fys88", ("instance",)), ("https://imgflip.com/memegenerator", ("other",)),
    ("https://imgflip.com/memetemplates", ("other",)), ("https://memegenerator.net/Doge", ("not_imgflip",)),
])
def test_classify_imgflip_url(url, want):
    got = ip.classify_imgflip_url(url)
    assert (got["kind"], got["template_id"], got["slug"])[:len(want)] == want


# -- search pages ------------------------------------------------------------------------------

def test_a_full_page_and_the_featured_template():
    got = ip.parse_search(page("search_distracted_boyfriend.html"))
    assert len(got["results"]) == ip.PAGE_SIZE and got["has_next"] and not got["no_results"]
    assert (got["skipped"], got["id_mismatches"]) == (0, 0)
    assert [r["rank"] for r in got["results"]] == list(range(ip.PAGE_SIZE))
    first, second = got["results"][:2]                                 # its id from the key
    assert (first["name"], first["template_id"], first["featured"], first["url"]) == \
        ("Distracted Boyfriend", 112126428, True, "https://imgflip.com/meme/Distracted-Boyfriend")
    assert (second["featured"], second["template_id"]) == (False, 112067320)


def test_animated_templates_are_flagged():
    results = ip.parse_search(page("search_this_is_fine.html"))["results"]
    gif = next(r for r in results if r["template_id"] == 214509333)
    assert gif["animated"] and "/2/" in gif["thumb_url"] and "/gif-maker/" in gif["caption_url"]
    assert not results[0]["animated"]


def test_no_results_and_the_last_page():
    empty = ip.parse_search(page("search_no_results.html"))
    assert (empty["results"], empty["no_results"], empty["has_next"]) == ([], True, False)
    last = ip.parse_search(page("search_doge_page51.html"))
    assert 0 < len(last["results"]) < ip.PAGE_SIZE and not last["has_next"]


@pytest.mark.parametrize("parse, html", [
    (ip.parse_search, "<html><title>Just a moment...</title></html>"),
    (ip.parse_search, page("template_distracted_boyfriend.html")),
    (ip.parse_template_page, page("search_distracted_boyfriend.html")),
])
def test_a_page_of_the_wrong_kind_raises(parse, html):
    with pytest.raises(ip.ImgflipParseError):
        parse(html)


# -- template pages ---------------------------------------------------------------------------

def test_the_featured_template_page():
    got = ip.parse_template_page(page("template_distracted_boyfriend.html"))
    assert (got["template_id"], got["key"], got["name"], got["file_type"], got["width"], got["height"]) == \
        (112126428, "1ur9b0", "Distracted Boyfriend", "jpg", 1200, 800)
    assert {"distracted bf", "jealous girlfriend"} <= set(got["alt_names"])
    assert len(got["alt_names"]) == len({n.lower() for n in got["alt_names"]})


@pytest.mark.parametrize("name, want", [
    ("template_variant_112067320.html",                          # no alternate names
     {"template_id": 112067320, "name": "Distracted boyfriend", "alt_names": [], "width": 1200, "height": 707}),
    ("template_this_is_fine_gif.html",
     {"template_id": 214509333, "file_type": "mp4", "description": "Fire!", "key": "3jpogl"}),
])
def test_other_template_pages(name, want):
    got = ip.parse_template_page(page(name))
    assert {k: got[k] for k in want} == want
