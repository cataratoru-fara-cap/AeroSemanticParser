"""Tests for imgflip_parse.py, against pages saved from imgflip on 2026-09-28
(tests/fixtures/imgflip/). No network.

What these pin:

  * **The template id comes from the image key** (base 36), so imgflip's
    featured templates — whose URL carries no id — still get one.
  * **Featured and animated are read, not guessed**: featured = the URL
    has no id; animated = imgflip's own label.
  * **"No results" is not a parse failure**, and a page that is not a
    search page at all IS one (a block page must never read as "imgflip
    has nothing").
  * **A template page gives its alternate names, format and size**, and
    its title loses imgflip's "Meme Template" suffix.
"""
import unittest
from pathlib import Path

from modules import imgflip_parse as ip

FIX = Path(__file__).resolve().parent / "fixtures" / "imgflip"


def page(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


class KeyTests(unittest.TestCase):
    def test_key_is_the_id_in_base_36(self):
        self.assertEqual(ip.template_id_from_key("1ur9b0"), 112126428)
        self.assertEqual(ip.key_from_template_id(112126428), "1ur9b0")
        for n in (0, 35, 36, 8072285, 247375501):
            self.assertEqual(ip.template_id_from_key(ip.key_from_template_id(n)), n)

    def test_image_key_reads_every_size_directory(self):
        self.assertEqual(ip.image_key("//i.imgflip.com/4/1ur9b0.jpg"), ("1ur9b0", "jpg"))
        self.assertEqual(ip.image_key("https://i.imgflip.com/2/3jpogl.jpg"), ("3jpogl", "jpg"))
        self.assertEqual(ip.image_key("https://i.imgflip.com/3jpogl.mp4"), ("3jpogl", "mp4"))
        self.assertIsNone(ip.image_key("/s/meme/Distracted-Boyfriend.jpg"))
        self.assertIsNone(ip.image_key(None))

    def test_thumbnails_for_animated_templates_are_the_small_still(self):
        self.assertEqual(ip.thumb_urls("3jpogl", animated=True),
                         ["https://i.imgflip.com/2/3jpogl.jpg"])
        self.assertEqual(ip.thumb_urls("1ur9b0")[0], "https://i.imgflip.com/4/1ur9b0.jpg")


class ClassifyTests(unittest.TestCase):
    def test_links_kym_uses(self):
        cases = {
            "https://imgflip.com/memegenerator/118783322/My-disappointment":
                ("memegenerator", 118783322, "My-disappointment"),
            "https://imgflip.com/memegenerator/Distracted-Boyfriend":
                ("memegenerator", None, "Distracted-Boyfriend"),
            "https://imgflip.com/meme/Distracted-Boyfriend":
                ("meme", None, "Distracted-Boyfriend"),
            "https://imgflip.com/memetemplate/112126428": ("memetemplate", 112126428, None),
            "https://imgflip.com/gif-maker/214509333/This-is-fine":
                ("gif-maker", 214509333, "This-is-fine"),
        }
        for url, (kind, tid, slug) in cases.items():
            got = ip.classify_imgflip_url(url)
            self.assertEqual((got["kind"], got["template_id"], got["slug"]),
                             (kind, tid, slug), url)

    def test_instances_other_pages_and_other_sites(self):
        self.assertEqual(ip.classify_imgflip_url("https://imgflip.com/i/3fys88")["kind"],
                         "instance")
        self.assertEqual(ip.classify_imgflip_url("https://imgflip.com/memegenerator")["kind"],
                         "other")
        self.assertEqual(ip.classify_imgflip_url("https://imgflip.com/memetemplates")["kind"],
                         "other")
        self.assertEqual(ip.classify_imgflip_url("https://memegenerator.net/Doge")["kind"],
                         "not_imgflip")


class SearchPageTests(unittest.TestCase):
    def test_a_full_page(self):
        got = ip.parse_search(page("search_distracted_boyfriend.html"))
        self.assertEqual(len(got["results"]), ip.PAGE_SIZE)
        self.assertTrue(got["has_next"])
        self.assertFalse(got["no_results"])
        self.assertEqual((got["skipped"], got["id_mismatches"]), (0, 0))
        self.assertEqual([r["rank"] for r in got["results"]], list(range(ip.PAGE_SIZE)))

    def test_the_featured_template_gets_its_id_from_the_key(self):
        first = ip.parse_search(page("search_distracted_boyfriend.html"))["results"][0]
        self.assertEqual(first["name"], "Distracted Boyfriend")
        self.assertEqual(first["template_id"], 112126428)
        self.assertTrue(first["featured"])
        self.assertEqual(first["url"], "https://imgflip.com/meme/Distracted-Boyfriend")
        second = ip.parse_search(page("search_distracted_boyfriend.html"))["results"][1]
        self.assertFalse(second["featured"])
        self.assertEqual(second["template_id"], 112067320)

    def test_animated_templates_are_flagged(self):
        results = ip.parse_search(page("search_this_is_fine.html"))["results"]
        gif = next(r for r in results if r["template_id"] == 214509333)
        self.assertTrue(gif["animated"])
        self.assertIn("/2/", gif["thumb_url"])
        self.assertIn("/gif-maker/", gif["caption_url"])
        self.assertFalse(results[0]["animated"])

    def test_no_results_and_the_last_page(self):
        empty = ip.parse_search(page("search_no_results.html"))
        self.assertEqual((empty["results"], empty["no_results"], empty["has_next"]),
                         ([], True, False))
        last = ip.parse_search(page("search_doge_page51.html"))
        self.assertTrue(0 < len(last["results"]) < ip.PAGE_SIZE)
        self.assertFalse(last["has_next"])

    def test_a_page_that_is_not_a_search_page_raises(self):
        with self.assertRaises(ip.ImgflipParseError):
            ip.parse_search("<html><title>Just a moment...</title></html>")
        with self.assertRaises(ip.ImgflipParseError):
            ip.parse_search(page("template_distracted_boyfriend.html"))


class TemplatePageTests(unittest.TestCase):
    def test_the_featured_template(self):
        got = ip.parse_template_page(page("template_distracted_boyfriend.html"))
        self.assertEqual(got["template_id"], 112126428)
        self.assertEqual(got["key"], "1ur9b0")
        self.assertEqual(got["name"], "Distracted Boyfriend")
        self.assertEqual((got["file_type"], got["width"], got["height"]), ("jpg", 1200, 800))
        self.assertIn("distracted bf", got["alt_names"])
        self.assertIn("jealous girlfriend", got["alt_names"])
        self.assertEqual(len(got["alt_names"]), len(set(n.lower() for n in got["alt_names"])))

    def test_a_variant_without_alternate_names(self):
        got = ip.parse_template_page(page("template_variant_112067320.html"))
        self.assertEqual((got["template_id"], got["name"], got["alt_names"]),
                         (112067320, "Distracted boyfriend", []))
        self.assertEqual((got["width"], got["height"]), (1200, 707))

    def test_an_animated_template(self):
        got = ip.parse_template_page(page("template_this_is_fine_gif.html"))
        self.assertEqual((got["template_id"], got["file_type"], got["description"]),
                         (214509333, "mp4", "Fire!"))
        self.assertEqual(got["key"], "3jpogl")

    def test_a_page_that_is_not_a_template_page_raises(self):
        with self.assertRaises(ip.ImgflipParseError):
            ip.parse_template_page(page("search_distracted_boyfriend.html"))


if __name__ == "__main__":
    unittest.main()
