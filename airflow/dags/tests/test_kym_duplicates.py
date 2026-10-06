"""One entry, one address (gap 14): kym_discover's Phase 3, pure.

The cases are the ones found in the corpus on 2026-10-05: Doge moved into
KYM's sensitive section (both addresses answer), Hide the Pain Harold moved
back out, HRjak renamed after a move, an emoji slug spelled two ways, and
four photos that share a title without being one entry.
"""
import os
import unittest
from datetime import datetime, timezone

from modules import kym_discover as kd

K = "https://knowyourmeme.com"
FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "doge.html")


def when(month: int, day: int = 1) -> datetime:
    return datetime(2026, month, day, tzinfo=timezone.utc)


def page(fetched, named=None, title=None) -> dict:
    return {"fetched_at": fetched, "page_url": named, "page_title": title}


class AddressTests(unittest.TestCase):
    def test_one_address_two_spellings(self):
        self.assertEqual(kd.address_key(f"{K}/memes/folk-%F0%9F%98%AD"),
                         kd.address_key(f"{K}/memes/folk-😭/"))
        self.assertEqual(kd.address_key("http://www.knowyourmeme.com/memes/doge"),
                         "knowyourmeme.com/memes/doge")

    def test_the_sensitive_root_names_the_same_entry(self):
        self.assertEqual(kd.entry_key(f"{K}/sensitive/memes/doge"),
                         kd.entry_key(f"{K}/memes/doge"))
        self.assertEqual(kd.entry_key(f"{K}/sensitive/memes/events/x"),
                         kd.entry_key(f"{K}/memes/events/x"))
        # Only the leading root: a slug that says "sensitive" is a slug.
        self.assertNotEqual(kd.entry_key(f"{K}/memes/sensitive/x"),
                            kd.entry_key(f"{K}/memes/x"))

    def test_media_frames_are_the_meme_entries(self):
        self.assertTrue(kd.is_media_frame(f"{K}/memes/doge"))
        self.assertTrue(kd.is_media_frame(f"{K}/sensitive/memes/people/x"))
        self.assertFalse(kd.is_media_frame(f"{K}/photos/3228068-rule-63"))
        self.assertFalse(kd.is_media_frame(f"{K}/editorials/poll"))
        self.assertFalse(kd.is_media_frame(f"{K}/memes"))


class PageIdentityTests(unittest.TestCase):
    def test_the_stored_doge_page(self):
        with open(FIXTURE, encoding="utf-8") as fh:
            got = kd.page_identity(fh.read())
        self.assertEqual(got, {"page_url": f"{K}/memes/doge", "page_title": "Doge"})

    def test_attribute_order_quotes_and_entities(self):
        html = ("<html><head>"
                "<meta content=\"https://knowyourmeme.com/memes/x\" property=\"og:url\">"
                "<meta property='og:title' content='Bird in Cage / &quot;That&#39;s You&quot;"
                " | Know Your Meme' />"
                "</head><body><meta property='og:url' content='nope'></body></html>")
        self.assertEqual(kd.page_identity(html),
                         {"page_url": f"{K}/memes/x",
                          "page_title": 'Bird in Cage / "That\'s You"'})

    def test_a_page_without_the_tags(self):
        self.assertEqual(kd.page_identity("<html><head></head></html>"),
                         {"page_url": None, "page_title": None})


class ResolveTests(unittest.TestCase):
    def test_moved_into_sensitive_keeps_the_new_address(self):
        pub, sens = f"{K}/memes/doge", f"{K}/sensitive/memes/doge"
        groups = kd.resolve_duplicates([pub, sens], {
            pub: page(when(7, 10), pub, "Doge"),
            sens: page(when(9, 9), sens, "Doge")})
        self.assertEqual(groups, [{"kept": sens, "dropped": [pub],
                                   "by": ["address", "title"]}])

    def test_the_newest_page_decides_even_from_the_old_address(self):
        # KYM today serves /memes/doge with og:url = the sensitive address.
        pub, sens = f"{K}/memes/doge", f"{K}/sensitive/memes/doge"
        (g,) = kd.resolve_duplicates([pub, sens], {
            pub: page(when(10, 5), sens, "Doge"),
            sens: page(when(9, 9), sens, "Doge")})
        self.assertEqual(g["kept"], sens)

    def test_moved_back_out_keeps_the_public_address(self):
        pub, sens = f"{K}/memes/hide-the-pain-harold", f"{K}/sensitive/memes/hide-the-pain-harold"
        (g,) = kd.resolve_duplicates([pub, sens], {
            pub: page(when(7, 10), pub, "Hide the Pain Harold"),
            sens: page(when(9, 9), pub, "Hide the Pain Harold")})
        self.assertEqual((g["kept"], g["dropped"]), (pub, [sens]))
        self.assertIn("page_url", g["by"])

    def test_a_rename_after_a_move_is_one_entry_at_three_addresses(self):
        old, sens, new = (f"{K}/memes/hrjak", f"{K}/sensitive/memes/hrjak",
                          f"{K}/memes/anne-hathaway-hr-meme-hrjak")
        (g,) = kd.resolve_duplicates([old, sens, new], {
            old: page(when(7, 9), old, "HRjak"),
            sens: page(when(9, 9), new, "Anne Hathaway HR Meme / HRjak"),
            new: page(when(8, 1), new, "Anne Hathaway HR Meme / HRjak")})
        self.assertEqual(g, {"kept": new, "dropped": sorted([old, sens]),
                             "by": ["address", "page_url", "title"]})

    def test_a_named_address_without_a_page_is_dropped_into_the_one_we_hold(self):
        # The newest page names an address discovery knows but never fetched:
        # keep the page we hold; its links to the new address come to it.
        old, new = (f"{K}/memes/trump-flinches-beside-xi-jinping",
                    f"{K}/memes/donald-trump-flinches-beside-xi-jinping")
        (g,) = kd.resolve_duplicates([old, new], {old: page(when(10), new, "x")})
        self.assertEqual((g["kept"], g["dropped"], g["by"]), (old, [new], ["page_url"]))

    def test_same_title_meme_entries_are_one_entry(self):
        a, b = f"{K}/memes/somebody-get-these-beggars-out-of-here", f"{K}/memes/robert-pattinson-beggars"
        (g,) = kd.resolve_duplicates([a, b], {
            a: page(when(7), a, "Robert Pattinson Beggars"),
            b: page(when(9), b, "Robert Pattinson Beggars")})
        self.assertEqual((g["kept"], g["by"]), (b, ["title"]))

    def test_photos_and_pages_may_share_a_title(self):
        photos = [f"{K}/photos/{n}-rule-63" for n in (3228068, 3209972)]
        nav = [f"{K}/editorials/poll", f"{K}/editorials/insights"]
        pages = {u: page(when(7), u, "Link | Rule 63") for u in photos}
        pages.update({u: page(when(7), None, "Know Your Meme") for u in nav})
        self.assertEqual(kd.resolve_duplicates([*photos, *nav], pages), [])

    def test_a_page_naming_itself_in_another_spelling_is_not_a_duplicate(self):
        url = f"{K}/memes/folk-%F0%9F%98%AD"
        self.assertEqual(kd.resolve_duplicates([url], {
            url: page(when(7), f"{K}/memes/folk-😭", "Folk 😭")}), [])

    def test_an_entry_with_no_page_is_left_alone(self):
        self.assertEqual(kd.resolve_duplicates(
            [f"{K}/memes/x", f"{K}/sensitive/memes/x"], {}), [])

    def test_an_unfetched_twin_is_dropped_for_the_fetched_one(self):
        pub, sens = f"{K}/memes/people/pedro-pascal", f"{K}/sensitive/memes/people/pedro-pascal"
        (g,) = kd.resolve_duplicates([pub, sens], {sens: page(when(9), sens, "Pedro Pascal")})
        self.assertEqual((g["kept"], g["dropped"]), (sens, [pub]))

    def test_ties_and_order_are_deterministic(self):
        pub, sens = f"{K}/memes/x", f"{K}/sensitive/memes/x"
        pages = {pub: page(when(9), None, "X"), sens: page(when(9), None, "X")}
        first = kd.resolve_duplicates([pub, sens], pages)
        self.assertEqual(first, kd.resolve_duplicates([sens, pub], dict(reversed(pages.items()))))
        self.assertEqual(len(first), 1)


class StatsTests(unittest.TestCase):
    def test_the_run_summary_numbers(self):
        groups = [
            {"kept": f"{K}/sensitive/memes/doge", "dropped": [f"{K}/memes/doge"],
             "by": ["address", "title"]},
            {"kept": f"{K}/memes/b", "dropped": [f"{K}/memes/a", f"{K}/memes/c"],
             "by": ["title"]},
        ]
        stats = kd.duplicate_stats(groups, collected=[f"{K}/memes/doge", f"{K}/memes/a"])
        self.assertEqual(
            {k: stats[k] for k in ("entries_with_duplicates", "addresses_dropped",
                                   "pages_dropped", "kept_sensitive", "kept_public")},
            {"entries_with_duplicates": 2, "addresses_dropped": 3, "pages_dropped": 2,
             "kept_sensitive": 1, "kept_public": 1})
        self.assertEqual(stats["by_evidence"], {"address": 1, "page_url": 0, "title": 2})
        self.assertEqual(stats["title_only"], [groups[1]])


if __name__ == "__main__":
    unittest.main()
