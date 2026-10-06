"""One entry, one address (gap 14): kym_discover's Phase 3, pure. The cases are
the ones found in the corpus on 2026-10-05: Doge moved into KYM's sensitive
section (both addresses answer), Hide the Pain Harold moved back out, HRjak
renamed after a move, an emoji slug spelled two ways, and four photos that
share a title without being one entry."""
from datetime import datetime, timezone

import pytest

from helpers import FIXTURES
from modules import kym_discover as kd

K = "https://knowyourmeme.com"
PUB, SENS = f"{K}/memes/doge", f"{K}/sensitive/memes/doge"


def when(month, day=1):
    return datetime(2026, month, day, tzinfo=timezone.utc)


def page(fetched, named=None, title=None):
    return {"fetched_at": fetched, "page_url": named, "page_title": title}


# -- addresses ---------------------------------------------------------------------------------

def test_address_and_entry_keys():
    assert kd.address_key(f"{K}/memes/folk-%F0%9F%98%AD") == kd.address_key(f"{K}/memes/folk-😭/")   # two spellings
    assert kd.address_key("http://www.knowyourmeme.com/memes/doge") == "knowyourmeme.com/memes/doge"
    assert kd.entry_key(SENS) == kd.entry_key(PUB)                                 # the sensitive root
    assert kd.entry_key(f"{K}/sensitive/memes/events/x") == kd.entry_key(f"{K}/memes/events/x")
    assert kd.entry_key(f"{K}/memes/sensitive/x") != kd.entry_key(f"{K}/memes/x")  # only the leading root


@pytest.mark.parametrize("url, frame", [
    (PUB, True), (f"{K}/sensitive/memes/people/x", True),
    (f"{K}/photos/3228068-rule-63", False), (f"{K}/editorials/poll", False), (f"{K}/memes", False),
])
def test_media_frames_are_the_meme_entries(url, frame):
    assert kd.is_media_frame(url) is frame


@pytest.mark.parametrize("html, want", [
    ((FIXTURES / "doge.html").read_text(encoding="utf-8"), (PUB, "Doge")),        # the stored Doge page
    # attribute order, quotes, entities; only the head counts
    ("<html><head><meta content=\"https://knowyourmeme.com/memes/x\" property=\"og:url\">"
     "<meta property='og:title' content='Bird in Cage / &quot;That&#39;s You&quot; | Know Your Meme' />"
     "</head><body><meta property='og:url' content='nope'></body></html>", (f"{K}/memes/x", 'Bird in Cage / "That\'s You"')),
    ("<html><head></head></html>", (None, None)),
])
def test_page_identity(html, want):
    assert kd.page_identity(html) == dict(zip(("page_url", "page_title"), want))


# -- resolution -----------------------------------------------------------------------------------

HAROLD, HAROLD_S = f"{K}/memes/hide-the-pain-harold", f"{K}/sensitive/memes/hide-the-pain-harold"
HR_OLD, HR_SENS, HR_NEW = f"{K}/memes/hrjak", f"{K}/sensitive/memes/hrjak", f"{K}/memes/anne-hathaway-hr-meme-hrjak"
FLINCH_OLD, FLINCH_NEW = f"{K}/memes/trump-flinches-beside-xi-jinping", f"{K}/memes/donald-trump-flinches-beside-xi-jinping"
BEG_A, BEG_B = f"{K}/memes/somebody-get-these-beggars-out-of-here", f"{K}/memes/robert-pattinson-beggars"
PASCAL, PASCAL_S = f"{K}/memes/people/pedro-pascal", f"{K}/sensitive/memes/people/pedro-pascal"


@pytest.mark.parametrize("urls, pages, want", [
    pytest.param([PUB, SENS], {PUB: page(when(7, 10), PUB, "Doge"), SENS: page(when(9, 9), SENS, "Doge")},
                 {"kept": SENS, "dropped": [PUB], "by": ["address", "title"]}, id="moved-into-sensitive"),
    # KYM today serves /memes/doge with og:url = the sensitive address
    pytest.param([PUB, SENS], {PUB: page(when(10, 5), SENS, "Doge"), SENS: page(when(9, 9), SENS, "Doge")},
                 {"kept": SENS, "dropped": [PUB], "by": ["address", "page_url", "title"]}, id="newest-from-old-address"),
    pytest.param([HAROLD, HAROLD_S], {HAROLD: page(when(7, 10), HAROLD, "Hide the Pain Harold"),
                                      HAROLD_S: page(when(9, 9), HAROLD, "Hide the Pain Harold")},
                 {"kept": HAROLD, "dropped": [HAROLD_S], "by": ["address", "page_url", "title"]}, id="moved-back-out"),
    pytest.param([HR_OLD, HR_SENS, HR_NEW], {HR_OLD: page(when(7, 9), HR_OLD, "HRjak"),
                                             HR_SENS: page(when(9, 9), HR_NEW, "Anne Hathaway HR Meme / HRjak"),
                                             HR_NEW: page(when(8, 1), HR_NEW, "Anne Hathaway HR Meme / HRjak")},
                 {"kept": HR_NEW, "dropped": sorted([HR_OLD, HR_SENS]), "by": ["address", "page_url", "title"]},
                 id="renamed-after-a-move"),
    # the newest page names an address discovery knows but never fetched: keep the page we hold
    pytest.param([FLINCH_OLD, FLINCH_NEW], {FLINCH_OLD: page(when(10), FLINCH_NEW, "x")},
                 {"kept": FLINCH_OLD, "dropped": [FLINCH_NEW], "by": ["page_url"]}, id="named-but-unfetched"),
    pytest.param([BEG_A, BEG_B], {BEG_A: page(when(7), BEG_A, "Robert Pattinson Beggars"),
                                  BEG_B: page(when(9), BEG_B, "Robert Pattinson Beggars")},
                 {"kept": BEG_B, "dropped": [BEG_A], "by": ["title"]}, id="same-title-memes"),
    pytest.param([PASCAL, PASCAL_S], {PASCAL_S: page(when(9), PASCAL_S, "Pedro Pascal")},
                 {"kept": PASCAL_S, "dropped": [PASCAL], "by": ["address"]}, id="unfetched-twin"),
])
def test_one_entry_many_addresses(urls, pages, want):
    assert kd.resolve_duplicates(urls, pages) == [want]


PHOTOS = [f"{K}/photos/{n}-rule-63" for n in (3228068, 3209972)]
NAV = [f"{K}/editorials/poll", f"{K}/editorials/insights"]
FOLK = f"{K}/memes/folk-%F0%9F%98%AD"


@pytest.mark.parametrize("urls, pages", [
    pytest.param([*PHOTOS, *NAV], {**{u: page(when(7), u, "Link | Rule 63") for u in PHOTOS},
                                   **{u: page(when(7), None, "Know Your Meme") for u in NAV}}, id="photos-share-titles"),
    pytest.param([FOLK], {FOLK: page(when(7), f"{K}/memes/folk-😭", "Folk 😭")}, id="names-itself-respelled"),
    pytest.param([f"{K}/memes/x", f"{K}/sensitive/memes/x"], {}, id="no-page"),
])
def test_not_duplicates(urls, pages):
    assert kd.resolve_duplicates(urls, pages) == []


def test_ties_and_order_are_deterministic():
    pub, sens = f"{K}/memes/x", f"{K}/sensitive/memes/x"
    pages = {pub: page(when(9), None, "X"), sens: page(when(9), None, "X")}
    first = kd.resolve_duplicates([pub, sens], pages)
    assert len(first) == 1 and first == kd.resolve_duplicates([sens, pub], dict(reversed(pages.items())))


def test_the_run_summary_numbers():
    groups = [{"kept": SENS, "dropped": [PUB], "by": ["address", "title"]},
              {"kept": f"{K}/memes/b", "dropped": [f"{K}/memes/a", f"{K}/memes/c"], "by": ["title"]}]
    stats = kd.duplicate_stats(groups, collected=[PUB, f"{K}/memes/a"])
    assert {k: stats[k] for k in ("entries_with_duplicates", "addresses_dropped", "pages_dropped", "kept_sensitive",
                                  "kept_public")} == {"entries_with_duplicates": 2, "addresses_dropped": 3,
                                                      "pages_dropped": 2, "kept_sensitive": 1, "kept_public": 1}
    assert (stats["by_evidence"], stats["title_only"]) == ({"address": 1, "page_url": 0, "title": 2}, [groups[1]])
