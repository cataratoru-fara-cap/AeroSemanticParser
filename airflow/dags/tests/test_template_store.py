"""template_store.py and template_search.py (mongomock, a fake imgflip client).
Pinned: a failed refetch never overwrites a good page and a cached one costs no
request; every search stamp re-queues on its own, as do an edited source, a
failed search and age; a later search never downgrades a template's details;
an assignment writes only what changed and records its run (the KG build
snapshots at a completed one); dropped duplicates are recorded as `leader`."""
import io
import random
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from helpers import mock_store
from modules import imgflip_client as ic
from modules import imgflip_parse as ip
from modules import template_search as ts
from modules import template_store as st
from modules.kg import templates as kt

FRAME = "https://knowyourmeme.com/memes/distracted-boyfriend"
STAMPS = kt.stamps()
TEMPLATE_PAGE = """<html><h1 id="mtm-title">Distracted Boyfriend Meme Template</h1>
<h2 id="mtm-subtitle">also called: distracted bf, guy looking back</h2>
<p>Template ID: 10</p><p>Format: jpg</p><p>Dimensions: 1200x800 px</p></html>"""


@pytest.fixture
def store():
    return mock_store(st.TemplateStore)


def entry_doc(url=FRAME, title="Distracted Boyfriend", **over):
    return {"_id": kt.frame_key(url), "url": url, "title": title, "category": "meme", "entry_type": ["exploitable"],
            "additional_references": [], "og_image": "https://i.kym-cdn.com/entries/icons/original/000/1/db.jpg",
            "sections": [], **over}


def jpeg(seed):
    rnd = random.Random(seed)
    img = Image.new("RGB", (200, 140), (rnd.randrange(256),) * 3)
    d = ImageDraw.Draw(img)
    for _ in range(10):
        x, y = rnd.randrange(200), rnd.randrange(140)
        d.rectangle([x, y, x + rnd.randrange(20, 90), y + rnd.randrange(20, 60)],
                    fill=(rnd.randrange(256), rnd.randrange(256), rnd.randrange(256)))
    buf = io.BytesIO()
    img.save(buf, "JPEG")
    return buf.getvalue()


def search_html(results):
    boxes = "".join(f'<div class="mt-box"><h3 class="mt-title"><a href="{href}">{name}</a></h3>'
                    f'<div class="mt-img-wrap"><img class="shadow" src="//i.imgflip.com/4/'
                    f'{ip.key_from_template_id(tid)}.jpg"/></div></div>' for tid, name, href in results)
    return f'<html><div id="mt-boxes-wrap"><div class="mt-boxes">{boxes}</div></div></html>'


class FakeClient:
    """Search pages from a dict, images from seeds."""

    def __init__(self, pages, images):
        self.pages, self.images, self.page_urls = pages, images, []
        self.requests, self.credits_used = {"html": 0, "image": 0}, 0

    def fetch_page(self, url, *, markers, end_of_results_on_500=False):
        self.requests["html"] += 1
        self.page_urls.append(url)
        html = self.pages.get(url)
        if html is None:
            return ic.PageResult(url=url, ok=False, status_code=500, error="500",
                                 error_kind="end_of_results" if end_of_results_on_500 else "retryable")
        return ic.PageResult(url=url, ok=True, html=html, final_url=url, status_code=200)

    def fetch_image(self, url):
        self.requests["image"] += 1
        m = re.search(r"/([0-9a-z]+)\.(jpg|png)$", url)
        data = self.images.get(m.group(1)) if m else None
        if data is None:
            return ic.ImageResult(url=url, ok=False, status_code=404, error_kind="permanent")
        return ic.ImageResult(url=url, ok=True, content=data, content_type="image/jpeg", status_code=200)


def test_the_page_archive(store):
    url = "https://imgflip.com/x"
    ok = ic.PageResult(url=url, ok=True, html="<html>é</html>", status_code=200).as_doc()
    assert store.save_page(ok, kind="search", query="x", page=1) == "saved"
    assert store.get_page(url)["html"] == "<html>é</html>" and store.get_page(url, max_age_days=-1) is None
    bad = ic.PageResult(url=url, ok=False, error="503", error_kind="retryable").as_doc()
    assert store.save_page(bad, kind="search") == "kept_ok" and store.get_page(url)["html"] == "<html>é</html>"


# -- what is pending ---------------------------------------------------------------------

@pytest.fixture
def frames(store):
    store.entries.insert_many([entry_doc(), entry_doc("https://knowyourmeme.com/memes/some-event", "Some Event",
                                                      category="event")])
    units = list(store.iter_units())

    def searched(**over):
        store.save_search({"unit_id": units[0]["unit_id"], "frame_url": FRAME, "source_sha256": units[0]["source_sha256"],
                           "search_status": "searched", "candidates": [], "templates": {}, "hashes": {}, **over}, STAMPS)

    def pending(stamps=STAMPS, **kw):
        return store.select_pending(units, stamps=stamps, **kw)
    return units, searched, pending, store


def test_only_eligible_frames_are_units(frames):
    assert [u["frame_url"] for u in frames[0]] == [FRAME]


@pytest.mark.parametrize("key", list(st.SEARCH_STAMP_KEYS))
def test_a_search_is_done_until_one_of_its_stamps_moves(frames, key):
    _, searched, pending, _ = frames
    searched()
    assert pending() == [] and len(pending({**STAMPS, key: "moved"})) == 1


def test_failed_edited_and_old_searches_requeue(frames):
    _, searched, pending, store = frames
    searched(search_status="failed")
    assert len(pending()) == 1
    searched(source_sha256="edited")
    assert len(pending()) == 1
    searched()
    store.frames.update_one({}, {"$set": {"searched_at": datetime(2020, 1, 1, tzinfo=timezone.utc)}})
    assert len(pending(research_after_days=180)) == 1 and pending() == []


def test_a_search_never_downgrades_details(store):
    store.save_details(7, {"alt_names": ["guy looking back"], "file_type": "jpg"})
    store.upsert_templates({7: {"template_id": 7, "key": "7", "name": "Distracted Boyfriend",
                                "url": "https://imgflip.com/meme/7/x", "featured": False, "animated": False}})
    doc = store.templates.find_one({"_id": 7})
    assert (doc["alt_names"], doc["name"]) == (["guy looking back"], "Distracted Boyfriend")


# -- search_units -> run_assignment -> fetch_details, over fakes ---------------------------

@pytest.fixture
def stage(store, tmp_path):
    store.entries.insert_one(entry_doc())
    pic, k = jpeg(1), ip.key_from_template_id
    results = [(10, "Distracted Boyfriend", "/meme/Distracted-Boyfriend"), (11, "Distracted boyfriend", "/meme/11/Distracted-boyfriend"),
               (12, "Distracted Boyfriend Reversed", "/meme/12/x"), (13, "zzz", "/meme/13/zzz")]
    client = FakeClient(pages={ic.search_url("distracted boyfriend"): search_html(results),
                               ip.template_page_url(10): TEMPLATE_PAGE},
                        images={k(10): pic, k(11): pic, k(12): jpeg(2), k(13): jpeg(3), k(10) + "-blank": pic})
    return store, client, ts.StoreIO(client, store, tmp_path, max_page_age_days=180)


def test_the_whole_stage(stage):
    store, _client, io_ = stage
    tally = ts.search_units(store.units_for([kt.frame_key(FRAME)]), io_, STAMPS)
    assert (tally["frames"], tally["searched"]) == (1, 1)
    counts = ts.run_assignment(store, "run-1")
    frame = store.frames.find_one({"_id": kt.frame_key(FRAME)})
    kept = [x["template_id"] for x in frame["selected"]]
    assert frame["status"] == "selected" and kept[0] == 10 and 11 not in kept    # the featured upload; 11 merged
    assert store.templates.find_one({"_id": 11})["leader"] == 10 and 11 in frame["selected"][0]["members"]
    assert store.assignments.find_one({"_id": "run-1"})["completed_at"] is not None and counts["written"] == 1
    again = ts.run_assignment(store, "run-2")                                      # nothing changed
    assert (again["written"], again["unchanged"]) == (0, 1)
    # a frame's own imgflip link, resolved in the search, reads the page without its image
    store.save_details(10, {"name": "Distracted Boyfriend"})
    assert 10 in store.templates_needing_details()
    got = ts.fetch_details([10], io_)
    doc = store.templates.find_one({"_id": 10})
    assert (got["details"], got["blanks"], doc["alt_names"], doc["url"]) == \
        (1, 1, ["distracted bf", "guy looking back"], "https://imgflip.com/meme/Distracted-Boyfriend")
    assert Path(doc["blank_path"]).exists() and 10 not in store.templates_needing_details()


def test_a_second_frame_with_the_same_query_costs_no_request(stage):
    store, client, io_ = stage
    store.entries.insert_one(entry_doc("https://knowyourmeme.com/memes/db-2"))
    ts.search_units(store.units_for([kt.frame_key(FRAME), kt.frame_key("https://knowyourmeme.com/memes/db-2")]),
                    io_, STAMPS)
    assert client.page_urls.count(ic.search_url("distracted boyfriend")) == 1 and io_.tally["pages_cached"] == 1


def test_a_stratified_sample_is_proportional_and_reproducible():
    units = ([{"unit_id": f"a{i:03d}", "priority": 1} for i in range(10)]
             + [{"unit_id": f"b{i:03d}", "priority": 2} for i in range(30)]
             + [{"unit_id": f"c{i:03d}", "priority": 3} for i in range(60)])
    got = st.stratified_sample(units, 20, seed=7)
    assert [sum(u["priority"] == p for u in got) for p in (1, 2, 3)] == [2, 6, 12]
    assert got == st.stratified_sample(list(reversed(units)), 20, seed=7) != st.stratified_sample(units, 20, seed=8)
    assert [u["priority"] for u in got] == sorted(u["priority"] for u in got)
