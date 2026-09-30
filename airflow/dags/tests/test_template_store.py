"""Tests for template_store.py and template_search.py (mongomock, a fake
imgflip client, no network).

What these pin:

  * **A failed refetch never overwrites a good page**, and a cached page
    is served without a request.
  * **Every search stamp re-queues on its own**, as do an edited source, a
    failed search, and age.
  * **A later search never downgrades a template's details** (its
    alternate names survive a search that does not show them).
  * **An assignment writes only what changed**, and records its run so the
    KG build can snapshot at a completed one.
  * **Dropped duplicates are recorded on imgflip_templates, as ``leader``**.
  * **The DAG calls only exported facades** (the kg_store lesson).
"""
import io
import re
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import mongomock
from PIL import Image, ImageDraw

from modules import imgflip_client as ic
from modules import imgflip_parse as ip
from modules import template_search as ts
from modules import template_store as st
from modules.kg import templates as kt

FRAME = "https://knowyourmeme.com/memes/distracted-boyfriend"
STAMPS = kt.stamps()


def fresh_store() -> st.TemplateStore:
    client = mongomock.MongoClient()
    s = st.TemplateStore.__new__(st.TemplateStore)
    s.client, s.db = client, client["memes"]
    s.entries = s.db["entries"]
    s.pages, s.templates = s.db["imgflip_pages"], s.db["imgflip_templates"]
    s.frames, s.assignments = s.db["frame_templates"], s.db["template_assignments"]
    return s


def entry_doc(url=FRAME, title="Distracted Boyfriend", **over) -> dict:
    doc = {"_id": kt.frame_key(url), "url": url, "title": title, "category": "meme",
           "entry_type": ["exploitable"], "additional_references": [],
           "og_image": "https://i.kym-cdn.com/entries/icons/original/000/1/db.jpg",
           "sections": []}
    doc.update(over)
    return doc


def jpeg(seed: int) -> bytes:
    import random
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


def search_html(results) -> str:
    boxes = "".join(
        f'<div class="mt-box"><h3 class="mt-title"><a href="{href}">{name}</a></h3>'
        f'<div class="mt-img-wrap"><img class="shadow" src="//i.imgflip.com/4/'
        f'{ip.key_from_template_id(tid)}.jpg"/></div></div>'
        for tid, name, href in results)
    return f'<html><div id="mt-boxes-wrap"><div class="mt-boxes">{boxes}</div></div></html>'


class FakeClient:
    """Serves search pages from a dict and images from seeds."""

    def __init__(self, pages: dict, images: dict):
        self.pages, self.images = pages, images
        self.requests = {"html": 0, "image": 0}
        self.credits_used = 0
        self.page_urls: list[str] = []

    def fetch_page(self, url, *, markers, end_of_results_on_500=False):
        self.requests["html"] += 1
        self.page_urls.append(url)
        html = self.pages.get(url)
        if html is None:
            return ic.PageResult(url=url, ok=False, status_code=500,
                                 error_kind="end_of_results" if end_of_results_on_500
                                 else "retryable", error="500")
        return ic.PageResult(url=url, ok=True, html=html, final_url=url, status_code=200)

    def fetch_image(self, url):
        self.requests["image"] += 1
        m = re.search(r"/([0-9a-z]+)\.(jpg|png)$", url)
        data = self.images.get(m.group(1)) if m else None
        if data is None:
            return ic.ImageResult(url=url, ok=False, status_code=404, error_kind="permanent")
        return ic.ImageResult(url=url, ok=True, content=data, content_type="image/jpeg",
                              status_code=200)


class PageArchiveTests(unittest.TestCase):
    def setUp(self):
        self.s = fresh_store()

    def test_round_trip_and_age(self):
        ok = ic.PageResult(url="https://imgflip.com/x", ok=True, html="<html>é</html>",
                           status_code=200).as_doc()
        self.assertEqual(self.s.save_page(ok, kind="search", query="x", page=1), "saved")
        self.assertEqual(self.s.get_page("https://imgflip.com/x")["html"], "<html>é</html>")
        self.assertIsNone(self.s.get_page("https://imgflip.com/x", max_age_days=-1))

    def test_a_failure_never_overwrites_a_good_page(self):
        url = "https://imgflip.com/x"
        self.s.save_page(ic.PageResult(url=url, ok=True, html="<html/>").as_doc(), kind="search")
        bad = ic.PageResult(url=url, ok=False, error="503", error_kind="retryable").as_doc()
        self.assertEqual(self.s.save_page(bad, kind="search"), "kept_ok")
        self.assertEqual(self.s.get_page(url)["html"], "<html/>")


class SelectionOfFramesTests(unittest.TestCase):
    def setUp(self):
        self.s = fresh_store()
        self.s.entries.insert_many([entry_doc(), entry_doc(
            "https://knowyourmeme.com/memes/some-event", "Some Event", category="event")])
        self.units = list(self.s.iter_units())

    def test_only_eligible_frames_are_units(self):
        self.assertEqual([u["frame_url"] for u in self.units], [FRAME])

    def searched(self, **over):
        record = {"unit_id": self.units[0]["unit_id"], "frame_url": FRAME,
                  "source_sha256": self.units[0]["source_sha256"],
                  "search_status": "searched", "candidates": [], "templates": {},
                  "hashes": {}}
        record.update(over)
        self.s.save_search(record, STAMPS)

    def test_searched_is_not_pending_and_each_stamp_requeues(self):
        self.searched()
        self.assertEqual(self.s.select_pending(self.units, stamps=STAMPS), [])
        for key in st.SEARCH_STAMP_KEYS:
            moved = dict(STAMPS, **{key: "moved"})
            self.assertEqual(len(self.s.select_pending(self.units, stamps=moved)), 1, key)

    def test_failed_edited_and_old_searches_requeue(self):
        self.searched(search_status="failed")
        self.assertEqual(len(self.s.select_pending(self.units, stamps=STAMPS)), 1)
        self.searched(source_sha256="edited")
        self.assertEqual(len(self.s.select_pending(self.units, stamps=STAMPS)), 1)
        self.searched()
        self.s.frames.update_one({}, {"$set": {"searched_at": kt_datetime(2020)}})
        self.assertEqual(len(self.s.select_pending(self.units, stamps=STAMPS,
                                                   research_after_days=180)), 1)
        self.assertEqual(self.s.select_pending(self.units, stamps=STAMPS), [])


def kt_datetime(year):
    from datetime import datetime, timezone
    return datetime(year, 1, 1, tzinfo=timezone.utc)


class TemplateMergeTests(unittest.TestCase):
    def test_a_search_never_downgrades_details(self):
        s = fresh_store()
        s.save_details(7, {"alt_names": ["guy looking back"], "file_type": "jpg"})
        s.upsert_templates({7: {"template_id": 7, "key": "7", "name": "Distracted Boyfriend",
                                "url": "https://imgflip.com/meme/7/x", "featured": False,
                                "animated": False}})
        doc = s.templates.find_one({"_id": 7})
        self.assertEqual((doc["alt_names"], doc["name"]),
                         (["guy looking back"], "Distracted Boyfriend"))


class EndToEndTests(unittest.TestCase):
    """search_units -> run_assignment -> fetch_details, over fakes."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.s = fresh_store()
        self.s.entries.insert_one(entry_doc())
        pic, other = jpeg(1), jpeg(2)
        k = ip.key_from_template_id
        results = [(10, "Distracted Boyfriend", "/meme/Distracted-Boyfriend"),
                   (11, "Distracted boyfriend", "/meme/11/Distracted-boyfriend"),
                   (12, "Distracted Boyfriend Reversed", "/meme/12/x"),
                   (13, "zzz", "/meme/13/zzz")]
        self.client = FakeClient(
            pages={ic.search_url("distracted boyfriend"): search_html(results),
                   ip.template_page_url(10): TEMPLATE_PAGE},
            images={k(10): pic, k(11): pic, k(12): other, k(13): jpeg(3),
                    k(10) + "-blank": pic})
        self.io = ts.StoreIO(self.client, self.s, Path(self.tmp.name), max_page_age_days=180)

    def tearDown(self):
        self.tmp.cleanup()

    def test_the_whole_stage(self):
        units = self.s.units_for([kt.frame_key(FRAME)])
        tally = ts.search_units(units, self.io, STAMPS)
        self.assertEqual((tally["frames"], tally["searched"]), (1, 1))

        counts = ts.run_assignment(self.s, "run-1")
        frame = self.s.frames.find_one({"_id": kt.frame_key(FRAME)})
        self.assertEqual(frame["status"], "selected")
        kept = [x["template_id"] for x in frame["selected"]]
        self.assertEqual(kept[0], 10)                  # the featured upload represents
        self.assertNotIn(11, kept)                     # its duplicate is merged
        self.assertEqual(self.s.templates.find_one({"_id": 11})["leader"], 10)
        self.assertIn(11, frame["selected"][0]["members"])
        self.assertIsNotNone(self.s.assignments.find_one({"_id": "run-1"})["completed_at"])
        self.assertEqual(counts["written"], 1)

        again = ts.run_assignment(self.s, "run-2")      # nothing changed
        self.assertEqual((again["written"], again["unchanged"]), (0, 1))

        # a frame's own imgflip link, resolved during the search, reads the
        # template's page without its image: still needs its details step
        self.s.save_details(10, {"name": "Distracted Boyfriend"})
        self.assertIn(10, self.s.templates_needing_details())
        got = ts.fetch_details([10], self.io)
        self.assertEqual((got["details"], got["blanks"]), (1, 1))
        doc = self.s.templates.find_one({"_id": 10})
        self.assertEqual(doc["alt_names"], ["distracted bf", "guy looking back"])
        self.assertTrue(Path(doc["blank_path"]).exists())
        self.assertEqual(doc["url"], "https://imgflip.com/meme/Distracted-Boyfriend")
        self.assertNotIn(10, self.s.templates_needing_details())

    def test_a_second_frame_with_the_same_query_costs_no_request(self):
        self.s.entries.insert_one(entry_doc("https://knowyourmeme.com/memes/db-2"))
        units = self.s.units_for([kt.frame_key(FRAME),
                                  kt.frame_key("https://knowyourmeme.com/memes/db-2")])
        ts.search_units(units, self.io, STAMPS)
        self.assertEqual(self.client.page_urls.count(ic.search_url("distracted boyfriend")), 1)
        self.assertEqual(self.io.tally["pages_cached"], 1)


TEMPLATE_PAGE = """<html><h1 id="mtm-title">Distracted Boyfriend Meme Template</h1>
<h2 id="mtm-subtitle">also called: distracted bf, guy looking back</h2>
<p>Template ID: 10</p><p>Format: jpg</p><p>Dimensions: 1200x800 px</p></html>"""


class FacadeContractTests(unittest.TestCase):
    """Every ``store.<name>`` the DAG calls exists and is exported."""

    def test_every_facade_the_dag_calls_exists_and_is_exported(self):
        dag = (Path(__file__).resolve().parents[1] / "kym_templates_dag.py").read_text()
        called = set(re.findall(r"\bstore\.([a-z_]+)\(", dag))
        self.assertTrue(called)
        for name in called:
            self.assertTrue(hasattr(st, name), name)
            self.assertIn(name, st.__all__, name)


if __name__ == "__main__":
    unittest.main()


class StratifiedSampleTests(unittest.TestCase):
    """The pool-measuring sample: each priority in proportion, reproducible."""

    def units(self):
        return ([{"unit_id": f"a{i:03d}", "priority": 1} for i in range(10)]
                + [{"unit_id": f"b{i:03d}", "priority": 2} for i in range(30)]
                + [{"unit_id": f"c{i:03d}", "priority": 3} for i in range(60)])

    def test_proportional_and_reproducible(self):
        from modules.template_store import stratified_sample
        got = stratified_sample(self.units(), 20, seed=7)
        self.assertEqual([sum(u["priority"] == p for u in got) for p in (1, 2, 3)], [2, 6, 12])
        self.assertEqual(got, stratified_sample(list(reversed(self.units())), 20, seed=7))
        self.assertNotEqual(got, stratified_sample(self.units(), 20, seed=8))
        self.assertEqual([u["priority"] for u in got], sorted(u["priority"] for u in got))
