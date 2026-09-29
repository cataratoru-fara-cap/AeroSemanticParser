"""Tests for kg/visual.py — synthetic images, no network.

What these pin:

  * **The same picture resized, re-encoded or mirrored stays close**, and
    a different picture does not — Gabi's duplicate rule.
  * **Transparency is flattened onto white**, so hidden RGB under an alpha
    channel cannot change a hash.
  * **BandIndex misses nothing within range**: every pair within the
    threshold that a brute-force scan finds, the index finds too.
"""
import io
import random
import unittest

from PIL import Image, ImageDraw

from modules.kg import visual


def picture(seed: int, size=(240, 160)) -> Image.Image:
    rnd = random.Random(seed)
    img = Image.new("RGB", size, (rnd.randrange(256), rnd.randrange(256), rnd.randrange(256)))
    draw = ImageDraw.Draw(img)
    for _ in range(12):
        x0, y0 = rnd.randrange(size[0]), rnd.randrange(size[1])
        x1, y1 = x0 + rnd.randrange(20, 120), y0 + rnd.randrange(20, 90)
        draw.rectangle([x0, y0, x1, y1], fill=(rnd.randrange(256), rnd.randrange(256),
                                                rnd.randrange(256)))
    return img


def encode(img: Image.Image, fmt="JPEG", **kw) -> bytes:
    buf = io.BytesIO()
    img.save(buf, fmt, **kw)
    return buf.getvalue()


def h(img: Image.Image, **kw) -> dict:
    return visual.hashes(encode(img, **kw)).as_doc()


class DuplicateRuleTests(unittest.TestCase):
    def setUp(self):
        self.base = picture(1)

    def test_resize_and_reencode_are_the_same_picture(self):
        a = h(self.base, quality=95)
        b = h(self.base.resize((150, 100)), quality=60)
        self.assertTrue(visual.same_picture(a, b), visual.distance(a, b))

    def test_a_mirror_image_is_the_same_picture(self):
        a = h(self.base)
        b = h(self.base.transpose(Image.Transpose.FLIP_LEFT_RIGHT))
        self.assertTrue(visual.same_picture(a, b), visual.distance(a, b))

    def test_a_different_picture_is_not(self):
        a, b = h(self.base), h(picture(2))
        self.assertFalse(visual.same_picture(a, b), visual.distance(a, b))

    def test_identical_bytes_always_match(self):
        data = encode(self.base)
        a = visual.hashes(data).as_doc()
        self.assertTrue(visual.same_picture(a, dict(a), max_phash=-1, max_dhash=-1))

    def test_hashes_are_64_bit_hex(self):
        got = h(self.base)
        for k in ("phash", "phash_mirror", "dhash", "dhash_mirror"):
            self.assertRegex(got[k], r"^[0-9a-f]{16}$")

    def test_transparency_is_flattened_onto_white(self):
        rgba = Image.new("RGBA", (100, 100), (255, 0, 0, 0))     # invisible red
        ImageDraw.Draw(rgba).ellipse([20, 20, 80, 80], fill=(0, 0, 0, 255))
        other = Image.new("RGBA", (100, 100), (0, 255, 0, 0))    # invisible green
        ImageDraw.Draw(other).ellipse([20, 20, 80, 80], fill=(0, 0, 0, 255))
        a = visual.hashes(encode(rgba, "PNG")).as_doc()
        b = visual.hashes(encode(other, "PNG")).as_doc()
        self.assertEqual((a["phash"], a["dhash"]), (b["phash"], b["dhash"]))

    def test_letterbox_bars_and_padding_do_not_change_the_picture(self):
        framed = Image.new("RGB", (240, 240), (0, 0, 0))            # bars top/bottom
        framed.paste(self.base, (0, 40))
        padded = Image.new("RGB", (300, 200), (255, 255, 255))      # white padding
        padded.paste(self.base, (30, 20))
        a = h(self.base)
        base_raw = visual.phash(visual.decode(encode(self.base)))
        for img in (framed, padded):
            other = h(img)
            self.assertTrue(visual.same_picture(a, other), visual.distance(a, other))
            # ...which is the trimming's doing: untrimmed, the bars move the hash.
            raw = visual.phash(visual.decode(encode(img)))
            self.assertGreater(visual.hamming(base_raw, raw), visual.distance(a, other)[0])

    def test_a_mostly_flat_image_is_not_trimmed_away(self):
        flat = Image.new("RGB", (200, 200), (255, 255, 255))
        ImageDraw.Draw(flat).rectangle([95, 95, 105, 105], fill=(0, 0, 0))
        self.assertEqual(visual.trim_borders(flat).size, (200, 200))

    def test_undecodable_bytes_raise(self):
        with self.assertRaises(visual.ImageDecodeError):
            visual.hashes(b"<html>404</html>")


class BandIndexTests(unittest.TestCase):
    def test_the_index_finds_every_pair_brute_force_finds(self):
        rnd = random.Random(3)
        items = []
        for n in range(300):
            value = rnd.getrandbits(64)
            if n % 3 and items:         # plant near neighbours
                base = int(items[rnd.randrange(len(items))][1]["phash"], 16)
                value = base
                for _ in range(rnd.randrange(0, 11)):
                    value ^= 1 << rnd.randrange(64)
            hx = f"{value:016x}"
            items.append((n, {"phash": hx, "phash_mirror": f"{rnd.getrandbits(64):016x}",
                              "dhash": "0" * 16, "dhash_mirror": "0" * 16}))
        brute = {(a, b) for i, (a, ha) in enumerate(items) for b, hb in items[i + 1:]
                 if visual.distance(ha, hb)[0] <= 8}
        found = {(a, b) for a, b, p, _d in visual.iter_pairs_within(items, 8)}
        self.assertEqual(found, brute)
        self.assertTrue(brute)


if __name__ == "__main__":
    unittest.main()
