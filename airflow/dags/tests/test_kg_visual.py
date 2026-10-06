"""kg/visual.py: synthetic images, no network. Pinned: the same picture resized,
re-encoded, mirrored, letterboxed or padded stays close, a different picture
does not (Gabi's duplicate rule); transparency is flattened onto white, so
hidden RGB under an alpha channel cannot change a hash; BandIndex misses
nothing within range (every pair a brute-force scan finds, it finds too)."""
import io
import random

import pytest
from PIL import Image, ImageDraw

from modules.kg import visual


def picture(seed, size=(240, 160)):
    rnd = random.Random(seed)
    colour = lambda: (rnd.randrange(256), rnd.randrange(256), rnd.randrange(256))
    img = Image.new("RGB", size, colour())
    draw = ImageDraw.Draw(img)
    for _ in range(12):
        x0, y0 = rnd.randrange(size[0]), rnd.randrange(size[1])
        draw.rectangle([x0, y0, x0 + rnd.randrange(20, 120), y0 + rnd.randrange(20, 90)], fill=colour())
    return img


def encode(img, fmt="JPEG", **kw):
    buf = io.BytesIO()
    img.save(buf, fmt, **kw)
    return buf.getvalue()


def h(img, fmt="JPEG", **kw):
    return visual.hashes(encode(img, fmt, **kw)).as_doc()


BASE = picture(1)


def on(size, colour, at):
    canvas = Image.new("RGB", size, colour)
    canvas.paste(BASE, at)
    return canvas


@pytest.mark.parametrize("a, other, same", [
    pytest.param(h(BASE, quality=95), h(BASE.resize((150, 100)), quality=60), True, id="resized-and-reencoded"),
    pytest.param(h(BASE), h(BASE.transpose(Image.Transpose.FLIP_LEFT_RIGHT)), True, id="mirrored"),
    pytest.param(h(BASE), h(picture(2)), False, id="a-different-picture"),
])
def test_the_duplicate_rule(a, other, same):
    assert visual.same_picture(a, other) is same, visual.distance(a, other)


@pytest.mark.parametrize("framed", [on((240, 240), (0, 0, 0), (0, 40)),          # bars top and bottom
                                    on((300, 200), (255, 255, 255), (30, 20))])    # white padding
def test_letterbox_bars_and_padding_do_not_change_the_picture(framed):
    a, other = h(BASE), h(framed)
    assert visual.same_picture(a, other), visual.distance(a, other)
    # ...which is the trimming's doing: untrimmed, the bars move the hash
    raw = lambda img: visual.phash(visual.decode(encode(img)))
    assert visual.hamming(raw(BASE), raw(framed)) > visual.distance(a, other)[0]


def test_identical_bytes_always_match_and_hashes_are_64_bit_hex():
    a = h(BASE)
    assert visual.same_picture(a, dict(a), max_phash=-1, max_dhash=-1)
    for k in ("phash", "phash_mirror", "dhash", "dhash_mirror"):
        assert len(a[k]) == 16 and set(a[k]) <= set("0123456789abcdef")


def test_transparency_is_flattened_onto_white():
    def disc(hidden):
        rgba = Image.new("RGBA", (100, 100), (*hidden, 0))                         # an invisible colour
        ImageDraw.Draw(rgba).ellipse([20, 20, 80, 80], fill=(0, 0, 0, 255))
        got = h(rgba, "PNG")
        return got["phash"], got["dhash"]
    assert disc((255, 0, 0)) == disc((0, 255, 0))


def test_a_mostly_flat_image_is_not_trimmed_away_and_undecodable_bytes_raise():
    flat = Image.new("RGB", (200, 200), (255, 255, 255))
    ImageDraw.Draw(flat).rectangle([95, 95, 105, 105], fill=(0, 0, 0))
    assert visual.trim_borders(flat).size == (200, 200)
    with pytest.raises(visual.ImageDecodeError):
        visual.hashes(b"<html>404</html>")


def test_the_band_index_finds_every_pair_brute_force_finds():
    rnd = random.Random(3)
    items = []
    for n in range(300):
        value = rnd.getrandbits(64)
        if n % 3 and items:                                                        # plant near neighbours
            value = int(items[rnd.randrange(len(items))][1]["phash"], 16)
            for _ in range(rnd.randrange(0, 11)):
                value ^= 1 << rnd.randrange(64)
        items.append((n, {"phash": f"{value:016x}", "phash_mirror": f"{rnd.getrandbits(64):016x}",
                          "dhash": "0" * 16, "dhash_mirror": "0" * 16}))
    brute = {(a, b) for i, (a, ha) in enumerate(items) for b, hb in items[i + 1:] if visual.distance(ha, hb)[0] <= 8}
    assert brute and {(a, b) for a, b, _p, _d in visual.iter_pairs_within(items, 8)} == brute
