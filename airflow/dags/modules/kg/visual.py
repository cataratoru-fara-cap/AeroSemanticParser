"""
visual.py — perceptual hashes for near-duplicate meme templates
================================================================
Pure: image bytes in, 64-bit hashes out. Pillow decodes; numpy and scipy do
the rest. No HTTP, no Mongo.

Why perceptual hashes, and which
--------------------------------
imgflip holds many uploads of one picture. On the "distracted boyfriend"
search, about 25 of the 40 results are the same photo (2026-09-28). A
few are byte-identical, but most are resized, re-encoded, or mirrored. An
MD5 catches only the byte-identical ones. A perceptual hash catches the
others: two images that LOOK alike get hashes a few bits apart.

The rule Gabi set is "same picture": exact copies, resizes, re-encodes and
horizontal mirrors merge. A crop, a multi-panel extension, or a copy with
labels drawn on stays a separate template, because the pool should be as
VARIED as possible. Two hashes enforce that rule better than one:

    phash   the DCT of a 32x32 grey image, top-left 8x8 against its median.
            Robust to scaling and JPEG. The same algorithm as imagehash's
            ``phash``, reimplemented here so the values depend only on this
            file, numpy, scipy and Pillow's resampler.
    dhash   the sign of horizontal gradients of a 9x8 grey image. It sees
            layout that pHash smooths over, so requiring BOTH to be close
            keeps a labelled edit apart from its blank.

A mirrored upload has its own pHash; ``phash_mirror`` is the pHash of the
flipped image, so "A equals B mirrored" is ``hamming(phash_mirror(A),
phash(B))``.

Hashes are stored as 16-character hex strings, not ints: an unsigned
64-bit value does not fit Mongo's signed int64.

Pillow's resampling filters change between releases, so VISUAL_VERSION
includes Pillow's version, and a new Pillow re-hashes rather than mixing
two hash spaces.
"""

from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from typing import Iterable, Iterator

import numpy as np

HASH_BITS = 64
# 1.1.0: uniform borders (letterbox bars, white padding) are trimmed before
# hashing — the pilot's commonest leak was one picture uploaded with and
# without bars.
_ALGORITHM_VERSION = "1.1.0"

# A border row/column: at least BORDER_SHARE of its pixels within
# BORDER_TOLERANCE grey levels of that edge's own colour (JPEG noise).
BORDER_TOLERANCE = 24
BORDER_SHARE = 0.98
# Trimming may take at most this share of either axis; past it the image is
# mostly flat colour and the "border" is the picture.
BORDER_MAX_TRIM = 0.5


def _pillow_version() -> str:
    try:
        import PIL
        return PIL.__version__
    except ImportError:              # pragma: no cover - worker without Pillow
        return "none"


VISUAL_VERSION = f"{_ALGORITHM_VERSION}+pillow{_pillow_version()}"


class ImageDecodeError(ValueError):
    """The bytes are not an image Pillow can read."""


@dataclass(frozen=True)
class Hashes:
    md5: str
    phash: str
    phash_mirror: str
    dhash: str
    dhash_mirror: str
    width: int
    height: int

    def as_doc(self) -> dict:
        return {"md5": self.md5, "phash": self.phash,
                "phash_mirror": self.phash_mirror, "dhash": self.dhash,
                "dhash_mirror": self.dhash_mirror,
                "width": self.width, "height": self.height}


# ---------------------------------------------------------------------------
# Decoding and hashing
# ---------------------------------------------------------------------------

def decode(data: bytes):
    """Bytes -> an RGB PIL image, with any transparency flattened onto white.

    A transparent template (imgflip flags ``has_transparency``) would
    otherwise hash its hidden RGB channels, which are arbitrary.
    """
    from PIL import Image

    try:
        img = Image.open(io.BytesIO(data))
        img.seek(0)                  # the first frame of a GIF
        img.load()
    except Exception as exc:         # Pillow raises a zoo of types
        raise ImageDecodeError(f"{exc.__class__.__name__}: {exc}") from exc
    if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
        rgba = img.convert("RGBA")
        white = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
        return Image.alpha_composite(white, rgba).convert("RGB")
    return img.convert("RGB")


def trim_borders(img):
    """The image without uniform bars on any side: letterboxing, white
    padding, a frame. Each side is judged against its own edge colour, so
    black bars above and white padding below both go."""
    grey = np.asarray(img.convert("L"), dtype=np.int16)
    h, w = grey.shape
    if h < 16 or w < 16:
        return img

    def flat(line: np.ndarray, ref: float) -> bool:
        return np.mean(np.abs(line - ref) <= BORDER_TOLERANCE) >= BORDER_SHARE

    def depth(lines, limit: int) -> int:
        ref = float(np.median(lines(0)))
        n = 0
        while n < limit and flat(lines(n), ref):
            n += 1
        return n

    max_h, max_w = int(h * BORDER_MAX_TRIM), int(w * BORDER_MAX_TRIM)
    top = depth(lambda i: grey[i, :], max_h)
    bottom = depth(lambda i: grey[h - 1 - i, :], max_h)
    left = depth(lambda i: grey[:, i], max_w)
    right = depth(lambda i: grey[:, w - 1 - i], max_w)
    if top + bottom >= max_h:
        top = bottom = 0
    if left + right >= max_w:
        left = right = 0
    if not (top or bottom or left or right):
        return img
    return img.crop((left, top, w - right, h - bottom))


def _bits_to_hex(bits: np.ndarray) -> str:
    value = 0
    for bit in bits.flatten():
        value = (value << 1) | int(bool(bit))
    return f"{value:0{HASH_BITS // 4}x}"


def phash(img) -> str:
    from PIL import Image
    from scipy.fft import dct

    grey = img.convert("L").resize((32, 32), Image.Resampling.LANCZOS)
    pixels = np.asarray(grey, dtype=np.float64)
    freq = dct(dct(pixels, axis=0), axis=1)
    low = freq[:8, :8]
    return _bits_to_hex(low > np.median(low))


def dhash(img) -> str:
    from PIL import Image

    grey = img.convert("L").resize((9, 8), Image.Resampling.LANCZOS)
    pixels = np.asarray(grey, dtype=np.int16)
    return _bits_to_hex(pixels[:, 1:] > pixels[:, :-1])


def hashes(data: bytes) -> Hashes:
    """Every hash the dedup rule uses, from one image's bytes."""
    from PIL import ImageOps

    img = decode(data)
    width, height = img.width, img.height
    img = trim_borders(img)
    mirrored = ImageOps.mirror(img)
    return Hashes(md5=hashlib.md5(data).hexdigest(),
                  phash=phash(img), phash_mirror=phash(mirrored),
                  dhash=dhash(img), dhash_mirror=dhash(mirrored),
                  width=width, height=height)


def hamming(a: str, b: str) -> int:
    return (int(a, 16) ^ int(b, 16)).bit_count()


# ---------------------------------------------------------------------------
# The duplicate rule
# ---------------------------------------------------------------------------

# Starting thresholds; kg/templates.py owns the calibrated values and passes
# them in. Measured, not chosen: see the phase-0 calibration notes in
# kg/templates.py.
DEFAULT_MAX_PHASH = 8
DEFAULT_MAX_DHASH = 12


def distance(a: dict, b: dict) -> tuple[int, int]:
    """(pHash distance, dHash distance) between two hashed images, taking
    the closer of "as is" and "one of them mirrored"."""
    straight = (hamming(a["phash"], b["phash"]), hamming(a["dhash"], b["dhash"]))
    flipped = (hamming(a["phash_mirror"], b["phash"]),
               hamming(a["dhash_mirror"], b["dhash"]))
    return min(straight, flipped)


def same_picture(a: dict, b: dict, *, max_phash: int = DEFAULT_MAX_PHASH,
                 max_dhash: int = DEFAULT_MAX_DHASH) -> bool:
    """Gabi's rule: the same picture, resized, re-encoded or mirrored."""
    if a.get("md5") and a.get("md5") == b.get("md5"):
        return True
    p, d = distance(a, b)
    return p <= max_phash and d <= max_dhash


# ---------------------------------------------------------------------------
# Candidate retrieval: multi-index hashing
# ---------------------------------------------------------------------------

def _bands(hex_hash: str, n_bands: int) -> Iterator[tuple[int, int]]:
    """Split a 64-bit hash into ``n_bands`` contiguous bit ranges."""
    value = int(hex_hash, 16)
    bounds = np.linspace(0, HASH_BITS, n_bands + 1).astype(int)
    for i in range(n_bands):
        lo, hi = int(bounds[i]), int(bounds[i + 1])
        width = hi - lo
        yield i, (value >> (HASH_BITS - hi)) & ((1 << width) - 1)


class BandIndex:
    """Find every stored pHash within ``max_distance`` bits of a query.

    By pigeonhole, two 64-bit hashes that differ in at most ``d`` bits agree
    exactly on at least one of ``d + 1`` disjoint bands. So indexing each
    band and taking the union of exact band matches returns a superset of
    the true neighbours, which ``same_picture`` then verifies. Linear
    scanning 100k leaders for each of 100k candidates would be 10^10
    comparisons; this is a few dict lookups each.
    """

    def __init__(self, max_distance: int = DEFAULT_MAX_PHASH):
        self.n_bands = max_distance + 1
        self._tables: list[dict[int, list[int]]] = [{} for _ in range(self.n_bands)]
        self._items: list[tuple[object, dict]] = []

    def add(self, key: object, hashed: dict) -> None:
        pos = len(self._items)
        self._items.append((key, hashed))
        for band, value in _bands(hashed["phash"], self.n_bands):
            self._tables[band].setdefault(value, []).append(pos)

    def candidates(self, hashed: dict) -> list[tuple[object, dict]]:
        """Stored items that MAY be within range of ``hashed`` or of its
        mirror image, in insertion order."""
        seen: set[int] = set()
        for probe in (hashed["phash"], hashed["phash_mirror"]):
            for band, value in _bands(probe, self.n_bands):
                seen.update(self._tables[band].get(value, ()))
        return [self._items[pos] for pos in sorted(seen)]

    def __len__(self) -> int:
        return len(self._items)


def iter_pairs_within(items: Iterable[tuple[object, dict]],
                      max_phash: int) -> Iterator[tuple[object, object, int, int]]:
    """Every pair of ``items`` within ``max_phash`` (straight or mirrored),
    with its (pHash, dHash) distance. For calibration and review, not the
    hot path."""
    index = BandIndex(max_phash)
    for key, hashed in items:
        for other_key, other in index.candidates(hashed):
            p, d = distance(hashed, other)
            if p <= max_phash:
                yield other_key, key, p, d
        index.add(key, hashed)
