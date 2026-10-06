"""kg/tag_normalize.py: plural folding for the tags folksonomy. The four
grounding pairs are real corpus counts (2026-09-17 census): catchphrase/
catchphrases 645/632, exploitable/exploitables 895/798, image macro/image
macros 800/514, meme/memes 1050/631, the fragmentation this module exists to
fix."""
import pytest

from modules.kg import tag_normalize as tn


@pytest.mark.parametrize("tag, folded", [
    ("catchphrase", "catchphrase"), ("catchphrases", "catchphrase"), ("exploitable", "exploitable"),
    ("exploitables", "exploitable"), ("image macro", "image macro"), ("image macros", "image macro"),
    ("meme", "meme"), ("memes", "meme"),                          # the four grounding pairs
    ("gas", "gas"), ("bus", "bus"), ("ai", "ai"), ("a", "a"),     # under 4 characters: "gas" would become "ga"
    ("boss", "boss"),                                             # a double s is not the plain s rule
    ("glasses", "glass"), ("boxes", "box"),                       # sibilant -es folds two characters
    ("", ""),
])
def test_fold(tag, folded):
    assert tn.fold(tag) == folded and tn.fold(folded) == folded   # idempotent


def test_the_denylist():
    deny = frozenset({"news", "star wars"})
    assert (tn.fold("news", deny), tn.fold("star wars", deny), tn.fold("memes", frozenset({"news"}))) == \
        ("news", "star wars", "meme")


@pytest.mark.parametrize("text, want", [
    ("do_not_fold:\n  - News\n  - Star Wars\n", frozenset({"news", "star wars"})),   # lowercased
    ("other_key: []\n", frozenset()),                                                 # a missing key: empty
])
def test_load_denylist(tmp_path, text, want):
    path = tmp_path / "deny.yaml"
    path.write_text(text, encoding="utf-8")
    deny = tn.load_denylist(str(path))
    assert deny == want and tn.fold("news", deny) == ("news" if want else "new")      # round-trips through fold
