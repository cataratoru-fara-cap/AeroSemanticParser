"""kg/curation.py, pure (fixture lexicon, fake model client). Pinned: the rules
in order (a title is always kept; the frame's own item is kept; a deny item
beats a platform; a platform, by class tree or list, beats a deny class; deny
classes match DIRECT classes only; agreement with the title, or of a tag with
the About, keeps; anything else waits for the judge); an item with only P279
(t-shirt) is classed by its own ancestors; mention identity is order-free; the
judge is extractive (every number exactly once), batched, and a failed call is
data; the shipped lists and schema load, and a QID on two lists is refused."""
import json
from types import SimpleNamespace

import pytest

from helpers import KG_CONFIG
from modules.kg import curation as C

LISTS = C.Lists(platform_classes={100: "platform"}, platform_items={200: "Twitter"}, deny_classes={300: "anatomy"},
                deny_items={400: "popularity"}, version="v")


def mention(field, qid, start=0, end=4, *, tag_index=None, method="noun_chunk", text="word", label=None,
            description=None):
    m = {"field": field, "qid": qid, "start": start, "end": end, "method": method, "text": text,
         "label": label or qid, "description": description}
    return m if tag_index is None else {**m, "tag_index": tag_index}


class FakeClasses:
    def __init__(self, closure=None, direct=None):
        self._closure, self._direct = closure or {}, direct or {}

    def closure(self, q):
        return frozenset(self._closure.get(q, ()))

    def direct(self, q):
        return frozenset(self._direct.get(q, ()))


def lists(**over):
    return C.Lists(**{**LISTS.__dict__, **over})


def decide(mentions, classes=None, lists_=LISTS, **record):
    return [(d["keep"], d["basis"]) for d in C.apply_rules({"mentions": mentions, **record}, lists_,
                                                           classes or FakeClasses())]


# -- lists and mention identity ---------------------------------------------------------

def test_the_shipped_lists_load(tmp_path):
    lst = C.load_lists(str(KG_CONFIG / "entity_curation.yaml"))
    assert 6002242 in lst.format_items and 478798 in lst.generic_items         # image macro; image
    assert 2927074 not in lst.generic_items                                    # internet meme: the judge's (Gabi)
    assert 15718485 in lst.platform_items and 112826905 in lst.deny_classes and 1357284 in lst.deny_items
    assert len(lst.version) == 16
    path = tmp_path / "c.yaml"
    path.write_text("platform_items:\n  - {qid: Q1, label: a}\ndeny_items:\n  - {qid: Q1, label: a}\n")
    with pytest.raises(ValueError, match="Q1 is on both"):
        C.load_lists(str(path))
    path.write_text("deny_items:\n  - {qid: X1}\n")
    with pytest.raises(ValueError, match="not a QID"):
        C.load_lists(str(path))


def test_mention_key_and_sha():
    a, b = mention("about", "Q1", 3, 7), mention("tag", "Q2", 0, 4, tag_index=2)
    assert (C.mention_key(a), C.mention_key(b)) == ("about|-1|3|7|Q1", "tag|2|0|4|Q2")
    assert C.mentions_sha([a, b]) == C.mentions_sha([b, a]) != C.mentions_sha([a, dict(b, qid="Q3")])


# -- the rules ----------------------------------------------------------------------------

@pytest.mark.parametrize("mentions, classes, lists_, record, want", [
    ([mention("title", "Q400"), mention("about", "Q9", method="kym_id"), mention("tag", "Q8", tag_index=0)],
     None, LISTS, {"self_qid": "Q8"}, [(True, "title"), (True, "own_item"), (True, "own_item")]),  # even a deny item
    ([mention("about", "Q200")], None, lists(deny_items={200: "x"}, platform_items={}), {}, [(False, "deny_item")]),
    ([mention("about", "Q5"), mention("about", "Q200")], FakeClasses(closure={5: {77, 100}}, direct={5: {300}}),
     LISTS, {}, [(True, "platform"), (True, "platform")]),                    # platform beats a deny class
    # Q6's class TREE reaches 300 but its direct classes do not
    ([mention("about", "Q6"), mention("about", "Q7")], FakeClasses(closure={6: {300}, 7: {300}}, direct={7: {300}}),
     LISTS, {}, [(None, C.PENDING), (False, "deny_class")]),
    ([mention("title", "Q1"), mention("about", "Q1", 10, 14), mention("tag", "Q2", tag_index=0),
      mention("about", "Q2", 20, 24), mention("about", "Q3", 30, 34), mention("tag", "Q4", tag_index=1)],
     None, LISTS, {}, [(True, "title"), (True, "title_agrees"), (True, "tag_and_text"), (True, "tag_and_text"),
                       (None, C.PENDING), (None, C.PENDING)]),
    ([mention("tag", "Q5", tag_index=0, method="tag", label="Drake"),
      mention("tag", "Q6", tag_index=1, method="tag", label="hat"),
      mention("tag", "Q7", tag_index=2, method="noun_chunk", label="Doge"),
      mention("tag", "Q8", tag_index=3, method="tag", label="2014")],
     None, LISTS, {}, [(True, "tag_named"), (None, C.PENDING), (None, C.PENDING), (None, C.PENDING)]),
    # formats keep; generic words drop only from the About, and agreement still wins
    ([mention("about", "Q500"), mention("about", "Q600", 10, 15), mention("tag", "Q600", tag_index=0),
      mention("about", "Q601", 20, 25), mention("title", "Q601", 0, 4)],
     None, lists(format_items={500: "image macro"}, generic_items={600: "image", 601: "man"}), {},
     [(True, "format"), (True, "tag_and_text"), (True, "tag_and_text"), (True, "title_agrees"), (True, "title")]),
    ([mention("about", "Q600"), mention("tag", "Q602", tag_index=0)], None,
     lists(format_items={500: "image macro"}, generic_items={600: "image", 601: "man"}), {},
     [(False, "generic_item"), (None, C.PENDING)]),
    # denied classes spare tags and what the title names
    ([mention("tag", "Q7", tag_index=0, label="muscle"), mention("about", "Q7", 10, 14)],
     FakeClasses(direct={7: {300}}), LISTS, {}, [(True, "tag_and_text"), (True, "tag_and_text")]),
    ([mention("tag", "Q7", tag_index=0, label="muscle")], FakeClasses(direct={7: {300}}), LISTS, {}, [(None, C.PENDING)]),
    ([mention("title", "Q7"), mention("about", "Q7", 10, 14)], FakeClasses(direct={7: {300}}), LISTS, {},
     [(True, "title"), (True, "title_agrees")]),
], ids=["title and own item", "deny item beats platform", "platform by tree or list", "deny classes are direct",
        "agreement keeps, the rest waits", "a whole tag naming something", "formats and generic words",
        "generic About words drop", "denied class spared by a tag", "a lone denied tag waits",
        "denied class spared by the title"])
def test_rules(mentions, classes, lists_, record, want):
    assert decide(mentions, classes, lists_, **record) == want


def test_resolve_fills_only_what_the_judge_answered():
    dec = C.apply_rules({"mentions": [mention("about", "Q3"), mention("tag", "Q4", tag_index=0),
                                      mention("about", "Q400", 9, 12)]}, LISTS, FakeClasses())
    assert [(d["keep"], d["basis"]) for d in C.resolve(dec, {"Q3": True, "Q400": True})] == \
        [(True, "judge"), (None, C.PENDING), (False, "deny_item")]


def test_the_class_index_and_the_rules_on_the_fixture_lexicon(fixture_lexicon):
    classes = C.ClassIndex(fixture_lexicon)
    assert 3220391 in classes.direct(918) and 35127 in classes.closure(918)      # X: a social network, a website
    assert 35127 not in classes.direct(918)
    assert 11460 in classes.closure(131151) and 11460 in classes.direct(131151)   # t-shirt: P279 only
    lst = C.Lists(platform_classes={35127: "website"}, platform_items={},
                  deny_classes={112826905: "anatomical entity", 24034552: "mathematical concept"},
                  deny_items={1357284: "popularity"}, version="v")
    rec = {"mentions": [mention("about", "Q918"), mention("about", "Q37017", 5, 9),
                        mention("about", "Q170198", 10, 16), mention("about", "Q1357284", 20, 30),
                        mention("about", "Q7725310", 40, 46)]}
    assert [(d["qid"], d["basis"]) for d in C.apply_rules(rec, lst, classes)] == [
        ("Q918", "platform"), ("Q37017", "deny_class"), ("Q170198", "deny_class"), ("Q1357284", "deny_item"),
        ("Q7725310", C.PENDING)]


# -- the judge ------------------------------------------------------------------------------

class FakeClient:
    """Validates like the real client: a reply the validator refuses is a failed call."""

    def __init__(self, replies):
        self.replies, self.calls = list(replies), []

    def chat(self, messages, request, *, purpose, format, options, think, validate):
        self.calls.append({"messages": messages, "format": format, "think": think, "options": options,
                           "purpose": purpose})
        content = self.replies.pop(0)
        try:
            parsed = validate(content)
        except ValueError as exc:
            return SimpleNamespace(ok=False, parsed=None, error=str(exc), error_kind="invalid", attempts=1,
                                   model=None, digest=None, host=None)
        return SimpleNamespace(ok=True, parsed=parsed, error=None, error_kind=None, attempts=1,
                               model=request.model, digest="d", host="h")


def reply(*keeps):
    """True: a kept role, False: a dropped one, a str: that role."""
    roles = [k if isinstance(k, str) else ("subject" if k else "incidental") for k in keeps]
    return json.dumps({"items": [{"n": i, "role": r} for i, r in enumerate(roles, 1)]})


SCHEMA, SHA = C.load_schema(str(KG_CONFIG / "entity_curation_schema.json"))
REQUEST = SimpleNamespace(model="m")
CONTEXT = C.frame_context({"title": "Doge", "tags": ["shiba inu"],
                           "sections": [{"kind": "about", "text": ["Doge is a Shiba Inu."]}]})


def items(n):
    return [{"qid": f"Q{i}", "label": f"item {i}", "description": "d", "texts": [f"w{i}"], "fields": ["about"]}
            for i in range(1, n + 1)]


def judge(client, its, **kw):
    return C.judge_frame(client, REQUEST, CONTEXT, its, schema=SCHEMA, **kw)


def test_verdicts_by_number():
    client = FakeClient([reply(True, False)])
    got = judge(client, items(2))
    assert got["ok"] and got["verdicts"] == {"Q1": True, "Q2": False}
    call = client.calls[0]
    assert (call["purpose"], call["think"], call["options"]["temperature"]) == (C.JUDGE_PURPOSE, False, 0)
    assert call["options"]["num_predict"] > 2 * 12
    rows = call["format"]["properties"]["items"]
    assert (rows["items"]["properties"]["n"]["enum"], rows["minItems"], rows["maxItems"]) == ([1, 2], 2, 2)
    assert "Doge is a Shiba Inu." in call["messages"][1]["content"]


def test_roles_decide_keep():
    roles = ["subject", "source", "format", "platform", "incidental", "wrong_sense"]
    got = judge(FakeClient([reply(*roles)]), items(6))
    assert list(got["verdicts"].values()) == [True] * 4 + [False] * 2 and list(got["roles"].values()) == roles
    assert set(C.KEEP_ROLES) | set(C.DROP_ROLES) == set(SCHEMA["properties"]["items"]["items"]["properties"]["role"]["enum"])


def row(n, role="subject", **x):
    return {"n": n, "role": role, **x}


@pytest.mark.parametrize("bad", [
    reply(True),                                                   # one missing
    json.dumps({"items": [row(1), row(1)]}), json.dumps({"items": [row(1), row(3)]}),
    json.dumps({"items": [row(1, why="x"), row(2)]}), json.dumps({"items": [row(1), row(2, "relevant")]}),
    json.dumps({"items": [{"n": 1, "keep": True}, row(2)]}), "not json",
])
def test_every_number_exactly_once_and_nothing_else(bad):
    got = judge(FakeClient([bad]), items(2))
    assert (got["ok"], got["error_kind"]) == (False, "invalid")


def test_large_frames_are_asked_in_batches_and_a_failed_batch_keeps_what_was_answered():
    n = C.MAX_JUDGE_ITEMS + 3
    client = FakeClient([reply(*[True] * C.MAX_JUDGE_ITEMS), reply(False, False, True)])
    got = judge(client, items(n))
    assert got["ok"] and len(client.calls) == 2 and len(got["verdicts"]) == n
    assert (got["verdicts"][f"Q{n}"], got["verdicts"][f"Q{n - 1}"]) == (True, False)
    got = judge(FakeClient([reply(*[True] * C.MAX_JUDGE_ITEMS), "nope"]), items(C.MAX_JUDGE_ITEMS + 1))
    assert not got["ok"] and len(got["verdicts"]) == C.MAX_JUDGE_ITEMS


def test_a_second_model_confirms_about_only_keeps():
    its = items(3)
    its[1]["fields"] = ["tag"]
    client = FakeClient([reply(True, True, False), reply("incidental")])     # the confirmer sees Q1 only
    got = judge(client, its, confirm=SimpleNamespace(model="c"))
    assert got["ok"] and got["verdicts"] == {"Q1": False, "Q2": True, "Q3": False}
    assert got["roles"]["Q1"] == "subject" and got["confirm"]["roles"] == {"Q1": "incidental"}
    assert [c["purpose"] for c in client.calls] == [C.JUDGE_PURPOSE, C.CONFIRM_PURPOSE]
    assert [c["messages"][0]["content"] for c in client.calls] == [C.SYSTEM_PROMPT, C.CONFIRM_PROMPT]
    assert C.SYSTEM_PROMPT != C.CONFIRM_PROMPT
    assert "1. item 1" in client.calls[1]["messages"][1]["content"]
    assert "item 2" not in client.calls[1]["messages"][1]["content"]


def test_nothing_to_confirm_asks_once_and_a_failed_confirm_fails():
    client = FakeClient([reply(False, False)])
    assert judge(client, items(2), confirm=SimpleNamespace(model="c"))["ok"] and len(client.calls) == 1
    got = judge(FakeClient([reply(True), "nope"]), items(1), confirm=SimpleNamespace(model="c"))
    assert not got["ok"] and got["error"].startswith("confirm:")


def test_items_carry_what_they_were_read_from():
    rec = {"mentions": [mention("about", "Q1", 11, 20, text="Shiba Inu", label="Shiba Inu", description="dog breed"),
                        mention("tag", "Q1", 0, 9, tag_index=0, text="shiba inu"),
                        mention("about", "Q2", 4, 7, text="dog", label="dog")]}
    its = C.judge_items(rec, C.apply_rules(rec, LISTS, FakeClasses()),
                        about="The dog, a Shiba Inu named Kabosu, became Doge.")
    assert [it["qid"] for it in its] == ["Q2"]                                 # Q1: tag_and_text
    assert its[0]["snippet"] == "The [[dog]], a Shiba Inu named Kabosu, became Doge."
    text = C.render_items(its)
    assert '1. dog (read from "dog" in the About)' in text and "context: The [[dog]]" in text


def test_stamps_and_the_judge_never_falls_back():
    assert C.judge_stamps(REQUEST, "s", SimpleNamespace(model="c")) == {
        "judge_prompt_version": C.JUDGE_PROMPT_VERSION, "judge_schema_sha": "s", "judge_model": "m",
        "judge_confirm_model": "c"}
    assert C.judge_stamps(REQUEST, "s")["judge_confirm_model"] == ""
    assert set(C.rule_stamps(LISTS, "lex")) == {"curation_version", "curation_lists_version", "lexicon_version"}
    # a verdict is stamped with the REQUESTED model: another's answer would be filed under its name
    for req in (C.model_request({}), C.confirm_request({}), C.model_request({"KG_CURATION_MODEL": "x"})):
        assert not req.allow_fallback
    assert (C.model_request({}).model, C.model_request({"KG_CURATION_MODEL": "x"}).model) == (C.DEFAULT_JUDGE_MODEL, "x")
