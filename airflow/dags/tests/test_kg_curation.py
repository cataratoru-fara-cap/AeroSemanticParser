"""Tests for kg/curation.py (pure: fixture lexicon, fake model client).

What these pin:

  * **The rules, in order.** A title is always kept; the frame's own item
    is kept; a deny item beats a platform; a platform (by the class tree,
    or listed) beats a deny class; deny classes match DIRECT classes only;
    agreement with the title, or between a tag and the About, keeps;
    anything else waits for the judge.
  * **An item with only P279** (t-shirt) is classed by its own ancestors.
  * **Mention identity** is order-free and moves with any mention.
  * **The judge is extractive**: every number exactly once, nothing else;
    batches of MAX_JUDGE_ITEMS; a failed call is data.
  * **The shipped lists and schema load**, and a qid on two lists is refused.
"""
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from modules.kg import curation as C, wikidata as wd
from wikidata_fixture import write_dump

CONFIG = Path(__file__).resolve().parents[1] / "kg_config"


def mention(field, qid, start=0, end=4, *, tag_index=None, method="noun_chunk",
            text="word", label=None, description=None):
    m = {"field": field, "qid": qid, "start": start, "end": end, "method": method,
         "text": text, "label": label or qid, "description": description}
    if tag_index is not None:
        m["tag_index"] = tag_index
    return m


class FakeClasses:
    def __init__(self, closure=None, direct=None):
        self._closure, self._direct = closure or {}, direct or {}

    def closure(self, q):
        return frozenset(self._closure.get(q, ()))

    def direct(self, q):
        return frozenset(self._direct.get(q, ()))


LISTS = C.Lists(platform_classes={100: "platform"}, platform_items={200: "Twitter"},
                deny_classes={300: "anatomy"}, deny_items={400: "popularity"},
                version="v")


class ListTests(unittest.TestCase):
    def test_shipped_lists_load(self):
        lists = C.load_lists(str(CONFIG / "entity_curation.yaml"))
        self.assertIn(6002242, lists.format_items)             # image macro
        self.assertIn(478798, lists.generic_items)             # image
        self.assertNotIn(2927074, lists.generic_items)         # internet meme: the judge's (Gabi)
        self.assertIn(15718485, lists.platform_items)          # Twitter
        self.assertIn(112826905, lists.deny_classes)           # anatomical entity
        self.assertIn(1357284, lists.deny_items)               # popularity
        self.assertEqual(len(lists.version), 16)

    def test_a_qid_on_two_lists_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "c.yaml")
            Path(path).write_text("platform_items:\n  - {qid: Q1, label: a}\n"
                                  "deny_items:\n  - {qid: Q1, label: a}\n")
            with self.assertRaisesRegex(ValueError, "Q1 is on both"):
                C.load_lists(path)
            Path(path).write_text("deny_items:\n  - {qid: X1}\n")
            with self.assertRaisesRegex(ValueError, "not a QID"):
                C.load_lists(path)


class MentionKeyTests(unittest.TestCase):
    def test_key_and_sha(self):
        a = mention("about", "Q1", 3, 7)
        b = mention("tag", "Q2", 0, 4, tag_index=2)
        self.assertEqual(C.mention_key(a), "about|-1|3|7|Q1")
        self.assertEqual(C.mention_key(b), "tag|2|0|4|Q2")
        self.assertEqual(C.mentions_sha([a, b]), C.mentions_sha([b, a]))
        self.assertNotEqual(C.mentions_sha([a, b]),
                            C.mentions_sha([a, dict(b, qid="Q3")]))


class RuleTests(unittest.TestCase):
    def decide(self, mentions, classes=None, **record):
        rec = {"mentions": mentions, **record}
        return [(d["keep"], d["basis"]) for d in
                C.apply_rules(rec, LISTS, classes or FakeClasses())]

    def test_title_and_own_item_are_always_kept(self):
        got = self.decide([mention("title", "Q400"),               # even a deny item
                           mention("about", "Q9", method="kym_id"),
                           mention("tag", "Q8", tag_index=0)], self_qid="Q8")
        self.assertEqual(got, [(True, "title"), (True, "own_item"), (True, "own_item")])

    def test_deny_item_beats_platform(self):
        lists = C.Lists(**{**LISTS.__dict__, "deny_items": {200: "x"},
                           "platform_items": {}})
        got = C.apply_rules({"mentions": [mention("about", "Q200")]}, lists, FakeClasses())
        self.assertEqual((got[0]["keep"], got[0]["basis"]), (False, "deny_item"))

    def test_platform_by_class_tree_or_list_beats_deny_class(self):
        classes = FakeClasses(closure={5: {77, 100}}, direct={5: {300}})
        got = self.decide([mention("about", "Q5"), mention("about", "Q200")], classes)
        self.assertEqual(got, [(True, "platform"), (True, "platform")])

    def test_deny_classes_match_direct_classes_only(self):
        # Q6's class TREE reaches 300 but its direct classes do not
        classes = FakeClasses(closure={6: {300}, 7: {300}}, direct={7: {300}})
        got = self.decide([mention("about", "Q6"), mention("about", "Q7")], classes)
        self.assertEqual(got, [(None, C.PENDING), (False, "deny_class")])

    def test_agreement_keeps_and_the_rest_waits(self):
        got = self.decide([mention("title", "Q1"), mention("about", "Q1", 10, 14),
                           mention("tag", "Q2", tag_index=0), mention("about", "Q2", 20, 24),
                           mention("about", "Q3", 30, 34), mention("tag", "Q4", tag_index=1)])
        self.assertEqual(got, [(True, "title"), (True, "title_agrees"),
                               (True, "tag_and_text"), (True, "tag_and_text"),
                               (None, C.PENDING), (None, C.PENDING)])

    def test_a_whole_tag_naming_something_is_kept(self):
        got = self.decide([mention("tag", "Q5", tag_index=0, method="tag", label="Drake"),
                           mention("tag", "Q6", tag_index=1, method="tag", label="hat"),
                           mention("tag", "Q7", tag_index=2, method="noun_chunk", label="Doge"),
                           mention("tag", "Q8", tag_index=3, method="tag", label="2014")])
        self.assertEqual(got, [(True, "tag_named"), (None, C.PENDING), (None, C.PENDING),
                               (None, C.PENDING)])

    def test_formats_keep_and_generic_words_drop_only_from_the_about(self):
        lists = C.Lists(**{**LISTS.__dict__, "format_items": {500: "image macro"},
                           "generic_items": {600: "image"}})
        rec = {"mentions": [mention("about", "Q500"),                       # a format
                            mention("about", "Q600", 10, 15),               # generic, About only
                            mention("tag", "Q600", tag_index=0),            # the same as a tag
                            mention("about", "Q601", 20, 25),
                            mention("title", "Q601", 0, 4)]}                # title agrees
        lists2 = C.Lists(**{**lists.__dict__, "generic_items": {600: "image", 601: "man"}})
        got = [(d["keep"], d["basis"]) for d in C.apply_rules(rec, lists2, FakeClasses())]
        self.assertEqual(got, [(True, "format"), (True, "tag_and_text"), (True, "tag_and_text"),
                               (True, "title_agrees"), (True, "title")])
        rec = {"mentions": [mention("about", "Q600"), mention("tag", "Q602", tag_index=0)]}
        got = [(d["keep"], d["basis"]) for d in C.apply_rules(rec, lists2, FakeClasses())]
        self.assertEqual(got, [(False, "generic_item"), (None, C.PENDING)])

    def test_denied_classes_spare_tags_and_what_the_title_names(self):
        classes = FakeClasses(direct={7: {300}})
        got = self.decide([mention("tag", "Q7", tag_index=0, label="muscle"),
                           mention("about", "Q7", 10, 14)], classes)
        self.assertEqual(got, [(True, "tag_and_text"), (True, "tag_and_text")])
        got = self.decide([mention("tag", "Q7", tag_index=0, label="muscle")], classes)
        self.assertEqual(got, [(None, C.PENDING)])
        got = self.decide([mention("title", "Q7"), mention("about", "Q7", 10, 14)], classes)
        self.assertEqual(got, [(True, "title"), (True, "title_agrees")])

    def test_resolve_fills_only_what_the_judge_answered(self):
        dec = C.apply_rules({"mentions": [mention("about", "Q3"), mention("tag", "Q4", tag_index=0),
                                          mention("about", "Q400", 9, 12)]},
                            LISTS, FakeClasses())
        got = C.resolve(dec, {"Q3": True, "Q400": True})
        self.assertEqual([(d["keep"], d["basis"]) for d in got],
                         [(True, "judge"), (None, C.PENDING), (False, "deny_item")])


class LexiconClassTests(unittest.TestCase):
    """ClassIndex over the real lexicon code (fixture dump)."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        dump = write_dump(os.path.join(cls._tmp.name, "dump.json.gz"))
        path = os.path.join(cls._tmp.name, "lexicon.sqlite")
        wd.build_lexicon(dump, path, workers=0, progress=lambda _l: None)
        cls.lexicon = wd.Lexicon(path)
        cls.classes = C.ClassIndex(cls.lexicon)
        cls.lists = C.Lists(platform_classes={35127: "website"}, platform_items={},
                            deny_classes={112826905: "anatomical entity",
                                          24034552: "mathematical concept"},
                            deny_items={1357284: "popularity"}, version="v")

    @classmethod
    def tearDownClass(cls):
        cls.lexicon.close()
        cls._tmp.cleanup()

    def test_classes(self):
        self.assertIn(3220391, self.classes.direct(918))           # X: a social network
        self.assertIn(35127, self.classes.closure(918))            # ... which is a website
        self.assertNotIn(35127, self.classes.direct(918))
        self.assertIn(11460, self.classes.closure(131151))         # t-shirt: P279 only
        self.assertIn(11460, self.classes.direct(131151))

    def test_rules_on_the_fixture(self):
        rec = {"mentions": [mention("about", "Q918"), mention("about", "Q37017", 5, 9),
                            mention("about", "Q170198", 10, 16),
                            mention("about", "Q1357284", 20, 30),
                            mention("about", "Q7725310", 40, 46)]}
        got = [(d["qid"], d["basis"]) for d in C.apply_rules(rec, self.lists, self.classes)]
        self.assertEqual(got, [("Q918", "platform"), ("Q37017", "deny_class"),
                               ("Q170198", "deny_class"), ("Q1357284", "deny_item"),
                               ("Q7725310", C.PENDING)])


class FakeClient:
    """Answers each chat with the given replies, validating like the real
    client: a reply the validator refuses is a failed call."""

    def __init__(self, replies):
        self.replies, self.calls = list(replies), []

    def chat(self, messages, request, *, purpose, format, options, think, validate):
        self.calls.append({"messages": messages, "format": format, "think": think,
                           "options": options, "purpose": purpose})
        content = self.replies.pop(0)
        try:
            parsed = validate(content)
        except ValueError as exc:
            return SimpleNamespace(ok=False, parsed=None, error=str(exc),
                                   error_kind="invalid", attempts=1, model=None,
                                   digest=None, host=None)
        return SimpleNamespace(ok=True, parsed=parsed, error=None, error_kind=None,
                               attempts=1, model=request.model, digest="d", host="h")


def reply(*keeps):
    """A valid answer: True -> a kept role, False -> a dropped one, a str
    -> that role."""
    roles = [k if isinstance(k, str) else ("subject" if k else "incidental") for k in keeps]
    return json.dumps({"items": [{"n": i, "role": r} for i, r in enumerate(roles, 1)]})


class JudgeTests(unittest.TestCase):
    def setUp(self):
        self.schema, self.sha = C.load_schema(str(CONFIG / "entity_curation_schema.json"))
        self.request = SimpleNamespace(model="m")
        self.context = C.frame_context({"title": "Doge", "tags": ["shiba inu"],
                                        "sections": [{"kind": "about",
                                                      "text": ["Doge is a Shiba Inu."]}]})

    def items(self, n):
        return [{"qid": f"Q{i}", "label": f"item {i}", "description": "d",
                 "texts": [f"w{i}"], "fields": ["about"]} for i in range(1, n + 1)]

    def test_verdicts_by_number(self):
        client = FakeClient([reply(True, False)])
        got = C.judge_frame(client, self.request, self.context, self.items(2),
                            schema=self.schema)
        self.assertTrue(got["ok"])
        self.assertEqual(got["verdicts"], {"Q1": True, "Q2": False})
        call = client.calls[0]
        self.assertEqual(call["purpose"], C.JUDGE_PURPOSE)
        self.assertFalse(call["think"])
        self.assertEqual(call["options"]["temperature"], 0)
        self.assertGreater(call["options"]["num_predict"], 2 * 12)
        rows = call["format"]["properties"]["items"]
        self.assertEqual(rows["items"]["properties"]["n"]["enum"], [1, 2])
        self.assertEqual((rows["minItems"], rows["maxItems"]), (2, 2))
        self.assertIn("Doge is a Shiba Inu.", call["messages"][1]["content"])

    def test_roles_decide_keep(self):
        roles = ["subject", "source", "format", "platform", "incidental", "wrong_sense"]
        got = C.judge_frame(FakeClient([reply(*roles)]), self.request, self.context,
                            self.items(6), schema=self.schema)
        self.assertEqual(list(got["verdicts"].values()), [True] * 4 + [False] * 2)
        self.assertEqual(list(got["roles"].values()), roles)
        self.assertEqual(set(C.KEEP_ROLES) | set(C.DROP_ROLES), set(
            self.schema["properties"]["items"]["items"]["properties"]["role"]["enum"]))

    def test_every_number_exactly_once(self):
        row = lambda n, role="subject", **x: {"n": n, "role": role, **x}
        for bad in (reply(True),                                        # one missing
                    json.dumps({"items": [row(1), row(1)]}),
                    json.dumps({"items": [row(1), row(3)]}),
                    json.dumps({"items": [row(1, why="x"), row(2)]}),
                    json.dumps({"items": [row(1), row(2, "relevant")]}),
                    json.dumps({"items": [{"n": 1, "keep": True}, row(2)]}),
                    "not json"):
            got = C.judge_frame(FakeClient([bad]), self.request, self.context,
                                self.items(2), schema=self.schema)
            self.assertFalse(got["ok"], bad)
            self.assertEqual(got["error_kind"], "invalid")

    def test_large_frames_are_asked_in_batches(self):
        n = C.MAX_JUDGE_ITEMS + 3
        client = FakeClient([reply(*[True] * C.MAX_JUDGE_ITEMS), reply(False, False, True)])
        got = C.judge_frame(client, self.request, self.context, self.items(n),
                            schema=self.schema)
        self.assertTrue(got["ok"])
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(len(got["verdicts"]), n)
        self.assertEqual(got["verdicts"][f"Q{n}"], True)
        self.assertEqual(got["verdicts"][f"Q{n - 1}"], False)

    def test_a_failed_batch_keeps_what_was_answered(self):
        client = FakeClient([reply(*[True] * C.MAX_JUDGE_ITEMS), "nope"])
        got = C.judge_frame(client, self.request, self.context,
                            self.items(C.MAX_JUDGE_ITEMS + 1), schema=self.schema)
        self.assertFalse(got["ok"])
        self.assertEqual(len(got["verdicts"]), C.MAX_JUDGE_ITEMS)

    def test_a_second_model_confirms_about_only_keeps(self):
        items = self.items(3)
        items[1]["fields"] = ["tag"]
        client = FakeClient([reply(True, True, False),        # the judge
                             reply("incidental")])            # the confirmer: Q1 only
        got = C.judge_frame(client, self.request, self.context, items, schema=self.schema,
                            confirm=SimpleNamespace(model="c"))
        self.assertTrue(got["ok"])
        self.assertEqual(got["verdicts"], {"Q1": False, "Q2": True, "Q3": False})
        self.assertEqual(got["roles"]["Q1"], "subject")              # the judge's own
        self.assertEqual(got["confirm"]["roles"], {"Q1": "incidental"})
        self.assertEqual([c["purpose"] for c in client.calls],
                         [C.JUDGE_PURPOSE, C.CONFIRM_PURPOSE])
        self.assertEqual([c["messages"][0]["content"] for c in client.calls],
                         [C.SYSTEM_PROMPT, C.CONFIRM_PROMPT])
        self.assertNotEqual(C.SYSTEM_PROMPT, C.CONFIRM_PROMPT)
        self.assertIn("1. item 1", client.calls[1]["messages"][1]["content"])
        self.assertNotIn("item 2", client.calls[1]["messages"][1]["content"])

    def test_nothing_to_confirm_asks_once_and_a_failed_confirm_fails(self):
        client = FakeClient([reply(False, False)])
        got = C.judge_frame(client, self.request, self.context, self.items(2),
                            schema=self.schema, confirm=SimpleNamespace(model="c"))
        self.assertTrue(got["ok"])
        self.assertEqual(len(client.calls), 1)
        got = C.judge_frame(FakeClient([reply(True), "nope"]), self.request, self.context,
                            self.items(1), schema=self.schema,
                            confirm=SimpleNamespace(model="c"))
        self.assertFalse(got["ok"])
        self.assertTrue(got["error"].startswith("confirm:"))

    def test_items_carry_what_they_were_read_from(self):
        about = "The dog, a Shiba Inu named Kabosu, became Doge."
        rec = {"mentions": [
            mention("about", "Q1", 11, 20, text="Shiba Inu", label="Shiba Inu",
                    description="dog breed"),
            mention("tag", "Q1", 0, 9, tag_index=0, text="shiba inu"),
            mention("about", "Q2", 4, 7, text="dog", label="dog")]}
        items = C.judge_items(rec, C.apply_rules(rec, LISTS, FakeClasses()), about=about)
        self.assertEqual([it["qid"] for it in items], ["Q2"])      # Q1: tag_and_text
        self.assertEqual(items[0]["snippet"], "The [[dog]], a Shiba Inu named Kabosu, became Doge.")
        text = C.render_items(items)
        self.assertIn('1. dog (read from "dog" in the About)', text)
        self.assertIn("context: The [[dog]]", text)

    def test_stamps(self):
        self.assertEqual(C.judge_stamps(self.request, "s", SimpleNamespace(model="c")),
                         {"judge_prompt_version": C.JUDGE_PROMPT_VERSION,
                          "judge_schema_sha": "s", "judge_model": "m",
                          "judge_confirm_model": "c"})
        self.assertEqual(C.judge_stamps(self.request, "s")["judge_confirm_model"], "")
        self.assertEqual(set(C.rule_stamps(LISTS, "lex")),
                         {"curation_version", "curation_lists_version", "lexicon_version"})


if __name__ == "__main__":
    unittest.main()
