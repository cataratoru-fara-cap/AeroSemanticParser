"""kg_config/neo4j_browser.grass, read the way Neo4j Browser reads it.

The Browser (2026.08, bundled with Neo4j 5.26) styles a node with several
labels by the label highest in its precedence list, and every node here
has two: ``KGNode`` and its kind. Its GraSS importer puts each ``node.<X>``
rule IN FRONT of the ones before it, so the LAST rule in the file wins and
``KGNode`` must come FIRST — otherwise its style covers every kind and the
whole graph is one colour (2026-10-05, Gabi's report). The parser below
follows the Browser's (``A_e`` / ``M_e`` / ``Q9`` in its bundle).
"""

from __future__ import annotations

import os
import re
import unittest

from modules.kg import build, loaders

GRASS = os.path.join(os.path.dirname(__file__), "..", "kg_config", "neo4j_browser.grass")

# The Browser's node size steps (its radius options, "1x" to "11x"), and how
# it turns a GraSS diameter into one: floor(diameter / 2 / 0.93).
BROWSER_SIZES = (18, 25, 30, 36, 42, 48, 54, 60, 66, 72, 78)


def parse_grass(text: str) -> dict[str, dict[str, str]]:
    """``selector { key: value; ... }`` -> {selector: {key: value}}, as the
    Browser's parser reads it: quotes toggle, whitespace outside a block is
    dropped, and anything outside a block joins the next selector."""
    rules: dict[str, str] = {}
    in_quote = in_block = False
    selector = body = ""
    for ch in text:
        if ch in "'\"":
            in_quote = not in_quote
            continue
        if not in_quote and ch == "{":
            in_block = True
            continue
        if not in_quote and ch == "}" and in_block:
            in_block = False
            rules[selector] = body
            selector = body = ""
            continue
        if in_block:
            body += ch
        elif not ch.isspace():
            selector += ch
    out: dict[str, dict[str, str]] = {}
    for sel, decls in rules.items():
        props = {}
        for decl in decls.split(";"):
            parts = decl.split(":")
            if len(parts) == 2:
                props[parts[0].strip()] = parts[1].strip()
        out[sel] = props
    return out


def precedence(rules: dict[str, dict[str, str]]) -> list[str]:
    """The labels in the Browser's order, highest priority first."""
    order: list[str] = []
    for sel in rules:
        tag, _, label = sel.partition(".")
        if tag == "node" and label:
            order.insert(0, label)
    return order


class GrassTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(GRASS, encoding="utf-8") as fh:
            cls.text = fh.read()
        cls.rules = parse_grass(cls.text)

    def test_every_rule_is_a_node_label(self):
        # A comment or stray text would join the next selector and the
        # Browser would drop that rule without a word.
        for sel in self.rules:
            self.assertRegex(sel, r"^node\.[A-Za-z]+$")
        self.assertNotIn("/*", self.text)

    def test_every_kind_the_loader_writes_has_a_style(self):
        labels = {sel.split(".", 1)[1] for sel in self.rules}
        kinds = {loaders.label_for_kind(k) for k in build.NODE_KINDS}
        self.assertEqual(labels - {"KGNode"}, kinds)

    def test_kgnode_has_the_lowest_priority(self):
        self.assertEqual(precedence(self.rules)[-1], "KGNode")
        self.assertEqual(precedence(self.rules)[0], "Frame")

    def test_each_rule_sets_colour_size_and_caption(self):
        for sel, props in self.rules.items():
            with self.subTest(sel=sel):
                self.assertRegex(props.get("color", ""), r"^#[0-9a-f]{6}$")
                self.assertRegex(props.get("caption", ""), r"^\{[a-z_]+\}$")
                diameter = int(re.match(r"(\d+)px$", props["diameter"]).group(1))
                self.assertIn(int(diameter / 2 / 0.93), BROWSER_SIZES)

    def test_the_main_kinds_keep_distinct_colours(self):
        # The three validated hues (all pairs, light and dark) and the darker
        # step of Frame's blue for a stub — see KG_QUERIES.md.
        colours = {sel.split(".")[1]: p["color"] for sel, p in self.rules.items()}
        main = [colours[k] for k in ("Frame", "FrameStub", "WikidataEntity", "Template",
                                     "Event", "TagConcept", "Image")]
        self.assertEqual(len(set(main)), len(main))


if __name__ == "__main__":
    unittest.main()
