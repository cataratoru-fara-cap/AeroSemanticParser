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

import re

import pytest

from helpers import KG_CONFIG
from modules.kg import build, loaders

TEXT = (KG_CONFIG / "neo4j_browser.grass").read_text(encoding="utf-8")

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


RULES = parse_grass(TEXT)


def test_every_rule_is_a_node_label():
    # a comment or stray text would join the next selector and the Browser would drop that rule without a word
    assert all(re.fullmatch(r"node\.[A-Za-z]+", sel) for sel in RULES) and "/*" not in TEXT


def test_every_kind_the_loader_writes_has_a_style_and_kgnode_the_lowest_priority():
    assert {sel.split(".", 1)[1] for sel in RULES} - {"KGNode"} == {loaders.label_for_kind(k) for k in build.NODE_KINDS}
    order = precedence(RULES)
    assert (order[0], order[-1]) == ("Frame", "KGNode")


@pytest.mark.parametrize("sel", list(RULES))
def test_each_rule_sets_colour_size_and_caption(sel):
    props = RULES[sel]
    assert re.fullmatch(r"#[0-9a-f]{6}", props.get("color", "")) and re.fullmatch(r"\{[a-z_]+\}", props.get("caption", ""))
    assert int(int(re.fullmatch(r"(\d+)px", props["diameter"]).group(1)) / 2 / 0.93) in BROWSER_SIZES


def test_the_main_kinds_keep_distinct_colours():
    # the three validated hues (all pairs, light and dark) and the darker step of Frame's blue for a stub
    # (KG_QUERIES.md)
    main = [RULES[f"node.{k}"]["color"] for k in ("Frame", "FrameStub", "WikidataEntity", "Template", "Event",
                                                    "TagConcept", "Image")]
    assert len(set(main)) == len(main)
