"""The monthly chain: which DAG triggers which, and with what.

    kym_scrape -> kym_parse -> kym_entities -> kym_entity_curation -> kym_events
               -> kym_templates -> kym_template_entities -> kym_frame_image_entities
               -> kym_wikidata_statements -> kym_kg

Each stage selects only what is missing or stale, so the chain does the
month's new and changed frames and nothing else — as long as no link passes a
force flag or a cap that would leave part of the month pending. Built from the
real DAG objects (Airflow is in the venv). And every store function a DAG calls
exists and is exported.
"""
import importlib

import pytest

from helpers import assert_dag_calls_are_exported

CHAIN = ["kym_parse", "kym_entities", "kym_entity_curation", "kym_events", "kym_templates", "kym_template_entities",
         "kym_frame_image_entities", "kym_wikidata_statements", "kym_kg"]


def build(dag_id, factory=None):
    return getattr(importlib.import_module(f"{dag_id}_dag"), factory or f"{dag_id}_dag")()


def triggers(dag):
    return [t for t in dag.tasks if t.task_type == "TriggerDagRunOperator"]


@pytest.fixture(scope="module")
def dags():
    pytest.importorskip("airflow", reason="Airflow is not installed")    # the facade checks below need none
    return {d: build(d) for d in CHAIN} | {"kym_scrape": build("kym_scrape", "kym_scrape")}


def test_each_stage_triggers_the_next_and_only_that(dags):
    for here, nxt in zip(["kym_scrape"] + CHAIN, CHAIN):
        assert [t.trigger_dag_id for t in triggers(dags[here])] == [nxt], here
    assert triggers(dags["kym_kg"]) == []


@pytest.mark.parametrize("dag_id, param", [
    ("kym_scrape", "trigger_parse"), ("kym_parse", "trigger_entities"), ("kym_entities", "trigger_curation"),
    ("kym_entity_curation", "trigger_events"), ("kym_events", "trigger_templates")])
def test_the_cascade_is_on_by_default_where_the_chain_does_not_pass_it(dags, dag_id, param):
    assert dags[dag_id].params[param] is True


def test_no_link_forces_a_recompute_or_caps_the_month(dags):
    for dag_id in ["kym_scrape"] + CHAIN[:-1]:
        for t in triggers(dags[dag_id]):
            conf = t.conf if isinstance(t.conf, dict) else {}
            assert not [k for k in conf if k.startswith("force")] and conf.get("batch_size", 0) == 0, dag_id


def test_what_events_and_curation_hand_on(dags):
    (t,) = triggers(dags["kym_events"])
    assert t.conf == {"batch_size": 0, "research_after_days": 0, "trigger_template_entities": True, "trigger_kg": True}
    assert set(t.conf) <= set(dags["kym_templates"].params.keys())          # every key is a real param
    (t,) = triggers(dags["kym_entity_curation"])
    assert t.conf == {"batch_size": 0}                                       # every pending section


def test_templates_forward_trigger_kg_to_the_reader(dags):
    dag = dags["kym_templates"]
    conf_task = dag.get_task("template_entities_conf")
    (t,) = triggers(dag)
    assert "template_entities_conf" in t.upstream_task_ids
    assert "should_trigger_template_entities" in conf_task.upstream_task_ids
    assert conf_task.python_callable(params={"trigger_kg": True}) == {"batch_size": 0, "trigger_kg": True}
    # a manual search batch never builds the graph
    assert conf_task.python_callable(params={}) == {"batch_size": 0, "trigger_kg": False}
    assert dag.params["trigger_kg"] is False


@pytest.mark.parametrize("dag_id, conf", [
    # 7.1.0: each link runs only when trigger_kg is set: a hand-started run never builds the graph
    ("kym_template_entities", {"batch_size": 0, "trigger_kg": True}),
    ("kym_frame_image_entities", {"trigger_kg": True}),
    ("kym_wikidata_statements", None),
])
def test_the_graph_stages_carry_trigger_kg_on_to_kym_kg(dags, dag_id, conf):
    dag = dags[dag_id]
    (t,) = triggers(dag)
    assert dag.params["trigger_kg"] is False and "should_trigger_kg" in t.upstream_task_ids
    if conf is not None:
        assert t.conf == conf and set(t.conf) <= set(dags[t.trigger_dag_id].params.keys())


def test_one_entry_one_address_runs_after_the_fetch_and_parse_retires_it(dags):
    # gap 14
    step = dags["kym_scrape"].get_task("resolve_duplicates")
    assert "scrape_chunk" in step.upstream_task_ids and "summarize" in step.downstream_task_ids
    (t,) = triggers(dags["kym_scrape"])
    assert "should_trigger_parse" in t.upstream_task_ids
    assert "summarize" in dags["kym_parse"].get_task("retire_duplicates").downstream_task_ids


@pytest.mark.parametrize("dag_file, store, at_least", [
    ("kym_templates_dag.py", "template_store", set()), ("kym_entity_curation_dag.py", "entity_curation_store", set()),
    ("kym_entities_dag.py", "entity_store", {"pending_units", "units_for", "save_links", "entity_stats", "get_store"}),
    ("kym_template_entities_dag.py", "template_entity_store", set()),
    ("kym_frame_image_entities_dag.py", "frame_image_store", set()), ("kym_parse_dag.py", "parse_store", set()),
    ("kym_events_dag.py", "event_store", set()), ("kym_wikidata_statements_dag.py", "wikidata_statement_store", set()),
])
def test_every_store_function_a_dag_calls_is_exported(dag_file, store, at_least):
    assert at_least <= assert_dag_calls_are_exported(dag_file, importlib.import_module(f"modules.{store}"))
