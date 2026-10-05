"""The monthly chain: which DAG triggers which, and with what.

    kym_parse -> kym_entities -> kym_entity_curation -> kym_events
              -> kym_templates -> kym_template_entities
              -> kym_frame_image_entities -> kym_wikidata_statements -> kym_kg

Each stage selects only what is missing or stale, so the chain does the
month's new and changed frames and nothing else — as long as no link of it
passes a force flag, or a cap that would leave part of the month pending.
These tests build the real DAG objects (Airflow is in the venv) and pin both.
"""

from __future__ import annotations

import importlib
import unittest

try:
    import airflow  # noqa: F401
    HAVE_AIRFLOW = True
except ImportError:  # pragma: no cover - the venv has it
    HAVE_AIRFLOW = False

CHAIN = ["kym_parse", "kym_entities", "kym_entity_curation", "kym_events",
         "kym_templates", "kym_template_entities", "kym_frame_image_entities",
         "kym_wikidata_statements", "kym_kg"]


def build(dag_id: str):
    return getattr(importlib.import_module(f"{dag_id}_dag"), f"{dag_id}_dag")()


def triggers(dag) -> list:
    return [t for t in dag.tasks if t.task_type == "TriggerDagRunOperator"]


@unittest.skipUnless(HAVE_AIRFLOW, "Airflow is not installed")
class ChainTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dags = {d: build(d) for d in CHAIN}

    def test_each_stage_triggers_the_next_and_only_that(self):
        for here, nxt in zip(CHAIN, CHAIN[1:]):
            with self.subTest(dag=here):
                self.assertEqual([t.trigger_dag_id for t in triggers(self.dags[here])], [nxt])
        self.assertEqual(triggers(self.dags["kym_kg"]), [])

    def test_the_cascade_is_on_by_default_where_the_chain_does_not_pass_it(self):
        defaults = {"kym_parse": "trigger_entities", "kym_entities": "trigger_curation",
                    "kym_entity_curation": "trigger_events", "kym_events": "trigger_templates"}
        for dag_id, param in defaults.items():
            with self.subTest(dag=dag_id):
                self.assertIs(self.dags[dag_id].params[param], True)

    def test_no_link_forces_a_recompute_or_caps_the_month(self):
        for dag_id in CHAIN[:-1]:
            for t in triggers(self.dags[dag_id]):
                conf = t.conf if isinstance(t.conf, dict) else {}
                with self.subTest(dag=dag_id):
                    self.assertFalse([k for k in conf if k.startswith("force")])
                    self.assertEqual(conf.get("batch_size", 0), 0)

    def test_events_hand_the_template_stages_the_chain_settings(self):
        (t,) = triggers(self.dags["kym_events"])
        self.assertEqual(t.conf, {"batch_size": 0, "research_after_days": 0,
                                  "trigger_template_entities": True, "trigger_kg": True})
        params = self.dags["kym_templates"].params
        self.assertTrue(set(t.conf) <= set(params.keys()))     # every key is a real param

    def test_curation_asks_events_for_every_pending_section(self):
        (t,) = triggers(self.dags["kym_entity_curation"])
        self.assertEqual(t.conf, {"batch_size": 0})

    def test_templates_forward_trigger_kg_to_the_reader(self):
        dag = self.dags["kym_templates"]
        conf_task = dag.get_task("template_entities_conf")
        (t,) = triggers(dag)
        self.assertIn("template_entities_conf", t.upstream_task_ids)
        self.assertIn("should_trigger_template_entities", conf_task.upstream_task_ids)
        self.assertEqual(conf_task.python_callable(params={"trigger_kg": True}),
                         {"batch_size": 0, "trigger_kg": True})
        # A manual search batch never builds the graph.
        self.assertEqual(conf_task.python_callable(params={}),
                         {"batch_size": 0, "trigger_kg": False})
        self.assertIs(dag.params["trigger_kg"], False)

    def test_the_graph_stages_carry_trigger_kg_on_to_kym_kg(self):
        # 7.1.0: from the template reader, through the frames' own images and
        # the Wikidata statements; each link runs only when trigger_kg is set,
        # so a hand-started run of any of them never builds the graph.
        expected = {"kym_template_entities": {"batch_size": 0, "trigger_kg": True},
                    "kym_frame_image_entities": {"trigger_kg": True}}
        for dag_id in ("kym_template_entities", "kym_frame_image_entities",
                       "kym_wikidata_statements"):
            dag = self.dags[dag_id]
            (t,) = triggers(dag)
            with self.subTest(dag=dag_id):
                self.assertIs(dag.params["trigger_kg"], False)
                self.assertIn("should_trigger_kg", t.upstream_task_ids)
                if dag_id in expected:
                    self.assertEqual(t.conf, expected[dag_id])
                    nxt = self.dags[t.trigger_dag_id]
                    self.assertTrue(set(t.conf) <= set(nxt.params.keys()))


if __name__ == "__main__":
    unittest.main()
