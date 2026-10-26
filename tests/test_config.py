import copy
import json

from helpers import DOC_WF, EngineTestCase
from wfengine.config import load_workflow
from wfengine.errors import WorkflowConfigError

BASE = {"id": "w", "nodes": [
    {"id": "a", "type": "function", "params": {"function": "t_echo"}},
    {"id": "b", "type": "function", "depends_on": ["a"], "params": {"function": "t_echo"},
     "inputs": {"value": "$nodes.a.output.value"}}]}


class ConfigValidationTest(EngineTestCase):
    def assertConfigError(self, wf, fragment):
        with self.assertRaises(WorkflowConfigError) as cm:
            load_workflow(wf)
        self.assertTrue(any(fragment in e for e in cm.exception.errors), cm.exception.errors)

    def test_valid_workflows_load(self):
        wf = load_workflow(DOC_WF)
        self.assertEqual(wf["id"], "doc_review")
        load_workflow(copy.deepcopy(BASE))

    def test_cycle_detected(self):
        wf = copy.deepcopy(BASE)
        wf["nodes"][0]["depends_on"] = ["b"]
        self.assertConfigError(wf, "dependency cycle detected among nodes: a, b")

    def test_unknown_dependency(self):
        wf = copy.deepcopy(BASE)
        wf["nodes"][1]["depends_on"] = ["zzz"]
        self.assertConfigError(wf, "depends_on unknown node 'zzz'")

    def test_reference_must_be_ancestor(self):
        wf = copy.deepcopy(BASE)
        wf["nodes"][1]["depends_on"] = []
        self.assertConfigError(wf, "needs 'a' to be an (indirect) dependency")

    def test_unknown_node_type_and_function(self):
        wf = copy.deepcopy(BASE)
        wf["nodes"][0]["type"] = "nope"
        wf["nodes"][1]["params"]["function"] = "missing_fn"
        with self.assertRaises(WorkflowConfigError) as cm:
            load_workflow(wf)
        text = "\n".join(cm.exception.errors)
        self.assertIn("unknown node type 'nope'", text)
        self.assertIn("function 'missing_fn' is not registered", text)

    def test_bad_retry_join_when_and_duplicate(self):
        wf = copy.deepcopy(BASE)
        wf["nodes"][0]["retry"] = {"max_attempts": 0}
        wf["nodes"][1]["join"] = "whatever"
        wf["nodes"][1]["when"] = {"xor": [1, 2]}
        self.assertConfigError(wf, "retry.max_attempts must be an integer >= 1")
        wf2 = copy.deepcopy(BASE)
        wf2["nodes"][1]["id"] = "a"
        self.assertConfigError(wf2, "duplicate node id 'a'")

    def test_bad_plugin_and_bad_ref(self):
        wf = copy.deepcopy(BASE)
        wf["plugins"] = ["no.such.module"]
        self.assertConfigError(wf, "cannot import plugin 'no.such.module'")
        wf2 = copy.deepcopy(BASE)
        wf2["nodes"][1]["inputs"] = {"value": "$nodes.a.result"}
        self.assertConfigError(wf2, "must look like $nodes.<id>.output")

    def test_cli_validate_reports_cycle(self):
        import os
        import subprocess
        import sys
        from helpers import ROOT
        wf = {"id": "cyc", "nodes": [
            {"id": "a", "type": "condition", "depends_on": ["b"], "params": {"expr": True}},
            {"id": "b", "type": "condition", "depends_on": ["a"], "params": {"expr": True}}]}
        path = os.path.join(self.tmp, "cyc.json")
        with open(path, "w") as f:
            json.dump(wf, f)
        p = subprocess.run([sys.executable, "-m", "wfengine", "validate", path], cwd=ROOT,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        self.assertEqual(p.returncode, 2)
        self.assertIn("dependency cycle", p.stderr)
