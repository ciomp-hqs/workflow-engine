import os
import subprocess
import sys
import time

from helpers import ROOT, EngineTestCase
from wfengine.errors import InvalidStateError
from wfengine.registry import NodeType, node_types, register_node


def fn(nid, function, deps=(), inputs=None, **kw):
    n = {"id": nid, "type": "function", "depends_on": list(deps), "params": {"function": function},
         "inputs": inputs or {}}
    n.update(kw)
    return n


class JoinTest(EngineTestCase):
    def wf(self, flag):
        return {"id": "join_test", "nodes": [
            fn("start", "t_echo", inputs={"value": flag}),
            {"id": "gate", "type": "condition", "depends_on": ["start"],
             "params": {"expr": {"ref": "$nodes.start.output.value"}}},
            fn("branch", "t_echo", ["gate"], when={"ref": "$nodes.gate.output.result"}),
            fn("after_branch", "t_echo", ["branch"]),  # default all_success: skip propagates
            fn("j_all_success", "t_echo", ["start", "branch"]),
            fn("j_none_failed", "t_echo", ["start", "branch"], join="none_failed",
               inputs={"value": "$nodes.branch.output.value"}),
            fn("j_any", "t_echo", ["start", "branch"], join="any_success"),
            fn("j_all_done", "t_echo", ["start", "after_branch"], join="all_done"),
        ]}

    def test_unselected_branch_does_not_block_join(self):
        eng = self.engine()
        rid = eng.start(self.wf(False), {})
        self.assertEqual(eng.wait(rid, timeout=5), "SUCCEEDED")
        st = self.statuses(eng, rid)
        self.assertEqual(st["branch"], "SKIPPED")
        self.assertEqual(st["after_branch"], "SKIPPED")
        self.assertEqual(st["j_all_success"], "SKIPPED")
        self.assertEqual(st["j_none_failed"], "SUCCEEDED")
        self.assertIsNone(self.node(eng, rid, "j_none_failed")["output"]["value"])  # skipped ref -> None
        self.assertEqual(st["j_any"], "SUCCEEDED")
        self.assertEqual(st["j_all_done"], "SUCCEEDED")
        self.assertNotIn("PENDING", st.values())

    def test_selected_branch(self):
        eng = self.engine()
        rid = eng.start(self.wf(True), {})
        st = self.statuses(eng, rid)
        self.assertTrue(all(s == "SUCCEEDED" for s in st.values()), st)


class FailureRetryTest(EngineTestCase):
    def test_retry_until_success_counts_attempts(self):
        eng = self.engine()
        wf = {"id": "r", "nodes": [fn("f", "t_flaky", inputs={"succeed_on": 3},
                                      retry={"max_attempts": 5, "backoff_seconds": 0.01})]}
        rid = eng.start(wf, {})
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        self.assertEqual(self.node(eng, rid, "f")["attempts"], 3)
        self.assertEqual([a["status"] for a in eng.store.get_attempts(rid, "f")], ["FAILED", "FAILED", "SUCCEEDED"])

    def test_retry_limit_then_failed(self):
        eng = self.engine()
        wf = {"id": "r", "nodes": [fn("f", "t_flaky", retry={"max_attempts": 2, "backoff_seconds": 0}),
                                   fn("g", "t_echo", ["f"])]}
        rid = eng.start(wf, {})
        run = eng.store.get_run(rid)
        self.assertEqual(run["status"], "FAILED")
        f = self.node(eng, rid, "f")
        self.assertEqual((f["status"], f["attempts"]), ("FAILED", 2))
        self.assertIn("flaky failure on attempt 2", f["error"])
        self.assertEqual(self.node(eng, rid, "g")["status"], "CANCELLED")

    def test_timeout_counts_as_failed_attempt(self):
        eng = self.engine()
        wf = {"id": "t", "nodes": [fn("slow", "t_block_forever", inputs={"seconds": 1.0}, timeout_seconds=0.2,
                                      retry={"max_attempts": 2})]}
        t0 = time.time()
        rid = eng.start(wf, {})
        self.assertLess(time.time() - t0, 1.5)
        n = self.node(eng, rid, "slow")
        self.assertEqual((n["status"], n["attempts"]), ("FAILED", 2))
        self.assertIn("AttemptTimeout", n["error"])

    def test_parallel_partial_failure_fail_fast(self):
        eng = self.engine()
        wf = {"id": "p", "on_failure": "fail_fast", "nodes": [
            fn("ok", "t_echo"),
            fn("bad", "t_flaky", retry={"max_attempts": 2, "backoff_seconds": 0.05}),
            fn("slow", "t_sleep", inputs={"seconds": 5}),
            fn("final", "t_echo", ["ok", "bad", "slow"])]}
        t0 = time.time()
        rid = eng.start(wf, {})
        self.assertLess(time.time() - t0, 3)  # slow sibling was cancelled, not awaited
        run = eng.store.get_run(rid)
        self.assertEqual(run["status"], "FAILED")
        self.assertIn("bad", run["reason"])
        st = self.statuses(eng, rid)
        self.assertEqual(st, {"ok": "SUCCEEDED", "bad": "FAILED", "slow": "CANCELLED", "final": "CANCELLED"})
        self.assertEqual(self.node(eng, rid, "bad")["attempts"], 2)
        self.assertIsNotNone(self.node(eng, rid, "ok")["output"])  # succeeded sibling's result kept

    def test_parallel_partial_failure_continue(self):
        eng = self.engine()
        wf = {"id": "p", "on_failure": "continue", "nodes": [
            fn("ok", "t_echo"),
            fn("bad", "t_flaky"),
            fn("slow", "t_sleep", inputs={"seconds": 0.2}),
            fn("final", "t_echo", ["ok", "bad", "slow"]),
            fn("cleanup", "t_echo", ["ok", "bad", "slow"], join="all_done")]}
        rid = eng.start(wf, {})
        self.assertEqual(eng.store.get_run_status(rid), "FAILED")
        st = self.statuses(eng, rid)
        self.assertEqual(st, {"ok": "SUCCEEDED", "bad": "FAILED", "slow": "SUCCEEDED", "final": "SKIPPED",
                              "cleanup": "SUCCEEDED"})

    def test_rerun_failed_node_after_fix(self):
        eng = self.engine()
        wf = {"id": "r", "nodes": [fn("f", "t_flaky", inputs={"succeed_on": "$input.ok_on"}),
                                   fn("g", "t_echo", ["f"])]}
        rid = eng.start(wf, {"ok_on": 0})
        self.assertEqual(eng.store.get_run_status(rid), "FAILED")
        eng.rerun(rid, "f", new_input={"ok_on": 1})
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        self.assertEqual(self.node(eng, rid, "f")["attempts"], 2)  # attempt history is cumulative


class CancelTest(EngineTestCase):
    WF = {"id": "c", "nodes": [fn("a", "t_sleep", inputs={"seconds": 5}), fn("b", "t_echo", ["a"])]}

    def _wait_running(self, eng, rid, nid):
        for _ in range(200):
            if self.node(eng, rid, nid)["status"] == "RUNNING":
                return
            time.sleep(0.01)
        self.fail("node never started")

    def test_cancel_running_run(self):
        eng = self.engine()
        rid = eng.start(self.WF, {}, wait=False)
        self._wait_running(eng, rid, "a")
        t0 = time.time()
        res = eng.cancel(rid)
        self.assertTrue(res["changed"])
        self.assertEqual(eng.wait(rid, timeout=5), "CANCELLED")
        self.assertLess(time.time() - t0, 2)
        self.assertEqual(self.statuses(eng, rid), {"a": "CANCELLED", "b": "CANCELLED"})
        self.assertFalse(eng.cancel(rid)["changed"])  # idempotent

    def test_cancel_from_other_engine_instance(self):
        eng, other = self.engine(), self.engine()
        rid = eng.start(self.WF, {}, wait=False)
        self._wait_running(eng, rid, "a")
        other.cancel(rid)  # no in-memory signal reaches `eng`: it must notice via the DB
        self.assertEqual(eng.wait(rid, timeout=5), "CANCELLED")
        self.assertEqual(self.node(eng, rid, "a")["status"], "CANCELLED")

    def test_cancel_waiting_approval(self):
        from helpers import DOC_WF, demo
        eng = self.engine()
        rid = eng.start(DOC_WF, demo("doc_sensitive.json"))
        [apr] = eng.pending_approvals(rid)
        eng.cancel(rid)
        self.assertEqual(eng.store.get_run_status(rid), "CANCELLED")
        self.assertEqual(self.node(eng, rid, "approve")["status"], "CANCELLED")
        self.assertEqual(eng.store.get_approvals(rid)[-1]["status"], "CANCELLED")
        with self.assertRaises(InvalidStateError):
            eng.approve(rid, "approve", apr["content_hash"], "alice")


class RecoveryTest(EngineTestCase):
    def _dead_owner(self):
        p = subprocess.Popen([sys.executable, "-c", "pass"])
        p.wait()
        return "%d:deadbeef" % p.pid

    def test_running_nodes_after_crash_idempotent_requeued_other_failed(self):
        eng = self.engine()
        wf = {"id": "rec", "nodes": [fn("idem", "t_echo", idempotent=True), fn("side", "t_echo"),
                                     fn("after", "t_echo", ["idem", "side"])]}
        from wfengine.config import load_workflow
        wf = load_workflow(wf)
        rid = eng.store.create_run(wf, {})
        dead = self._dead_owner()
        for nid in ("idem", "side"):  # simulate a process that died mid-execution
            self.assertTrue(eng.store.claim_node(rid, nid, dead))
            eng.store.start_attempt(rid, nid, dead)
        eng2 = self.engine()
        rep = eng2.resume(rid)
        self.assertEqual(rep["requeued"], ["idem"])
        self.assertEqual(rep["failed_interrupted"], ["side"])
        side = self.node(eng2, rid, "side")
        self.assertEqual(side["status"], "FAILED")
        self.assertIn("not idempotent", side["reason"])
        self.assertEqual(eng2.store.get_run_status(rid), "FAILED")
        self.assertEqual(eng2.store.get_attempts(rid, "side")[0]["status"], "INTERRUPTED")
        # operator decides to re-run the interrupted step
        eng2.rerun(rid, "side")
        self.assertEqual(eng2.store.get_run_status(rid), "SUCCEEDED")
        self.assertEqual(self.node(eng2, rid, "idem")["attempts"], 2)

    def test_idempotent_only_resumes_automatically(self):
        eng = self.engine()
        from wfengine.config import load_workflow
        wf = load_workflow({"id": "rec2", "nodes": [fn("a", "t_echo", idempotent=True), fn("b", "t_echo", ["a"])]})
        rid = eng.store.create_run(wf, {})
        dead = self._dead_owner()
        eng.store.claim_node(rid, "a", dead)
        eng.store.start_attempt(rid, "a", dead)
        rep = self.engine().resume(rid)
        self.assertEqual(rep["status"], "SUCCEEDED")
        self.assertEqual([x["status"] for x in eng.store.get_attempts(rid, "a")], ["INTERRUPTED", "SUCCEEDED"])

    def test_live_owner_is_not_stolen(self):
        eng = self.engine()
        rid = eng.start(CancelTest.WF, {}, wait=False)
        for _ in range(200):
            if self.node(eng, rid, "a")["status"] == "RUNNING":
                break
            time.sleep(0.01)
        rep = self.engine().resume(rid, wait=False)
        self.assertEqual(rep["running_elsewhere"], ["a"])
        eng.cancel(rid)
        eng.wait(rid)

    def test_kill9_process_then_resume(self):
        """Real crash: a child process is SIGKILLed while a non-idempotent node runs."""
        script = ("import sys; sys.path.insert(0, %r); sys.path.insert(0, %r); import helpers; "
                  "from wfengine.engine import Engine; "
                  "e = Engine(db_path=%r, settings={'max_workers': 2}); "
                  "e.start({'id': 'k', 'nodes': [{'id': 'work', 'type': 'function', "
                  "'params': {'function': 't_sleep'}, 'inputs': {'seconds': '$input.seconds'}}]}, {'seconds': 30})"
                  % (ROOT, os.path.join(ROOT, "tests"), self.db))
        p = subprocess.Popen([sys.executable, "-c", script])
        eng = self.engine()
        rid = None
        for _ in range(500):
            runs = eng.store.list_runs()
            if runs and self.node(eng, runs[0]["run_id"], "work")["status"] == "RUNNING":
                rid = runs[0]["run_id"]
                break
            time.sleep(0.02)
        self.assertIsNotNone(rid)
        p.kill()
        p.wait()
        rep = eng.resume(rid)
        self.assertEqual(rep["failed_interrupted"], ["work"])
        self.assertEqual(eng.store.get_run_status(rid), "FAILED")
        eng.rerun(rid, "work", new_input={"seconds": 0.05})
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")


class ExtensionTest(EngineTestCase):
    def tearDown(self):
        node_types.unregister("t_uppercase")
        super().tearDown()

    def test_register_new_node_type_without_engine_change(self):
        @register_node("t_uppercase")
        class Upper(NodeType):
            def validate(self, n):
                return [] if "text" in n.get("inputs", {}) else ["inputs.text required"]

            def run(self, ctx):
                return {"text": ctx.inputs["text"].upper()}

        eng = self.engine()
        rid = eng.start({"id": "ext", "nodes": [
            {"id": "u", "type": "t_uppercase", "inputs": {"text": "$input.t"}},
            {"id": "n", "type": "notify", "depends_on": ["u"], "inputs": {"text": "$nodes.u.output.text"},
             "params": {"message": "got {text}", "channel": "outbox"}}],
            "plugins": ["wfengine.plugins.notify"]}, {"t": "hello"})
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        self.assertEqual(self.node(eng, rid, "n")["output"]["message"], "got HELLO")

    def test_notify_lives_outside_engine_core(self):
        for core in ("engine.py", "store.py", "config.py", "registry.py", "nodes/builtin.py"):
            with open(os.path.join(ROOT, "wfengine", core), encoding="utf-8") as f:
                src = f.read()
            for word in ("notify", "records", "summarize_doc", "transform_orders"):
                self.assertNotIn(word, src, "%s mentions %s" % (core, word))

    def test_notify_stdout_channel_and_validation(self):
        import contextlib
        import io
        from wfengine.config import load_workflow
        from wfengine.errors import WorkflowConfigError
        with self.assertRaises(WorkflowConfigError):
            load_workflow({"id": "x", "plugins": ["wfengine.plugins.notify"],
                           "nodes": [{"id": "n", "type": "notify", "params": {"channel": "sms"}}]})
        eng = self.engine()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            eng.start({"id": "x", "plugins": ["wfengine.plugins.notify"], "nodes": [
                {"id": "n", "type": "notify", "params": {"message": "hi {who}", "channel": "stdout"},
                 "inputs": {"who": "$input.who"}}]}, {"who": "team"})
        self.assertIn("[notify] hi team", buf.getvalue())
