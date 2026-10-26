import os

from helpers import DOC_WF, EngineTestCase, demo


class DocReviewFlowTest(EngineTestCase):
    def test_clean_doc_skips_approval_and_writes_report(self):
        eng = self.engine()
        rid = eng.start(DOC_WF, demo("doc_clean.json"))
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        st = self.statuses(eng, rid)
        self.assertEqual(st["approve"], "SKIPPED")
        self.assertEqual(st["report"], "SUCCEEDED")  # join after skipped branch did not hang
        self.assertEqual(st["notify_done"], "SUCCEEDED")
        path = self.node(eng, rid, "report")["output"]["path"]
        with open(path, encoding="utf-8") as f:
            self.assertIn("无需审批", f.read())

    def test_sensitive_doc_waits_then_approve_writes_report(self):
        eng = self.engine()
        rid = eng.start(DOC_WF, demo("doc_sensitive.json"))
        self.assertEqual(eng.store.get_run_status(rid), "WAITING_APPROVAL")
        self.assertEqual(self.statuses(eng, rid)["report"], "PENDING")
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "reports", rid + ".md")))
        [apr] = eng.pending_approvals(rid)
        self.assertIn("phone_number", apr["content"])
        self.assertIn("138******78", apr["content"])
        res = eng.approve(rid, "approve", apr["content_hash"], "alice", "ok to publish")
        self.assertTrue(res["changed"])
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        with open(os.path.join(self.tmp, "reports", rid + ".md"), encoding="utf-8") as f:
            text = f.read()
        self.assertIn("已批准: alice", text)
        self.assertIn(apr["content_hash"], text)

    def test_reject_ends_run_and_no_report(self):
        eng = self.engine()
        rid = eng.start(DOC_WF, demo("doc_sensitive.json"))
        [apr] = eng.pending_approvals(rid)
        res = eng.reject(rid, "approve", apr["content_hash"], "bob", "contains ID numbers")
        self.assertEqual(res["run_status"], "REJECTED")
        st = self.statuses(eng, rid)
        self.assertEqual(st["approve"], "REJECTED")
        self.assertEqual(st["report"], "CANCELLED")
        self.assertEqual(st["notify_done"], "CANCELLED")
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "reports", rid + ".md")))
        run = eng.store.get_run(rid)
        self.assertIn("rejected by bob", run["reason"])
        # repeating the same reject is idempotent; approving afterwards is refused
        self.assertFalse(eng.reject(rid, "approve", apr["content_hash"], "bob")["changed"])
        from wfengine.errors import InvalidStateError
        with self.assertRaises(InvalidStateError):
            eng.approve(rid, "approve", apr["content_hash"], "alice")

    def test_extract_and_check_run_in_parallel(self):
        eng = self.engine(latency_seconds=0.3)
        rid = eng.start(DOC_WF, demo("doc_clean.json"))
        n = eng.store.get_nodes(rid)
        e, c = n["extract"], n["check"]
        # intervals overlap -> they ran concurrently
        self.assertLess(max(e["started_at"], c["started_at"]), min(e["finished_at"], c["finished_at"]))

    def test_llm_retry_then_success_attempts_recorded(self):
        eng = self.engine(fail_first_n={"extract_points": 2})
        rid = eng.start(DOC_WF, demo("doc_clean.json"))
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        self.assertEqual(self.node(eng, rid, "extract")["attempts"], 3)
        atts = eng.store.get_attempts(rid, "extract")
        self.assertEqual([a["status"] for a in atts], ["FAILED", "FAILED", "SUCCEEDED"])
        self.assertIn("injected transient failure", atts[0]["error"])

    def test_llm_permanent_failure_fails_run_with_reason(self):
        eng = self.engine(always_fail=["sensitive_check"])
        rid = eng.start(DOC_WF, demo("doc_clean.json"))
        run = eng.store.get_run(rid)
        self.assertEqual(run["status"], "FAILED")
        self.assertIn("check", run["reason"])
        node = self.node(eng, rid, "check")
        self.assertEqual(node["status"], "FAILED")
        self.assertEqual(node["attempts"], 3)  # retry.max_attempts=3, no more
        self.assertIn("failed after 3 attempt(s)", node["reason"])
        st = self.statuses(eng, rid)
        self.assertEqual(st["extract"], "SUCCEEDED")  # sibling result kept
        self.assertEqual(st["summarize"], "CANCELLED")
        self.assertEqual(st["report"], "CANCELLED")
