import json
import os
import threading

from helpers import DOC_WF, EngineTestCase, demo
from wfengine.errors import InvalidStateError, StaleApprovalError


class ApprovalTest(EngineTestCase):
    def _waiting_run(self, eng):
        rid = eng.start(DOC_WF, demo("doc_sensitive.json"))
        self.assertEqual(eng.store.get_run_status(rid), "WAITING_APPROVAL")
        [apr] = eng.pending_approvals(rid)
        return rid, apr

    def _outbox_lines(self, rid):
        path = os.path.join(self.tmp, "outbox", "notifications.jsonl")
        with open(path, encoding="utf-8") as f:
            return [l for l in (json.loads(x) for x in f if x.strip()) if l["run_id"] == rid]

    def test_hash_matches_rendered_content(self):
        import hashlib
        eng = self.engine()
        rid, apr = self._waiting_run(eng)
        self.assertEqual(apr["content_hash"], hashlib.sha256(apr["content"].encode("utf-8")).hexdigest())

    def test_wrong_hash_rejected(self):
        eng = self.engine()
        rid, apr = self._waiting_run(eng)
        with self.assertRaises(StaleApprovalError) as cm:
            eng.approve(rid, "approve", "0" * 64, "alice")
        self.assertEqual(cm.exception.current_hash, apr["content_hash"])
        self.assertEqual(eng.store.get_run_status(rid), "WAITING_APPROVAL")

    def test_repeated_approve_does_not_rerun_step(self):
        eng = self.engine()
        rid, apr = self._waiting_run(eng)
        r1 = eng.approve(rid, "approve", apr["content_hash"], "alice")
        r2 = eng.approve(rid, "approve", apr["content_hash"], "alice")
        eng.resume(rid)
        eng.resume(rid)
        self.assertTrue(r1["changed"])
        self.assertFalse(r2["changed"])
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        self.assertEqual(self.node(eng, rid, "report")["attempts"], 1)
        self.assertEqual(self.node(eng, rid, "notify_done")["attempts"], 1)
        self.assertEqual(len(eng.store.get_attempts(rid, "approve")), 1)
        self.assertEqual(len(self._outbox_lines(rid)), 1)

    def test_concurrent_approve_threads_single_winner(self):
        eng = self.engine()
        eng2 = self.engine()  # a second engine instance on the same DB (another "process")
        rid, apr = self._waiting_run(eng)
        results, errors = [], []
        barrier = threading.Barrier(10)

        def worker(e):
            barrier.wait()
            try:
                results.append(e.approve(rid, "approve", apr["content_hash"], "alice"))
            except Exception as ex:  # noqa: BLE001
                errors.append(ex)

        ts = [threading.Thread(target=worker, args=(eng if i % 2 else eng2,)) for i in range(10)]
        [t.start() for t in ts]
        [t.join(20) for t in ts]
        self.assertEqual(errors, [])
        self.assertEqual(sum(1 for r in results if r["changed"]), 1)
        eng.wait(rid)
        eng2.wait(rid)
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        self.assertEqual(self.node(eng, rid, "report")["attempts"], 1)
        self.assertEqual(len(eng.store.get_attempts(rid, "report")), 1)
        self.assertEqual(len(self._outbox_lines(rid)), 1)
        decided = [a for a in eng.store.get_approvals(rid) if a["status"] == "APPROVED"]
        self.assertEqual(len(decided), 1)

    def test_concurrent_resume_while_waiting_starts_nothing(self):
        eng, eng2 = self.engine(), self.engine()
        rid, apr = self._waiting_run(eng)
        before = {k: v["attempts"] for k, v in eng.store.get_nodes(rid).items()}
        ts = [threading.Thread(target=(eng if i % 2 else eng2).resume, args=(rid,)) for i in range(8)]
        [t.start() for t in ts]
        [t.join(20) for t in ts]
        after = {k: v["attempts"] for k, v in eng.store.get_nodes(rid).items()}
        self.assertEqual(before, after)
        self.assertEqual(eng.store.get_run_status(rid), "WAITING_APPROVAL")
        self.assertEqual(len(eng.pending_approvals(rid)), 1)

    def test_node_claim_is_atomic(self):
        eng = self.engine()
        rid = eng.store.create_run({"id": "x", "nodes": [{"id": "a", "type": "function"}]}, {})
        wins = []
        barrier = threading.Barrier(16)

        def claim(i):
            barrier.wait()
            if eng.store.claim_node(rid, "a", "owner-%d" % i):
                wins.append(i)

        ts = [threading.Thread(target=claim, args=(i,)) for i in range(16)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(len(wins), 1)

    def test_restart_while_waiting_then_approve(self):
        eng1 = self.engine()
        rid, apr = self._waiting_run(eng1)
        eng1.close()  # "process exits"
        eng2 = self.engine()  # fresh engine, empty caches, same DB
        rep = eng2.resume(rid)
        self.assertEqual(rep["status"], "WAITING_APPROVAL")
        self.assertEqual(rep["requeued"] + rep["failed_interrupted"], [])
        [apr2] = eng2.pending_approvals(rid)
        self.assertEqual(apr2["content_hash"], apr["content_hash"])
        eng2.approve(rid, "approve", apr["content_hash"], "carol")
        self.assertEqual(eng2.store.get_run_status(rid), "SUCCEEDED")
        self.assertTrue(os.path.exists(os.path.join(self.tmp, "reports", rid + ".md")))
        self.assertEqual(self.node(eng2, rid, "extract")["attempts"], 1)  # upstream not re-run

    def test_restart_while_waiting_then_reject(self):
        eng1 = self.engine()
        rid, apr = self._waiting_run(eng1)
        eng1.close()
        eng2 = self.engine()
        eng2.reject(rid, "approve", apr["content_hash"], "dave", "no")
        self.assertEqual(eng2.store.get_run_status(rid), "REJECTED")

    def test_stale_hash_after_input_changed_requires_reapproval(self):
        eng = self.engine()
        rid, apr = self._waiting_run(eng)
        old_hash = apr["content_hash"]
        doc = demo("doc_sensitive.json")
        doc["doc"]["text"] += "新增条款：薪资信息另行通知。"
        eng.rerun(rid, "extract", new_input=doc)  # content the approver saw is now outdated
        self.assertEqual(eng.store.get_run_status(rid), "WAITING_APPROVAL")
        [new_apr] = eng.pending_approvals(rid)
        self.assertNotEqual(new_apr["content_hash"], old_hash)
        self.assertIn("salary", new_apr["content"])
        statuses = [a["status"] for a in eng.store.get_approvals(rid)]
        self.assertEqual(statuses, ["SUPERSEDED", "PENDING"])
        with self.assertRaises(StaleApprovalError):
            eng.approve(rid, "approve", old_hash, "alice")
        eng.approve(rid, "approve", new_apr["content_hash"], "alice")
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")

    def test_stale_hash_after_upstream_output_tampered(self):
        eng = self.engine()
        rid, apr = self._waiting_run(eng)
        # someone edits the upstream result in the DB after the request was created
        with eng.store._conn() as c:
            row = c.execute("SELECT output_json FROM node_states WHERE run_id=? AND node_id='summarize'",
                            (rid,)).fetchone()
            out = json.loads(row["output_json"])
            out["summary"] = "（已删除敏感信息）"
            c.execute("UPDATE node_states SET output_json=? WHERE run_id=? AND node_id='summarize'",
                      (json.dumps(out, ensure_ascii=False), rid))
        with self.assertRaises(StaleApprovalError) as cm:
            eng.approve(rid, "approve", apr["content_hash"], "alice")
        new_hash = cm.exception.current_hash
        self.assertTrue(new_hash and new_hash != apr["content_hash"])
        self.assertEqual(eng.store.get_run_status(rid), "WAITING_APPROVAL")
        [pending] = eng.pending_approvals(rid)
        self.assertIn("（已删除敏感信息）", pending["content"])
        eng.approve(rid, "approve", new_hash, "alice")
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")

    def test_approve_requires_user_and_known_node(self):
        eng = self.engine()
        rid, apr = self._waiting_run(eng)
        with self.assertRaises(InvalidStateError):
            eng.approve(rid, "approve", apr["content_hash"], "")
        with self.assertRaises(InvalidStateError):
            eng.approve(rid, "report", apr["content_hash"], "alice")
