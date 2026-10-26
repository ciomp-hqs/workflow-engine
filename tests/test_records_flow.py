import json
import os

from helpers import EngineTestCase, REC_WF, demo


class RecordTransformFlowTest(EngineTestCase):
    def test_orders_branch(self):
        eng = self.engine()
        rid = eng.start(REC_WF, demo("records_orders_valid.json"))
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        st = self.statuses(eng, rid)
        self.assertEqual(st["transform_orders"], "SUCCEEDED")
        self.assertEqual(st["transform_users"], "SKIPPED")
        self.assertEqual(st["merge"], "SUCCEEDED")  # any_success join after branch
        self.assertEqual(st["alert_invalid"], "SKIPPED")
        path = self.node(eng, rid, "write_output")["output"]["path"]
        with open(path, encoding="utf-8") as f:
            out = json.load(f)
        self.assertEqual([i["amount_minor"] for i in out["items"]], [1250, 2, 19900])
        self.assertEqual(out["items"][0]["currency"], "CNY")
        self.assertEqual(out["branch"], "orders")

    def test_users_branch_with_invalid_records_and_notify(self):
        eng = self.engine()
        rid = eng.start(REC_WF, demo("records_users_mixed.json"))
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        st = self.statuses(eng, rid)
        self.assertEqual(st["transform_orders"], "SKIPPED")
        self.assertEqual(st["transform_users"], "SUCCEEDED")
        merged = self.node(eng, rid, "merge")["output"]
        self.assertEqual(merged["transformed_count"], 2)
        self.assertEqual(merged["invalid_count"], 2)
        self.assertEqual(merged["items"][0]["email"], "alice@example.com")
        self.assertEqual(merged["items"][0]["name"], "Alice")
        with open(os.path.join(self.tmp, "outbox", "notifications.jsonl"), encoding="utf-8") as f:
            lines = [json.loads(l) for l in f if l.strip()]
        self.assertEqual(lines[-1]["message"], "记录校验发现 2 条无效记录")

    def test_unknown_record_type_fails_with_reason_no_retry(self):
        eng = self.engine()
        rid = eng.start(REC_WF, demo("records_invalid_type.json"))
        self.assertEqual(eng.store.get_run_status(rid), "FAILED")
        v = self.node(eng, rid, "validate")
        self.assertEqual(v["attempts"], 1)  # NonRetryableError: no retry
        self.assertIn("unknown record_type 'invoice'", v["error"])

    def test_flow2_uses_no_model(self):
        eng = self.engine(always_fail=["extract_points", "sensitive_check", "summarize"])
        rid = eng.start(REC_WF, demo("records_orders_valid.json"))
        self.assertEqual(eng.store.get_run_status(rid), "SUCCEEDED")
        self.assertEqual(eng.services.llm.calls, {})
        with open(REC_WF, encoding="utf-8") as f:
            self.assertNotIn('"llm"', f.read())
