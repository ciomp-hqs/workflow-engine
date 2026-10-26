"""End-to-end through the CLI, each command in a separate process (= restart between steps)."""
import os
import re
import subprocess
import sys

from helpers import ROOT, EngineTestCase


class CLITest(EngineTestCase):
    def cli(self, *args):
        cfg = os.path.join(self.tmp, "cfg.json")
        if not os.path.exists(cfg):
            import json
            with open(cfg, "w") as f:
                json.dump(self.settings(), f)
        p = subprocess.run([sys.executable, "-m", "wfengine", "--config", cfg, "--db", self.db] + list(args),
                           cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        return p.returncode, p.stdout, p.stderr

    def test_sensitive_doc_approve_in_new_process(self):
        code, out, _ = self.cli("run", "workflows/doc_review.json", "--input", "demo/doc_sensitive.json")
        self.assertEqual(code, 0)
        self.assertIn("WAITING_APPROVAL", out)
        rid = re.search(r"^run\s+(\S+)", out, re.M).group(1)
        h = re.search(r"PENDING APPROVAL node=approve hash=([0-9a-f]{64})", out).group(1)
        code, out, err = self.cli("approve", rid, "--node", "approve", "--hash", "f" * 64, "--user", "u")
        self.assertEqual(code, 2)
        self.assertIn("STALE APPROVAL", err)
        code, out, _ = self.cli("resume", rid)
        self.assertIn("status=WAITING_APPROVAL", out)
        code, out, _ = self.cli("approve", rid, "--node", "approve", "--hash", h, "--user", "alice")
        self.assertEqual(code, 0, out)
        self.assertIn("APPROVED: changed=True run_status=SUCCEEDED", out)
        code, out, _ = self.cli("approve", rid, "--node", "approve", "--hash", h, "--user", "alice")
        self.assertIn("changed=False", out)
        code, out, _ = self.cli("status", rid, "--json")
        import json
        st = json.loads(out)
        self.assertEqual(st["nodes"]["report"]["attempts"], 1)
        self.assertTrue(os.path.exists(os.path.join(self.tmp, "reports", rid + ".md")))
        code, out, _ = self.cli("list")
        self.assertIn(rid, out)

    def test_records_and_failure_reason_via_cli(self):
        code, out, _ = self.cli("run", "workflows/record_transform.json", "--input", "demo/records_invalid_type.json")
        self.assertEqual(code, 1)
        self.assertIn("unknown record_type 'invoice'", out)
        code, out, _ = self.cli("run", "workflows/record_transform.json", "--input", "demo/records_users_mixed.json")
        self.assertEqual(code, 0)
        self.assertIn("SUCCEEDED", out)
