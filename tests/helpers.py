import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from wfengine.engine import Engine  # noqa: E402
from wfengine.errors import NodeError  # noqa: E402
from wfengine.registry import register_function  # noqa: E402

DOC_WF = os.path.join(ROOT, "workflows", "doc_review.json")
REC_WF = os.path.join(ROOT, "workflows", "record_transform.json")


def demo(name):
    with open(os.path.join(ROOT, "demo", name), "r", encoding="utf-8") as f:
        return json.load(f)


# ---- test-only business functions (registered like any plugin would) ----
CALLS = {}
CALLS_LOCK = threading.Lock()


def _count(name):
    with CALLS_LOCK:
        CALLS[name] = CALLS.get(name, 0) + 1
        return CALLS[name]


@register_function("t_echo", replace=True)
def t_echo(inputs, ctx):
    _count("t_echo:%s" % ctx.node_id)
    return {"value": inputs.get("value"), "node": ctx.node_id, "t": time.time()}


@register_function("t_sleep", replace=True)
def t_sleep(inputs, ctx):
    """Cooperative sleep: honours cancellation."""
    start = time.time()
    _count("t_sleep:%s" % ctx.node_id)
    ctx.sleep(float(inputs.get("seconds", 0.3)))
    return {"start": start, "end": time.time()}


@register_function("t_flaky", replace=True)
def t_flaky(inputs, ctx):
    """Fails until ctx.attempt reaches inputs.succeed_on (0 = always fail)."""
    _count("t_flaky:%s" % ctx.node_id)
    ok_on = int(inputs.get("succeed_on", 0))
    if ok_on and ctx.attempt >= ok_on:
        return {"attempt": ctx.attempt}
    raise NodeError("flaky failure on attempt %d" % ctx.attempt)


@register_function("t_block_forever", replace=True)
def t_block_forever(inputs, ctx):
    """Ignores cancellation (to test timeouts)."""
    time.sleep(float(inputs.get("seconds", 2)))
    return {}


class EngineTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wftest_")
        self.db = os.path.join(self.tmp, "wf.db")
        self.engines = []
        with CALLS_LOCK:
            CALLS.clear()

    def tearDown(self):
        for e in self.engines:
            e.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def settings(self, **llm):
        return {"report_dir": os.path.join(self.tmp, "reports"), "output_dir": os.path.join(self.tmp, "output"),
                "outbox_dir": os.path.join(self.tmp, "outbox"), "max_workers": 4, "llm": llm}

    def engine(self, **llm):
        e = Engine(db_path=self.db, settings=self.settings(**llm))
        self.engines.append(e)
        return e

    def node(self, eng, run_id, node_id):
        return eng.store.get_nodes(run_id)[node_id]

    def statuses(self, eng, run_id):
        return {k: v["status"] for k, v in eng.store.get_nodes(run_id).items()}
