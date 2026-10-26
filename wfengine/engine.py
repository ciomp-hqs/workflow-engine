"""Workflow engine core: scheduling, joins, retries, timeouts, cancellation, approvals, recovery.

The core knows nothing about specific business flows. Node behaviour comes from the
registry (``wfengine.registry``); business code lives in plugin modules listed by the
workflow config.
"""
import concurrent.futures as cf
import hashlib
import json
import os
import threading
import time
import uuid

from . import config as wfconfig
from . import expr
from .errors import (AttemptTimeout, InvalidStateError, NodeCancelled, NonRetryableError, RefError,
                     StaleApprovalError)
from .mock_llm import build_llm_client
from .registry import node_types
from .store import NODE_TERMINAL, RUN_ACTIVE, RUN_TERMINAL, Store

DEFAULT_SETTINGS = {
    "db_path": "data/wf.db",
    "report_dir": "reports",
    "output_dir": "output",
    "outbox_dir": "outbox",
    "max_workers": 4,
    "llm": {"mode": "inprocess", "fail_first_n": {}, "always_fail": [], "latency_seconds": 0},
}


def content_hash(content):
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class ApprovalRequired:
    """Returned by a handler (via ctx.require_approval) to pause for a human decision."""

    def __init__(self, content):
        self.content = content
        self.content_hash = content_hash(content)


class Services:
    """Shared resources exposed to node handlers as ``ctx.services``."""

    def __init__(self, settings):
        self.settings = settings
        self.llm = build_llm_client(settings.get("llm"))
        self.report_dir = settings["report_dir"]
        self.output_dir = settings["output_dir"]
        self.outbox_dir = settings["outbox_dir"]


class NodeContext:
    """What a node handler receives.

    Attributes: run_id, node_id, node (config dict), inputs (resolved), params (resolved),
    raw_params, run_input, attempt (1-based), services, store.
    Methods: check_cancelled(), sleep(seconds), eval(cond), require_approval(content), log(kind, detail).
    """

    def __init__(self, engine, run_id, node, inputs, params, run_input, node_states, attempt, cancel_event):
        self._engine = engine
        self.run_id = run_id
        self.node_id = node["id"]
        self.node = node
        self.inputs = inputs
        self.params = params
        self.raw_params = node.get("params", {})
        self.run_input = run_input
        self._node_states = node_states
        self.attempt = attempt
        self.services = engine.services
        self.store = engine.store
        self._cancel_event = cancel_event
        self._last_db_check = 0.0

    def cancelled(self):
        if self._cancel_event.is_set():
            return True
        now = time.time()
        if now - self._last_db_check > 0.2:  # notice cancels issued by another process
            self._last_db_check = now
            if self.store.get_run_status(self.run_id) in RUN_TERMINAL:
                self._cancel_event.set()
                return True
        return False

    def check_cancelled(self):
        if self.cancelled():
            raise NodeCancelled("run %s was cancelled" % self.run_id)

    def sleep(self, seconds):
        """Cancellation-aware sleep for long-running handlers."""
        end = time.time() + seconds
        while True:
            self.check_cancelled()
            left = end - time.time()
            if left <= 0:
                return
            self._cancel_event.wait(min(left, 0.05))

    def eval(self, cond):
        return expr.eval_cond(cond, self.run_input, self._node_states)

    def require_approval(self, content):
        return ApprovalRequired(content)

    def log(self, kind, detail=None):
        self.store.event(self.run_id, kind, self.node_id, detail)


def _run_with_timeout(fn, timeout):
    if not timeout:
        return fn()
    box = {}

    def target():
        try:
            box["value"] = fn()
        except BaseException as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        # Python threads cannot be killed: the abandoned thread keeps running in the background
        # (its result is discarded). Handlers should call ctx.check_cancelled()/ctx.sleep().
        raise AttemptTimeout("attempt exceeded timeout_seconds=%s" % timeout)
    if "error" in box:
        raise box["error"]
    return box.get("value")


def _short(e):
    return "%s: %s" % (type(e).__name__, e)


class Engine:
    _live_owners = set()  # engine ids alive in this process (for crash detection)

    def __init__(self, db_path=None, settings=None):
        s = json.loads(json.dumps(DEFAULT_SETTINGS))
        for k, v in (settings or {}).items():
            if isinstance(v, dict) and isinstance(s.get(k), dict):
                s[k].update(v)
            else:
                s[k] = v
        if db_path:
            s["db_path"] = db_path
        self.settings = s
        self.store = Store(s["db_path"])
        self.services = Services(s)
        self.owner = "%d:%s" % (os.getpid(), uuid.uuid4().hex[:8])
        Engine._live_owners.add(self.owner)
        self._pool = cf.ThreadPoolExecutor(max_workers=int(s["max_workers"]), thread_name_prefix="wf-node")
        self._cancel_events = {}
        self._wf_cache = {}
        self._drive_locks = {}
        self._kick_flags = {}
        self._lock = threading.Lock()
        self._closed = False

    # ------------------------------------------------------------ lifecycle
    def close(self):
        if not self._closed:
            self._closed = True
            self._pool.shutdown(wait=True)
            Engine._live_owners.discard(self.owner)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------ public API
    def start(self, workflow, run_input, wait=True):
        """Create a run and drive it. ``workflow``: path, JSON string or dict."""
        wf = wfconfig.load_workflow(workflow)
        run_id = self.store.create_run(wf, run_input if run_input is not None else {})
        self._wf_cache[run_id] = wf
        self.kick(run_id, wait=wait)
        return run_id

    def kick(self, run_id, wait=True):
        """Drive a run forward until nothing more can be scheduled by this engine."""
        if wait:
            self._drive(run_id)
        else:
            threading.Thread(target=self._drive, args=(run_id,), daemon=True).start()

    def wait(self, run_id, timeout=30, statuses=RUN_TERMINAL + ("WAITING_APPROVAL",)):
        end = time.time() + timeout
        while time.time() < end:
            st = self.store.get_run_status(run_id)
            if st in statuses and not self._is_driving(run_id):
                return st
            time.sleep(0.02)
        raise TimeoutError("run %s still %s" % (run_id, self.store.get_run_status(run_id)))

    def status(self, run_id):
        run = self.store.get_run(run_id)
        if run is None:
            raise InvalidStateError("unknown run %s" % run_id)
        run["nodes"] = self.store.get_nodes(run_id)
        run["attempts"] = self.store.get_attempts(run_id)
        run["approvals"] = self.store.get_approvals(run_id)
        return run

    def pending_approvals(self, run_id):
        return [a for a in self.store.get_approvals(run_id) if a["status"] == "PENDING"]

    def approve(self, run_id, node_id, content_hash_, user, comment=None, wait=True):
        return self._decide(run_id, node_id, content_hash_, user, comment, "APPROVED", wait)

    def reject(self, run_id, node_id, content_hash_, user, comment=None):
        return self._decide(run_id, node_id, content_hash_, user, comment, "REJECTED", True)

    def cancel(self, run_id, reason="cancelled by user"):
        run = self._require_run(run_id)
        if not self.store.cas_run(run_id, RUN_ACTIVE, "CANCELLED", reason=reason):
            return {"run_id": run_id, "status": self.store.get_run_status(run_id), "changed": False}
        self._cancel_event(run_id).set()
        cancelled = self.store.cancel_open_nodes(run_id, reason, statuses=("PENDING", "WAITING_APPROVAL"))
        self.store.event(run_id, "run_cancelled", detail={"reason": reason, "cancelled_nodes": cancelled})
        # RUNNING nodes observe the cancel cooperatively and finish as CANCELLED.
        return {"run_id": run_id, "status": "CANCELLED", "changed": True, "cancelled_nodes": cancelled,
                "previous_status": run["status"]}

    def resume(self, run_id, wait=True):
        """Recover after a restart and continue. Safe to call repeatedly / concurrently."""
        self._require_run(run_id)
        report = self._recover(run_id)
        if self.store.get_run_status(run_id) in RUN_ACTIVE:
            self.kick(run_id, wait=wait)
        report["status"] = self.store.get_run_status(run_id)
        return report

    def rerun(self, run_id, from_node, new_input=None, wait=True):
        """Re-execute ``from_node`` and everything downstream of it (plus failed/cancelled nodes).

        Used for manual recovery of interrupted non-idempotent nodes and for "content changed"
        scenarios: a pending approval downstream is SUPERSEDED and re-requested with a new hash.
        """
        run = self._require_run(run_id)
        wf = self._workflow(run_id)
        ids = {n["id"] for n in wf["nodes"]}
        if from_node not in ids:
            raise InvalidStateError("unknown node %r" % from_node)
        nodes = self.store.get_nodes(run_id)
        busy = [nid for nid, st in nodes.items() if st["status"] == "RUNNING"]
        if busy:
            raise InvalidStateError("cannot rerun while nodes are RUNNING: %s" % busy)
        targets = {from_node} | wfconfig.descendants(wf, from_node)
        if new_input is not None:
            # every node that reads $input is affected by the new input, not only from_node's subtree
            for n in wf["nodes"]:
                refs = list(expr.iter_refs([n.get("inputs"), n.get("params"), n.get("when")]))
                if any(r.startswith("$input") for r in refs):
                    targets |= {n["id"]} | wfconfig.descendants(wf, n["id"])
        for nid, st in nodes.items():
            if st["status"] in ("FAILED", "CANCELLED", "REJECTED"):
                targets |= {nid} | wfconfig.descendants(wf, nid)
        if new_input is not None:
            self.store.set_run_input(run_id, new_input)
        self.store.reset_nodes(run_id, sorted(targets))
        self.store.cas_run(run_id, ("RUNNING", "WAITING_APPROVAL", "FAILED", "CANCELLED", "REJECTED", "SUCCEEDED"),
                           "RUNNING", reason=None)
        with self._lock:
            self._cancel_events[run_id] = threading.Event()
        self.store.event(run_id, "rerun", from_node, {"reset": sorted(targets), "input_changed": new_input is not None,
                                                     "previous_status": run["status"]})
        self.kick(run_id, wait=wait)
        return sorted(targets)

    # ------------------------------------------------------------ internals
    def _require_run(self, run_id):
        run = self.store.get_run(run_id)
        if run is None:
            raise InvalidStateError("unknown run %s" % run_id)
        return run

    def _workflow(self, run_id):
        wf = self._wf_cache.get(run_id)
        if wf is None:
            run = self._require_run(run_id)
            wf = wfconfig.load_workflow(run["workflow"])  # re-imports plugins from the snapshot
            self._wf_cache[run_id] = wf
        return wf

    def _cancel_event(self, run_id):
        with self._lock:
            ev = self._cancel_events.get(run_id)
            if ev is None:
                ev = self._cancel_events[run_id] = threading.Event()
            return ev

    def _is_driving(self, run_id):
        lock = self._drive_locks.get(run_id)
        return lock is not None and lock.locked()

    def _drive(self, run_id):
        with self._lock:
            lock = self._drive_locks.setdefault(run_id, threading.Lock())
            self._kick_flags[run_id] = True
        # Only one driver per run per engine; a concurrent kick just sets the flag so the
        # active driver does one more scheduling pass before it exits.
        while True:
            if not lock.acquire(blocking=False):
                return
            try:
                with self._lock:
                    self._kick_flags[run_id] = False
                self._drive_locked(run_id)
            finally:
                lock.release()
            with self._lock:
                if not self._kick_flags.get(run_id):
                    return

    def _drive_locked(self, run_id):
        wf = self._workflow(run_id)
        by_id = {n["id"]: n for n in wf["nodes"]}
        order, _ = wfconfig.topo_order(wf["nodes"])
        futures = {}
        cancel_ev = self._cancel_event(run_id)
        while True:
            run_status = self.store.get_run_status(run_id)
            active = run_status in RUN_ACTIVE
            nodes = self.store.get_nodes(run_id)
            progressed = False
            if active and not cancel_ev.is_set():
                run = self.store.get_run(run_id)
                for nid in order:
                    st = nodes[nid]
                    if st["status"] != "PENDING":
                        continue
                    deps = [nodes[d] for d in by_id[nid]["depends_on"]]
                    if any(d["status"] not in NODE_TERMINAL for d in deps):
                        continue
                    decision, reason = self._ready_decision(by_id[nid], deps, run["input"], nodes)
                    if decision == "skip":
                        if self.store.skip_node(run_id, nid, reason):
                            self.store.event(run_id, "node_skipped", nid, reason)
                            nodes[nid]["status"] = "SKIPPED"
                            progressed = True
                    elif decision == "fail":
                        if self.store.claim_node(run_id, nid, self.owner):
                            self.store.finish_node(run_id, nid, self.owner, "FAILED", error=reason)
                            self.store.event(run_id, "node_failed", nid, reason)
                            progressed = True
                    elif self.store.claim_node(run_id, nid, self.owner):
                        if run_status == "WAITING_APPROVAL":
                            self.store.cas_run(run_id, ("WAITING_APPROVAL",), "RUNNING")
                        self.store.event(run_id, "node_started", nid)
                        nodes[nid]["status"] = "RUNNING"
                        futures[self._pool.submit(self._execute, run_id, by_id[nid], cancel_ev)] = nid
            if progressed:
                self._apply_failure_policy(run_id, wf)
                continue
            if not futures:
                self._finalize(run_id)
                return
            done, _ = cf.wait(list(futures), timeout=0.2, return_when=cf.FIRST_COMPLETED)
            for f in done:
                futures.pop(f)
                try:
                    f.result()
                except Exception as e:  # noqa: BLE001 - engine bug safety net
                    self.store.event(run_id, "engine_error", None, _short(e))
            if done:
                self._apply_failure_policy(run_id, wf)
            elif self.store.get_run_status(run_id) in RUN_TERMINAL:
                cancel_ev.set()  # cancelled from another process: let running handlers notice

    def _ready_decision(self, node, deps, run_input, nodes):
        """Join semantics. Returns ("run"|"skip"|"fail", reason)."""
        statuses = [d["status"] for d in deps]
        join = node["join"]
        if join == "all_success":
            if any(s != "SUCCEEDED" for s in statuses):
                bad = sorted({s for s in statuses if s != "SUCCEEDED"})
                return "skip", "upstream not all succeeded (%s) [join=all_success]" % ",".join(bad)
        elif join == "none_failed":
            if any(s in ("FAILED", "CANCELLED", "REJECTED") for s in statuses):
                return "skip", "an upstream node failed/cancelled/rejected [join=none_failed]"
            if deps and all(s == "SKIPPED" for s in statuses):
                return "skip", "all upstream nodes skipped [join=none_failed]"
        elif join == "any_success":
            if deps and not any(s == "SUCCEEDED" for s in statuses):
                return "skip", "no upstream node succeeded [join=any_success]"
        # all_done: always run once deps are terminal
        if "when" in node:
            try:
                ok = expr.eval_cond(node["when"], run_input, nodes)
            except RefError as e:
                return "fail", "when condition error: %s" % e
            if not ok:
                return "skip", "when condition false (branch not selected)"
        return "run", None

    def _apply_failure_policy(self, run_id, wf):
        if wf["on_failure"] != "fail_fast":
            return
        nodes = self.store.get_nodes(run_id)
        failed = [nid for nid, st in nodes.items() if st["status"] == "FAILED"]
        if not failed:
            return
        reason = "fail_fast: node %s failed" % ", ".join(sorted(failed))
        if self.store.cas_run(run_id, RUN_ACTIVE, "FAILED", reason=reason):
            self._cancel_event(run_id).set()
            cancelled = self.store.cancel_open_nodes(run_id, reason, statuses=("PENDING", "WAITING_APPROVAL"))
            self.store.event(run_id, "run_failed", None, {"reason": reason, "cancelled_nodes": cancelled})

    def _finalize(self, run_id):
        run = self.store.get_run(run_id)
        nodes = self.store.get_nodes(run_id)
        if run["status"] in RUN_TERMINAL:
            # drain: nothing may stay PENDING in a finished run
            left = self.store.cancel_open_nodes(run_id, "run %s" % run["status"].lower())
            if left:
                self.store.event(run_id, "nodes_cancelled", None, left)
            return
        statuses = {st["status"] for st in nodes.values()}
        if "RUNNING" in statuses:
            return  # another engine/process owns running work; it will finalize
        if "WAITING_APPROVAL" in statuses:
            if self.store.cas_run(run_id, ("RUNNING",), "WAITING_APPROVAL"):
                waiting = sorted(n for n, st in nodes.items() if st["status"] == "WAITING_APPROVAL")
                self.store.event(run_id, "run_waiting_approval", None, waiting)
            return
        if "PENDING" in statuses:
            return  # a concurrent driver is mid-transition; it will finalize
        failed = sorted(n for n, st in nodes.items() if st["status"] == "FAILED")
        result = self._result(run_id, nodes)
        if failed:
            ok = self.store.cas_run(run_id, RUN_ACTIVE, "FAILED", reason="failed nodes: %s" % ", ".join(failed),
                                    result=result)
        else:
            ok = self.store.cas_run(run_id, RUN_ACTIVE, "SUCCEEDED", result=result)
        if ok:
            self.store.event(run_id, "run_finished", None, self.store.get_run_status(run_id))

    def _result(self, run_id, nodes):
        wf = self._workflow(run_id)
        has_child = {d for n in wf["nodes"] for d in n["depends_on"]}
        return {nid: st["output"] for nid, st in nodes.items()
                if nid not in has_child and st["status"] == "SUCCEEDED"}

    def _build_ctx(self, run_id, node, attempt, cancel_ev):
        run = self.store.get_run(run_id)
        nodes = self.store.get_nodes(run_id)
        inputs = expr.resolve(node.get("inputs", {}), run["input"], nodes)
        params = expr.resolve(node.get("params", {}), run["input"], nodes)
        return NodeContext(self, run_id, node, inputs, params, run["input"], nodes, attempt, cancel_ev)

    def _execute(self, run_id, node, cancel_ev):
        """Run all attempts of one claimed node (in a worker thread)."""
        nid = node["id"]
        handler = node_types.get(node["type"])
        retry = node["retry"]
        max_attempts = int(retry["max_attempts"])
        backoff = float(retry["backoff_seconds"])
        mult = float(retry.get("backoff_multiplier", 1) or 1)
        timeout = node.get("timeout_seconds")
        last_error = None
        for i in range(max_attempts):
            started = self.store.start_attempt(run_id, nid, self.owner)
            if started is None:
                return  # lost ownership (e.g. recovered by another engine)
            att_id, att_no = started
            if cancel_ev.is_set() or self.store.get_run_status(run_id) in RUN_TERMINAL:
                self.store.finish_attempt(att_id, "CANCELLED", "run cancelled before attempt started")
                self.store.finish_node(run_id, nid, self.owner, "CANCELLED", reason="run cancelled")
                return
            try:
                ctx = self._build_ctx(run_id, node, att_no, cancel_ev)
                out = _run_with_timeout(lambda: handler.run(ctx), timeout)
            except NodeCancelled as e:
                self.store.finish_attempt(att_id, "CANCELLED", str(e))
                self.store.finish_node(run_id, nid, self.owner, "CANCELLED", reason=str(e))
                self.store.event(run_id, "node_cancelled", nid, str(e))
                return
            except NonRetryableError as e:
                last_error = _short(e)
                self.store.finish_attempt(att_id, "FAILED", last_error)
                self.store.event(run_id, "attempt_failed", nid, {"attempt": att_no, "error": last_error,
                                                                 "retryable": False})
                break
            except Exception as e:  # noqa: BLE001 - any handler exception is a retryable failure
                last_error = _short(e)
                self.store.finish_attempt(att_id, "FAILED", last_error)
                self.store.event(run_id, "attempt_failed", nid, {"attempt": att_no, "error": last_error})
                if i + 1 < max_attempts:
                    delay = backoff * (mult ** i)
                    if delay and cancel_ev.wait(delay):
                        self.store.finish_node(run_id, nid, self.owner, "CANCELLED", reason="cancelled during backoff")
                        return
                    continue
                break
            if isinstance(out, ApprovalRequired):
                apr = self.store.enter_waiting_approval(run_id, nid, self.owner, out.content, out.content_hash, att_id)
                if apr:
                    self.store.event(run_id, "approval_requested", nid,
                                     {"approval_id": apr, "content_hash": out.content_hash})
                return
            if out is not None and not isinstance(out, dict):
                out = {"value": out}
            self.store.finish_attempt(att_id, "SUCCEEDED")
            if self.store.finish_node(run_id, nid, self.owner, "SUCCEEDED", output=out or {}):
                if cancel_ev.is_set():
                    self.store.event(run_id, "node_succeeded_after_cancel", nid)
                else:
                    self.store.event(run_id, "node_succeeded", nid)
            return
        reason = "failed after %d attempt(s): %s" % (self.store.get_nodes(run_id)[nid]["attempts"], last_error)
        self.store.finish_node(run_id, nid, self.owner, "FAILED", error=last_error, reason=reason)
        self.store.event(run_id, "node_failed", nid, reason)

    # ------------------------------------------------------------ approvals
    def _render_current(self, run_id, node):
        """Re-render what the approver would see *now* (from current DB state)."""
        handler = node_types.get(node["type"])
        ctx = self._build_ctx(run_id, node, 0, threading.Event())
        out = handler.run(ctx)
        return out if isinstance(out, ApprovalRequired) else None

    def _decide(self, run_id, node_id, hash_, user, comment, decision, wait):
        if not user:
            raise InvalidStateError("approver user is required")
        self._require_run(run_id)
        wf = self._workflow(run_id)
        node = next((n for n in wf["nodes"] if n["id"] == node_id), None)
        if node is None:
            raise InvalidStateError("unknown node %r" % node_id)
        apr = self.store.latest_approval(run_id, node_id)
        if apr is None:
            raise InvalidStateError("node %s has no approval request" % node_id)
        if apr["status"] in ("APPROVED", "REJECTED"):
            if apr["status"] == decision and apr["content_hash"] == hash_:
                # idempotent repeat: report the existing decision, start nothing
                return {"run_id": run_id, "node_id": node_id, "decision": decision, "changed": False,
                        "decided_by": apr["decided_by"], "run_status": self.store.get_run_status(run_id)}
            raise InvalidStateError("approval already decided: %s by %s" % (apr["status"], apr["decided_by"]))
        if apr["status"] != "PENDING":
            raise InvalidStateError("approval request is %s (run status %s)" %
                                    (apr["status"], self.store.get_run_status(run_id)))
        if hash_ != apr["content_hash"]:
            raise StaleApprovalError("content hash mismatch: approver saw %s, pending content is %s; "
                                     "re-review with `status`" % (hash_, apr["content_hash"]),
                                     current_hash=apr["content_hash"])
        # Content may have changed since the request (upstream output edited / re-run): re-render.
        current = self._render_current(run_id, node)
        if current is None or current.content_hash != apr["content_hash"]:
            new_hash = None
            if current is not None:
                self.store.supersede_approval(apr["approval_id"])
                new_hash = current.content_hash
                self._rerequest(run_id, node_id, current)
            raise StaleApprovalError("content changed since approval was requested; re-approval required "
                                     "(new hash %s)" % new_hash, current_hash=new_hash)
        if not self.store.decide_approval(apr["approval_id"], hash_, decision, user, comment):
            # lost a race with a concurrent decision: re-read and report idempotently
            again = self.store.latest_approval(run_id, node_id)
            if again["status"] == decision and again["content_hash"] == hash_:
                return {"run_id": run_id, "node_id": node_id, "decision": decision, "changed": False,
                        "decided_by": again["decided_by"], "run_status": self.store.get_run_status(run_id)}
            raise InvalidStateError("approval was concurrently decided as %s" % again["status"])
        self.store.event(run_id, "approval_" + decision.lower(), node_id, {"user": user, "hash": hash_})
        if decision == "APPROVED":
            out = {"approved": True, "approved_by": user, "content_hash": hash_, "comment": comment}
            self.store.cas_node(run_id, node_id, "WAITING_APPROVAL", "SUCCEEDED", output=out, set_output=True)
            self.store.cas_run(run_id, ("WAITING_APPROVAL",), "RUNNING")
            self.kick(run_id, wait=wait)
        else:
            reason = "rejected by %s%s" % (user, (": " + comment) if comment else "")
            out = {"approved": False, "rejected_by": user, "content_hash": hash_, "comment": comment}
            self.store.cas_node(run_id, node_id, "WAITING_APPROVAL", "REJECTED", output=out, set_output=True,
                                reason=reason)
            if self.store.cas_run(run_id, RUN_ACTIVE, "REJECTED", reason=reason):
                self._cancel_event(run_id).set()
                left = self.store.cancel_open_nodes(run_id, "run rejected at %s" % node_id,
                                                    statuses=("PENDING", "WAITING_APPROVAL"))
                self.store.event(run_id, "run_rejected", node_id, {"reason": reason, "cancelled_nodes": left})
        return {"run_id": run_id, "node_id": node_id, "decision": decision, "changed": True, "decided_by": user,
                "run_status": self.store.get_run_status(run_id)}

    def _rerequest(self, run_id, node_id, current):
        """Replace a stale request with a new one for the current content (node stays WAITING_APPROVAL)."""
        self.store.add_approval(run_id, node_id, current.content, current.content_hash)
        self.store.event(run_id, "approval_rerequested", node_id, {"content_hash": current.content_hash})

    # ------------------------------------------------------------ recovery
    @staticmethod
    def _owner_alive(owner):
        if not owner:
            return False
        if owner in Engine._live_owners:
            return True
        try:
            pid = int(owner.split(":", 1)[0])
        except ValueError:
            return False
        if pid == os.getpid():
            return False  # same process but engine instance is gone -> crashed/closed
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, OverflowError):
            return False
        except PermissionError:
            return True
        return True

    def _recover(self, run_id):
        """Handle nodes left RUNNING by a dead engine (crash/kill).

        idempotent=true  -> attempt marked INTERRUPTED, node back to PENDING (re-run automatically)
        idempotent=false -> node FAILED(reason=interrupted); needs `rerun --from <node>` by an operator
        """
        wf = self._workflow(run_id)
        by_id = {n["id"]: n for n in wf["nodes"]}
        report = {"run_id": run_id, "requeued": [], "failed_interrupted": [], "running_elsewhere": []}
        for nid, st in self.store.get_nodes(run_id).items():
            if st["status"] != "RUNNING":
                continue
            if self._owner_alive(st["owner"]):
                report["running_elsewhere"].append(nid)
                continue
            msg = "interrupted: engine %s died while node was RUNNING" % st["owner"]
            self.store.interrupt_attempts(run_id, nid, msg)
            if by_id[nid].get("idempotent"):
                if self.store.requeue_interrupted(run_id, nid, st["owner"], msg + " -> re-queued (idempotent)"):
                    report["requeued"].append(nid)
                    self.store.event(run_id, "node_requeued", nid, msg)
            elif self.store.finish_node(run_id, nid, st["owner"], "FAILED", error="interrupted",
                                        reason=msg + "; node not idempotent -> manual decision: "
                                                     "`rerun %s --from %s` or `cancel`" % (run_id, nid)):
                report["failed_interrupted"].append(nid)
                self.store.event(run_id, "node_interrupted", nid, msg)
        if report["failed_interrupted"]:
            self._apply_failure_policy(run_id, wf)
        return report
