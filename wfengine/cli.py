"""Command line interface: python -m wfengine <command> ...

Exit codes: 0 ok (incl. WAITING_APPROVAL), 1 run ended FAILED/REJECTED/CANCELLED,
2 usage/config/state error (e.g. stale approval hash).
"""
import argparse
import json
import os
import sys
import time

from .engine import Engine
from .errors import StaleApprovalError, WorkflowConfigError, WorkflowError
from . import config as wfconfig


def load_settings(path):
    if path is None:
        path = "config.json" if os.path.exists("config.json") else None
    if path is None:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _fmt_ts(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else "-"


def _short(v, n=160):
    s = json.dumps(v, ensure_ascii=False, sort_keys=True) if not isinstance(v, str) else v
    return s if len(s) <= n else s[:n] + "..."


def print_status(eng, run_id, as_json=False, out=sys.stdout):
    st = eng.status(run_id)
    if as_json:
        st.pop("workflow", None)
        print(json.dumps(st, ensure_ascii=False, indent=2, default=str), file=out)
        return st
    wf = st["workflow"]
    print("run      %s" % st["run_id"], file=out)
    print("workflow %s" % st["workflow_id"], file=out)
    print("status   %s%s" % (st["status"], ("  (%s)" % st["reason"]) if st["reason"] else ""), file=out)
    print("updated  %s" % _fmt_ts(st["updated_at"]), file=out)
    print("", file=out)
    print("%-18s %-15s %-17s %-8s %s" % ("NODE", "TYPE", "STATUS", "ATTEMPTS", "DETAIL"), file=out)
    for n in wf["nodes"]:
        ns = st["nodes"][n["id"]]
        detail = ns["error"] or ns["reason"] or ""
        if ns["status"] == "SUCCEEDED" and ns["output"] is not None:
            detail = "output=" + _short(ns["output"], 90)
        print("%-18s %-15s %-17s %-8s %s" % (n["id"], ns["node_type"], ns["status"], ns["attempts"], detail),
              file=out)
    failed_attempts = [a for a in st["attempts"] if a["status"] in ("FAILED", "INTERRUPTED")]
    if failed_attempts:
        print("\nfailed attempts:", file=out)
        for a in failed_attempts:
            print("  %s #%d %s: %s" % (a["node_id"], a["attempt"], a["status"], a["error"]), file=out)
    pend = [a for a in st["approvals"] if a["status"] == "PENDING"]
    for a in pend:
        print("\nPENDING APPROVAL node=%s hash=%s" % (a["node_id"], a["content_hash"]), file=out)
        print("---- content shown to approver ----", file=out)
        print(a["content"].rstrip(), file=out)
        print("-----------------------------------", file=out)
        print("approve: python -m wfengine approve %s --node %s --hash %s --user <you>" %
              (run_id, a["node_id"], a["content_hash"]), file=out)
        print("reject:  python -m wfengine reject %s --node %s --hash %s --user <you> --comment ..." %
              (run_id, a["node_id"], a["content_hash"]), file=out)
    decided = [a for a in st["approvals"] if a["status"] in ("APPROVED", "REJECTED", "SUPERSEDED")]
    if decided:
        print("\napproval history:", file=out)
        for a in decided:
            print("  %s %s by=%s hash=%s" % (a["node_id"], a["status"], a["decided_by"] or "-",
                                             a["content_hash"][:16]), file=out)
    if st["result"]:
        print("\nresult: %s" % _short(st["result"], 400), file=out)
    return st


def _exit_for(status):
    return 1 if status in ("FAILED", "REJECTED", "CANCELLED") else 0


def build_parser():
    p = argparse.ArgumentParser(prog="python -m wfengine", description="config-driven workflow executor")
    p.add_argument("--config", help="settings JSON (default ./config.json if present)")
    p.add_argument("--db", help="SQLite path (overrides config; default data/wf.db)")
    sub = p.add_subparsers(dest="cmd")
    s = sub.add_parser("validate", help="validate a workflow config")
    s.add_argument("workflow")
    s = sub.add_parser("run", help="start a run")
    s.add_argument("workflow")
    s.add_argument("--input", required=True, help="JSON file with the run input")
    s.add_argument("--json", action="store_true")
    s = sub.add_parser("status", help="show run, node states, results, failure reasons")
    s.add_argument("run_id")
    s.add_argument("--json", action="store_true")
    s.add_argument("--events", action="store_true", help="also print the event log")
    s = sub.add_parser("list", help="list runs")
    s.add_argument("--limit", type=int, default=20)
    for name in ("approve", "reject"):
        s = sub.add_parser(name, help="%s a pending human_approval node" % name)
        s.add_argument("run_id")
        s.add_argument("--node", required=True)
        s.add_argument("--hash", required=True, help="content hash shown by `status`")
        s.add_argument("--user", required=True)
        s.add_argument("--comment")
    s = sub.add_parser("cancel", help="cancel a run")
    s.add_argument("run_id")
    s.add_argument("--reason", default="cancelled by user")
    s = sub.add_parser("resume", help="recover after restart and continue a run")
    s.add_argument("run_id", nargs="?")
    s.add_argument("--all", action="store_true", help="resume every RUNNING/WAITING_APPROVAL run")
    s = sub.add_parser("rerun", help="re-execute a node and its downstream (manual recovery)")
    s.add_argument("run_id")
    s.add_argument("--from", dest="from_node", required=True)
    s.add_argument("--input", help="replace the run input with this JSON file first")
    s = sub.add_parser("mock-llm", help="serve the mock model over HTTP")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8765)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    if not args.cmd:
        build_parser().print_help()
        return 2
    try:
        settings = load_settings(args.config)
        if args.cmd == "validate":
            wf = wfconfig.load_workflow(args.workflow)
            print("OK: workflow %r, %d nodes, order: %s" % (wf["id"], len(wf["nodes"]),
                                                          " -> ".join(wfconfig.topo_order(wf["nodes"])[0])))
            return 0
        if args.cmd == "mock-llm":
            from .mock_llm import MockLLMService, make_http_server
            llm = settings.get("llm", {})
            srv = make_http_server(MockLLMService(llm.get("fail_first_n"), llm.get("always_fail"),
                                                  llm.get("latency_seconds", 0)), args.host, args.port)
            print("mock-llm listening on http://%s:%d" % (args.host, args.port), flush=True)
            srv.serve_forever()
            return 0
        with Engine(db_path=args.db, settings=settings) as eng:
            return _dispatch(eng, args)
    except WorkflowConfigError as e:
        print("CONFIG ERROR:", file=sys.stderr)
        for err in e.errors:
            print("  - " + err, file=sys.stderr)
        return 2
    except StaleApprovalError as e:
        print("STALE APPROVAL: %s" % e, file=sys.stderr)
        return 2
    except WorkflowError as e:
        print("ERROR: %s" % e, file=sys.stderr)
        return 2


def _dispatch(eng, args):
    if args.cmd == "run":
        with open(args.input, "r", encoding="utf-8") as f:
            run_input = json.load(f)
        run_id = eng.start(args.workflow, run_input)
        st = print_status(eng, run_id, as_json=args.json)
        return _exit_for(st["status"])
    if args.cmd == "status":
        print_status(eng, args.run_id, as_json=args.json)
        if args.events:
            print("\nevents:")
            for e in eng.store.get_events(args.run_id):
                print("  %s %-24s %-14s %s" % (_fmt_ts(e["ts"]), e["kind"], e["node_id"] or "", e["detail"] or ""))
        return 0
    if args.cmd == "list":
        rows = eng.store.list_runs(args.limit)
        print("%-32s %-18s %-17s %-20s %s" % ("RUN_ID", "WORKFLOW", "STATUS", "CREATED", "REASON"))
        for r in rows:
            print("%-32s %-18s %-17s %-20s %s" % (r["run_id"], r["workflow_id"], r["status"],
                                                 _fmt_ts(r["created_at"]), r["reason"] or ""))
        return 0
    if args.cmd in ("approve", "reject"):
        fn = eng.approve if args.cmd == "approve" else eng.reject
        res = fn(args.run_id, args.node, args.hash, args.user, args.comment)
        print("%s: changed=%s run_status=%s" % (res["decision"], res["changed"], res["run_status"]))
        if not res["changed"]:
            print("(already %s by %s; nothing restarted)" % (res["decision"].lower(), res["decided_by"]))
        print()
        st = print_status(eng, args.run_id)
        return _exit_for(st["status"])
    if args.cmd == "cancel":
        res = eng.cancel(args.run_id, args.reason)
        print("cancel: changed=%s status=%s" % (res["changed"], res["status"]))
        if res.get("cancelled_nodes"):
            print("cancelled nodes: %s" % ", ".join(res["cancelled_nodes"]))
        return 0
    if args.cmd == "resume":
        ids = [args.run_id] if args.run_id else []
        if args.all:
            ids = [r["run_id"] for r in eng.store.list_runs(1000) if r["status"] in ("RUNNING", "WAITING_APPROVAL")]
        if not ids:
            print("nothing to resume")
            return 0
        for rid in ids:
            rep = eng.resume(rid)
            print("resume %s: status=%s requeued=%s failed_interrupted=%s running_elsewhere=%s" % (
                rid, rep["status"], rep["requeued"], rep["failed_interrupted"], rep["running_elsewhere"]))
        if len(ids) == 1:
            print()
            print_status(eng, ids[0])
        return 0
    if args.cmd == "rerun":
        new_input = None
        if args.input:
            with open(args.input, "r", encoding="utf-8") as f:
                new_input = json.load(f)
        reset = eng.rerun(args.run_id, args.from_node, new_input=new_input)
        print("rerun: reset nodes %s" % ", ".join(reset))
        print()
        st = print_status(eng, args.run_id)
        return _exit_for(st["status"])
    return 2
