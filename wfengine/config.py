"""Workflow config loading, normalization and validation (DAG checks at load time)."""
import copy
import importlib
import json
import re

from . import expr
from .errors import WorkflowConfigError
from .registry import node_types

JOIN_POLICIES = ("all_success", "none_failed", "any_success", "all_done")
FAILURE_POLICIES = ("fail_fast", "continue")
ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]*$")
NODE_KEYS = {"id", "type", "depends_on", "inputs", "params", "when", "join", "retry",
             "timeout_seconds", "idempotent", "description"}

# built-in node types are always available
BUILTIN_MODULES = ("wfengine.nodes.builtin",)


def import_plugins(modules):
    errors = []
    for mod in list(BUILTIN_MODULES) + list(modules or []):
        try:
            importlib.import_module(mod)
        except Exception as e:  # noqa: BLE001 - report import errors as config errors
            errors.append("cannot import plugin %r: %s" % (mod, e))
    return errors


def load_workflow(source):
    """Load from a file path, JSON string or dict; import plugins; validate; return normalized dict."""
    if isinstance(source, dict):
        wf = copy.deepcopy(source)
    else:
        text = str(source)
        if text.lstrip().startswith("{"):
            wf = json.loads(text)
        else:
            with open(text, "r", encoding="utf-8") as f:
                try:
                    wf = json.load(f)
                except ValueError as e:
                    raise WorkflowConfigError(["%s: invalid JSON: %s" % (text, e)])
    if not isinstance(wf, dict):
        raise WorkflowConfigError(["workflow must be a JSON object"])
    errors = import_plugins(wf.get("plugins", []))
    wf = normalize(wf)
    errors.extend(validate(wf))
    if errors:
        raise WorkflowConfigError(errors)
    return wf


def normalize(wf):
    wf.setdefault("version", 1)
    wf.setdefault("plugins", [])
    wf.setdefault("on_failure", "fail_fast")
    nodes = wf.get("nodes")
    if not isinstance(nodes, list):
        return wf
    for n in nodes:
        if not isinstance(n, dict):
            continue
        n.setdefault("depends_on", [])
        n.setdefault("inputs", {})
        n.setdefault("params", {})
        n.setdefault("join", "all_success")
        r = n.get("retry") or {}
        if isinstance(r, dict):
            r.setdefault("max_attempts", 1)
            r.setdefault("backoff_seconds", 0)
            r.setdefault("backoff_multiplier", 1)
        n["retry"] = r
        if "idempotent" not in n:
            nt = node_types.get(n.get("type"))
            n["idempotent"] = bool(getattr(nt, "default_idempotent", False))
    return wf


def topo_order(nodes):
    """Kahn's algorithm. Return (order, nodes_in_cycle)."""
    deps = {n["id"]: [d for d in n["depends_on"]] for n in nodes}
    indeg = {nid: 0 for nid in deps}
    children = {nid: [] for nid in deps}
    for nid, ds in deps.items():
        for d in ds:
            if d in deps:
                indeg[nid] += 1
                children[d].append(nid)
    order, queue = [], [nid for nid in deps if indeg[nid] == 0]
    while queue:
        cur = queue.pop(0)
        order.append(cur)
        for c in children[cur]:
            indeg[c] -= 1
            if indeg[c] == 0:
                queue.append(c)
    cycle = sorted(nid for nid in deps if nid not in order)
    return order, cycle


def ancestors(wf):
    """Map node id -> set of all transitive dependencies."""
    deps = {n["id"]: set(n["depends_on"]) for n in wf["nodes"]}
    memo = {}

    def anc(nid, stack=()):
        if nid in memo:
            return memo[nid]
        out = set()
        for d in deps.get(nid, ()):
            if d in stack:
                continue
            out.add(d)
            out |= anc(d, stack + (nid,))
        memo[nid] = out
        return out

    return {nid: anc(nid) for nid in deps}


def descendants(wf, node_id):
    children = {}
    for n in wf["nodes"]:
        for d in n["depends_on"]:
            children.setdefault(d, []).append(n["id"])
    out, stack = set(), [node_id]
    while stack:
        for c in children.get(stack.pop(), []):
            if c not in out:
                out.add(c)
                stack.append(c)
    return out


def validate(wf):
    errors = []
    if not wf.get("id") or not isinstance(wf.get("id"), str):
        errors.append("workflow.id is required (string)")
    if wf.get("on_failure") not in FAILURE_POLICIES:
        errors.append("workflow.on_failure must be one of %s" % (FAILURE_POLICIES,))
    nodes = wf.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        errors.append("workflow.nodes must be a non-empty list")
        return errors
    seen = set()
    for i, n in enumerate(nodes):
        if not isinstance(n, dict):
            errors.append("nodes[%d] must be an object" % i)
            continue
        nid = n.get("id")
        if not isinstance(nid, str) or not ID_RE.match(nid):
            errors.append("nodes[%d].id %r invalid (letters, digits, _ and -, starting with a letter)" % (i, nid))
            continue
        if nid in seen:
            errors.append("duplicate node id %r" % nid)
        seen.add(nid)
    if errors:
        return errors

    for n in nodes:
        nid = n["id"]
        where = "node %r" % nid
        unknown = set(n) - NODE_KEYS
        if unknown:
            errors.append("%s: unknown keys %s" % (where, sorted(unknown)))
        nt = node_types.get(n.get("type"))
        if nt is None:
            errors.append("%s: unknown node type %r (registered: %s)" % (where, n.get("type"), node_types.names()))
        if not isinstance(n["depends_on"], list):
            errors.append("%s: depends_on must be a list" % where)
            n["depends_on"] = []
        for d in n["depends_on"]:
            if d not in seen:
                errors.append("%s: depends_on unknown node %r" % (where, d))
            if d == nid:
                errors.append("%s: depends on itself" % where)
        if n["join"] not in JOIN_POLICIES:
            errors.append("%s: join must be one of %s" % (where, JOIN_POLICIES))
        r = n["retry"]
        if not isinstance(r, dict) or not isinstance(r.get("max_attempts"), int) or r["max_attempts"] < 1:
            errors.append("%s: retry.max_attempts must be an integer >= 1" % where)
        elif not isinstance(r.get("backoff_seconds"), (int, float)) or r["backoff_seconds"] < 0:
            errors.append("%s: retry.backoff_seconds must be >= 0" % where)
        t = n.get("timeout_seconds")
        if t is not None and (not isinstance(t, (int, float)) or t <= 0):
            errors.append("%s: timeout_seconds must be > 0" % where)
        if "when" in n:
            errors.extend(expr.validate_cond(n["when"], where + ".when"))
        if nt is not None:
            try:
                errors.extend("%s: %s" % (where, e) for e in (nt.validate(n) or []))
            except Exception as e:  # noqa: BLE001
                errors.append("%s: validate() crashed: %s" % (where, e))
    if errors:
        return errors

    order, cycle = topo_order(nodes)
    if cycle:
        errors.append("dependency cycle detected among nodes: %s (v1 supports acyclic workflows only)" % ", ".join(cycle))
        return errors

    anc = ancestors(wf)
    for n in nodes:
        where = "node %r" % n["id"]
        for part in ("inputs", "params", "when"):
            for ref in expr.iter_refs(n.get(part)):
                try:
                    root, ref_node, _f, _p = expr.parse_ref(ref)
                except ValueError as e:
                    errors.append("%s.%s: %s" % (where, part, e))
                    continue
                if root == "nodes":
                    if ref_node not in seen:
                        errors.append("%s.%s: reference %s points to unknown node %r" % (where, part, ref, ref_node))
                    elif ref_node not in anc[n["id"]]:
                        errors.append("%s.%s: reference %s needs %r to be an (indirect) dependency" % (where, part, ref, ref_node))
    return errors
