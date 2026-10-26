"""Reference resolution ($input..., $nodes.<id>...) and the small condition language.

References
  "$input"                       whole run input
  "$input.a.b"                   field path inside the run input
  "$nodes.<id>.output"           whole output of node <id>
  "$nodes.<id>.output.x.y"       field path inside that output
  "$nodes.<id>.status"           status string of node <id>
A string that is exactly a reference is replaced by the value (any JSON type).
Strings not starting with "$" are literals. Use "$$" to write a literal "$...".

Conditions (used by node ``when`` and by the ``condition`` node type)
  {"ref": "$..."}                         truthiness of the value
  {"eq": [a, b]}  ne / gt / gte / lt / lte
  {"in": [item, list]}  {"contains": [container, item]}
  {"not": cond}  {"and": [c1, c2, ...]}  {"or": [...]}
  true / false                            constant
Operands may be references or literals.
"""
import re

from .errors import RefError

REF_RE = re.compile(r"^\$(input|nodes)(\.[A-Za-z0-9_\-]+)*$")
COND_OPS = ("ref", "eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "not", "and", "or")


def is_ref(value):
    return isinstance(value, str) and value.startswith("$") and not value.startswith("$$")


def parse_ref(ref):
    """Return (root, node_id_or_None, field, path_list). Raise ValueError if malformed."""
    if not REF_RE.match(ref):
        raise ValueError("malformed reference %r" % ref)
    parts = ref[1:].split(".")
    if parts[0] == "input":
        return "input", None, None, parts[1:]
    if len(parts) < 3 or parts[2] not in ("output", "status"):
        raise ValueError("reference %r must look like $nodes.<id>.output[.path] or $nodes.<id>.status" % ref)
    if parts[2] == "status" and len(parts) > 3:
        raise ValueError("reference %r: status has no sub-fields" % ref)
    return "nodes", parts[1], parts[2], parts[3:]


def iter_refs(value):
    """Yield every reference string inside a (nested) value."""
    if isinstance(value, str):
        if is_ref(value):
            yield value
    elif isinstance(value, dict):
        for v in value.values():
            for r in iter_refs(v):
                yield r
    elif isinstance(value, list):
        for v in value:
            for r in iter_refs(v):
                yield r


def _walk(value, path, ref):
    cur = value
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        elif isinstance(cur, list) and key.isdigit() and int(key) < len(cur):
            cur = cur[int(key)]
        else:
            raise RefError("reference %s: key %r not found" % (ref, key))
    return cur


def lookup(ref, run_input, node_states):
    """Resolve one reference. node_states: {node_id: {"status":..., "output":...}}.

    Output of a node that did not SUCCEED (e.g. SKIPPED branch) resolves to None,
    so a join node can reference an optional branch safely.
    """
    root, node_id, field, path = parse_ref(ref)
    if root == "input":
        return _walk(run_input, path, ref)
    st = node_states.get(node_id)
    if st is None:
        raise RefError("reference %s: unknown node %r" % (ref, node_id))
    if field == "status":
        return st["status"]
    if st["status"] != "SUCCEEDED":
        return None
    return _walk(st.get("output"), path, ref)


def resolve(value, run_input, node_states):
    if isinstance(value, str):
        if value.startswith("$$"):
            return value[1:]
        if is_ref(value):
            return lookup(value, run_input, node_states)
        return value
    if isinstance(value, dict):
        return {k: resolve(v, run_input, node_states) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, run_input, node_states) for v in value]
    return value


def validate_cond(cond, where):
    """Return a list of error strings for a condition expression."""
    if isinstance(cond, bool):
        return []
    if not isinstance(cond, dict) or len(cond) != 1:
        return ["%s: condition must be a bool or an object with exactly one operator" % where]
    op, arg = next(iter(cond.items()))
    if op not in COND_OPS:
        return ["%s: unknown condition operator %r" % (where, op)]
    if op in ("and", "or"):
        if not isinstance(arg, list) or not arg:
            return ["%s: %r needs a non-empty list" % (where, op)]
        errs = []
        for i, c in enumerate(arg):
            errs.extend(validate_cond(c, "%s.%s[%d]" % (where, op, i)))
        return errs
    if op == "not":
        return validate_cond(arg, where + ".not")
    if op == "ref":
        return [] if is_ref(arg) else ["%s: 'ref' needs a reference string" % where]
    if not isinstance(arg, list) or len(arg) != 2:
        return ["%s: %r needs a list of two operands" % (where, op)]
    return []


def eval_cond(cond, run_input, node_states):
    """Evaluate a condition. Operands are resolved here."""
    if isinstance(cond, bool):
        return cond
    op, arg = next(iter(cond.items()))
    if op == "and":
        return all(eval_cond(c, run_input, node_states) for c in arg)
    if op == "or":
        return any(eval_cond(c, run_input, node_states) for c in arg)
    if op == "not":
        return not eval_cond(arg, run_input, node_states)
    if op == "ref":
        return bool(resolve(arg, run_input, node_states))
    a, b = resolve(arg, run_input, node_states)
    try:
        if op == "eq":
            return a == b
        if op == "ne":
            return a != b
        if op == "gt":
            return a > b
        if op == "gte":
            return a >= b
        if op == "lt":
            return a < b
        if op == "lte":
            return a <= b
        if op == "in":
            return a in (b or [])
        if op == "contains":
            return b in (a or [])
    except TypeError as e:
        raise RefError("condition %r: %s" % (cond, e))
    raise RefError("unknown operator %r" % op)
