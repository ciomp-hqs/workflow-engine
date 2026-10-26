"""Built-in node types: function, llm, condition, human_approval.

All of them use only the public NodeContext API, exactly like plugin node types.
"""
import json

from .. import expr
from ..errors import NonRetryableError
from ..registry import NodeType, functions, register_node


@register_node("function")
class FunctionNode(NodeType):
    """params.function: name registered with @register_function. Called as fn(inputs, ctx)."""

    def validate(self, n):
        name = n.get("params", {}).get("function")
        if not name:
            return ["params.function is required"]
        if name not in functions:
            return ["function %r is not registered (registered: %s)" % (name, functions.names())]
        return []

    def run(self, ctx):
        fn = functions.get(ctx.params["function"])
        out = fn(ctx.inputs, ctx)
        return out if isinstance(out, dict) else {"value": out}


@register_node("llm")
class LLMNode(NodeType):
    """params.task: mock model task name; inputs are sent as the request payload."""

    default_idempotent = True  # model calls have no side effects -> safe to re-run after a crash

    def validate(self, n):
        return [] if n.get("params", {}).get("task") else ["params.task is required"]

    def run(self, ctx):
        ctx.check_cancelled()
        payload = dict(ctx.inputs)
        payload.update({k: v for k, v in ctx.params.items() if k != "task"})
        return ctx.services.llm.complete(ctx.params["task"], payload, check_cancelled=ctx.check_cancelled)


@register_node("condition")
class ConditionNode(NodeType):
    """Either params.expr (-> {"result": bool}) or params.switch + cases/default (-> {"branch": str}).

    Downstream nodes select a branch with ``when``, e.g.
    {"eq": ["$nodes.gate.output.branch", "orders"]} or {"ref": "$nodes.gate.output.result"}.
    """

    default_idempotent = True

    def validate(self, n):
        p = n.get("params", {})
        if "expr" in p:
            return expr.validate_cond(p["expr"], "params.expr")
        if "switch" in p:
            if not isinstance(p.get("cases"), dict):
                return ["params.cases must be an object mapping value -> branch name"]
            return []
        return ["condition node needs params.expr or params.switch"]

    def run(self, ctx):
        p = ctx.raw_params
        if "expr" in p:
            return {"result": bool(ctx.eval(p["expr"]))}
        value = ctx.params["switch"]
        cases = ctx.params["cases"]
        key = value if isinstance(value, str) else json.dumps(value)
        if key in cases:
            return {"branch": cases[key], "value": value}
        if "default" in ctx.params:
            return {"branch": ctx.params["default"], "value": value}
        raise NonRetryableError("condition: value %r matches no case and no default given" % (value,))


def _render_value(v):
    if isinstance(v, list):
        if not v:
            return "- (无)"
        return "\n".join("- " + (json.dumps(i, ensure_ascii=False, sort_keys=True) if isinstance(i, (dict, list))
                                 else str(i)) for i in v)
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False, sort_keys=True, indent=2)
    return str(v)


def render_approval(title, inputs, fields=None):
    """Deterministic rendering of exactly what the approver sees; the hash is taken over this text."""
    lines = ["# 审批: %s" % title, ""]
    for k in (fields or sorted(inputs)):
        lines.append("## %s" % k)
        lines.append(_render_value(inputs.get(k)))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


@register_node("human_approval")
class HumanApprovalNode(NodeType):
    """Pauses the run. params.title, params.fields (ordered input keys to show; default all).

    On approve the node output is {"approved": true, "approved_by", "content_hash", "comment"}.
    On reject the node becomes REJECTED and the run becomes REJECTED.
    """

    def run(self, ctx):
        content = render_approval(ctx.params.get("title", ctx.node_id), ctx.inputs, ctx.params.get("fields"))
        return ctx.require_approval(content)
