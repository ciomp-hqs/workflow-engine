"""Extension example: a ``notify`` node type registered from a plugin module (no engine change).

params:
  message  str.format template; fields come from the node's resolved ``inputs``
           (missing fields render as "<missing:name>")
  channel  "outbox" (default; appends JSON lines to <outbox_dir>/notifications.jsonl) or "stdout"
  level    free text, default "info"
Idempotent: each record has key "<run_id>:<node_id>"; an existing key is not written twice,
so a re-run after a crash does not send a duplicate notification.
"""
import json
import os
import string
import time

from ..registry import NodeType, register_node


class _SafeDict(dict):
    def __missing__(self, key):
        return "<missing:%s>" % key


@register_node("notify")
class NotifyNode(NodeType):
    default_idempotent = True

    def validate(self, n):
        p = n.get("params", {})
        errs = []
        if not isinstance(p.get("message"), str):
            errs.append("params.message (string template) is required")
        if p.get("channel", "outbox") not in ("outbox", "stdout"):
            errs.append("params.channel must be 'outbox' or 'stdout'")
        return errs

    def run(self, ctx):
        msg = string.Formatter().vformat(ctx.params["message"], (), _SafeDict(ctx.inputs))
        key = "%s:%s" % (ctx.run_id, ctx.node_id)
        rec = {"key": key, "run_id": ctx.run_id, "node_id": ctx.node_id, "level": ctx.params.get("level", "info"),
               "message": msg, "ts": time.time()}
        channel = ctx.params.get("channel", "outbox")
        if channel == "stdout":
            print("[notify] %s" % msg, flush=True)
            return {"channel": channel, "message": msg, "delivered": True}
        os.makedirs(ctx.services.outbox_dir, exist_ok=True)
        path = os.path.join(ctx.services.outbox_dir, "notifications.jsonl")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip() and json.loads(line).get("key") == key:
                        return {"channel": channel, "message": msg, "path": path, "delivered": False,
                                "duplicate": True}
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return {"channel": channel, "message": msg, "path": path, "delivered": True}
