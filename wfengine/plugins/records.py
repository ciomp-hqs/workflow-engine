"""Business functions for workflows/record_transform.json (record transform flow, no model calls).

This module + the JSON config are everything flow 2 needs: the engine core is untouched.
"""
import json
import os
import re
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from ..errors import NonRetryableError
from ..registry import register_function

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _check(value, kind):
    if kind == "str":
        return isinstance(value, str) and value.strip() != ""
    if kind == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if kind == "positive_number":
        return _check(value, "number") and value > 0
    if kind == "email":
        return isinstance(value, str) and bool(EMAIL_RE.match(value.strip()))
    if kind.startswith("enum:"):
        return value in kind[5:].split("|")
    raise NonRetryableError("unknown field rule %r" % kind)


@register_function("validate_records")
def validate_records(inputs, ctx):
    """params.schemas: {record_type: {field: rule}}. Rules: str, number, positive_number, email, enum:a|b."""
    rtype = inputs.get("record_type")
    records = inputs.get("records")
    schemas = ctx.params.get("schemas") or {}
    if rtype not in schemas:
        raise NonRetryableError("unknown record_type %r (supported: %s)" % (rtype, sorted(schemas)))
    if not isinstance(records, list):
        raise NonRetryableError("input.records must be a list")
    valid, invalid = [], []
    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            invalid.append({"index": i, "record": rec, "errors": ["record is not an object"]})
            continue
        errs = []
        for field, rule in schemas[rtype].items():
            if field not in rec:
                errs.append("missing field %s" % field)
            elif not _check(rec[field], rule):
                errs.append("field %s fails rule %s (got %r)" % (field, rule, rec[field]))
        if errs:
            invalid.append({"index": i, "record": rec, "errors": errs})
        else:
            valid.append(rec)
    return {"record_type": rtype, "valid": valid, "invalid": invalid,
            "valid_count": len(valid), "invalid_count": len(invalid)}


@register_function("transform_orders")
def transform_orders(inputs, ctx):
    """amount (yuan, number) -> amount_minor (int cents, ROUND_HALF_UP); currency upper-cased."""
    out = []
    for r in inputs.get("items") or []:
        try:
            minor = int((Decimal(str(r["amount"])) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        except (InvalidOperation, KeyError) as e:
            raise NonRetryableError("bad order %r: %s" % (r, e))
        out.append({"order_id": r["order_id"], "amount_minor": minor,
                    "currency": str(r.get("currency", "CNY")).upper(), "kind": "order"})
    return {"items": out, "count": len(out)}


@register_function("transform_users")
def transform_users(inputs, ctx):
    """email lower-cased, name trimmed, email domain extracted."""
    out = []
    for r in inputs.get("items") or []:
        email = r["email"].strip().lower()
        out.append({"user_id": r["user_id"], "name": r["name"].strip(), "email": email,
                    "email_domain": email.split("@", 1)[1], "kind": "user"})
    return {"items": out, "count": len(out)}


@register_function("merge_results")
def merge_results(inputs, ctx):
    """Join after the branch: exactly one of the branch outputs is non-null (the other was SKIPPED)."""
    branches = {k: v for k, v in (inputs.get("branches") or {}).items() if v is not None}
    items = []
    for name in sorted(branches):
        items.extend(branches[name].get("items") or [])
    return {"record_type": inputs.get("record_type"), "branch": ",".join(sorted(branches)) or None,
            "items": items, "transformed_count": len(items), "invalid": inputs.get("invalid") or [],
            "invalid_count": len(inputs.get("invalid") or [])}


@register_function("write_json_output")
def write_json_output(inputs, ctx):
    os.makedirs(ctx.services.output_dir, exist_ok=True)
    path = os.path.join(ctx.services.output_dir, "%s.json" % ctx.run_id)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(inputs.get("result"), f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)
    return {"path": path, "count": (inputs.get("result") or {}).get("transformed_count")}
