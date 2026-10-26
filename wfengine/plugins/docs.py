"""Business functions for workflows/doc_review.json (document review flow)."""
import os

from ..registry import register_function


@register_function("summarize_doc")
def summarize_doc(inputs, ctx):
    points = inputs.get("points") or []
    findings = inputs.get("findings") or []
    rules = sorted({f["rule"] for f in findings})
    summary = "《%s》要点 %d 条：%s" % (inputs.get("title") or "未命名", len(points), "；".join(points) or "无")
    if findings:
        summary += "。检测到敏感内容 %d 处（%s）" % (len(findings), ", ".join(rules))
    return {"title": inputs.get("title"), "summary": summary, "points": points,
            "sensitive": bool(inputs.get("sensitive")), "findings": findings, "finding_count": len(findings)}


@register_function("write_report")
def write_report(inputs, ctx):
    """Write reports/<run_id>.md. Same path each time -> safe to re-run (idempotent)."""
    s = inputs.get("summary") or {}
    approval = inputs.get("approval")
    lines = ["# 文档审阅报告: %s" % (s.get("title") or ""), "",
             "- run_id: %s" % ctx.run_id,
             "- 敏感内容: %s" % ("是" if s.get("sensitive") else "否"), "",
             "## 摘要", s.get("summary") or "", "", "## 要点"]
    lines += ["- " + p for p in s.get("points") or []] or ["- (无)"]
    lines += ["", "## 敏感内容检查"]
    lines += ["- %s: %s" % (f["rule"], f["match"]) for f in s.get("findings") or []] or ["- 未发现"]
    lines += ["", "## 审批"]
    if approval:
        lines += ["- 已批准: %s" % approval.get("approved_by"),
                  "- 审批内容哈希: %s" % approval.get("content_hash")]
        if approval.get("comment"):
            lines.append("- 备注: %s" % approval["comment"])
    else:
        lines.append("- 无需审批（未检测到敏感内容，审批分支未选中）")
    os.makedirs(ctx.services.report_dir, exist_ok=True)
    path = os.path.join(ctx.services.report_dir, "%s.md" % ctx.run_id)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    os.replace(tmp, path)
    return {"path": path, "approved": bool(approval)}
