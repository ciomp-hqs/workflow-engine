#!/bin/bash
# 端到端演示：每条命令都是一个独立进程（= 每步之间都"重启"了一次）。
# 用法：bash demo/run_demo.sh   （在仓库根目录执行；会清空 data/ reports/ output/ outbox/）
set -u
cd "$(dirname "$0")/.."
rm -rf data reports output outbox
W="python3 -m wfengine"
step() { echo; echo "==> $*"; }

step "流程一 / 无敏感内容：审批分支被跳过，报告照常生成"
$W run workflows/doc_review.json --input demo/doc_clean.json

step "流程一 / 敏感文档：进入 WAITING_APPROVAL，进程退出"
$W run workflows/doc_review.json --input demo/doc_sensitive.json | tee /tmp/wf_demo_run.txt
RID=$(grep -m1 '^run ' /tmp/wf_demo_run.txt | awk '{print $2}')
HASH=$(grep -m1 'PENDING APPROVAL' /tmp/wf_demo_run.txt | sed 's/.*hash=//')

step "新进程：用错误哈希批准 -> 被拒绝 (exit 2)"
$W approve "$RID" --node approve --hash 0000 --user alice; echo "exit=$?"

step "新进程：resume（重启恢复），仍在等待审批"
$W resume "$RID" | head -1

step "新进程：正确哈希批准 -> 继续执行并写报告"
$W approve "$RID" --node approve --hash "$HASH" --user alice --comment "已确认"

step "重复批准：不会重复启动后续步骤"
$W approve "$RID" --node approve --hash "$HASH" --user alice | head -2
cat "reports/$RID.md"

step "流程一 / 拒绝：run=REJECTED，不写报告"
$W run workflows/doc_review.json --input demo/doc_sensitive.json > /tmp/wf_demo_run2.txt
RID2=$(grep -m1 '^run ' /tmp/wf_demo_run2.txt | awk '{print $2}')
HASH2=$(grep -m1 'PENDING APPROVAL' /tmp/wf_demo_run2.txt | sed 's/.*hash=//')
$W reject "$RID2" --node approve --hash "$HASH2" --user bob --comment "含身份证号"
ls reports/

step "流程二 / 订单记录（orders 分支）"
$W run workflows/record_transform.json --input demo/records_orders_valid.json
step "流程二 / 用户记录（users 分支，含无效记录 -> notify 告警）"
$W run workflows/record_transform.json --input demo/records_users_mixed.json
step "流程二 / 未知记录类型 -> 节点失败（不可重试），失败原因可查"
$W run workflows/record_transform.json --input demo/records_invalid_type.json

step "通知 outbox"
cat outbox/notifications.jsonl
step "运行列表"
$W list
