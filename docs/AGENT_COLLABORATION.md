# Agent 协作记录

## 1. 任务拆分

| 角色 | 负责内容 |
|---|---|
| 主 Agent（与考生对话） | 阅读题面，选定方向 B；写出实现规格（节点类型、状态机、汇合语义、审批哈希、幂等要求、恢复策略、CLI、测试清单、交付物清单）；在规定时间内委派任务；审查结果 |
| 子 Agent（实现者，本记录作者） | 按规格实现执行器核心、存储、配置校验、内置节点、模拟模型、业务插件、两条流程、CLI、演示数据、unittest 测试、README 和本记录；运行测试和端到端演示；提交 git |

子 Agent 内部的开发顺序：
1. 公共层：`errors.py`、`registry.py`、`expr.py`（引用与条件表达式）
2. `config.py`：规范化与校验（环、未知依赖、引用必须指向祖先、类型/函数已注册）
3. `store.py`：SQLite 表结构，全部状态迁移写成条件 UPDATE
4. `mock_llm.py`、`nodes/builtin.py`
5. `engine.py`：调度循环、join、重试/超时、取消、审批、恢复
6. 插件 `plugins/docs.py`、`plugins/records.py`、`plugins/notify.py`，以及两个 workflow JSON
7. `cli.py`，手动冒烟测试，第一次提交
8. 测试（49 个），修复测试发现的问题，第二次提交
9. 端到端演示、README、本记录，最终提交

## 2. 关键指令节选（主 Agent 给子 Agent 的原始指令）

> Workflow defined in JSON config files (workflows/*.json): nodes with id, type, inputs (references like "$input.doc", "$nodes.extract.output.points"), depends_on, optional `when` condition (for conditional branches), retry {max_attempts, backoff_seconds}, timeout_seconds. DAG validation (cycles, unknown refs/deps) at load time; v1 acyclic only.

> Join semantics: a node runs when all deps are terminal; deps that are SKIPPED (branch not selected) propagate skip unless the node has join policy (e.g. `join: "any_success"` / `"all_done"`) so a merge after a condition never waits forever. Document the semantics.

> Approval: when entering human_approval, compute a content hash of exactly what is shown to the approver (rendered summary content); approval request must include that hash; approving with a stale hash (content changed, e.g. re-run upstream or edited input) is rejected and requires re-approval. Approve/reject must be idempotent ... use an atomic conditional update in SQLite (UPDATE ... WHERE status='WAITING_APPROVAL' and check rowcount) and node-level claim (UPDATE node SET status='RUNNING' WHERE status='PENDING') so a step is only claimed once even with concurrent continue calls.

> Nodes RUNNING at crash time: document & implement policy, e.g. mark as interrupted and re-run only if node declared `idempotent: true`, else mark FAILED(interrupted) needing manual decision.

> Flow 2 (record_transform.json) ... Must be added ONLY by config + new business node/function registration in a separate module ... not engine core changes. Make that explicit in README.

> Environment: macOS, Python 3.9.6 only, NO pytest installed, no node. Use ONLY the Python standard library ... Must run on Python 3.9.

## 3. 接口约定

### 3.1 节点处理接口

```python
class NodeType:
    default_idempotent = False
    def validate(self, node_cfg: dict) -> list[str]: ...   # 加载时，返回错误列表
    def run(self, ctx: NodeContext) -> dict | ApprovalRequired: ...

@register_node("type_name")            # 注册类或 handler(ctx) 函数
@register_function("fn_name")          # function 节点用：fn(inputs: dict, ctx) -> dict
```

约定：抛任意异常 = 本次尝试失败（可重试）；`NonRetryableError` = 不再重试；`NodeCancelled`（由 `ctx.check_cancelled()` 抛出）= 节点 CANCELLED；返回 `ctx.require_approval(content)` = 进入 WAITING_APPROVAL。

`NodeContext` 字段：`run_id, node_id, node, inputs, params, raw_params, run_input, attempt, services, store`；方法：`check_cancelled(), sleep(s), eval(cond), require_approval(content), log(kind, detail)`。

模型服务接口：`services.llm.complete(task: str, payload: dict, check_cancelled=None) -> dict`，进程内（`MockLLMService`）和 HTTP（`HTTPLLMClient`，`POST /v1/tasks/<task>`）实现相同接口。

### 3.2 配置 schema

见 README "配置格式"。顶层 `id, version, plugins, on_failure`；节点 `id, type, depends_on, inputs, params, when, join, retry{max_attempts, backoff_seconds, backoff_multiplier}, timeout_seconds, idempotent, description`。出现未知键会报错。

### 3.3 数据库 schema（wfengine/store.py）

- `runs(run_id PK, workflow_id, workflow_json, input_json, status, reason, result_json, created_at, updated_at)`
- `node_states(run_id, node_id, node_type, status, attempts, output_json, error, reason, owner, generation, started_at, finished_at, updated_at, PK(run_id,node_id))`
- `attempts(id PK, run_id, node_id, attempt, generation, status, error, owner, started_at, finished_at)`
- `approvals(approval_id PK, run_id, node_id, generation, content, content_hash, status, decided_by, comment, created_at, decided_at)`
- `events(id PK, run_id, node_id, kind, detail, ts)`

关键条件更新：
- 领取节点：`UPDATE node_states SET status='RUNNING', owner=? ... WHERE run_id=? AND node_id=? AND status='PENDING'`（rowcount=1 才执行）
- 完成节点：`... WHERE status='RUNNING' AND owner=?`（被恢复流程接管后，旧 owner 的写入变成空操作）
- 审批决定：`UPDATE approvals SET status=? ... WHERE approval_id=? AND status='PENDING' AND content_hash=?`
- 运行状态迁移：`UPDATE runs SET status=? WHERE run_id=? AND status IN (...)`

### 3.4 CLI

`validate | run | status [--json] [--events] | list | approve | reject | cancel | resume [--all] | rerun --from | mock-llm`，全局 `--db`、`--config`。退出码 0/1/2 定义见 README。

## 4. 生成结果的检查方法

1. 自动测试：`python3 -m unittest discover -s tests -v`，49 个测试，覆盖题面验收项：

| 验收项 | 测试 |
|---|---|
| 运行两条流程 | `test_doc_flow.*`、`test_records_flow.*`、`test_cli.*` |
| 批准 / 拒绝 | `test_sensitive_doc_waits_then_approve_writes_report`、`test_reject_ends_run_and_no_report`、`test_restart_while_waiting_then_reject` |
| 分支汇合（未选中分支不阻塞） | `JoinTest.*`、`test_clean_doc_skips_approval_and_writes_report`、`test_orders_branch` |
| 节点失败与有限重试 | `FailureRetryTest.*`、`test_llm_retry_then_success_attempts_recorded`、`test_llm_permanent_failure_fails_run_with_reason`、`test_unknown_record_type_fails_with_reason_no_retry` |
| 重复继续不重复启动 | `test_repeated_approve_does_not_rerun_step`、`test_concurrent_approve_threads_single_winner`（10 线程、2 个引擎实例）、`test_concurrent_resume_while_waiting_starts_nothing`、`test_node_claim_is_atomic`（16 线程抢同一节点） |
| 等待审批时重启 | `test_restart_while_waiting_then_approve`、`test_sensitive_doc_approve_in_new_process`（CLI，每步独立进程） |
| 内容变化需重新批准 | `test_stale_hash_after_input_changed_requires_reapproval`、`test_stale_hash_after_upstream_output_tampered`、`test_wrong_hash_rejected` |
| 取消 | `CancelTest.*`（含另一个引擎实例发起的取消） |
| 并行部分失败 | `test_parallel_partial_failure_fail_fast`、`test_parallel_partial_failure_continue`、`test_extract_and_check_run_in_parallel` |
| 运行中节点崩溃恢复 | `RecoveryTest.*`（含真实 SIGKILL 子进程） |
| 配置校验 | `test_config.*`（环、未知依赖、非祖先引用、未注册类型/函数、非法 retry/join/when、重复 id、插件导入失败、CLI 报错） |
| 通知节点与扩展 | `ExtensionTest.*` |

2. 稳定性：并发相关的两个测试文件连续跑 15 轮（共 30 次）全部通过，排除偶发失败。
3. 端到端手动演示：`bash demo/run_demo.sh`，每条命令都是独立进程，输出节选见第 6 节。
4. 审查清单（逐项人工核对）：
   - 核心文件（engine/store/config/registry/builtin）不出现业务名称（也写成了测试）
   - 所有状态迁移都带 `WHERE status=...` 条件并检查 rowcount
   - 处理函数异常不会让调度循环崩溃；引擎内部异常记录为 `engine_error` 事件
   - 审批内容渲染是确定性的（JSON `sort_keys`、字段顺序由配置给出）
   - 只用标准库：没有 `match`、没有运行时 `X | Y` 类型注解；在 Python 3.9.6 上运行
   - `.gitignore` 排除 data/ reports/ output/ outbox/ 和本地 config.json；仓库中没有密钥

## 5. 方案调整与原因

相对主 Agent 规格的调整：

1. 新增 join 策略 `none_failed`。规格中只举了 `any_success` / `all_done`。流程一的 report 同时依赖 summarize 和 approve：`all_done` 在审批被拒绝时也会执行（不对），`any_success` 在 summarize 成功时总是执行（也不对）。所以增加 `none_failed`：依赖可以 SKIPPED，但不能 FAILED/CANCELLED/REJECTED。
2. 新增 `rerun --from <node> [--input]` 命令。规格要求"非幂等节点中断后需人工决定"和"内容变化后旧哈希失效"，两者都需要一个"重跑某节点及下游"的入口，否则无法演示和测试。
3. 审批时除了比对哈希，还按当前数据库状态重新渲染内容并比对。原因：只比对请求里存的哈希，发现不了"请求创建后上游输出被修改"的情况。
4. 测试中发现的问题：`rerun --from extract --input new.json` 最初只重置 extract 的下游，与 extract 并行、同样读取 `$input` 的 check 节点仍保留旧结果，审批内容也没有变化。改为提供新输入时，重置所有引用 `$input` 的节点及其下游（提交 82e70f7）。
5. 拒绝也要求带哈希，和批准一样绑定到具体内容。

其余部分按规格实现，没有调整。

## 6. 验证输出节选

### 6.1 测试

```
$ python3 -m unittest discover -s tests -v
test_approve_requires_user_and_known_node (test_approval.ApprovalTest) ... ok
test_concurrent_approve_threads_single_winner (test_approval.ApprovalTest) ... ok
test_concurrent_resume_while_waiting_starts_nothing (test_approval.ApprovalTest) ... ok
...（共 49 项，全部 ok）
test_kill9_process_then_resume (test_semantics.RecoveryTest)
Real crash: a child process is SIGKILLed while a non-idempotent node runs. ... ok
test_live_owner_is_not_stolen (test_semantics.RecoveryTest) ... ok
test_running_nodes_after_crash_idempotent_requeued_other_failed (test_semantics.RecoveryTest) ... ok
----------------------------------------------------------------------
Ran 49 tests in 7.851s

OK
```

### 6.2 流程一：无敏感内容（审批分支跳过，汇合不阻塞）

```
$ python3 -m wfengine run workflows/doc_review.json --input demo/doc_clean.json
status   SUCCEEDED
NODE               TYPE            STATUS            ATTEMPTS DETAIL
extract            llm             SUCCEEDED         1        output={"model": "mock-llm-v1", "points": [...]}
check              llm             SUCCEEDED         1        output={"findings": [], "model": "mock-llm-v1", "sensitive": false}
summarize          function        SUCCEEDED         1        ...
gate               condition       SUCCEEDED         1        output={"result": false}
approve            human_approval  SKIPPED           0        when condition false (branch not selected)
report             function        SUCCEEDED         1        output={"approved": false, "path": "reports/run_20261026110926_bf79da.md"}
notify_done        notify          SUCCEEDED         1        ...
```

### 6.3 流程一：敏感文档，等待审批 -> 新进程恢复 -> 批准

```
$ python3 -m wfengine run workflows/doc_review.json --input demo/doc_sensitive.json   # 进程1
status   WAITING_APPROVAL
approve            human_approval  WAITING_APPROVAL  1
report             function        PENDING           0
PENDING APPROVAL node=approve hash=48b7c6f3dc7846cc2ed19f5be9f9052ef033a752b009b3bba4538f280ee1906a
---- content shown to approver ----
# 审批: 敏感文档报告发布审批
## title
供应商合作说明（内部）
## summary
《供应商合作说明（内部）》要点 3 条：...检测到敏感内容 5 处（confidential, id_card_keyword, id_number, password, phone_number）
## findings
- {"match": "身份证", "rule": "id_card_keyword", "type": "keyword"}
- {"match": "138******78", "rule": "phone_number", "type": "pattern"}
...

$ python3 -m wfengine approve run_20261026110926_55ea6a --node approve --hash 0000 --user alice   # 进程2
STALE APPROVAL: content hash mismatch: approver saw 0000, pending content is 48b7c6f3...; re-review with `status`
(exit=2)

$ python3 -m wfengine resume run_20261026110926_55ea6a   # 进程3（重启恢复）
resume run_20261026110926_55ea6a: status=WAITING_APPROVAL requeued=[] failed_interrupted=[] running_elsewhere=[]

$ python3 -m wfengine approve run_20261026110926_55ea6a --node approve --hash 48b7c6f3... --user alice --comment 已确认   # 进程4
APPROVED: changed=True run_status=SUCCEEDED
approve            human_approval  SUCCEEDED         1        output={"approved": true, "approved_by": "alice", ...}
report             function        SUCCEEDED         1        output={"approved": true, "path": "reports/run_20261026110926_55ea6a.md"}
notify_done        notify          SUCCEEDED         1        ...

$ python3 -m wfengine approve run_20261026110926_55ea6a --node approve --hash 48b7c6f3... --user alice   # 进程5（重复）
APPROVED: changed=False run_status=SUCCEEDED
(already approved by alice; nothing restarted)
```

### 6.4 流程一：拒绝

```
$ python3 -m wfengine reject run_20261026110927_0b0522 --node approve --hash <hash> --user bob --comment 含身份证号
REJECTED: changed=True run_status=REJECTED
status   REJECTED  (rejected by bob: 含身份证号)
approve            human_approval  REJECTED          1        rejected by bob: 含身份证号
report             function        CANCELLED         0        run rejected at approve
notify_done        notify          CANCELLED         0        run rejected at approve
(exit=1)
$ ls reports/        # 被拒绝的 run 没有报告
run_20261026110926_55ea6a.md
run_20261026110926_bf79da.md
```

### 6.5 流程二

```
$ python3 -m wfengine run workflows/record_transform.json --input demo/records_orders_valid.json
status   SUCCEEDED
route              condition       SUCCEEDED         1        output={"branch": "orders", "value": "order"}
transform_orders   function        SUCCEEDED         1        output={"count": 3, "items": [{"amount_minor": 1250, ...
transform_users    function        SKIPPED           0        when condition false (branch not selected)
merge              function        SUCCEEDED         1        ...
write_output       function        SUCCEEDED         1        output={"count": 3, "path": "output/run_20261026110927_94d2d9.json"}

$ python3 -m wfengine run workflows/record_transform.json --input demo/records_users_mixed.json
status   SUCCEEDED
transform_users    function        SUCCEEDED         1        output={"count": 2, ...
alert_invalid      notify          SUCCEEDED         1        output={... "message": "记录校验发现 2 条无效记录" ...}

$ python3 -m wfengine run workflows/record_transform.json --input demo/records_invalid_type.json
status   FAILED  (fail_fast: node validate failed)
validate           function        FAILED            1        NonRetryableError: unknown record_type 'invoice' (supported: ['order', 'user'])
route              condition       CANCELLED         0        fail_fast: node validate failed
failed attempts:
  validate #1 FAILED: NonRetryableError: unknown record_type 'invoice' (supported: ['order', 'user'])
(exit=1)
```

### 6.6 失败注入与有限重试

```
$ echo '{"llm": {"fail_first_n": {"extract_points": 2}}}' > config.json
$ python3 -m wfengine run workflows/doc_review.json --input demo/doc_clean.json
status   SUCCEEDED
extract            llm             SUCCEEDED         3        ...
failed attempts:
  extract #1 FAILED: MockLLMError: mock-llm: injected transient failure for task extract_points (call #1 of fail_first_n=2)
  extract #2 FAILED: MockLLMError: mock-llm: injected transient failure for task extract_points (call #2 of fail_first_n=2)
```

### 6.7 跨进程取消（模拟模型延迟 5 秒）

```
$ python3 -m wfengine --config slow.json run workflows/doc_review.json --input demo/doc_clean.json &
$ python3 -m wfengine cancel run_20261026110952_bc44e9
cancel: changed=True status=CANCELLED
cancelled nodes: approve, gate, notify_done, report, summarize
# 原进程 run 的输出：
status   CANCELLED  (cancelled by user)
extract            llm             CANCELLED         1        run run_20261026110952_bc44e9 was cancelled
check              llm             CANCELLED         1        run run_20261026110952_bc44e9 was cancelled
summarize          function        CANCELLED         0        cancelled by user
```

### 6.8 HTTP 模拟模型

```
$ python3 -m wfengine mock-llm --port 8799 &
$ python3 -m wfengine --config http.json run workflows/doc_review.json --input demo/doc_sensitive.json   # llm.mode=http
status   WAITING_APPROVAL
```
