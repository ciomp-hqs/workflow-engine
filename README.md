# wfengine：配置驱动的通用工作流执行器（考试方向 B）

用 JSON 配置描述流程（节点类型、输入输出引用、依赖、条件分支、并行汇合），执行器负责调度、并行、重试、超时、取消、人工审批和 SQLite 持久化。新增业务流程只需要新增配置和业务插件模块，不改执行器核心。

- 依赖：只用 Python 标准库，Python >= 3.9（在 macOS 的 Python 3.9.6 上开发和测试）。无需 pip 安装、无需网络、无需任何账号或密钥。
- 外部服务（模型）全部由本地模拟服务 `wfengine/mock_llm.py` 提供，规则见下文。
- 无 UI，全部通过 CLI / Python API 操作。

## 快速开始

```bash
cd workflow-engine
python3 -m unittest discover -s tests -v                 # 自动测试（49 个）
bash demo/run_demo.sh                                    # 一键端到端演示（每步一个独立进程）

python3 -m wfengine validate workflows/doc_review.json   # 校验配置（DAG/引用/类型）
python3 -m wfengine run workflows/doc_review.json --input demo/doc_sensitive.json
python3 -m wfengine status <run_id> [--json] [--events]
python3 -m wfengine list
python3 -m wfengine approve <run_id> --node approve --hash <hash> --user alice [--comment ...]
python3 -m wfengine reject  <run_id> --node approve --hash <hash> --user bob --comment ...
python3 -m wfengine cancel  <run_id>
python3 -m wfengine resume  <run_id> | --all              # 重启后恢复
python3 -m wfengine rerun   <run_id> --from <node> [--input new.json]   # 人工恢复/内容变更重跑
python3 -m wfengine mock-llm --port 8765                  # 可选：以 HTTP 方式启动模拟模型
```

全局选项：`--db PATH`（默认 `data/wf.db`）、`--config FILE`（默认读取当前目录的 `config.json`，不存在则用内置默认值）。配置样例见 `config.example.json`：

```jsonc
{"db_path": "data/wf.db", "report_dir": "reports", "output_dir": "output", "outbox_dir": "outbox",
 "max_workers": 4,
 "llm": {"mode": "inprocess", "base_url": "http://127.0.0.1:8765",
         "fail_first_n": {}, "always_fail": [], "latency_seconds": 0}}
```

退出码：0 = 成功或等待审批；1 = 运行结束于 FAILED/REJECTED/CANCELLED；2 = 配置错误、状态错误、审批哈希过期。

## 演示步骤（手动版，与 demo/run_demo.sh 等价）

```bash
# 流程一：无敏感内容 -> 审批分支 SKIPPED，报告照常写入 reports/<run_id>.md
python3 -m wfengine run workflows/doc_review.json --input demo/doc_clean.json

# 流程一：敏感文档 -> WAITING_APPROVAL，进程退出；status 会打印审批内容与 hash
python3 -m wfengine run workflows/doc_review.json --input demo/doc_sensitive.json
python3 -m wfengine status <run_id>
# 另起进程（模拟重启）恢复并批准
python3 -m wfengine resume <run_id>
python3 -m wfengine approve <run_id> --node approve --hash <hash> --user alice
python3 -m wfengine approve <run_id> --node approve --hash <hash> --user alice   # 重复：changed=False
cat reports/<run_id>.md
# 拒绝路径：run=REJECTED，report/notify_done=CANCELLED，不写报告
python3 -m wfengine reject <run_id2> --node approve --hash <hash2> --user bob --comment 含身份证号

# 流程二（不调用模型）
python3 -m wfengine run workflows/record_transform.json --input demo/records_orders_valid.json
python3 -m wfengine run workflows/record_transform.json --input demo/records_users_mixed.json
python3 -m wfengine run workflows/record_transform.json --input demo/records_invalid_type.json  # 失败原因可查

# 失败注入：建一个 config.json，令 extract 前两次调用失败，观察 attempts=3
echo '{"llm": {"fail_first_n": {"extract_points": 2}}}' > config.json
python3 -m wfengine run workflows/doc_review.json --input demo/doc_clean.json && rm config.json
```

演示数据（`demo/`，均为虚构数据）：`doc_sensitive.json`（含手机号、身份证号、密码、"机密"）、`doc_clean.json`、`records_orders_valid.json`、`records_users_mixed.json`（2 条有效 2 条无效）、`records_invalid_type.json`（未知记录类型）。

## 目录结构

```
wfengine/
  engine.py        执行器核心：调度、汇合、重试、超时、取消、审批、恢复
  store.py         SQLite 持久化（所有状态迁移都是条件 UPDATE）
  config.py        配置加载、规范化、校验（环、未知依赖/引用/类型）
  registry.py      节点类型 / 业务函数注册表
  expr.py          引用解析与条件表达式
  mock_llm.py      本地模拟模型服务（进程内 / HTTP）
  nodes/builtin.py 内置节点：function, llm, condition, human_approval
  plugins/         业务插件（执行器核心不 import 它们）
    docs.py        流程一的业务函数：summarize_doc, write_report
    records.py     流程二的业务函数：validate_records, transform_*, merge_results, write_json_output
    notify.py      扩展示例：notify 节点类型
  cli.py, __main__.py
workflows/doc_review.json, workflows/record_transform.json
demo/              演示数据 + run_demo.sh
tests/             unittest 测试
docs/AGENT_COLLABORATION.md   Agent 协作记录
```

## 配置格式

```jsonc
{
  "id": "doc_review",                 // 必填
  "version": 1,
  "plugins": ["wfengine.plugins.docs"],   // 加载时 import，用于注册节点类型/函数
  "on_failure": "fail_fast",          // fail_fast（默认）| continue
  "nodes": [
    {
      "id": "extract",                // 字母开头，字母数字 _ -
      "type": "llm",                  // 注册表中的节点类型
      "depends_on": [],               // 依赖的节点 id
      "inputs": {"text": "$input.doc.text"},   // 传给节点的输入，可含引用
      "params": {"task": "extract_points"},    // 节点类型自己的参数，也可含引用
      "when": {"ref": "$nodes.gate.output.result"},  // 可选，条件为假则 SKIPPED
      "join": "all_success",          // all_success | none_failed | any_success | all_done
      "retry": {"max_attempts": 3, "backoff_seconds": 0.1, "backoff_multiplier": 2},
      "timeout_seconds": 10,          // 每次尝试的超时
      "idempotent": true              // 崩溃后能否自动重跑（llm/condition/notify 默认 true，其余默认 false）
    }
  ]
}
```

引用：`$input`、`$input.a.b`、`$nodes.<id>.output`、`$nodes.<id>.output.x.y`、`$nodes.<id>.status`。整个字符串是引用时替换为对应 JSON 值；`$$` 开头表示字面量 `$`。引用的节点必须是当前节点的（间接）依赖，否则加载时报错。未成功（如 SKIPPED）的节点，其 output 引用解析为 `null`，便于汇合节点引用可选分支；成功节点的输出中缺少的字段则是不可重试错误。

条件表达式（`when` 和 condition 节点共用）：`{"ref": x}`、`{"eq"|"ne"|"gt"|"gte"|"lt"|"lte": [a, b]}`、`{"in": [item, list]}`、`{"contains": [container, item]}`、`{"not": c}`、`{"and": [...]}`、`{"or": [...]}`、`true/false`。

加载时校验：JSON 合法、id 唯一、节点类型已注册、函数已注册、依赖存在、无环（Kahn 拓扑排序，报出环上的节点）、引用格式合法且指向祖先节点、retry/timeout/join/when 合法、节点类型自己的 `validate()`。所有错误一次性列出。

## 内置节点类型

| 类型 | 说明 | 输出 |
|---|---|---|
| function | `params.function` 为 `@register_function` 注册的名字，调用 `fn(inputs, ctx)` | 函数返回的 dict |
| llm | `params.task` 调用模拟模型，inputs 和其余 params 作为请求体 | 模型返回 |
| condition | `params.expr`（布尔）或 `params.switch` + `cases` + `default`（多路） | `{"result": bool}` 或 `{"branch": name}` |
| human_approval | 渲染审批内容并暂停；`params.title`、`params.fields` | 批准后 `{"approved", "approved_by", "content_hash", "comment"}` |
| notify（插件） | 写入 `outbox/notifications.jsonl` 或 stdout | `{"channel", "message", "delivered"}` |

分支用 condition 节点 + 下游的 `when` 表达：例如 `{"eq": ["$nodes.route.output.branch", "orders"]}`。

## 状态与语义

节点状态：`PENDING`、`RUNNING`、`SUCCEEDED`、`FAILED`、`SKIPPED`、`WAITING_APPROVAL`、`CANCELLED`、`REJECTED`。
运行状态：`RUNNING`、`WAITING_APPROVAL`、`SUCCEEDED`、`FAILED`、`CANCELLED`、`REJECTED`。

终态节点 = SUCCEEDED/FAILED/SKIPPED/CANCELLED/REJECTED。一个节点在所有依赖都是终态时才判断是否执行：

1. 汇合策略 `join`：
   - `all_success`（默认）：任一依赖不是 SUCCEEDED -> 本节点 SKIPPED（跳过会沿链传播）。
   - `none_failed`：任一依赖 FAILED/CANCELLED/REJECTED -> SKIPPED；全部依赖 SKIPPED -> SKIPPED；否则执行。适合"条件分支后的汇合"：流程一的 `report` 依赖 `summarize` 和 `approve`，approve 被跳过时 report 照常执行。
   - `any_success`：至少一个依赖 SUCCEEDED 就执行。流程二的 `merge` 用它汇合 orders/users 两个互斥分支。
   - `all_done`：依赖全部终态就执行（清理/通知类节点）。
2. 然后计算 `when`，为假 -> SKIPPED（原因"branch not selected"）。
3. 所以未选中的分支总会落到终态 SKIPPED，汇合节点不会永久等待（测试 `JoinTest` 覆盖）。

并行：所有就绪节点提交到线程池（`max_workers`）。流程一的 extract 和 check 并行执行（测试用延迟验证执行区间重叠）。

失败与重试：处理函数抛出任意异常即为本次尝试失败，按 `retry.max_attempts` 有限重试，退避 `backoff_seconds * multiplier^(n-1)`；抛 `NonRetryableError`（如字段校验类错误、引用错误）立即失败不重试；超过 `timeout_seconds` 记为 `AttemptTimeout`，可重试。每次尝试都写入 `attempts` 表（序号、状态、错误、时间），节点记录 `attempts` 计数、`error` 和 `reason`（如 `failed after 3 attempt(s): MockLLMError: ...`）。

并行部分失败：
- `on_failure: "fail_fast"`（默认）：某节点重试耗尽后失败 -> run 立即 FAILED；所有 PENDING 节点 CANCELLED；正在运行的兄弟节点收到取消信号，协作式结束为 CANCELLED；已成功节点的结果保留并可查询。
- `on_failure: "continue"`：其他独立分支继续执行；依赖失败节点的下游按 join 规则 SKIPPED（`all_done` 节点仍会执行）；最终 run 为 FAILED，reason 列出失败节点。

取消：`cancel` 用条件更新把 run 置为 CANCELLED（只对 RUNNING/WAITING_APPROVAL 生效，重复取消无副作用），PENDING/WAITING_APPROVAL 节点 CANCELLED，待审批请求 CANCELLED。RUNNING 节点是协作式取消：`ctx.check_cancelled()` / `ctx.sleep()` / 模拟模型的延迟循环会检查内存中的取消事件，并每 0.2 秒查询一次数据库，所以从另一个进程执行 `cancel` 也能生效。限制：Python 线程不能被强制终止，不检查取消的处理函数会运行到结束；如果它成功了，节点如实记为 SUCCEEDED（事件 `node_succeeded_after_cancel`），但下游不会再调度。

拒绝：`reject` 后审批节点 REJECTED，run REJECTED（reason 记录拒绝人和备注），剩余 PENDING 节点 CANCELLED，报告不写入。拒绝同样需要正确的内容哈希；重复相同拒绝是幂等的；拒绝后再批准会被拒绝。

## 审批与内容哈希

1. human_approval 节点运行时把"审批人看到的内容"确定性地渲染成文本（标题 + 按 `fields` 顺序的字段，JSON 键排序），计算 `sha256(内容)`，在一个事务里写入 `approvals`（PENDING）并把节点置为 WAITING_APPROVAL。`status` 显示的正是这段文本和哈希。
2. `approve/reject` 必须带 `--hash`。判断顺序：
   - 最新请求已是同样的决定且哈希相同 -> 幂等返回 `changed=False`，不启动任何东西；
   - 哈希与待审批请求不同 -> `StaleApprovalError`（exit 2）；
   - 按当前数据库状态重新渲染内容。若与请求时不同（上游输出被修改、输入被改等），旧请求 SUPERSEDED，按新内容生成新请求和新哈希，本次批准被拒绝，需要重新审批；
   - 通过后执行 `UPDATE approvals SET status='APPROVED' ... WHERE approval_id=? AND status='PENDING' AND content_hash=?`，只有 rowcount=1 的调用者会继续；并发失败者重新读取后幂等返回。
3. `rerun --from X [--input new.json]` 会重置 X 的下游（给了新输入时，还会重置所有引用 `$input` 的节点及其下游），待审批请求 SUPERSEDED，审批节点重新进入等待并生成新哈希，旧哈希失效。

重复继续不重复启动：节点只能通过 `UPDATE node_states SET status='RUNNING' ... WHERE status='PENDING'` 被领取，rowcount=0 的调用者不会执行；同一引擎内每个 run 只有一个调度循环（并发调用只置一个"再调度一次"的标志）。测试中 10 个线程跨两个引擎实例同时 approve，结果是只有 1 个 `changed=True`，下游 report 只有 1 次尝试，outbox 只有 1 条通知。

## 持久化与重启恢复

SQLite 表：`runs`（含本次运行使用的配置快照和输入）、`node_states`、`attempts`、`approvals`、`events`。WAL 模式 + busy_timeout，每次操作独立连接，线程和进程都安全。

恢复程度：
- 等待审批（完全恢复）：状态全部在数据库里。新进程 `status/approve/reject/resume` 即可，不重跑上游（测试 `test_restart_while_waiting_*`，CLI 测试每步都是独立进程）。插件按配置快照重新 import。
- RUNNING 中途崩溃的节点（`resume` 时处理）：节点记录了 owner（`pid:engine_id`）。owner 进程仍存活 -> 不动（`running_elsewhere`）。owner 已死：
  - `idempotent: true`：本次尝试记为 INTERRUPTED，节点回到 PENDING 自动重跑（尝试次数累计）。
  - 否则：节点 FAILED，reason 写明 `interrupted ... not idempotent -> manual decision`，run 按 fail_fast 置 FAILED。不自动重跑，以免重复副作用。操作员核实后用 `rerun <run_id> --from <node>` 继续，或保持失败。
  - 测试 `test_kill9_process_then_resume` 真实 SIGKILL 一个子进程验证。
- 崩溃发生在两个节点之间（run RUNNING 但没有 RUNNING 节点）：`resume` 直接继续调度。
- 不做自动后台恢复：重启后需显式执行 `resume <run_id>` 或 `resume --all`。

业务插件应保持幂等：`write_report` / `write_json_output` 写固定路径（先写临时文件再原子 rename），notify 按 `run_id:node_id` 去重。

## 模拟模型规则（wfengine/mock_llm.py）

- `extract_points`：按 `。！？!?` 和换行切句，保留长度 >= 4 的句子，取前 `max_points`（默认 3）句，每句截断 60 字。
- `sensitive_check`：关键词（身份证、密码/password、银行卡、机密/confidential/内部绝密、薪资/salary）+ 正则（手机号 `1[3-9]\d{9}`、18 位身份证号）。返回 `sensitive` 和 `findings`，正则命中做掩码（`138******78`）。
- `summarize`：前两条要点用"；"连接。
- 确定性：相同输入必得相同输出（审批哈希依赖这一点）。
- 失败注入：`llm.fail_first_n: {"task": N}` 前 N 次调用抛出可重试错误；`llm.always_fail: ["task"]` 每次都失败；文本中包含 `[[MOCK_FAIL:<task>]]` 时该任务失败；`llm.latency_seconds` 模拟延迟（期间检查取消）。
- 部署方式：默认进程内调用；`python3 -m wfengine mock-llm` 启动 HTTP 版（`POST /v1/tasks/<task>`，`GET /health`），配置 `llm.mode="http"` 即可切换。

## 新增节点类型（notify 示例）

节点接口（`wfengine/registry.py`）：

```python
from wfengine.registry import NodeType, register_node

@register_node("notify")
class NotifyNode(NodeType):
    default_idempotent = True
    def validate(self, node_cfg):      # 加载时调用，返回错误字符串列表
        return [] if "message" in node_cfg.get("params", {}) else ["params.message is required"]
    def run(self, ctx):                # ctx: NodeContext
        ...
        return {"delivered": True}     # 输出 -> $nodes.<id>.output
```

`ctx` 提供：`run_id`、`node_id`、`inputs`、`params`（已解析引用）、`run_input`、`attempt`、`services`（llm、report_dir、output_dir、outbox_dir）、`check_cancelled()`、`sleep(s)`、`eval(cond)`、`require_approval(content)`、`log(kind, detail)`。抛异常表示本次尝试失败；抛 `NonRetryableError` 表示不再重试。

使用：把模块加入工作流的 `"plugins": ["wfengine.plugins.notify"]`，节点写 `"type": "notify"`。完整实现见 `wfengine/plugins/notify.py`，执行器核心不需要改动（测试 `test_notify_lives_outside_engine_core` 会检查核心文件中不出现业务名称；`test_register_new_node_type_without_engine_change` 在测试中临时注册一个新类型并运行）。

普通业务函数更简单：

```python
from wfengine.registry import register_function

@register_function("transform_orders")
def transform_orders(inputs, ctx):
    return {"items": [...]}
```

## 流程二如何接入（不改执行器核心）

流程二只新增了两个文件：`workflows/record_transform.json`（配置）和 `wfengine/plugins/records.py`（用 `@register_function` 注册的 5 个业务函数）。分支用内置 condition 节点的 `switch`，汇合用 `join: "any_success"`，无效记录告警复用 notify 节点。整个流程不调用模型（测试 `test_flow2_uses_no_model` 验证模型调用计数为 0）。插件放在 `wfengine/plugins/` 下只是为了方便 import，放在任何可 import 的包里都可以。

## 已知限制

- 只支持无环流程（v1），不支持循环、子流程、动态并行（map）。
- 单机调度；多个进程可以安全地同时操作同一个数据库（条件更新保证不重复领取），但没有分布式调度、租约续期或高可用。
- 不能强制终止正在运行的 Python 线程：超时的尝试会被放弃（结果丢弃）但线程仍在后台运行；取消依赖处理函数配合检查。
- 崩溃检测基于 owner 的 pid 是否存活；pid 被系统复用时可能误判为"仍在运行"（此时需人工处理）。
- 重启后不会自动恢复，需执行 `resume`。
- `run` 命令在前台执行到结束或等待审批为止；没有后台守护进程或 HTTP API。
- 审批没有身份认证，`--user` 只是记录。生产使用需要接入认证和权限。
- 报告包含原文要点（可能含敏感信息），由审批环节把关；未实现脱敏改写。
- 模拟模型是规则匹配，不代表真实模型质量；没有接入真实模型服务。

## 复用说明

- 只使用 Python 3.9 标准库：`sqlite3`、`concurrent.futures`、`threading`、`json`、`hashlib`、`argparse`、`http.server`、`urllib`、`unittest`、`subprocess`、`decimal`、`re`。
- 没有复制或引入任何第三方库或开源项目代码。全部代码由本人主导、AI Agent 辅助在本仓库中编写，过程见 `docs/AGENT_COLLABORATION.md`。

## 提交信息

- 所选方向：B 通用工作流执行器
- GitHub 仓库链接：`<待填写>`
- 最终 commit SHA：`<待填写，git rev-parse HEAD>`
- 启动命令：`python3 -m wfengine run workflows/doc_review.json --input demo/doc_sensitive.json`（其余见"快速开始"）
- 测试命令：`python3 -m unittest discover -s tests -v`
- 演示步骤：`bash demo/run_demo.sh`，或上文"演示步骤"
- 已完成内容：配置驱动执行器（函数/模型/条件/人工确认节点）、引用与依赖、条件分支与汇合、并行、有限重试与超时、取消、并行部分失败策略、SQLite 持久化与重启恢复、审批内容哈希与重新审批、幂等审批与节点领取、两条业务流程、notify 扩展节点、CLI、模拟模型（含 HTTP 版和失败注入）、49 个自动测试
- 未完成项和限制：见"已知限制"
- Agent 协作记录：`docs/AGENT_COLLABORATION.md`
