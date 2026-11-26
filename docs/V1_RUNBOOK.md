# Orchestrator v1.0 安装与运行

## 当前可执行范围

v1.0 安装包包含统一私有 SQLite 队列、GitHub PR 只读观察、确定性领取、证据存档、终审请求和受控恢复。现有 Codex 巡检可调用 CLI 读取任务，再使用已有授权执行。命令本身不启动 AI，不发 GitHub 评论，不操作合并。

| 路径 | 运行边界 |
| --- | --- |
| Codex | 从统一队列领取已获授权的审核、整改、开发和研究 |
| ChatGPT | 终审请求在审核结算的同一事务生成。现有每小时任务可在获准、能访问台账时取件。无可用连接器发布调用时为 BLOCKED_NO_CONNECTOR，不能视为送达 |
| Writer | 保留已有 GitHub 更新接收。v1 记录目标和授权，尚无经过核验的自动发布连接 |
| Integration / Memory | 沿用 Control PR #3 协议。Memory 经 Integration。只有批准的独占 Control 编号池才可领取编号；适配器保存发送水位和唯一评论关联，不盲目补发。更宽松的多行证据匹配只接受缩进续行，含糊的多段结果仍拒绝 |
| Editor / DeepSeek | Editor 不进入自动角色路由。P5 付费 API 和私有 bootstrap 硬阻断保留 |

目前没有可调用的既有 ChatGPT GitHub 发布能力，不能以 Actions token 代替。Integration 正在准备的接收端补丁未提供可核验的版本或接口证据，无法证明独占编号、分发身份及最终回执格式已匹配。真实发文和自动认领 Integration 回执继续 BLOCKED。不改 Control 订阅或权限。

## 安装

Python 3.12+，无应用运行时依赖。按审核后的 commit 在受控环境安装。所有本地配置、数据库和日志应位于权限受限且**不在 Git checkout 内**的持久目录。CLI 以掩码 0077 创建文件，仍须配置目录访问权限和保护已有文件。

```sh
python3.12 -m venv /path/to/private/orch-venv
/path/to/private/orch-venv/bin/python -m pip install .
/path/to/private/orch-venv/bin/orchestrator-v1 --help
```

只读 PR 观察需已有 gh 认证。可用既有受信 GitHub 连接器导出等价的结构化元数据。CI 的实际 run URL 和被测 commit 需独立核对后补入观察数据。缺证据不推广为已验证。公开仓库无真实配置。

## 最小配置

以下 JSON 是合成例子。默认 stop 为关闭执行，只有明确设置 false 才可领取。在私有配置中维护可信任务和独立作者授权。局部试验可先使用虚构座标。

```json
{
  "stop": true,
  "tasks": [
    {"id":"fixture-1","repository":"example/alpha","kind":"research","authorization":"approval-v1",
     "priority":20,"units":0,"scope":"fixture-research","dependencies":[]}
  ],
  "authorizations":{"fixture-1":"approval-v1"},
  "observations":{},
  "budget":{"window":"fixture-period","remaining":10,"reserve":2}
}
```

任务需稳定 ID，非 PR 任务以仓库、类型与稳定 scope 去重。PR 任务再加入 PR、完整 HEAD 和 base。改变授权不应把已审技术版本再执行一次。id 被重新绑定到不同意图时拒绝。提供不同版本的新工作需补新的可信任务记录；依赖用已登记任务 ID 表示，仅 DONE 解除。超过预留、额度未知或已触及预留时不领取有消耗工作，零单位仅可用于已确认无需新增资源的工作。额度 window 使用已核实的套餐周期，不创建任意窗口绕过预留。状态读取失败、授权不明或不可验证时保持停止。

## 每轮运行

以下命令共用 `--db /private/queue.sqlite --config /private/task-policy.json`。

1. `sync` 导入可信配置任务。 `observe` 输出新鲜 PR HEAD/base，读权限不可用时停止受影响任务。操作方把核对结果写入私有 observations，键为 repository#pr。字段包括 state、head、base、checked（Unix 秒）、tested_sha、run。领取和提交证据分别要求 300 秒内的匹配观察。
2. `claim` 原子领取一项并持久预留，或返回 NOOP。只处理输出的精确任务与授权范围。15 分钟内完成或停止安全交接。候选 15 分钟巡检由现有运行设置另行批准，不在本 PR 启用。
3. `complete --task ID --lease TOKEN --evidence /private/evidence.json`。证据字典字段为 conclusion（PASS／REQUEST_CHANGES／BLOCKED）、run、tested_sha、passed、failed、risk、decision。风险／决定为短代码。PR 审核结算要求 CI 链接及被测 SHA 与已核验观察完全一致。HEAD、基线或授权变更时标记 STALE。自写、无法确认独立性或重要审核进入 WAITING_FINAL。
4. 当轮读取 `final-requests`，并由另行授权的操作方检查独立 ChatGPT 接收能力。请求包含完整 HEAD/base、测试 SHA、CI、结论、风险和待决事项。该命令只读导出待办，既不发文也不写 GitHub 审核。当前无连接器执行路径时保持 BLOCKED。通过已有独立渠道完成终审后由作者决定合并；应用没有审批或合并命令。
5. `external-handoffs` 导出待核验的角色与任务，保持 BLOCKED。未来发布端只接收受信私有请求，核验授权、目标、稳定任务 ID、预算、Control 接收能力与真实凭证主体，在持久领取后才可发布一次；以真实评论 ID 对账。正文、login 或 App 来源均不能证明审批。当前导出不得用于向真实 Control PR 发探针。

## 停机与恢复

设 `ORCHESTRATOR_V1_STOP=1` 或配置 stop=true 阻止新领取。保留数据库及一致性备份，停止消费者后再恢复软件版本。不得回退或另建空台账以重复领取。过期领取进入 UNCONFIRMED，后续领取停止，需独立检查旧工作和外部副作用。凭经核对的 run 调用 `reconcile --task ID --run URL` 将旧领取记为 BLOCKED，允许其他任务继续。恢复不回放旧领取或退还预留。校验失败时保留原台账及日志，不自动重建。

验证命令为 `python3.12 -B -m unittest discover -s tests -v`。全程使用合成测试和临时数据库。
