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

## 云端运行桥与实际可用边界

运行组件为 `orchestrator-v1-bridge --db /private/queue.sqlite --config /private/task-policy.json`，绑定主机回环地址。将进程及私有持久卷放在作者已经允许、能随电脑关机继续工作的常驻云宿主。由该宿主现有受信 TLS 代理暴露给获准的消费者；没有该代理时仅允许同宿主进程访问。需要固定容量及受监管进程管理、连接超时、备份和重启策略。不要将此 HTTP 入口直接公开。当前 Codex Cloud 仅核实为任务工作区，未取得常驻主机、稳定私有卷、私有 TLS 或入站连接许可。GitHub hosted runner 随作业退出，公开状态分支不能承载此私有台账。**实际云端托管和外部消费者连通性仍为 BLOCKED**。本 PR 不启动云端进程、不改变巡检。若作者没有符合条件的既有主机，保持关闭，不引入付费资源。

桥接凭证通过环境变量 ORCHESTRATOR_V1_BRIDGE_TOKENS 注入 JSON，四个角色 observer、codex、chatgpt、operator 使用不同的随机 bearer。仅可信宿主管理者可编辑配置和绑定凭证。共享账号评论、GitHub login、正文和未验证的 App ID 不能建立角色绑定。现有 ChatGPT/Codex 自动任务是否能够调用私有桥仍需另批连通验证。终审证据独立读取后写入可信配置；只给出 PR 链接并不构成证明。

### V1 事件与审批闭环

- POST /v1/event（observer）接收已审核配置中的 task_id。事件的认证与 PR/CI 元数据核验由受信上游完成。接口不从正文生成任务，不让请求覆盖授权、HEAD 或预算。配置应原子替换，由已验证的读路径及时刷新。
- POST /v1/next（codex）在同一持久台账按优先级、精确版本、有效授权和有效依赖领取。已有巡检可在下一次唤醒访问；生产变更需单独批准。POST /v1/result（codex）提交 task_id、lease、evidence，生成重要审核的终审任务。结算后消费者可请求下一次决策；未成功确认结算时先核对，不能重做工作。
- POST /v1/final/claim（chatgpt）仅由独立接收方使用。须配置 final_capacity_confirmed=true 证明现有套餐可覆盖。终审核对完整 HEAD、base、授权和已核验测试证据，并返回独立租约。POST /v1/final/result 提交 task_id、lease、head、base、authorization、outcome 和 GitHub 审核链接 evidence。可信上游另在 final_evidence[task_id] 记录匹配的 head、base、authorization、outcome、evidence、reviewer=chatgpt 后才可入账。相同决策去重。PASS 解开仍有效的技术依赖，合并仍为 AUTHOR_REQUIRED。旧 HEAD、改授权、缺独立证据无法解锁。
- 兼容同逻辑任务别名，依赖指向规范记录。新的可信授权可替换未领取任务、尚未被领取的终审以及有独立无副作用恢复证明的任务。完成的技术审核不因重复配置重跑。过期的普通和终审租约均握持到独立核对。POST /v1/reconcile 或 /v1/final/reconcile（operator）要求专属于该任务仓库的 run URL，以及 recovery 或 final_recovery 中精确对应 lease、run 且 no_active_or_external_effect=true 的可信证明。不能用另一仓库链接解锁，确认不明时不回放。
- 普通任务的 units=0 也至少预留一单位。预算 window、remaining、reserve 是已验证套餐的可信快照，不能从事件传入。单独的 ChatGPT 容量证明确认不了就不领终审。保留既有停用开关。DeepSeek Advisor 和 P5 私有调度接口保留，中央决策当前使用确定性规则，全部真实费用与部署硬阻断仍有效。

### Integration 与其它角色

POST /v1/handoffs 仅导出外部角色的 BLOCKED 请求。POST /v1/handoffs/observe（operator）仅核验并持久关联另行查证的 v1.0 传输证据：task_id、authorization、Control ID、目标仓库和 PR、publisher_app_id、publication 评论 ID 及严格在其后的 receipt 评论 ID。packet 必须等于可信配置中的 verified_handoffs[task_id]。完全相同的记录可回读，冲突拒绝。关联不等于已执行，不能解除依赖。

真实分发仍需接收端已审核补丁的确切版本，以及独占编号、发送 App 元数据、评论绑定、分段回执和持久对账合同。只读核查到的 Control PR #3 Git HEAD 仍为 2f0b00de20dac36a6b4f3d1e3eca47c63346ee60，未独立取得补丁并不能验证真实兼容。本 PR 继续沿用当前 v1.0 解析的保守边界，零匹配或多匹配时禁止补发。没有经过核验的 ChatGPT 发布能力，不利用 GITHUB_TOKEN 假装 ChatGPT App。Writer 现有 GitHub 更新路径不改，Memory 经 Integration，Editor 未接入。不得向公网台账写入真实任务、账单、凭证或正文。

**验证**：本地 HTTP 集成用例覆盖事件→可信策略决定→Codex 领取→结果→独立 ChatGPT 领取与回传→下一项可领取任务。它没有验证公网托管或真实连接器身份。已安装包的 CLI 验证为 bash scripts/check_v1_install.sh；已有 setuptools>=68 时该脚本在临时目录离线构建，在仓库之外运行已安装入口。
