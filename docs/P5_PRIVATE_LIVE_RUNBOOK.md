# P5 私有单次调度验证手册

本 PR 只授权开发和 Mock。部署、凭证配置、私有初始化、真实费用和触发分别由作者批准。现有 P4 单次用量、公开通信与 P5 Mock 台账完全独立。

## 部署参数

| 配置 | 用途 |
| --- | --- |
| 受保护环境 `p5-private-live` | 仅可信 main，可用的人工审批人和防自批规则必须先验证。源工作流自己的 GitHub token 只有 contents: read |
| `ORCHESTRATOR_P5_LEDGER_REPO`、`ORCHESTRATOR_P5_LEDGER_ID` | 同一所有者下专用于真实计费的私有非 fork 仓库。API 核验可见性、数字 ID 和名字。保持私有并限制改可见性 |
| `ORCHESTRATOR_P5_LEDGER_TOKEN` | 未来仅在环境内配置已有、短时、限制到该私有仓库的凭证。需要元数据读取和两条 Git 引用的写入。只注入 Git 网络命令，HTTP header 限定目标 URL，禁止重定向 |
| `ORCHESTRATOR_P5_STATE_ANCHOR`、`ORCHESTRATOR_P5_WITNESS_ANCHOR` | 已验证的私有 genesis SHA，固定在受保护配置，不推进，不采用公开或当前 tip |
| `ORCHESTRATOR_P5_BOOTSTRAP_APPROVED` | 默认关闭，仅用于另行批准的一次 bootstrap |
| `ORCHESTRATOR_P5_SCHEDULING_ENABLED`、`ORCHESTRATOR_P5_LIVE_APPROVED` | 两个独立开关，默认均关闭，维护仓库级作业条件和运行时检查 |
| `ORCHESTRATOR_P5_SOURCE_REPO_ID`、`ORCHESTRATOR_P5_TEST_PR` | 公开源仓库数字 ID 和独立隔离 PR。原生事件及 REST 再验证均用同一绑定。固定作者数字 ID 338499555 |
| `ORCHESTRATOR_P5_APPROVAL_ID`、`ORCHESTRATOR_P5_APPROVED_COMMENT` | 环境级的非秘密 32 位小写十六进制审批 ID 和作者该条固定合成命令评论的数字 ID。在受保护环境放行作业前核对 |
| `ORCHESTRATOR_P5_BUDGET_NANOYUAN`、`ORCHESTRATOR_P5_INPUT_BOUND` | 另批的整数预算和独立核实的输入上界。峰值 2,000/8,000 纳人民币每 token，上限 2.20 元、输出 64、并发 1、零重试 |
| `DEEPSEEK_API_KEY` | 仅未来配置在私有验证环境中。bootstrap 阶段不注入 |
| `ORCHESTRATOR_MODEL_STOP`／`ORCHESTRATOR_STOP` | 设为 1 阻止新模型请求。对已开始的作业，应同时取消作业并核对状态 |

## 后续实测顺序

1. 先验证专用私有仓库还未初始化，且两条目标引用均配置禁止强推、禁止删除、写入者无绕过。一个 GitHub 仓库级令牌并不天然绑定分支，必须由保护规则隔离。若仓库权限不满足，停用。双引用同属同一信任域，成对绕过回退不可仅靠 genesis 检测。
2. 经作者另批后，从本工作流的受信 main 手动触发 bootstrap，传入已核对主分支完整 SHA。诚实核验两个调度开关都关闭、私有锚点尚未填写。Git 只会尝试一次创建 `p5-live-charge-state` 和 `p5-live-charge-checkpoints`。通过已授权的私有读路径独立核对两条 genesis、环境名 `p5-private-live`、空台账和 sequence 0 后固定锚点，再关闭 bootstrap 开关。任何不确认结果都不得补推或重建。
3. 独立核验 CNY 实际计费、固定请求的全部可计费输入边界与确切价格。即使本地预留不超过 2.20 元，也需要供应商证据及独立费用授权。确保没有遗留审批和真实 Integration 订阅，再按审批启用原生事件入口。
4. 作者在指定隔离 PR 发一条精确的 `P5-SYNTHETIC-LIVE-ADVICE`。工作流等待环境审批。核对本次 run 的确切评论 ID，在环境里填入本次审批绑定，凭证存在且参数正确后，人工批准这个 run。首次云测先验证变量注入和停开关时的无副作用。如果环境等待期间无法可靠绑定参数或保护规则不可用，保持关闭。不要改为接受任意历史评论或任意重跑。
5. 作业以 REST 只读重查 PR 和原评论；先原子持久化授权、事件资格、一次性标记与预算，再在事务外发一次请求。单个私有台账只接受一个审批，改配置、重放或重启不会打开第二次机会。预算拒绝也按事件持久化。请求或账本结果不明确时保留标记和预留，禁止重试、清空历史或另建台账绕过。
6. 工作流 JSON 回执仅输出合成事件 ID、非秘密审批 ID、通信接收状态、模型状态和调度状态。其通信状态是接收核验，不代表 GitHub 发评论或 Bot 回复。不会公开用量、建议正文或凭证，不具备执行外部命令的能力。只有三个固定建议枚举可在私有台账中记录。验证人员通过私有路径核对账单与快照后关闭两个门禁。

离线验证命令为 `python3.12 -B -m unittest discover -s tests -v`。测试使用临时本地 Git 仓库、进程故障注入和 Mock HTTP。
