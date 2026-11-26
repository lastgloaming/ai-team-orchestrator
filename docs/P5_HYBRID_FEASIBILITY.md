# ORCH-P5-003 混合见证可行性报告

**真实部署仍 BLOCKED。** 两个临时本地 Git 仓库与 Mock 传输证明了正常顺序及故障时最多一次调用的设计。PR #8 的工作流和运行时真实执行／bootstrap 硬阻断保持原样。本变更不配置公网 witness，不使用真实费用或模型数据。

## 已实现的隔离验证

1. 单个预先固定的 witness 锚点分配一个全局一次性槽。签名快照绑定原 P5 配置、witness 仓库标识及锚点、256 位随机承诺、指定事件和 GitHub run ID，只接受首次 attempt。公开存储只有一个名称固定的文件，其内容仅为随机承诺。提交身份和消息是固定合成文本。Git 的公开时间和更新次数仍可观察，须单独获作者认可。
2. 首先验证签名、固定请求、选定仓库、锚点及源事件。用 Git compare-and-swap 快进 witness 并独立确认。只有本进程确认其写入成功才获得继续机会；已存在承诺、输出 up-to-date、写入被拒绝或确认丢失均不授予机会。不会尝试重新消耗已有承诺。
3. 下一步在私有双引用上提交事件、一次性标记与预算。模型 Mock 请求发生在事务之外，发送前重新读取公开 witness 和私有预留落点。中断在跨仓库间隙会损失机会。私有回退不能恢复已消耗的公开槽。预算和模型原文都不会出现在 witness。
4. 同一 author 不允许重新签发新的 run、重置 witness 或重建私有 ledger 来恢复失败的授权。Mock 只接受绝对本地目录，不存在生产路由。测试中的两个 repo ID 是夹具，生产必须用可信只读渠道核验 GitHub 仓库数字 ID、可见性、配置和锚点。

## GitHub Free 前提与未解决项

- GitHub Free 的公开仓库具备候选规则能力。生产须在受保护公开引用上独立验证不允许删除、强推、分支替换，不授予写入者绕过；同时须验证写入身份、签名信任根、预先固定的唯一锚点和工作流运行身份。当前本地 Git 的 compare-and-swap 只证明并发语义，未证明真实 GitHub 规则能由现有权限保持。若管理员能回退公开引用，这一模型无法防回放。 [规则适用范围](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets)
- 私有 GitHub Free 台账仍可被授权写入者回退。公开槽解决同一授权最多一次发送的问题，**不会恢复被回退或删除的费用账本**，也不能替代长期的跨审批总预算。当前证明只覆盖单一不可复用槽，不支持轮换锚点或下一次真实审批。若要求私有账单历史持续完整，还需经批准的持久恢复方案和独立核对。回退、确认丢失和崩溃一律停止；不为恢复而提高权限、发布私有数据或加购服务。
- 故障后可能无法区分未提交与已提交但未确认，不自动重试。生产候选必须严格验证 GitHub run ID 和首次 attempt；未确认的 run 不重新签名、不换 run、不改锚点。实际 GitHub 触发、规则及审批路径都未验证。
- 未确认可计费输入的供应商上界、费用与真实批准。不得启用 PR #8 的付费工作流。单账号环境独立审批仍未解决，已签快照并不等同第二审批人。

## 原生无 Secrets Mock

`p5-hybrid-source-mock.yml` 默认关闭。未来仅在作者另批时，对隔离 PR 启用，作者提交固定 `P5-HYBRID-SOURCE-MOCK` 触发。只用 GitHub 自带的 `contents: read` token 读取公开 PR 和评论，无仓库或环境 Secrets、评论写入、远端台账或模型。该工作流包含两个独立的验证步骤：

- **原生事件 GET 探针**读取当前 GitHub 事件，再用只读 token 核对公开 PR 和原评论，输出来源摘要。当前仅通过本地 HTTP Mock 验证两次 GET 及其目标。真实 GitHub GET 尚未运行；若返回 403 或源不匹配，作业失败。
- **本地签名快照测试**使用独立合成夹具、临时签名密钥和两个本地 Git 仓库，验证签名封套跨进程传递、替换拒绝及故障路径。它不签署 GET 探针收到的真实原生事件。工作流没有把原生事件封套传给第二个作业，本轮未验证从真实 GitHub 事件到签名授权的端到端链路。

[事件参考](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows)

验收命令为 `python3.12 -B -m unittest discover -s tests -v`。本地 Mock 已验证签名替换、不同 run/attempt、同一授权重放、并发、公私各一次拒绝、确认丢失、进程退出和私有回退。测试不接触真实远端台账。
