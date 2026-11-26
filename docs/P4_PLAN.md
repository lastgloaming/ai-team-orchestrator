# P4 最小实测方案（待作者批准）

## 协议证据与身份

2026-11-26 重读 AI Writer PR #3 Git head `2f0b00de20dac36a6b4f3d1e3eca47c63346ee60` 的 `docs/integration/CONTROL.md`。Git 可读 PR head，不能从中取得评论。GitHub Comments API 返回 403，未登录网页返回 404。等待已有连接器导出的四类脱敏原文与评论链接，再确认空行、字段顺序、多行、状态和 Integration 转述惯例。当前合成测试只证明与文档中的四种格式兼容，不把文档声明当作实测。

最低权限方案分两步：

1. **人工启动。** 在独立仓库受保护的测试入口由已授权操作者重新提交合成结构化命令。启用后由作者明确限定可按入口启动的操作者。共享 `lastgloaming` 可作为线索来源，不能当作作者、ChatGPT 或执行端的可归因身份，也不能仅凭同账号的另一次确认解除歧义。人工启动只代表已批准的测试作用域，不替代角色签名。
2. **可归因自动化。** 如果作者批准新增 GitHub App，源仓库的读取接收器仅订阅 `issue_comment`，用满足该事件的最低读取权限验证原始请求 HMAC、安装 ID、仓库 ID、PR 编号、`created`、delivery ID 与发送者。App 的签名只证明 GitHub 投递，角色仍由受信任且独立的主体映射。能使用独立 App 的执行端按稳定 Bot ID 绑定单一角色。共享账号的执行端若支持调用外部接口，可使用各自的短期凭据或独立签名密钥，签名覆盖任务 ID、动作、目标、时间窗口和 nonce，持久层防重放。无能力出示独立身份的执行端保持人工转交。若采用 `repository_dispatch`，该端点要求目标仓库 Contents: write，需仅限独立仓库的单独短时凭证，不给源仓库接收器该权限。优先使用认证的持久 inbox 和人工/定时消费者来避免新增跨仓库写权限。

若正式源仓库不能安装 App 或改动默认分支，保留人工转交。作者可另行考虑独立仓库的定时只读轮询，经批准使用现有仓库读取凭证。轮询可能延迟，不能提供即时唤醒。评论发送、身份认证和实际执行端响应分别验收。

## 执行端入口

| 执行端 | 现有入口与限制 | 获批后的最小验证 |
| --- | --- | --- |
| Integration | CONTROL.md 声明 PR #3 对话评论可唤醒，普通 Issue 不可用。共享账号限制身份归因，`GITHUB_TOKEN` 自触发限制与第三方 Bot 的实际订阅需分别验证 | 优先在隔离镜像注册同等路由。仅在作者单独许可且隔离入口不可用时，向 PR #3 发单条合成只读探针；核对 ACK、时间、请求 ID 和独立主体证明 |
| Memory | CONTROL.md 列为 Integration 路由目标。未查到直接外调入口 | Integration 通过后发独立 ID 的合成知识索引查询，检查 Memory 可归因 ACK；路由结果与直接唤醒分别记录 |
| Editor | CONTROL.md 列为 Integration 路由目标。未查到直接外调入口 | 用另一独立 ID 校验合成天气设定，核对 Editor 可归因 ACK；无独立信号时只记转发，保持人工 |
| Writer | AI Writer 开发文档描述人工开发和本地模型工作流。没有已验证远程唤醒 API | 先由维护者确认可用入口，再在批准的隔离副本发送只读状态探针。当前可用替代是人工转交，已有 GitHub CI 仅能证明测试任务可运行 |
| Codex | 官方 `openai/codex-action` 可在 GitHub-hosted Actions 中调用新的 Codex 执行，需受支持的模型凭证及单独的费用审批。对现有 Codex 会话未核验外部唤醒接口 | 先用 Mock 验证事件和结果回执，若批准 Codex 模型费用，再以只读档运行一条合成检查，核对 run ID 与任务 ID。现有独立审核仍可人工转交 |

## 跨 runner 状态

优先评估独立私有仓库的专用状态分支，沿用 GitHub 托管额度，无需新增服务。指定仅在批准后的可信工作流中由状态写入凭证维护，来源代码和状态分别检出。状态分支不执行代码且不加入任何部署触发。状态包括合成任务、去重键、发件箱租约、预算预留和 token 用量。使用 SQLite backup API 生成一致快照，附 schema 版本和校验值，正常快进推送实现基于读取版本的比较交换。冲突或远程确认丢失时先重新读取并比较操作 ID，再重新计算纯状态变更；禁止覆盖更新。

写入顺序是 durable inbox → 原子消费与入发件箱 → 持久化租约或费用预留 → 调用外部系统 → 通过租约 token 保存投递确认或按 reservation ID 结算。同步成功前禁止发生外部副作用。清理崩溃进程前，过期发件箱可凭稳定键重试，前提是接收端承诺幂等。普通 PR 评论接口没有已核实的幂等键，因此结果不明时先按唯一标记找回已发评论；查无结果且有歧义时交给操作者。模型请求一旦可能发送就保留预留，确认 worker 停止后才释放并发名额。费用结算冲突只重试状态写入。同一次费用预留不得用于重发模型请求。

Actions concurrency 是优化。新任务可能替换排队运行，需用持久 inbox 或经批准的定时补扫找回未消费事件。Cache 和制品适合加速与备份，不能承担唯一台账。若分支状态出现存储膨胀、争用或配额问题，再比较作者现有的事务数据库或免费托管事务存储。只有作者批准后才新增服务。验证两台独立 runner、冲突写入、进程被终止、备份校验失败和账本恢复失败；状态读取或写入失败都应停止外部调用。

## DeepSeek 与最小费用

官方 [模型和价格](https://api-docs.deepseek.com/quick_start/pricing) 和 [Chat Completions](https://api-docs.deepseek.com/api/create-chat-completion) 在当前云环境返回 403，平台页面也不可达。可读的 [DeepSeek-V3.2 官方模型仓库](https://github.com/deepseek-ai/DeepSeek-V3.2/tree/87e509a2e5a100d221c97df52c6e8be7835f0057) 能证明该开源发布，无法确认 2026-11-26 商用 API 的模型别名、价格或可用性。原型使用 OpenAI 兼容请求形状 `POST /chat/completions`，含 `response_format: {"type":"json_object"}` 和 `max_tokens`，均须按当期目标模型文档核验。P4 前从官方价格表取得日期、币种、缓存命中/未命中输入费率、输出费率、思考 token 计费与支持参数；从已有授权的读取渠道核对可用模型，并选可关闭思考且支持 JSON 的最低成本模型。暂不设真实模型默认值，也不填写推测费率。

最小付费验证为一次合成状态建议、并发 1、日/月各 1 次、零重试。拟测试输出上限为 64 tokens，须先确认供应商参数限制和思考模式。保守预留上限为 `(核实的输入 token 上界 × 缓存未命中单价 + 64 × 输出单价) / 1,000,000`，单价以每百万 token 报价，实际结果按返回 usage 和供应商账单核对。若存在最低计费、思考开销或税费，须一并计入。美元数值等待官方费率与参数核实，在此之前无法定出可审批金额。现有 Actions 私库额度和 Codex 费用单独核实。

## 执行顺序与停止条件

1. 先使用现有连接器收齐评论样本、官方费率和执行端证明，记录并审查证据。
2. 获批后在独立隔离入口用合成数据验证签名、共享账号拒绝、事件触发、跨 runner 恢复、重复投递及紧急停止。验证运行日志和远程账本不含密钥与私人内容。
3. 若另获 PR #3 写入许可，发送一次最小 Integration 探针；核对回执后再逐一测试已确认入口。无入口即记为人工，不试探未知触发方式。
4. 仅在作者明确批准单次费用上限后启用真实模型请求。一次超时、计费不符、身份无法归因或状态持久化失败即停止并保留证据。

参考 [GitHub Actions 事件](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows)、[repository_dispatch 权限](https://docs.github.com/en/rest/repos/repos#create-a-repository-dispatch-event)、[Codex Action](https://github.com/openai/codex-action/tree/4aac6da4b686168e147798ca841cf64ff0f027b8)。
