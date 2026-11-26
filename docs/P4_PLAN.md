# P4 最小实测方案（待作者批准）

## 协议证据与身份

2026-11-26 重读 AI Writer PR #3 Git head `2f0b00de20dac36a6b4f3d1e3eca47c63346ee60` 的 `docs/integration/CONTROL.md`。Git 可读 PR head，不能从中取得评论。GitHub Comments API 返回 403，未登录网页返回 404。ORCH-REV-20261126-001 通过已有连接器提供了四类历史证据及 ACK/RESULT 准确样例。历史回执已纳入只读解析，现有唤醒和 Integration 委托仍未实测。

最低权限方案分两步：

1. **人工启动。** 在独立仓库受保护的测试入口由已授权操作者重新提交合成结构化命令。启用后由作者明确限定可按入口启动的操作者。共享 `lastgloaming` 可作为线索来源，不能当作作者、ChatGPT 或执行端的可归因身份，也不能仅凭同账号的另一次确认解除歧义。人工启动只代表已批准的测试作用域，不替代角色签名。
2. **可归因自动化。** 如果作者批准新增 GitHub App，源仓库的读取接收器仅订阅 `issue_comment`，用满足该事件的最低读取权限验证原始请求 HMAC、安装 ID、仓库 ID、PR 编号、`created`、delivery ID 与发送者。App 的签名只证明 GitHub 投递，角色仍由受信任且独立的主体映射。能使用独立 App 的执行端按稳定 Bot ID 绑定单一角色。共享账号的执行端若支持调用外部接口，可使用各自的短期凭据或独立签名密钥，签名覆盖任务 ID、动作、目标、时间窗口和 nonce，持久层防重放。无能力出示独立身份的执行端保持人工转交。若采用 `repository_dispatch`，该端点要求目标仓库 Contents: write，需仅限独立仓库的单独短时凭证，不给源仓库接收器该权限。优先使用认证的持久 inbox 和人工/定时消费者来避免新增跨仓库写权限。

默认事件驱动，使用独立持久 ingress 保存已认证事件后再唤醒消费者。没有可用的 App、可信 ingress 或已批准入口时保留人工转交。评论发送、身份认证和实际执行端响应分别验收。

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

ORCH-REV-20261126-001 提供了 2026-11-26 官方只读资料核对结果。本云环境依然无法直接读取 API 文档。按该记录，`deepseek-flash` 对应 DeepSeek-V4.1-Flash-1017。每百万 token 的美元峰值费率，输入命中 / 未命中 / 输出分别为 0.006 / 0.30 / 1.20，非峰为一半。中国区人民币峰值为 0.04 / 2 / 8。`deepseek-v4-pro` 的美元峰值费率为 0.044 / 1.32 / 3.96。实际账号模型可用性及账单尚未验证。 [价格](https://api-docs.deepseek.com/quick_start/pricing/) · [请求参数](https://api-docs.deepseek.com/api/create-chat-completion/) · [JSON 模式](https://api-docs.deepseek.com/guides/json_mode/)

拟用 `POST https://api.deepseek.com/chat/completions`，指定 `deepseek-flash`、`thinking: {"type":"disabled"}`、`response_format: {"type":"json_object"}` 及 `max_tokens: 64`。并发 1、日/月各 1 次、零重试。按 1,024 输入 token 和 64 输出 token 的峰值估算为 0.000384 美元，输入上界必须在实测前独立证明。这只是样例估值，实际可审批上限还要核对供应商限制、计费与币种。

## 隔离 CI 的证据范围

现有 Mock CI 的 `isolated-prepare` 和 `isolated-restore` 使用两个独立 runner，使用同一次运行的一天期制品传递合成 SQLite 快照。覆盖认证、共享账号拒绝、篡改、领取后未发送、恢复去重、模拟执行端回执和已消耗的单次模型预算。测试凭据是公开合成值，模型使用注入 Mock，`ORCHESTRATOR_LIVE_APPROVED=0`。真实事件收件入口、状态分支争用与 CAS、跨运行持久化和外部执行端仍待验证。制品仅用于隔离 CI。

## 执行顺序与停止条件

1. 先使用现有连接器收齐评论样本、官方费率和执行端证明，记录并审查证据。
2. 获批后在独立隔离入口用合成数据验证签名、共享账号拒绝、事件触发、跨 runner 恢复、重复投递及紧急停止。验证运行日志和远程账本不含密钥与私人内容。
3. 若另获 PR #3 写入许可，发送一次最小 Integration 探针；核对回执后再逐一测试已确认入口。无入口即记为人工，不试探未知触发方式。
4. 仅在作者明确批准单次费用上限后启用真实模型请求。一次超时、计费不符、身份无法归因或状态持久化失败即停止并保留证据。

参考 [GitHub Actions 事件](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows)、[repository_dispatch 权限](https://docs.github.com/en/rest/repos/repos#create-a-repository-dispatch-event)、[Codex Action](https://github.com/openai/codex-action/tree/4aac6da4b686168e147798ca841cf64ff0f027b8)。

## P4 实现及授权步骤

- `p4-state` 只能由单独获批的 `initialize-state` 创建，根提交只包含 SQLite 台账。正常运行要求作者提供最后确认的完整状态 SHA。远端缺失、初始 SHA 不匹配、并发路径非已确认版本的后继或台账损坏时停止。每次提交以读取版本为父提交，预推送检查核对远端公告的旧版本，Git 服务端再比较更新前的引用。冲突时重新下载台账并重放纯状态操作，最多四次。发送方须在提交成功后才确认事件已接收。同一 ID 与同一数据重复提交为幂等，不同数据被拒绝。调用方中断在提交前必须使用相同 ID 重试。状态分支历史应保留，不要删除或重置，否则已消费事件及一次性预算记录会丢失。
- 独立工作流 `P4 gated verification` 仅支持可信 `main` 上的手动触发，当前 Draft 分支上的代码不会开启该入口。默认关闭。须由作者批准开启状态变量 `ORCHESTRATOR_P4_STATE_APPROVED` 及使用已有的独立仓库状态写入凭证 `ORCHESTRATOR_STATE_TOKEN`。工作流自己的 GitHub token 保持只读。待审批并按下节初始化后，以 `enqueue-probe` 持久化一个合成事件，再从另一次运行触发 `prepare-probe` 取得相同 ID 的合成握手，重复入队验证去重。断在消费提交前的事件应在下一次运行可取。这个入口仅实现合成探针的入队，服务端自动收件及可信 webhook 中继仍待单独批准与实现。
- `deepseek-once` 另需 `ORCHESTRATOR_LIVE_APPROVED=1`、Actions Secret `DEEPSEEK_API_KEY`、作者批准的 `ORCHESTRATOR_P4_BUDGET_NANODOLLARS` 和已独立验证的 `ORCHESTRATOR_P4_INPUT_TOKEN_BOUND`。固定 Flash、关闭思考、输出最多 64 token、零重试。请求前先远程提交一次性标记和预留。请求、远端确认或账本结算不明确时不得清除标记重发。按 usage 和账单人工验收，保留日志中的结果状态和台账 SHA。
- `send-probe` 另需 `ORCHESTRATOR_P4_PROBE_APPROVED=1`、经批准的独立仓库测试 PR 编号 `ORCHESTRATOR_P4_TEST_PR` 和已有评论发送凭证 `ORCHESTRATOR_PROBE_TOKEN`。先确认 Integration 已注册该隔离入口且有独立可认证身份。发件前先提交一次性领取记录，发送至独立仓库测试 PR。若超时，通过任务 ID 查找评论及回执，禁止自动重发或改 ID 绕过。记录发送评论 ID、ACK/RESULT 评论 ID 与可验证的 GitHub 主体，入队时把测试 PR 和独立身份绑定入台账。发送时配置必须与绑定一致。分别从受信任入口核对两条回执的仓库、PR 编号和独立数字 ID。用 `verify_probe` 校验匹配绑定的两条来源和任务状态。共享账号、评论正文 from 字段及转发声明均不能替代认证。缺少独立身份、超时、只有转发或没有两条回执时，保持未验收。
- 本轮仅使用本地裸 Git 仓库和 Mock HTTP。真实状态分支创建、跨工作流测试、付费调用及外部 Bot 触发均须单独批准。


### 可信 main 启用清单（均待作者授权）

1. 确认经审核的工作流代码位于独立仓库受保护的默认分支 `main`，要求 PR 审核并限制直接写入。为 `p4-verification` 配置允许 `main` 的部署分支规则及可用的审批人限制。若当前 GitHub 套餐无法强制这些规则，先保持停用。工作流自身的 `GITHUB_TOKEN` 仅 `contents: read`，不保留 checkout 凭证。
2. 为 `p4-state` 配置禁止删除及强推的规则，无绕过。仅允许受权状态写入身份正常快进，不给它修改 `main` 或写入 AI Writer 的权限。状态凭证使用作者批准的既有、短时、仅限独立仓库的 Contents 写入身份，通过环境 Secret `ORCHESTRATOR_STATE_TOKEN` 提供。将该凭证与 DeepSeek 及评论凭证分开。探针凭证只需读取指定 PR 及发评论，放在该环境的 `ORCHESTRATOR_PROBE_TOKEN`。不要借助全仓库默认写权限或可写源仓库的令牌。
3. 在状态分支从未初始化且无可信 checkpoint 时，单独审批一次 bootstrap。在独立仓库临时设置 `ORCHESTRATOR_P4_STATE_APPROVED=1` 和 `ORCHESTRATOR_P4_BOOTSTRAP_APPROVED=1`，在 `main` 手动触发 `initialize-state`，checkpoint 输入留空。用独立读路径核对返回 SHA，保存到状态分支之外由作者控制的记录，然后关闭 bootstrap 变量。初始化存在冲突或确认不明时先核验远端和历史，禁止重新初始化。
4. 以后每次手动运行由操作者提供外部记录中的最新完整 SHA。按工作流日志与独立 Git 读路径核对新 SHA 后再更新外部记录。并发运算只接受从已知版本快进的后继。人工记录落后时，先审查完整后继链和运行记录，再更新 checkpoint；不要用当前远程 SHA 自动覆盖可信记录。否则无法识别回退到仍然可读的旧台账。状态丢失、回退或损坏后停止，保留证据，由作者核对最近已确认账本、预算与待确认副作用后决定恢复。
5. 探针另行审批。配置隔离测试 PR 的 `ORCHESTRATOR_P4_TEST_PR`、通过可信注册独立确认的 `ORCHESTRATOR_P4_INTEGRATION_ACTOR` 和明确排除的 `ORCHESTRATOR_P4_SHARED_ACTOR`。两条回执的元数据从已有认证的 GitHub 读取渠道取得。运行期间改配置不会改变已入队绑定。付费开关和 DeepSeek 环境 Secret 只在费用另行审批后配置。
