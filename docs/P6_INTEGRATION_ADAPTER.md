# ORCH-P6-003 Integration 适配器

**真实发送及回执认证为 BLOCKED。** 默认关闭，显式启用也不能越过代码中的独立硬阻断。本 PR 不安装工作流、网络传输或凭证。P5 费用硬阻断保留。

## 协议证据

从 AI Writer Control PR #3 的只读 Git 快照 2f0b00de20dac36a6b4f3d1e3eca47c63346ee60 核对 CONTROL.md。Comments REST API 返回 Forbidden。先前连接器审查提供了标准 COMMAND、缺字段 REVIEW 和 legacy ACK／RESULT 样例。本次测试均为合成夹具，没有复制私有正文或评论证据值。Integration 报告的原生订阅和会话续接仍未独立实测。 [既有协议及历史索引](PROTOCOL.md)

发送沿用 v1.0 的 COMMAND／REVIEW，复用现有解析器读取标准或旧式 ACK／RESULT。编号沿用 CMD-YYYYMMDD-NNN。Mock 只从 Control 权威分配器预先独占授予的号码池领取，共享同一 SQLite 台账的进程原子消耗号码。无授权、池耗尽或仅有旧本地编号时停止。真实全局唯一性依赖接收方确认的独占号码池或唯一共享分配器，扫描评论不能防并发撞号，目前没有该生产契约。入队必须由可信上游确认显式作者委托。模板使用 from: author 表示作者委托的协议角色，使用该拼写前需与既有订阅核对；正文的身份声明不作为凭据。

## 状态、资源与权限

- 只接受可信上游提交的稳定事件 ID 与固定动作。允许 Integration 合成状态查询及经 Integration 转交的合成 Memory 索引核验。Editor 和其他未获授权动作进入 WAITING_AUTHOR。没有正文、核心架构、合并、冻结、增权、删库或增费执行入口。
- 已完成 COMMAND 可关联一次 REVIEW。重放同一上游事件返回原 ID，不同事件也不能为同一 COMMAND 重建 REVIEW。入站评论一律不进入任务创建接口。
- QUEUED → SENDING 在发送前持久化。可信传输返回的仓库、PR、作者、ID 和正文一并匹配才记 SENT。错误为 UNCONFIRMED，进程退出可能留下 SENDING。两种状态都禁止重发。认领时另存可信通道的发送前评论水位。仅允许通过完整可信观察找到水位之后唯一精确匹配的已发布评论来确认送达，旧精确匹配也不得认领。无匹配、多匹配继续阻塞，送达也不能证明执行。
- SENT 后记录认证 ACK 的 ACKED／REJECTED／BLOCKED，RESULT 可直接进入 DONE／BLOCKED，迟到 ACK 不改终态。去重使用评论 ID，关联检查任务 ID、通道、发送顺序和独立主体。多段、模糊字段或不支持的变体不推断完成，需人工核对；证据引用仅用仓库、PR 和评论 ID。回执原始正文和证据值不入库。
- 所有潜在唤醒都消耗保守的合成资源单位。余额未知、无效或预计达到预留线即阻塞。在同一台账累计计算已领取的尝试，终态不自动退回额度。实际额度、时间窗、价格和计费尚未验证，保持 BLOCKED。
- 发送凭证需证明现有权限实际代表 lastgloaming（数字 ID 338499555）且能评论。当前 Git 读取不能证明写入，REST 权限查询受阻，Actions 系统 Bot 不能假定符合白名单。当前接收方为共享账号，独立 Integration 身份未建立。共享账号回执不能证明 Integration 身份。任何共享账号受限操作须由作者单独接受，也不能授权重要合并。必须取得独立认证主体或经批准的等价来源证明。

- DUPLICATE ACK 进入 DUPLICATE_HOLD，不能证明当前意图已被接受。REVIEW／WAITING_AUTHOR／PARTIAL 为非终态。只接收水位以上且晚于最后已记录回执的状态更新。

## 部署参数和后续验收

固定目标为 lastgloaming/ai-writer 的永久 Draft PR #3。待另行批准配置可信上游身份、发送凭证、独立回执信任根、有效资源读数及私有跨运行事务存储。各 runner 需共享一个权威台账，不能各自编号或领取发送。公开仓库不保存真实台账、任务正文、账单或凭证。

1. 先只读核实订阅目标、作者格式、凭证身份和评论权限、独立回执身份及额度。无法验证则保持关闭。
2. 经单独批准后优先在等价隔离入口发送一条合成 COMMAND，记录上游事件 ID、任务 ID、评论 ID 与可归因 ACK／RESULT。真实 Control PR 需另行明确授权。确认不明时先核对已有评论，禁止补发。
3. 用另一条合成任务验收 Memory 转交与实际执行端回执，Editor 不参与。重复、乱序和崩溃先用 Mock 测。
4. GitHub 回执只表示评论到达。当前 ChatGPT 仍依赖有权限的连接器主动查询；无已验证的原生路径把回执实时送入当前会话。本 PR 不安装周期轮询。

回归命令：python3.12 -B -m unittest discover -s tests -v。所有适配器测试使用临时 SQLite、合成输入及 Mock 传输。
