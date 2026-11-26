# 协议

支持 CONTROL.md 中 COMMAND / REVIEW / ACK / RESULT 的精确首行和单行字段。PR #3 的 `created` 评论事件才进入解析。ACK 校验状态和 reason 后只作为回执识别，不变更任务；普通评论忽略。支持 LF 和 CRLF。RESULT 要求与任务目标角色匹配，兼容旧协议中 `id` 指代命令的做法。REVIEW 使用独立 `id` 及 `ref`。

原型的可自动调度命令使用以下字段。旧版非结构化任务或未配置的 scope 会进入 WAITING_AUTHOR。

```text
AI-INTEGRATION-COMMAND
id: CMD-20261126-001
from: ChatGPT
target: editor
scope: coordination
task: summarize_status
```

内部消息类型为 COMMAND、ACK、RESULT、REVIEW、BLOCKED、WAITING_AUTHOR。每条消息包含 `id, kind, source, target, scope, status, task_id`。允许的 COMMAND 附带枚举 `action`。审核消息保留原任务 ID。原型只生成发件箱记录，旧协议回执的实际格式化与投递待传输适配器实现。

状态迁移为 ACCEPTED → DONE 或 BLOCKED，DONE → REVIEWED。WAITING_AUTHOR 保持暂停，由作者经独立入口处理。重复评论 ID、命令 ID 或审核 ID 不重发回执。回执使用稳定的消息 ID，消费者应按该键去重。

合成回归样例来自 CONTROL.md。实际评论原文仍待已有连接器提供。遗留 Integration 回执默认只能更新分配给 Integration 的任务。若真实记录显示代理其他角色回执，应先批准并实现显式委托绑定。
