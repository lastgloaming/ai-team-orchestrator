# 测试规格

`python3.12 -B -m unittest discover -s tests -v` 在 Codex Cloud 执行，19 项通过。覆盖隔离的 COMMAND → ACK → RESULT → REVIEW、BLOCKED、WAITING_AUTHOR、重开数据库、并发去重、伪造身份、非法事件、提示注入、模型输出越权、超时重试、坏账保留、并发、日月费用及次数和停止开关。

P4 待批准后再运行。先验证官方费率、模型及接口，使用隔离 GitHub 目标验证默认分支、不同凭证产生的评论、跨仓库投递和每类 Bot 的实际 ACK。验证持久化状态跨 runner 恢复、并发和重复通知。最终仅在获批金额内进行最小真实调用。初始 11 项 GitHub-hosted CI 结果由作者提供。本轮新增合成协议边界、并发租约隔离、过期 token、独立进程崩溃后的预算预留与恢复，以及精确金额边界。真实 API、真实通信和生产存储均待验证。
