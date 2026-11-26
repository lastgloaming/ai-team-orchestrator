# 权限

| 角色 | 自动准入 |
| --- | --- |
| 作者、ChatGPT | 向 Integration、Memory、Editor 发出协调状态汇总或阻塞查询。可向任一执行端查询状态 |
| 指定执行端 | 提交其任务的 DONE 或 BLOCKED |
| ChatGPT | 给已完成、目标和 scope 相符的任务记录技术审核 |
| DeepSeek | 返回受限的建议枚举 |
| 共享账号、未知主体 | 拒绝 |

`coordination` 的动作是 `summarize_status` 或 `list_blockers`。`status` 的动作是 `report_status`。审核动作是 `record_review`。正文 `from` 只用作一致性检查。采用可信 GitHub 来源的数字 ID 到角色映射，排除共享身份。

内容政策、核心架构、正式小说正文与设定、合并、冻结、扩权、正式知识库删除及超预算操作都不在动作集合中。只有作者可另行批准，原型没有执行这些动作的入口。
