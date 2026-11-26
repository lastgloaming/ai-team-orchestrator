# P0 可行性报告

## 查证记录

- 通过平台 HTTPS Git 代理读取 `lastgloaming/ai-writer` 的 `refs/pull/3/head`，SHA 为 `2f0b00de20dac36a6b4f3d1e3eca47c63346ee60`。入口为 `docs/integration/CONTROL.md`。该 PR 永久 Draft，禁止合并。命令和审核仅接受精确首行，ACK/RESULT 是回执。兼容范围限于文档中的格式。实际评论读取因 GitHub API 代理返回 403、现有 CLI 凭证不可用而待补证。
- GitHub 官方文档仓库 SHA `8b038c5516187e33b1f81a905986639fa44e9ea9` 的 Actions 事件参考说明：`issue_comment` 接收本仓库 Issue 和 PR 评论，监听工作流必须在默认分支。应过滤 `created`、PR 标记及编号。独立仓库上的监听无法直接订阅 AI Writer 的 PR #3。
- GitHub 的 `GITHUB_TOKEN` 通常阻止评论引发新工作流。仓库 dispatch 是显式例外，但跨仓库授权须独立验证。GitHub App 安装令牌产生的事件与上述递归限制不同，具体 Bot 是否唤醒需要实测。当前部署候选是订阅 AI Writer 的 PR 评论的 GitHub App，经验证签名和事件来源后向独立仓库发出 `repository_dispatch`。需要作者批准 App 安装、最小权限，以及桥接服务。替代方案是经批准在源仓库默认分支部署中继。轮询只适合降级，无法即时交付。
- 挂载的仓库仅有 AI Writer。目标 URL 的 Git 探测未返回 HEAD，无法确认可写远端及可用于 PR 的基线。未创建远端或写入源仓库。
- DeepSeek 官方价格文档及 API 域名均被代理拒绝（403）。当前模型名、价格、上下文上限和 JSON 模式支持必须在 P4 前重新核对。Mock 模型名及费率仅为测试数据。

## 外部唤醒证据

| 执行端 | 当前证据 | 待作者批准后的独立检查 |
| --- | --- | --- |
| Integration | CONTROL.md 声明 PR 评论可唤醒、普通 Issue 评论不可用，实际接收未验证 | 使用唯一 ID 的无副作用请求，核对 ACK 和来源 |
| Memory | 被列为路由目标，未发现直接外部触发证据 | 确认 Integration 转发接口并取得可归因 ACK |
| Editor | 被列为路由目标，未发现直接外部触发证据 | 同上，使用单独 ID |
| Writer | 无已验证外部触发接口 | 核查独立入口、身份、作用域及回执 |
| Codex | 无已验证外部触发接口 | 核查独立入口、身份、作用域及回执 |

共享 `lastgloaming` 评论身份无法区分作者、ChatGPT 和 Integration。保留旧协议的字段解析，实际自动调度要求已认证的独立执行主体。共享账号输入需经独立身份重新提交。真实唤醒、跨仓库写入、GitHub Actions 部署及付费调用属于 P4，本轮均待授权。

参考：[GitHub Actions 事件](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows)、[触发工作流](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow)、[DeepSeek 模型和价格](https://api-docs.deepseek.com/quick_start/pricing)。
