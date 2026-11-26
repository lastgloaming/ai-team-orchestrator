# AI Team Orchestrator

基于 GitHub 事件模型的云端协调器离线原型。协调规则、任务状态和用量台账均由 Python 和 SQLite 管理。DeepSeek 客户端只提供结构化建议。原型公开的开发接口为 `Orchestrator.process`、`pending`、`claim_next`、`mark_delivered` 和 `Advisor.suggest`。

```bash
python3.12 -B -m unittest discover -s tests -v
```

无需安装依赖、提供密钥或开启服务。GitHub Actions 配置仅运行离线 Mock。生产入口、跨仓库投递及生产持久化需要经批准后设计验收。

- [P0 可行性](docs/FEASIBILITY.md) 和 [架构](docs/ARCHITECTURE.md)
- [协议](docs/PROTOCOL.md)、[权限](docs/PERMISSIONS.md) 和 [安全](docs/SECURITY.md)
- [预算](docs/BUDGET.md)、[测试](docs/TEST_SPEC.md) 和 [变更记录](docs/CHANGELOG.md)
- [P4 认证、唤醒、跨 runner 状态及最小实测方案](docs/P4_PLAN.md)
