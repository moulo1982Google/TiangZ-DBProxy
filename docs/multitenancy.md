# 多租户入口 v1（2026-09-16）

一个 DBProxy TCP 入口承载多个租户；握手令牌绑定服务端配置的独立后端，连接内不能切换。client_name 只用于日志，不参与授权。请求不自行指定 tenantId，多租户模式没有共享默认令牌后门。协议及 SDK 不变，游戏继续使用自己的凭据和普通 RecordKey。

## 使用

```powershell
cargo run -p tiangz-dbproxy-server -- --check-tenants configs/tenants.example.json
# 审查并准备独立存储及环境变量之后才显式运行：
cargo run -p tiangz-dbproxy-server -- --tenants configs/tenants.example.json
```

check-tenants 不读密钥、不联网，只验证配置结构；正式启动先解析全部配置并校验隔离，再打开后端连接。原 --config 单租户部署保持兼容，不自动变成多租户。

外层声明共享 listenAddr、总 maxConnections、tenant ID 和配置文件。子配置复用原 storage/backlog/cacheRepair/outbox/outboxRelay，authTokenEnv 属于该租户，server.maxConnections 是租户连接预算。子配置 listenAddr 被外层覆盖，不增加业务监听。公共 frame/payload/握手/关闭预算、Runtime 线程数和日志过滤必须一致。

认证前受总连接预算限制，认证后另取租户名额；满额拒绝握手，断开/异常/取消释放名额。租户指标与依赖健康通过各自 observability 端口呈现，端口必须互不冲突并限制网络来源。

## 隔离范围

- PostgreSQL 可共用实例，但必须使用不同的显式 database 名，建议配置独立最小权限账号。快照、幂等请求、事务回执、账本、Outbox 和修复任务都属于该数据库。
- Redis 可共用实例，但各租户必须使用不同逻辑 DB 编号；缓存、可靠 backlog、所有 Outbox publisher 都纳入冲突检查。租户内部可复用自己的端点。
- v1 保守要求 database 名与 Redis DB 编号在整个部署中唯一，即使端点地址不同也不能重复，防止主机别名绕过。不同物理集群也受此限制。
- Redis 逻辑 DB 不隔离 CPU、内存、AOF、管理权限或实例故障。v1 不支持 Redis Cluster 多租户部署；需要强隔离时使用独立实例，仍保留不同 DB 编号。共享实例不能采用会淘汰可靠队列的策略。
- 不提供跨租户原子事务。同一 namespace/key、requestId、operationId、tradeId、postingId、eventId 在各自数据库独立，业务无需手拼租户前缀。
- 管理命令仍使用租户自己的配置；不提供全租户清理或在线搬库。多副本必须使用相同租户映射及存储目标，不能切换到另一套库后仍声称重试幂等。

租户 ID 表示运营环境，不等于模块 ID。两个复用 MMORPG 模块的游戏也必须使用不同租户。TLS、在线凭据轮换、请求速率限制、CPU/内存硬配额、独立故障域与同表 tenantId 改造不在 v1 内。

## 验证边界

真实 TCP + 独立 MemoryBackend 测试验证同键、同幂等号、CommitRecords/追加事实/Outbox、重连、配额及无默认令牌后门；静态检查覆盖存储、publisher、凭据和监控冲突。内存测试不证明真实 PostgreSQL/Redis 重启恢复。

本次未改动现有 SLG/WoW335 部署、数据库或容器。正式使用前须在独立可丢弃环境补真实存储验收：后台队列不串租户、备份恢复、故障干扰与积压恢复。Redis 共实例是资源共享方案，不是强隔离承诺。
