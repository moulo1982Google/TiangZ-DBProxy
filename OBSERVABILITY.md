# DBProxy可观测性

## 通用 Outbox Relay 增量（本地开发，尚未部署）

保留原有全局 Outbox 指标；新增 `dbproxy_outbox_relay_*` 按 `producer,publisher,backend` 汇总发送成功/失败/超时、耗时累计、重试/死信/租约丢失、积压与最老年龄。标签来自启动配置，未知历史来源归入 `other`，不使用事件 ID、玩家 ID 或完整 Stream 地址。

例如按 Publisher 查看发送成功率（无发送时分母为零，不应作为故障报警）：

```promql
sum by (publisher) (rate(dbproxy_outbox_relay_publish_total{result="success"}[5m]))
/
sum by (publisher) (rate(dbproxy_outbox_relay_publish_total[5m]))
```

同时观察 `dbproxy_outbox_relay_dead`、`dbproxy_outbox_relay_oldest_age_seconds` 和 `dbproxy_outbox_relay_expired_leases`；后者是未发布记录的过期次数 gauge，不能使用 `rate()` 冒充累计计数。投递成功不是消费者完成，应由消费者另行报告 lag、inbox 去重和业务失败。管理操作、Redis 持久确认条件和验收限制见 [Outbox Relay](docs/outbox-relay.md)。当前远程仪表盘和演练版本不在本轮部署范围内。

DBProxy使用独立HTTP监听暴露Prometheus指标，Prometheus负责抓取和告警，Grafana只负责查询与展示：

```text
TiangZ Process -> DBProxy TCP 7800/7801
                       |
                       +-> /live /ready /dependencies /metrics 9090/9091
                                      |
                                  Prometheus
                                      |
                                    Grafana
```

观测HTTP端口不接受DBProxy业务令牌，也不会输出数据库连接串、认证令牌、RecordKey、玩家ID、requestId或operationId。它必须只绑定本机或运维内网，禁止通过公网Nginx转发。

## 本地启动

从`deploy/local/.env.example`复制开发环境变量后执行：

```powershell
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml up -d
$env:DBPROXY_AUTH_TOKEN = "tiangz-dbproxy-local-token-2026"
Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File tools/run_local.ps1 -ConfigFile configs/local-1.json" -WindowStyle Hidden
Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File tools/run_local.ps1 -ConfigFile configs/local-2.json" -WindowStyle Hidden
```

访问入口：

- Grafana：`http://127.0.0.1:3000`
- Prometheus：`http://127.0.0.1:9095`
- DBProxy 1：`http://127.0.0.1:9090/metrics`
- DBProxy 2：`http://127.0.0.1:9091/metrics`

Grafana会自动配置Prometheus数据源并加载`TiangZ DBProxy Overview`，不需要手工导入JSON。

## 指标边界

| 指标 | 含义 |
| --- | --- |
| `dbproxy_dependency_up{dependency="postgresql|redis"}` | 真实存储必要依赖的最近一次 5 秒采样结果；任一为 0 时 readiness 撤销 |
| `dbproxy_cache_hits_total` / `dbproxy_cache_misses_total` | Initial logical Redis cache result per requested record; hits include fresh, stale, and negative results |
| `dbproxy_cache_negative_hits_total` / `dbproxy_cache_negative_writes_total` | Negative-cache hits and missing-record markers written to Redis |
| `dbproxy_cache_stale_hits_total` | Stale snapshots served during the stale-while-revalidate window |
| `dbproxy_cache_refresh_started_total` / `dbproxy_cache_refresh_completed_total` / `dbproxy_cache_refresh_errors_total` | Background stale-cache refresh lifecycle |
| `dbproxy_cache_read_errors_total` / `dbproxy_cache_write_errors_total` | Redis read/decode and cache write/delete failures |
| `dbproxy_cache_writes_total` | Successful Redis snapshot cache writes |
| `dbproxy_postgres_fallbacks_total` | PostgreSQL fallback attempts after a Redis cache miss |
| `dbproxy_postgres_fallback_errors_total` / `dbproxy_postgres_fallback_timeouts_total` | Fallback read failures and configured timeout expirations |
| `dbproxy_postgres_fallback_circuit_open_total` | Fallback requests rejected while the PostgreSQL fallback circuit is open |
| `dbproxy_cache_fallback_lock_acquired_total` / `dbproxy_cache_fallback_lock_contention_total` | Redis cross-instance fallback locks acquired and contended |
| `dbproxy_cache_fallback_lock_timeouts_total` / `dbproxy_cache_fallback_lock_errors_total` | Lock waits that expired and lock/recheck failures that triggered unlocked fallback |
| `dbproxy_cache_fallback_lock_release_errors_total` | Lock release failures; Redis TTL remains the recovery path |
| `dbproxy_backlog_pending` / `dbproxy_backlog_processing` | Current pending and leased Redis backlog depth |
| `dbproxy_backlog_oldest_pending_age_seconds` | Age of the oldest pending backlog item; zero when empty |
| `dbproxy_cache_repair_pending` / `dbproxy_cache_repair_processing` / `dbproxy_cache_repair_dead_lettered` | PostgreSQL-backed cache repair queue state |
| `dbproxy_cache_repair_oldest_age_seconds` | Age of the oldest committed snapshot still awaiting cache repair |
| `dbproxy_cache_repair_worker_polls_total` | Repair worker outcomes: committed/retry/dead-letter/lease-lost/empty/failure |
| `dbproxy_outbox_pending` / `dbproxy_outbox_processing` / `dbproxy_outbox_dead_lettered` | PostgreSQL transactional Outbox state |
| `dbproxy_outbox_oldest_age_seconds` | Age of the oldest unpublished non-dead event |
| `dbproxy_outbox_worker_polls_total` | Outbox worker outcomes using the same fixed result labels |
| `dbproxy_live` / `dbproxy_ready` | 实例存活与接流量状态；真实存储 Ready 还要求 PostgreSQL/Redis 都 up |
| `dbproxy_connections_total` / `dbproxy_connections_active` | TCP连接累计值与当前值 |
| `dbproxy_connections_limit` / `dbproxy_connections_rejected_total` | 实例连接上限 / 满额后握手前关闭的连接累计数；不使用远端地址标签 |
| `dbproxy_handshake_rejections_total` | 按协议、令牌或客户端名称分类的握手拒绝 |
| `dbproxy_requests_in_flight` | 当前执行中的RPC数量 |
| `dbproxy_rpc_requests_total` | 按固定操作名统计的RPC请求 |
| `dbproxy_rpc_failures_total` | 按固定操作名统计的失败 |
| `dbproxy_rpc_errors_total` | 按固定错误码分类的失败原因 |
| `dbproxy_rpc_records_total` | 批量RPC处理的逻辑记录数 |
| `dbproxy_rpc_payload_bytes_total` | RPC请求中的二进制payload和事务result字节数 |
| `dbproxy_rpc_duration_seconds` | 可计算P50/P95/P99的Histogram |
| `dbproxy_backlog_polls_total` | Backlog提交、空轮询与失败次数 |
| `dbproxy_backlog_processing_seconds_total` | Backlog处理累计时间 |
| `tiangz_dbproxy_endpoint_*` | TiangZ侧连接尝试、请求失败与Endpoint切换 |

`operation`、`code`和Endpoint序号都是固定低基数标签。禁止为了排查单个玩家而增加`playerId`、`namespace`、`recordKey`或`operationId`标签；单次请求关联只进入Debug结构化日志。

## 默认告警

本地规则文件`deploy/local/observability/alert-rules.yml`包含：

- 实例30秒无法抓取；
- 实例持续30秒未Ready；
- PostgreSQL 或 Redis 必要依赖持续30秒不可达；
- 存储错误持续出现；
- P99持续5分钟超过100ms；
- Redis普通快照Backlog持续处理失败；
- PostgreSQL回源持续超时；
- PostgreSQL回源熔断器打开并持续拒绝回源；
- Redis跨实例回源锁协调持续失败或等待超时；
- 缓存后台刷新持续失败；
- Backlog待处理深度超过1000或最老项目超过5分钟；
- 缓存修复或Outbox最老项目持续超过1分钟；
- 缓存修复或Outbox出现任意死信（critical）。

`dbproxy_cache_stale_hits_total`用于观察SWR实际承接的流量，不单独触发告警；是否异常需要结合刷新失败、PostgreSQL回源和延迟指标判断。

本地Prometheus只计算告警，不配置通知渠道。生产环境由运维侧Alertmanager或云监控接收这些规则。

## 生产部署

两个DBProxy实例使用不同观测端口，例如`127.0.0.1:9090`和`127.0.0.1:9091`。Prometheus可以部署在同机，也可以通过防火墙允许专用监控网段访问。业务TCP、观测HTTP和PostgreSQL/Redis端口必须分别管理，不能因为Grafana需要指标就把任一端口暴露公网。

Dashboard新增缓存修复/Outbox状态与worker结果面板。它展示的是DBProxy服务和TiangZ客户端行为；PostgreSQL与Redis自身的连接池、慢查询、Buffer、AOF rewrite和实例资源仍应使用云厂商监控或官方Exporter，DBProxy不会冒充数据库内部指标的权威来源。

`/live` 只回答进程事件循环是否存活；`/ready` 用于接流量，真实存储模式要求生命周期就绪且两个必要依赖最近一次采样均成功；`/dependencies` 在降级时返回 503，并给出 `postgresql`、`redis` 的 `up/down`。检测存在最长约一个 5 秒采样周期，容器刚停机的瞬间不保证立即摘流。MemoryBackend 没有外部依赖，`/dependencies` 返回 `not-configured`。

本地Prometheus还会抓取TiangZ all-in-one的`7600`以及`cluster-dbproxy`中启用持久化的Process健康端口。未启动的开发拓扑会显示为Down，但不会触发`tiangz-dbproxy`实例告警；正式部署应通过服务发现或独立静态目标清单替换这些本机示例端口。
