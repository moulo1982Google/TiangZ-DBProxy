# 本机 DBProxy 依赖

这套 Compose 启动本机开发使用的 PostgreSQL、Redis、Prometheus和Grafana，不包含线上部署配置。

Redis 使用 AOF 和 Docker 命名卷保存普通快照 backlog。AOF 只保证本机部署下的恢复边界，不等于 Redis 集群或跨机高可用。

用户名：`tiangz`

密码：`tiangz_dev`

数据库：`tiangz`

连接地址：

```text
postgres://tiangz:tiangz_dev@127.0.0.1:5432/tiangz
redis://:tiangz_dev@127.0.0.1:6379/0
```

启动：

```powershell
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml up -d
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml ps
```

## 笔记本轻量模式

16 GiB 或更小内存的开发机使用 `docker-compose.laptop.yml` 覆盖配置。它只启动 PostgreSQL 和 Redis，限制 PostgreSQL 为 1 GiB、Redis 容器为 384 MiB，并默认关闭 Redis AOF；Prometheus 和 Grafana 不会启动。

```powershell
powershell -ExecutionPolicy Bypass -File tools/local_laptop.ps1 pull
powershell -ExecutionPolicy Bypass -File tools/local_laptop.ps1 up
powershell -ExecutionPolicy Bypass -File tools/network_smoke.ps1
powershell -ExecutionPolicy Bypass -File tools/local_laptop.ps1 down
```

轻量模式只用于功能回归和真实存储冒烟，不能作为容量结论。默认 AOF 关闭时，`EnqueueSnapshot` 和 Outbox publisher 会因无法取得 `WAITAOF` 确认而明确失败；需要测试这两个可靠路径或恢复边界时必须启用 AOF：

```powershell
powershell -ExecutionPolicy Bypass -File tools/local_laptop.ps1 up -Aof
```

Redis 使用 `noeviction`，因为当前同一实例同时承载缓存和普通快照 backlog；内存耗尽时应明确失败，不能驱逐 backlog 键。`down` 保留命名卷，不需要本机依赖时可以退出 Docker Desktop，并在确认没有其他 WSL 任务后执行 `wsl --shutdown` 释放 WSL 内存。

启动DBProxy后打开`http://127.0.0.1:3000`，使用`.env`中的Grafana管理员账号登录；`TiangZ / TiangZ DBProxy Overview`会自动出现。Prometheus本机入口为`http://127.0.0.1:9095`。本地DBProxy观测端口使用`0.0.0.0:9090/9091`，只为Docker Desktop或本机Linux容器抓取开放，不能照搬到公网部署。

依赖就绪后启动 DBProxy 网络服务：

```powershell
powershell -ExecutionPolicy Bypass -File tools/run_local.ps1
```

普通启动参数位于`configs/local.json`，`configs/dbproxy.schema.json`提供字段提示；连接串和认证令牌仍从`.env`引用的环境变量读取。
Redis 缓存 miss 回源 PostgreSQL 时，`storage.cacheFallbackConcurrency` 默认每个连接分片 16 个并发，`storage.cacheFallbackTimeoutMs` 默认 2000 毫秒；该超时也限制 Redis 读取、回写、删除和租约释放，避免半断连接无限挂住请求。`storage.cacheFallbackCircuitFailureThreshold` 默认 5 次，`storage.cacheFallbackCircuitCooldownMs` 默认 5000 毫秒。连续回源失败达到阈值后熔断器打开，冷却后放行一次恢复探针；跨实例锁默认租约 3000 毫秒、等待 1000 毫秒、轮询 25 毫秒，批量读取共享一次等待预算。笔记本或低连接池环境可先调小这些值。

缓存生命周期默认配置为`cacheTtlMs=300000`、`cacheTtlJitterMs=30000`、`cacheNegativeTtlMs=5000`和`cacheStaleWhileRevalidateMs=30000`。正缓存的hard TTL等于fresh TTL、按`RecordKey`生成的稳定抖动与SWR窗口之和；负缓存或SWR不需要时可分别设为0。stale命中会立即返回并在后台刷新，进程内按Key去重、跨实例复用Redis租约锁；升级前没有freshness元数据的缓存会按stale处理并后台迁移。

顶层`cacheRepair`和`outbox`分别配置持久缓存修复与PostgreSQL Outbox worker。默认各1个worker、30秒lease、1秒起步/60秒封顶的指数退避、最多20次；超过次数进入死信，不会被静默删除。数据库权威写入已经提交但Redis缓存失败时，客户端仍收到成功，`cacheRepair`负责补齐；交易Outbox按至少一次语义写入`dbproxy:outbox:{topic}`，消费者必须按event ID去重。
默认监听`127.0.0.1:7800`，本机 SDK 使用的开发令牌是
`tiangz-dbproxy-local-token-2026`。该令牌只用于回环地址开发，生产环境必须替换并通过密钥系统注入。

指定另一份配置时使用：

```powershell
powershell -ExecutionPolicy Bypass -File tools/run_local.ps1 -ConfigFile configs/local.json
```

本机启动两个对等 DBProxy：

```powershell
$env:DBPROXY_AUTH_TOKEN = "tiangz-dbproxy-local-token-2026"
Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File tools/run_local.ps1 -ConfigFile configs/local-1.json" -WindowStyle Hidden
Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File tools/run_local.ps1 -ConfigFile configs/local-2.json" -WindowStyle Hidden
```

客户端把 `127.0.0.1:7800` 配为首选，把 `127.0.0.1:7801` 配为备用。两份配置必须继续指向同一 PostgreSQL/Redis；停掉其中一个只验证客户端切换，不应删除共享数据卷。

两个实例的指标分别位于`http://127.0.0.1:9090/metrics`和`http://127.0.0.1:9091/metrics`。只启动一个实例时，Grafana会明确显示另一个Target为Down，这是预期状态。

停止容器但保留数据：

```powershell
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml down
```

删除本地数据卷前必须确认不再需要开发数据：

```powershell
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml down -v
```

运行故障矩阵（只启动笔记本限额的PostgreSQL/Redis并强制AOF；会短暂停止并恢复容器，不会删除数据卷；Redis database 15 会在每次演练前清空，database 0 不受影响）：

```powershell
powershell -ExecutionPolicy Bypass -File tools/fault_matrix.ps1
```

运行真实 TCP -> DBProxy -> Redis/PostgreSQL 冒烟：

```powershell
powershell -ExecutionPolicy Bypass -File tools/network_smoke.ps1
```
