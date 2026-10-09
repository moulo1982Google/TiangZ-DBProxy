# 本机 DBProxy 依赖

这套 Compose 启动本机开发使用的 PostgreSQL、Redis、Prometheus和Grafana，不包含线上部署配置。

## 测试环境与授权约定

本机真实数据库测试分两步推进，AI 助手必须遵守：

1. **先准备代码和测试用例。** 完成不依赖外部服务的检查，明确哪些测试尚未执行。报告：“代码和测试用例已准备好，下一步准备用真实 PG、Redis 测试。”同时说明所需 Docker 环境、镜像版本、测试范围及会创建的测试库。
2. **等用户确认环境和执行范围后再实测。** 例如用户回复：“我安装 Docker，你按 DP 手册的 PG 18.6 和 Redis 8.8.1 镜像测试。”随后才能在该范围内拉取镜像、启动测试容器、创建测试库并运行测试。已有明确授权的同一范围无需重复确认；需要更换环境、版本或扩大测试范围时，重新说明并确认。

当前规定的镜像与 `docker-compose.yml` 一致：

- PostgreSQL：`postgres:18.6-bookworm`
- Redis：`redis:8.8.1-trixie`

2026-09-22 已经用户授权从 PG 18.4 升至 18.6；原测试库数据核对、升级后读写和新库回归均通过，见[升级验证记录](../../docs/postgresql-18.6-upgrade-20260922.md)。历史报告保留当时使用的版本。

缺少 Docker 或数据库时，应报告“代码和用例已就绪，真实环境测试待执行”，等待用户安排。不得把“继续”“推进开发”解释为环境准备授权；不得自行下载安装 Docker、数据库软件或便携包，也不得擅自改用其他数据库版本、Windows 移植版或远程服务来补齐环境。目录放在 `target/`、只监听本机、未安装系统服务或测试后关闭，都不能代替事先授权。

测试结果必须区分代码检查、已授权环境中的实测和手册规定的正式验收。替代环境的测试结果不能冒充规定镜像的验收。本文中的启动、故障测试和清理命令是操作说明，不构成执行授权；删除数据卷或已有测试文件也不得作为“收尾”擅自进行。

此约定来自 2026-09-22 的协作纠正：助手曾在缺少 Docker 时自行下载、启动 Windows PG/Redis 临时环境。问题是把开发推进授权扩大成环境变更授权；后续按上述两步流程执行，保留实际测试记录，但不将其算作规定镜像验收。

## 本机依赖配置

Redis 使用 AOF 和 Docker 命名卷保存普通快照 backlog。AOF 只保证本机部署下的恢复边界，不等于 Redis 集群或跨机高可用。

用户名：`tiangz`

密码：`tiangz_dev`

数据库：`tiangz`

连接地址：

```text
postgres://tiangz:tiangz_dev@127.0.0.1:5432/tiangz
redis://:tiangz_dev@127.0.0.1:6379/0
```

当前新建数据库会把 `dbproxy_snapshots` 初始化为 32 个 HASH 分区。旧开发卷中的普通快照表不会自动改写；DBProxy 会明确拒绝该 schema。确认本地数据不再需要后，按本文后面的 `down -v` 命令删除数据卷并重新启动。分区布局和校验规则见[PostgreSQL 快照分区](../../docs/postgresql-partitioning.md)。

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

启动DBProxy后打开`http://127.0.0.1:3000`，使用`.env`中的Grafana管理员账号登录；`TiangZ / TiangZ DBProxy Overview`会自动出现。Prometheus本机入口为`http://127.0.0.1:9095`。默认本地DBProxy观测端口现在只绑定`127.0.0.1:9090/9091`。

容器中的Prometheus使用`host.docker.internal`抓取，默认loopback绑定在部分主机上不可达。需要容器抓取时，在运行配置的`observability`中显式设置可达的私网地址和`"allowNonLoopback": true`；若确实需要`0.0.0.0:9090/9091`，也必须开启该选项，并用主机防火墙限制为本机容器/运维来源。不能照搬到公网部署。直接公网地址和多播地址即使开启该选项也会被拒绝；通配绑定是否被公网路由或反代暴露仍由部署网络控制。

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

同一端口还提供 `/live`、`/ready` 和 `/dependencies`。真实存储模式下 PostgreSQL 或 Redis 任一不可达都会在最近一次 5 秒采样后令 `/ready` 与 `/dependencies` 返回 503；后者的 JSON 会指出具体依赖。`dbproxy_dependency_up` 已进入本地告警和 Grafana 面板。

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
## 独立 PG 数据路径对照

仅在本机演练需要比较存储路径时，设置 `DBPROXY_VALIDATION_PG_DATA_DIR` 为已存在的独立空目录，再在主配置、laptop、validation 三份文件之后叠加 `docker-compose.validation-postgres-bind.yml`，仅对 `postgres` 执行 `up -d --no-deps --wait`。该文件保留原 named volume，使用新的 PG 数据目录，不复制原业务库。不得让两个 PG 实例同时打开该目录。

回退时不叠加 bind 文件，使用原三份 Compose 配置对 `postgres` 执行 `up -d --no-deps --force-recreate`；原卷仍在。它不迁移 Docker Desktop 全局数据，不影响其他项目。路径对照必须保留镜像、持久化参数和验收负载，完整证据见[2026-09-07 存储路径验收](../../docs/storage-path-acceptance-2026-09-07.md)。

## 已授权的 4 核 / 8 GiB PG 测试配置

2026-09-22 用户确认本机资源足够并授权修改 Docker 测试配置。叠加顺序为基础配置、laptop、performance；最后一个文件覆盖 PG 的 CPU、内存和启动参数：

```powershell
$env:DBPROXY_REDIS_APPENDONLY = 'yes'
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml -f deploy/local/docker-compose.laptop.yml -f deploy/local/docker-compose.performance.yml up -d --wait postgres redis
```

使用自有测试项目时保留其 `-p` 项目名与原 env 文件，避免挂到另一组数据卷。本次实际项目为 `tiangz-dbproxy-index-validation`，env 文件为 `target/docker-index-validation/.env`。仍有固定容器名称，不能认为换项目名就能并行启动多个实例。

PG 18.6，CPU 配额 4 核，内存上限 8 GiB，无额外 swap 配额，shared_buffers 2 GiB，共享内存挂载上限 1 GiB；共享内存占用仍计入容器内存。work_mem 2 MiB、maintenance_work_mem 64 MiB、autovacuum_work_mem 32 MiB、max_connections 30、wal_buffers 8 MiB、jit off 保持固定。Redis 继续使用 laptop 配置和 AOF，不启动 Prometheus/Grafana。fsync、synchronous_commit、full_page_writes 保持开启。

容器 CPU 配额不代表独占物理核心。Docker 虚拟磁盘的容量也不等于宿主磁盘剩余空间；容量测试前另行核对实际存储。此前 1 GiB PG 的报告保留原配置，不改写为新规格结果。

本次检查记录见 `target/pg-4c8g-validation/`。采用独立空库进行索引、回执、网络和后台清理检查，跳过旧库升级用例；完整性能、持续运行和故障验收仍按[测试计划](../../docs/acceptance-performance-fault-test-plan.md)执行，不能用环境检查代替。
