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

启动DBProxy后打开`http://127.0.0.1:3000`，使用`.env`中的Grafana管理员账号登录；`TiangZ / TiangZ DBProxy Overview`会自动出现。Prometheus本机入口为`http://127.0.0.1:9095`。本地DBProxy观测端口使用`0.0.0.0:9090/9091`，只为Docker Desktop或本机Linux容器抓取开放，不能照搬到公网部署。

依赖就绪后启动 DBProxy 网络服务：

```powershell
powershell -ExecutionPolicy Bypass -File tools/run_local.ps1
```

普通启动参数位于`configs/local.json`，`configs/dbproxy.schema.json`提供字段提示；连接串和认证令牌仍从`.env`引用的环境变量读取。
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

运行故障矩阵（会短暂停止并恢复容器，不会删除数据卷）：

```powershell
powershell -ExecutionPolicy Bypass -File tools/fault_matrix.ps1
```

运行真实 TCP -> DBProxy -> Redis/PostgreSQL 冒烟：

```powershell
powershell -ExecutionPolicy Bypass -File tools/network_smoke.ps1
```
