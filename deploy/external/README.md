# 外网双 DBProxy

外网演示使用两个无状态对等 DBProxy，两个实例共享同一套 Redis 和 PostgreSQL：

```text
DBProxy 1: 127.0.0.1:7800
DBProxy 2: 127.0.0.1:7801
       \      /
        Redis + PostgreSQL
```

DBProxy 之间没有 Leader、复制或内部 RPC。TiangZ 的每个 Process 把 `7800` 配为首选、把 `7801` 配为故障切换地址；客户端切换地址时保留原 `requestId` 和 `operationId`。

## 部署

先把 `configs/external-1.json`、`configs/external-2.json`、发布二进制（命名为`/opt/tiangz-dbproxy/tiangz-dbproxy-server`）和本目录的 systemd 模板复制到 `/opt/tiangz-dbproxy`，并准备只允许 root 读取的 `/etc/tiangz/dbproxy.env`：

```text
DBPROXY_AUTH_TOKEN=<strong-token>
DBPROXY_POSTGRES_URL=postgres://<user>:<password>@127.0.0.1:5432/<db>
DBPROXY_REDIS_URL=redis://:<password>@127.0.0.1:6379/0
```

已有外网部署使用 `/etc/tiangz/dbproxy.env`，两个实例共享同一份环境文件；密码不要写入配置文件或 Git。

安装并启动两个实例：

```bash
install -m 0644 deploy/external/tiangz-dbproxy@.service /etc/systemd/system/tiangz-dbproxy@.service
systemctl daemon-reload
systemctl enable --now tiangz-dbproxy@1.service tiangz-dbproxy@2.service
systemctl status tiangz-dbproxy@1.service tiangz-dbproxy@2.service
ss -ltnp | grep -E ':7800|:7801'
```

TiangZ 外网多进程部署包是：

```text
configs/deploy/external-multiprocess/StartMachine.json
```

它启动 10 个 TiangZ Process：一个 LoginMgr、一个 MapManager、两个 Login、两个 Gate、两个静态 MapHost、一个动态副本 MapHost 和一个 Location。两个静态 MapHost 使用 `acceptDynamicMaps=false`；独立的 `dungeon_1` 使用 `acceptDynamicMaps=true` 承载动态副本。

## 4C8G 七日故障演练

单机开发演练使用 `docker-compose.chaos.yml`：PostgreSQL 固定为 18.4，Redis 固定为 8.8.1；前者限制为 2 GiB/1.5 CPU，后者限制为 768 MiB/0.5 CPU，并启用 AOF `everysec`、512 MiB `maxmemory` 和 `noeviction`。同一台 4C8G 主机不再部署 PostgreSQL standby 或 Redis replica：同机副本不能提供整机高可用，却会污染恢复故障边界和资源数据。主从、多节点自动切换与多可用区属于后续独立验收。

首次切换到分区 schema 会永久删除旧开发数据，必须先核对 Compose project 和两个旧数据卷的精确名称。新的固定卷为：

```text
tiangz-dbproxy-chaos-postgres-data
tiangz-dbproxy-chaos-redis-data
```

准备好只允许 root 读取的 `/opt/tiangz-dbproxy/.env` 后启动 fresh 存储：

```bash
docker compose --env-file /opt/tiangz-dbproxy/.env \
  -f /opt/tiangz-dbproxy/docker-compose.chaos.yml config --quiet
docker compose --env-file /opt/tiangz-dbproxy/.env \
  -f /opt/tiangz-dbproxy/docker-compose.chaos.yml up -d --wait --wait-timeout 180
systemctl restart tiangz-dbproxy@1.service tiangz-dbproxy@2.service
```

首个 DBProxy peer 在 PostgreSQL advisory lock 内执行 001 到 007 migration，第二个 peer 等待并复用结果。启动后必须确认 `dbproxy_snapshots` 的 `relkind=p`、叶子分区数为 32、migration 数为 7，并检查两个 `/dependencies` 都返回 PostgreSQL/Redis `up`。

`dbproxy_fault_soak` 是独立的 100 玩家正确性负载。安装 `tiangz-dbproxy-soak.service` 后，先完成短时预演，再启用七日服务：

```bash
install -m 0644 deploy/external/tiangz-dbproxy-soak.service \
  /etc/systemd/system/tiangz-dbproxy-soak.service
systemctl daemon-reload
systemd-run --unit=tiangz-dbproxy-soak-preview --collect \
  --property=User=tiangz --property=Group=tiangz \
  --property=WorkingDirectory=/opt/tiangz-dbproxy \
  --property=EnvironmentFile=/etc/tiangz/dbproxy.env \
  /opt/tiangz-dbproxy/dbproxy_fault_soak \
  --endpoint 127.0.0.1:7800 --failover-endpoint 127.0.0.1:7801 \
  --players 100 --duration 120 \
  --cycle-ms 1000 --read-pool-size 24 --write-pool-size 8 \
  --trade-interval-cycles 30 \
  --report-interval 15 --validation-timeout 120
journalctl -u tiangz-dbproxy-soak-preview.service --no-pager

# 只有短跑输出 SOAK_FINAL 且最终验证通过后才启用七日 unit。
systemctl enable --now tiangz-dbproxy-soak.service
journalctl -u tiangz-dbproxy-soak.service -f
```

它持续覆盖 revision、幂等事务、AOF backlog、交易状态、不可变账本和 Outbox，并在连接级传输故障时从 7800 切到 7801。unit 只 `Wants` 两个 peer，不能 `Requires` 任一 peer，否则故障计划停止该 peer 时 systemd 会把负载一起停止。它还显式使用 `Restart=no`：驱动没有可跨进程恢复的逐玩家内存状态，异常退出后静默重启会换 run ID、重算七天并漏掉中断 epoch 的最终对账，因此必须把退出视为本轮失败并人工重新开始。最终通过标准不是“进程一直活着”，而是逐玩家对账通过、账本总和为零、backlog/cache-repair/outbox 排空且无死信。完整的 500 游戏玩家、MapHost、动态副本和双重安全故障开关见 TiangZ 仓库的 `docs/tutorials/21-external-chaos-drill.md`。
