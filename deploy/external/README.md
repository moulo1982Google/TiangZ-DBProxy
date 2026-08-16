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

它启动 8 个 TiangZ Process：一个 LoginMgr、两个 Login、两个 Gate、两个静态 MapHost 和一个 Location。动态副本 MapHost 与 MapManager 暂不启动；两个 MapHost 使用 `acceptDynamicMaps=false`，只承载配置中的静态地图。
