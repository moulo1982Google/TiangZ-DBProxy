# 本机 DBProxy 依赖

这套 Compose 只启动本机开发使用的 PostgreSQL 和 Redis，不包含线上部署配置。

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
