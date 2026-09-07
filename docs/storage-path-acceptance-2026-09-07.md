# 缓存清理、批次提交与存储路径验收

2026-09-07。产品修复分别为 `24d66b7`（提交后缓存清理由既有维护 worker 有界批量处理）和 `4432808`（同一 PG 的 SaveMulti 批次一次提交）。两项都有旧实现失败、新实现通过的精确回归，不能由此推断本机 100 玩家全程验收已经通过。

## 机械盘路径的结果

- `20260907-cleanup-30m-r1`：清理候选启动通过，约第 18 分钟正常阶段游戏 RPC 超时，未完成 AOF/最终对账。
- `20260907-cleanup-batch-30m-r2`：前置真实测试等待 Redis 健康的约 10 秒窗口过短，未开始计时。测试等待改为覆盖既有 Compose 健康检查窗口的 70 秒。
- `20260907-cleanup-batch-30m-r3`：批次候选正常阶段通过，约第 23 分钟 AOF 重启仍在加载，控制器固定等待 10 秒后探测收到 BusyLoading，未最终对账。TiangZ `e6dfcc2` 改用既有 90 秒窗口等待所需 Redis 的真实健康状态；独立专项实际等待 18,798 ms 后就绪。原失败轮次及专项新轮次各 64 条已确认记录事后恢复通过，原轮次仍保留为失败。
- `20260907-cleanup-batch-30m-r4`：冻结 DBProxy `e364361` / TiangZ `e6dfcc2`，前置 139 项默认、33 项真实数据库/故障及 7 项控制器测试通过。北京时间 20:13:18 开始，20:34:26 因正常阶段地图 100 的 1 次探针和 1 次移动超时失败，业务 RPC 期限仍为 5 秒。cache、PG、可靠 Redis 三次恢复通过，PG 恢复有 7 次登录重试。最后分钟统计读取 114,213 次，读取错误 4、入队错误 300、事务错误 810、交易错误 19；旧读、缺失、不变量错误均 0。读取错误及 1 次新增事务错误发生在正常阶段，不能全部归入故障窗口。没有 AOF 强杀和最终对账，不能记作 30 分钟通过。

r4 失败前约 44 秒，主 DBProxy 提交后缓存同步均值约 5.3 ms，PG 排队约 428.5 ms、执行约 242.0 ms；两张地图 SDK 排队约 595/384 ms、交换约 777/766 ms。指标是相邻计数器差值，非 p99 相减；PG 执行不等同于纯 SQL 或 fsync。缓存清理已不再是该窗口的主要等待，但 PG 与 SDK 仍存在长尾。周期快照持有玩家有序 mailbox，底层等待会阻塞该玩家后续消息；连接排队预算不包含 SQL 执行，不能凭排队有界保证整个业务 RPC 低于 5 秒。

## 隔离存储路径对照

Docker 默认数据 VHD 位于 F 盘机械硬盘，D 盘为 NVMe。相同 PostgreSQL 18.4 `pg_test_fsync` 的单次 8 KB fdatasync，旧路径约 54 ms，D 盘专用 bind 路径约 1.06 ms。该对照同时改变设备和文件系统访问路径，不能直接宣称纯硬盘性能倍数，也不证明突然断电时的物理持久性。

两客户端、30 秒、相同单条 INSERT 事务对照，均确认 `fsync=on`、`synchronous_commit=on`、`wal_sync_method=fdatasync`：NVMe 路径 27,423 次成功、0 失败，平均 2.187 ms、914.4 TPS；机械盘路径 537 次成功、0 失败，平均 112.498 ms、17.78 TPS。该小事务对照支持先验证部署存储路径，不能当作完整游戏吞吐结论。

新增 `deploy/local/docker-compose.validation-postgres-bind.yml` 仅在显式叠加时替换本项目 PG 的数据挂载。必须设置 `DBPROXY_VALIDATION_PG_DATA_DIR` 为既有独立目录，保留 PG 18 父目录挂载、原镜像、资源限制、fsync、synchronous_commit、WAL 设置、连接数、Redis AOF/WAITAOF、负载和业务超时。可靠 Redis 实际为 AOF `everysec`，可靠写入额外使用 `WAITAOF 1 0 2000`，不是依赖 `appendfsync always`。缓存仍是无持久化 tmpfs。

对照不会迁移 Docker Desktop 全局数据，也不操作其他项目。原 PG named volume 留存；新路径只含演练数据，不复制原 `tiangz` 数据库。用原三个 Compose 文件、不叠加 bind 文件，执行 `up -d --no-deps --force-recreate postgres` 即可回到原卷；禁止同时让两个 PG 实例使用同一目录。新路径必须重新通过真实测试、100 玩家启动、完整 30 分钟及最终对账，结果待补。

原始证据位于工作区 `.build-tmp/local-validation/` 各唯一轮次及 `.build-tmp/repair-ack-20260907/`，不覆盖失败文件，不提交配置凭据或数据库文件。TiangZ 控制器修复后 `verify:quick` 23 步通过，codegen 无 Git 差异；DBProxy 格式、严格 Clippy 通过。七天长稳、完整发布门禁与 WoW335 实机验收未执行。
