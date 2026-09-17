# 登录容量测量与连接预算

## 原则

2026-09-17 用户明确纠正：数据库性能必须结合PG配置寻找合适并发，不允许用过载档位否定数据库性能。

在线玩家数、每秒登录到达量、同时在途登录数、DBProxy连接池大小、PG活动查询数和max_connections是不同的量。max_connections只是连接上限，不是最佳并发数。多个区服/多个DBProxy共享PG时，预算须汇总，而非每个进程分别用满。

先固定CPU、内存、磁盘/容器限额、PG版本与参数、数据量、payload、SQL和读写比例，再逐级增加并发。同步观察吞吐、P50/P95/P99、错误、CPU、I/O、连接等待和队列长度。变更参数后重新测量，不能只按CPU核数套常数。

参考：[PostgreSQL连接数说明](https://wiki.postgresql.org/wiki/Number_Of_Database_Connections)、[PG18资源参数](https://www.postgresql.org/docs/18/runtime-config-resource.html)。活跃连接增加到饱和点之后可能降低吞吐；work_mem还可能被多个查询节点/并发会话同时使用。

## 已实现的存储阶梯入口

```powershell
# TiangZ-DBProxy根目录；仅创建并停止本次专用测试容器
cargo build --release --bin login_storage_compare
node tools/login_storage_compare.mjs --sweep
# 将末尾打印的目录作为参数
node tools/login_capacity_summary.mjs target/login-storage-实际目录
node tools/summarize_login_storage.mjs target/login-storage-实际目录
node --test tools/login_capacity_summary.test.mjs
```

- 固定2 CPU/1GiB PG、512MiB tmpfs、max_connections=80；其他参数读取pg_settings记录，并非生产调优模板。默认effective_cache_size可能超过容器内存，此值是规划估计而非实际分配，不能当成推荐配置。
- 1/2/4/8/12/16并发，5/30数据域，两种存储路径，三轮、每场景5秒，共72场景。每轮交替执行顺序；三轮不能完全消除顺序及宿主机干扰。
- 每种数据形状seed后ANALYZE父表，使用与生产load_multi同形的参数化SQL记录EXPLAIN ANALYZE BUFFERS。诊断发生在独立连接上，不等于采集每个实际查询计划。
- 每worker建一个PG存储和一个tiered存储，因此有闲置PG连接；只有所选模式发起查询。并发字段表示活动请求上限，不是总连接数。
- 使用生产存储实现，无业务优化或缓存一致性修复。数据是128个合成角色，30域阶段保留之前5域记录；仅作当前小数据暖读基线。
- 汇总给出“已测峰值中位吞吐95%的最小并发”候选，并保留每档延迟区间。若峰值仍在最高档，提示边界峰值，不能宣布已找到饱和点。95%是比较规则，不是业务SLA。
- 未测固定到达速率/突发队列，未计连接池排队；不能拿闭环查询P95作完整登录P95。生产工作点还要按允许延迟、其他读写/后台任务留余量并复测。

## 完整登录验收：仍需独立端到端测量

不得用存储工具的结果填充下表中尚未测量的数字：

| 场景 | 前置条件 | 断言与计时 |
| --- | --- | --- |
| 保活重连 | 原Actor仍存在，身份和路由一致 | 角色Repository.Load增量为0；原Actor身份不变；客户端断开到收到完整恢复快照计时 |
| 冷登录恢复 | 原Actor不存在，存档已提交 | 默认LoadMulti读PG；单列鉴权、入服、PG排队/查询、Actor恢复和回包 |
| 进程恢复/批量接管 | 完成所有权交接及必要保存，多角色同时恢复 | 默认LoadMulti读PG；记录突发到达量、排队、P95/P99、错误及恢复总时长 |

新建账号/角色放在准备阶段，不混入已有角色重登。认证/账号查询另计，保活角色零快照读取不代表完全零数据库访问。清除缓存只允许测试专用快照缓存，禁止清空承载可靠backlog的Redis。

Examples新增retained_player_storage_boundary.test.ts调用真实MapHost进图分支，隔离网络/Native/存储依赖，验证保活不加载、不创建、不重新分配UnitId；已释放角色进入Repository；身份错误不会转成存储重建。这是控制流回归，不能冒充真实联机耗时或SLG实现。

当前map_probe_load测量窗口在setup_player之后；直接复用其业务RPC延迟作为登录耗时是错误的。端到端驱动应先补全上述阶段计时和来源断言，再启用完整登录容量报告。

## 本轮工具开发失败记录

- 参数化计划采集初次编译失败：tokio-postgres只声明于workspace.dependencies，根package未引用。显式加workspace依赖并由Cargo更新锁；禁止拿旧二进制跑新场景。
- 单独rustfmt默认Rust2015，拒绝async；使用rustfmt --edition 2024。测试实际二进制哈希与源副本保留在报告目录。
- 原始报告在target中不受Git跟踪，换机须单独打包；文档与测试器源码应随代码提交。

默认权威读取变更后，cache微基准已显式使用load_cached_multi，仅供允许旧读的查询比较。现有暖读结果不能冒充冷登录、进程恢复或批量接管容量，新契约容量仍待上述端到端验收。
