# 登录角色加载：Redis与PG首轮对比

## 结论与边界

后续已补充低并发阶梯和PG配置记录，见[登录容量方法](login-capacity-method.md)。下方首轮16/64并发数据包含过载效应，不能视作PG正常工作区间；评估时优先使用文末追加的阶梯结果。

保活期间原角色Actor仍存在且身份/路由匹配时，应复用Server内存，不重新从Redis或PG加载角色。本次代码核查确认MMORPG的SecondEnterMap路径如此；不能由此声称鉴权/选角完全不访问数据库，也不能当作SLG已完成对应实现。

本轮只测 **角色不在Server内存，需要加载快照时的存储阶段**。Redis在当前实现和测试条件下有明确收益，尤其缓解PG高并发CPU压力；目前没有依据因一致性问题直接删除缓存。完整登录耗时、保活重连耗时和可支持在线人数仍未测量。

## 方法

- 2026-09-17，本机独立Docker PG18.4与Redis8.8.1，每容器2 CPU / 1GiB，Windows Rust客户端通过回环映射端口访问。
- PG使用512MiB tmpfs、max_connections=400；PG数据预热。只比较内存命中的数据库查询，不代表磁盘冷读、持久化性能或正式容量。
- 测试器直接调用生产存储crate：TieredSnapshotStore.load_multi 与 PostgresSnapshotStore.load_multi。未经过DBProxy TCP/认证、连接池分片、游戏协议、Actor创建、业务恢复、回包与AOI。
- 每种数据形状128个虚拟角色：5域=6400字节，30域=38400字节。payload为便于逐字节校验的重复字节，不代表真实业务熵/TOAST压缩比例。30域阶段保留5域数据，不能将两阶段差异全部归于payload尺寸。
- 1/16/64个并发worker，各worker自有存储连接；初始化和连接不计入请求延迟。不是生产4分片连接池的容量模型。
- 每场景5秒满速闭环，两轮分别cache→PG、PG→cache，共24场景。延迟从load_multi开始到结果返回，内容校验不计入延迟、计入实际吞吐时间。
- 所有缓存场景postgresFallbacks=0、cacheStaleHits=0，命中数与成功操作数×域数匹配；所有读结果检查revision与payload。
- 不包含保活内存查表微基准，避免将它冒充重连性能；也没有故障注入或修改一致性实现。

## 两轮结果范围

吞吐为每秒完整角色快照批次，不是每秒完整登录人数。P95是两轮各自P95的范围，不是合并分位数。

| 数据域 | 并发 | 缓存批次/秒 | PG批次/秒 | 缓存P95(ms) | PG P95(ms) |
| --- | --- | --- | --- | --- | --- |
| 5 | 1 | 1878–1943 | 574–624 | 0.68–0.75 | 2.03–2.48 |
| 5 | 16 | 15319–16378 | 1042–1085 | 1.53–1.68 | 73.97–74.41 |
| 5 | 64 | 20516–20749 | 793–847 | 5.00 | 191.63–195.19 |
| 30 | 1 | 1208–1216 | 436–445 | 1.09–1.14 | 2.81–3.16 |
| 30 | 16 | 5006–5190 | 515–592 | 5.77–6.12 | 87.94–91.09 |
| 30 | 64 | 4901–4986 | 419–504 | 20.23–20.31 | 293.03–358.78 |

低并发P50：5域cache约0.49ms、PG1.54–1.61ms；30域cache0.755ms、PG2.14–2.15ms。完整登录还包含其他阶段，不能直接声称登录快三倍。

Docker CPU粗采样（100%约一个逻辑核）：16/64并发PG模式约166–174%，接近2核限制；cache模式Redis约35–65%。短场景切换和docker stats采样窗口会交叠，所以仅作瓶颈线索，不作为精确CPU/请求成本。resources.jsonl另保留内存、网络、BlockIO原始数据；tmpfs环境不能据此推算物理磁盘负载。

## 查询计划线索

30域数据已seed后，对同类五键批量SQL做一次EXPLAIN (ANALYZE, BUFFERS)，显示 Hash Semi Join + Append + 32个分区Seq Scan，扫描4480行返回5行，shared hit=344，规划约2.115ms、执行约1.762ms。

这是常量数组的诊断计划，并非每个基准请求的参数化计划。新建小表、统计信息、分区裁剪和参数计划均可能影响选择；不能仅凭Seq Scan就定为PG缺陷。下一步应在相同数据集下核验实际参数计划、统计信息及批量SQL优化后重新比较，不能拿当前吞吐当PG上限。

## 保活链路核查

Examples中的：

- GateScene.ResumeOrRecoverMapHost先调用现有Unit的SecondEnterMap；路由失效才进入恢复逻辑。
- MapHostComponent.EnterMapCore若players.Get(characterId)返回同地图角色，则在原Actor mailbox调用SecondEnterMap，不创建角色。
- MapComponent.SecondEnterMap与PlayerUnitSystem.SecondEnterMap读取现有TS组件和Rust Native状态，生成回包快照。
- 现有gate_recovery_destination.test.ts 7项测试通过，包含live instance resume绕过目标选择；这是控制流单测，不是带数据库计数器的端到端“零读取”验证。

后续端到端测量必须分组：保活角色复用（角色快照load计数应为0）、已释放角色重登、进程崩溃恢复。鉴权/账号查询、角色快照加载、恢复业务与回包单独计时。

## 失败留档与证据

- 初始磁盘卷场景target/login-storage-RadG9x：PG initdb同步落盘迟迟未就绪，0个有效测量；停止PG退出137。不是PG查询性能结果。
- 改用tmpfs场景target/login-storage-qXS7va：24/24完成；两个容器均已停止并退出0。tmpfs数据停止后不可恢复，原始日志和报告保留；既有SLG/kind未改动。
- 当前提交：0a45f12f2d1fd8af6cdc353812764ab47f1fb581 + 工作区测试代码。
- 二进制SHA-256：bdd606c7034d72449479e2ff2c613651f25e2346283299c2c755371812727886。
- report.json、stdout.log、resources.jsonl、benchmark.rs、controller.mjs、changes.patch保存于该target目录；target不受Git跟踪，迁机需另行复制。
- 首次编译把connect与connect_with_config参数混用，编译器拒绝；改按实际API后构建成功，未进入测量。Windows运行npx.ps1受执行策略限制，改用npx.cmd，不修改系统策略。

## 复现

```powershell
# DBProxy根目录；运行会创建独立容器并在结束后停止
cargo build --release --bin login_storage_compare
node tools/login_storage_compare.mjs
node tools/summarize_login_storage.mjs target/login-storage-qXS7va

# Examples根目录，保活路由控制流单测
npx.cmd --no-install vitest run tests/unit/gate_recovery_destination.test.ts
```

结论使用限制：这是一轮短时存储基线，不能外推为完整登录SLA、生产容量或取消Redis的依据。下一步优先验证PG批量查询成本及完整登录分段耗时，保持保活复用语义不变。

## 追加：按配置寻找并发工作区间（2026-09-17）

独立运行target/login-storage-7LE3Zv，72/72场景完成。PG仍为2 CPU/1GiB，max_connections调整为80；每档1/2/4/8/12/16活动请求，每形状与模式3轮，每轮5秒。ANALYZE后取同形参数化诊断计划，仍为Hash Semi Join加32分区扫描；5域规划1.989ms/执行0.818ms，30域规划1.569ms/执行1.306ms。不是生产大表计划，也没有修改SQL或关闭顺序扫描。

| PG并发 | 5域批次/秒（三轮中位数） | 5域P95区间ms | 30域批次/秒（三轮中位数） | 30域P95区间ms |
| --- | --- | --- | --- | --- |
| 1 | 583 | 2.22–2.47 | 435 | 2.79–3.05 |
| 2 | 1112 | 2.24–2.35 | 798 | 3.04–3.63 |
| 4 | 1995 | 2.37–3.35 | 1100 | 5.68–6.47 |
| 8 | 1360 | 45.90–47.13 | 727 | 53.50–61.82 |
| 12 | 1024 | 62.61–69.72 | 663 | 71.70–80.43 |
| 16 | 1032 | 73.81–76.15 | 556 | 88.47–89.78 |

这组固定配置/小数据暖读负载的已测候选是4并发，增加到8及以上反而吞吐退化。不是“PG只能4连接”，更不是4个在线玩家；生产连接池需按真正的混合负载、排队预算、全体DBProxy实例总量再确定，未自动修改部署参数。

同为4并发，缓存5域中位吞吐6433批/秒、P95 0.758–0.880ms；30域3724批/秒、P95 1.570–1.701ms。Redis收益仍存在，但不能再拿过载PG尾延迟放大收益。全部缓存场景无回源/过期命中，命中数与操作数×域数一致，全部请求校验快照版本与payload。

pg_settings：shared_buffers=128MiB，work_mem=4MiB，effective_cache_size=4GiB（默认规划估计，不是实际分配或推荐值），random_page_cost=4，max_parallel_workers_per_gather=2，jit=on，plan_cache_mode=auto。详见report.json。未完成生产参数调优、磁盘冷读、写入混合或固定到达率测试。

二进制SHA256：b302116ba35ceb30aa5724499dc3e368dc964a91a95f891590652a0ef38dc475。控制器/源副本和配置随报告保留；随后rustfmt仅格式化源文件。增加tokio-postgres根包依赖，由Cargo自动更新锁。容器只停止不删除；tmpfs数据随停止消失，测量证据保留。

验证：release基准构建成功，汇总器3项Node单测、Examples保活/路由/销毁竞态19项测试通过，Rust格式和JS语法检查、diff检查通过。未运行全仓库验收、500玩家长稳或三条完整登录端到端容量测试。无业务服务实现、协议或生成物修改。
