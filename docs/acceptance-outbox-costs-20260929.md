# P07：Outbox 分布与持续轮询成本

2026-09-29：正式三轮 `p07f_0929a` 完整通过。生产代码及 SQL 未修改；通过范围是固定速率队列组件，不代表应用容量或全部 P07 通过。

## 正式结果

UTC 14:24:25（北京时间 22:24:25）结束，容器退出 0、OOM=false。三项诊断及 12 个计时阶段均实际执行且各 `1 passed`，所有计时阶段 `full_timing=true`。原始 20,160 条领取记录含预热，正式样本 14,400 条。分析器核对 CSV 槽位、样本数、返回消息顺序、组计数、统计采样数和 18 份计划，结果 `COMPLETE_FIXED_RATE_COMPONENT_MATRIX`。

| 场景 | 统计关闭：三轮领取 P99（ms） | 统计开启：三轮领取 P99（ms） | 三轮中位数变化 |
| --- | --- | --- | --- |
| 十万有效租约、空领取 | 42.58 / 43.76 / 43.78 | 46.36 / 44.65 / 43.64 | 43.76 → 44.65ms，约 +2.0% |
| 阻塞组后两个独立组 | 13.06 / 13.18 / 12.76 | 14.35 / 12.76 / 13.21 | 13.06 → 13.21ms，约 +1.2% |

正式样本最大领取时延 81.02ms，最大调度落后 2.48ms 以内。每个阻塞场景两个独立组各按序确认 840 条，六个场景共 10,080 条（包含预热），被阻塞组零发布。统计调用自身的 P99 在十万租约场景为 78.27–82.67ms，在阻塞场景为 15.19–17.04ms。低频轮询下影响有限，但十万租约空领取仍有明显扫描成本；不能据此外推高频、多 worker 或完整应用负载。

502 份资源采样：宿主可用内存最低 34.40 GiB；PG cgroup 内存峰值 4.66 GiB（含文件缓存），工作台峰值 0.401 GiB。约十秒采样区间 CPU 峰值分别为 1.08 / 2.01 核，后者包含编译准备，不作纯业务 CPU 指标。未触发内存保护或容器 OOM。

全部原始证据已拉回 `target/server_20260929/outbox_p07f_0929a/`，包括 claims/stats CSV、各阶段 result.json、完整测试日志、计划及本地 analysis.json；同级 `p07f_0929a.*` 保存容器、镜像、源码摘要和资源记录。服务器原件保留。

## 已核实的短测

`p07s_0929a` 于 UTC 12:55:26–12:57:14 执行，容器退出 0、OOM=false。九种分布、31/32/33/40 锁定前缀和四类阻塞队首共三项真实 PG 测试各 `1 passed`。四个轮询场景也各 `1 passed`，但只有 2 秒预热、5 秒采样，分析器标记 `PARTIAL_OR_SMOKE`。

九种分布每种 100,001 条，每个旧/新 SQL 探针均完整 EXPLAIN ANALYZE，并实际领取核对返回 ID；探针事务回滚。旧/新 SQL 使用同一当前数据库结构，不是旧/新索引布局的成本对照。当前 SQL 单次执行时间：ready 39.41ms、dense-ready 36.30ms、backoff 0.26ms、leased 15.10ms、dead/backoff/leased-heads 约 132–133ms、all-blocked 134.71ms、none 15.27ms。完整计划保留，不把这些单次数据当作 P99。

原始证据在 `target/server_20260929/outbox_p07s_0929a/`，容器及资源信息在同级 `p07s_0929a.*`；服务器原件保留。分析器核对 18 份计划、实际测试执行数、逐条槽位/消息顺序、统计采样数和正式样本数。

## 正式三轮设计

`p07f_0929a` 于 UTC 12:58:42（北京时间 20:58:42）启动，容器 `dbproxy-p07-p07f_0929a`。源码短测提交 `4af17dc`，实测容器为 4 CPU/16 GiB、指定 cpuset。证据目录 `/data/dbproxy-test/evidence/outbox_p07f_0929a`，主机日志及资源为同级 `p07f_0929a.host.log` / `p07f_0929a.containers.jsonl`。运行期间仅做只读检查，未修改挂载源码或并行启动服务器编译、压力测试。

两个夹具，各比较统计关闭/开启，共 12 个阶段；每阶段新库、预热 120 秒、采样 300 秒，次轮倒序。预计约 84 分钟加准备时间。

- `leased`：100,000 条有效租约，整个测量期无可领取项，持续返回空。它是有积压但不可领取的空轮询，不是零行表。
- `blocked`：一条死信队首和 1,000 条同组后继，随后交错排入两个独立组，共 1,680 条可领取消息；逐条领取并确认，断言返回顺序和分组、两个组各完成 840 条，被阻塞组无发布。
- 领取固定 4 次/秒，统计开启时每秒调用一次实际 `stats()`；统计和领取共用生产队列连接/锁。记录等待后的完整领取时延、确认时延、调度落后量及统计时延，含预热原始样本。单调用 5 秒超时、调度落后 5 秒即失败；这些是测试运行边界，不是产品服务等级承诺。
- 工作台固定 cpuset `20-27,48-55`，4 CPU/16 GiB；宿主剩余内存连续 30 秒低于 8 GiB 则停止本轮工作台。基础服务限制不变，测试串行，无磁盘填满或共享服务故障。

这些是队列组件的固定速率对照，未发布到外部消息队列，也未同时施加完整应用混合读写；应用整体影响、更多 publisher 的性能矩阵仍需单独补测。同键多 publisher/destination 功能矩阵见 [A12 报告](acceptance-outbox-routes-20260929.md)。

## 双 publisher 补测方法

正式 `p7mf_0929a` 于 UTC 2026-09-29 14:58:07（北京时间 22:58:07）启动，UTC 16:23:59（北京时间 9 月 30 日 00:23:59）完成，容器 `dbproxy-p07-p7mf_0929a` exit=0/OOM=false，源码提交 `a1c3089`。保持 4 CPU/16 GiB、固定 cpuset。12 个阶段各 `1 passed`、full_timing=true、publishers=2，14,400 条正式采样、20,160 条含预热记录均经分析器核对，结果 `COMPLETE_FIXED_RATE_COMPONENT_MATRIX`。原始证据和同级资源/容器/镜像文件已拉回 `target/server_20260929/outbox_p7mf_0929a/` 及同级 `p7mf_0929a.*`，服务器原件保留。

| 双 publisher 场景 | 统计关闭：三轮领取 P99（ms） | 统计开启：三轮领取 P99（ms） | 三轮中位数变化 |
| --- | --- | --- | --- |
| 十万有效租约、空领取 | 48.10 / 49.87 / 48.16 | 49.96 / 48.64 / 50.32 | 48.16 → 49.96ms，约 +3.7% |
| 阻塞组后独立组 | 12.17 / 12.10 / 12.05 | 12.03 / 12.05 / 12.21 | 12.10 → 12.05ms，约 -0.4% |

六个阻塞场景每 publisher 各完成 840 条，共 10,080 条确认（含预热），每条返回归属、目标、分组及 ID 顺序正确，阻塞组零发布。正式样本最大领取时延 58.28ms、最大调度落后 2.164ms；统计自身 P99 空领取场景 83.66–85.30ms，阻塞场景 14.60–14.91ms。502 次资源采样，宿主可用内存最低 34.61 GiB，PG cgroup 内存峰值 5.364 GiB（含文件缓存），工作台 0.401 GiB，无 OOM。以上仅为单领取循环、固定速率下带 publisher 筛选的成本；应用负载和容量结论仍未覆盖。

短测 `p7ms_0929a` 在夹具准备阶段失败并保留：插入后 UPDATE publisher/destination 被 `outbox delivery route is immutable` 正确拒绝，尚未开始计时。已修正为先调用生产 API 注册 publisher/route，再按 route.key 插入，由生产触发器固化路由；没有关闭触发器、修改生产规则或放宽断言。失败日志和容器资料保存在 `target/server_20260929/outbox_p7ms_0929a/` 及同级文件，新库 `p7ms_0929b` 用于修正后的短测。

`p7ms_0929b` 在十万条带路由插入时触发准备连接的 5 秒语句超时，也未进入计时，证据同样拉回保留。调整为每批一万条、共十批，保留总量和全部路由触发器；5 秒语句/请求上限不变。新库 `p7ms_0929c` 验证分批准备后的工具。

`p7ms_0929c` 短测已通过，容器 exit=0/OOM=false，三项诊断和四个短计时场景各实际执行 `1 passed`。四场景 publishers=2，80 条正式样本、112 条含预热记录，阻塞场景每个 publisher 各确认 14 条。证据已拉回、分析器返回 `PARTIAL_OR_SMOKE`；不是正式三轮性能结果。定向 Clippy、格式、差异检查通过，旧单 publisher 完整证据也通过新分析器的兼容检查。

通过 `P07_PUBLISHERS=2` 使用两个已注册 publisher。十万有效租约平均分配给两者；阻塞夹具的死信队首和 1,000 后继属于 A，其余可领取消息交错分配给 A/B，使用相同 destination 和 partition key。每个槽位轮流指定 A/B 调用实际 `claim_for_publisher`，核对返回 publisher、destination、key、消息 ID 和确认结果；CSV 保存 publisher_filter，结果记录 publishers=2，分析器逐条核对。

总速率保持 4 次/秒（每 publisher 2 次/秒），仍是一个领取循环，不声称两个 worker 并发。统计开关、预热/采样及三轮顺序沿用前述设计，短测独立标记。与此前单 publisher 的比较同时改变了筛选条件和分组键布局，因此只能分别报告测得成本，不把差异全归因于 publisher 数量。它也不替代多 publisher 的九种分布全组合或整体应用负载对照。

复测：宿主 `bash deploy/remote-test/launch_p07.sh <全新RunId>`；正式默认 120/300 秒、3 轮。短测可覆盖 `P07_WARMUP_SECONDS/P07_SAMPLE_SECONDS/P07_ROUNDS`，结果不会标记为完整计时。结束后拉回 `outbox_<RunId>`，运行 `node tools/analyze_p07.mjs <证据目录>`，并核实容器退出状态、12 个成功阶段和资源记录。Windows 定向 Clippy、格式/差异检查、Linux 编译和短测、bash 语法检查通过。

## 应用发布核对基础（2026-09-30）

已核对正式启动代码：默认一个 Outbox worker 已执行真实 Redis 发布，存储指标循环每5秒调用 Outbox stats；既有六类业务每20请求产生一个 Outbox，但此前末尾仅核对PG效果内容，不能据此声称已逐条核对Redis发布。

新增 MIX_OUTBOX_AUDIT=1 仅验收驱动开关，将事件放入当前RunId专用topic，保留默认生产worker和统计。mixed_outbox_audit.rs在业务账本核对后有界等待30秒PG发布确认，逐条比较真实XRANGE的event_id、operation_id、partition_key、payload、occurred_at与PG，正常场景要求每事件恰好一条消息，无未知额外消息。保存outbox-publication.json中的PG确认、Redis流ID及核对结果。各事件使用独立分区键，不声称全局顺序或同键多事件顺序已测。

新库oaud_0930a三轮2秒预热/5秒采样，共420业务请求、1497业务快照、21条真实发布，三次1 passed、UTC02:53:22容器exit0/OOMfalse。每轮7条Redis消息与7条PG已确认效果一致、零业务错误漏发差异；原始证据已拉回target/server_20260929/fault_process_oaud_0930a_r{0,1,2}及同级oaud_0930a.*，分析全部SMOKE_ONLY。缓存修复未开启。独立副本篡改核对结果的负例被分析器REJECTED_LOAD，历史P06证据兼容分析通过。cargo check、定向Clippy -D warnings、fmt、node语法及bash语法通过。没有改生产协议或生成代码。

这一步是应用发布可核查基础，不是正式P07成本通过。下一步仍需明确统计/领取并行对照的变量、队列连续趋势、同键顺序和有界输入；现有生产5秒统计不能误称关闭。不得将另开数据库连接的统计成本冒充同一维护连接的竞争。

## 应用统计竞争对照工具

测试宿主acceptance_outbox复用server_process完整启动逻辑、清理及所有正式后台。生产入口继续传入None，不读取验收统计开关；仅测试例子传入一个附加worker，在同一StorageBackend上调用outbox_stats，竞争生产领取使用的维护连接。两组均保留原约5秒存储指标轮询：off表示无附加统计，on表示额外1次/秒，不能称全部统计关闭/开启。两组同样每秒写一条探针日志，on保存实际统计耗时、pending/processing/dead_lettered和失败；off记录null，不把缺失队列值当零。对照仅一个默认publisher/worker，不覆盖多worker。

业务保持20请求/秒、并发8、四SDK连接、六类比例不变，每秒一条业务Outbox消息；统一同topic同partition_key=ordered，最大420条/轮。无额外生成器，不会在业务guard停止后继续注入。Redis消息完整字段原始数组与流ID保存于outbox-publication.json，对照PG enqueue_order逐条核对同键发布顺序及确认，不允许漏发/重复或内容差异。outbox-stats.jsonl保存连续观察，requests.jsonl的measurement_start用于筛选300秒正式区间，分析器核对采样覆盖和统计调用无错误。

run_outbox_pairs.sh按off→on、on→off、off→on串行六个新库，MIX_SUITE=outbox入口；analyze_outbox_pairs.mjs核对完整顺序和业务账本、消息内容/顺序、统计样本，再汇总六类P99三轮中位及绝对/百分比差。默认120秒预热300秒采样。矩阵完整仅代表测量完成，超过20%参考线必须保留分析，不自动判性能通过。

组织短测opairs_0930a已完成：UTC2026-09-30 03:10:35 exit0/OOMfalse，六次1 passed及OUTBOX_PAIRS_COMPLETED。每轮2/5秒、140业务请求、499快照、7个同键Outbox，共840请求/42消息，全部Redis内容/PG确认/顺序核对一致；每轮正式短区间5条探针日志，on观测pending峰0，off值缺失按null处理。原始六目录与资源镜像容器/源码摘要已拉回target/server_20260929，analyze_outbox_pairs.mjs=SMOKE_ONLY。篡改Redis顺序和清空统计记录的副本均被分析器拒绝；历史发布核对回归通过。Rust定向Clippy -D warnings/格式、Node/bash语法和diff检查通过。生产协议与生成代码未变；测试例子之外不启用附加worker。

正式opairf_0930a已UTC2026-09-30 03:11:34启动（北京时间11:11:34），源码089682c，容器dbproxy-mixed-opairf_0930a，4CPU/16GiB、cpuset20-27,48-55核实。六轮各120/300秒，新库opairf_0930a_r{0,1,2}_{off,on}，顺序账本outbox_pairs_opairf_0930a/order.jsonl；每轮8400业务请求、420同键消息，预计UTC03:58附近完成，实际以容器状态为准。目前未形成正式结果，运行中只读且不并行服务器负载。结束拉回原始六目录、同级资源/镜像/容器/源码摘要，核对六次1 passed/OUTBOX_PAIRS_COMPLETED及exit/OOM，再运行配对和压力分析器。

## 正式应用统计对照结果及迁移交接

opairf_0930a于UTC2026-09-30 03:56:43（北京时间11:56:43）结束，exit0/OOMfalse，六次实际1 passed及OUTBOX_PAIRS_COMPLETED。六轮各120秒预热/300秒正式采样、8400请求/6000正式样本/420同键消息；总50400业务请求、36000正式样本、2520真实Redis消息。错误/未发送/业务核对/消息内容与顺序差异均为0，guard_stop=null，每轮统计正式区间300条日志；PG发布确认全部完成。原始六目录、顺序账本及资源/镜像/容器/源码摘要已拉回target/server_20260929，分析COMPLETE_OUTBOX_PAIRED_MATRIX。

| 操作 | 默认组三轮P99 ms | 附加1Hz组三轮P99 ms | 中位差 ms | 中位变化 |
| --- | --- | --- | --- | --- |
| load | 8.048/6.256/7.409 | 6.622/8.01/7.436 | 0.027 | 0.36% |
| load_multi | 8.475/7.699/7.801 | 8.507/8.347/7.819 | 0.546 | 7.00% |
| save | 8.758/7.776/8.009 | 7.74/8.559/7.758 | -0.251 | -3.13% |
| save_multi | 109.647/98.17/112.492 | 99.785/111.309/111.28 | 1.633 | 1.49% |
| transaction | 13.67/10.294/16.308 | 9.802/15.26/15.458 | 1.590 | 11.63% |
| commit_records | 18.057/16.283/17.581 | 17.316/18.144/17.843 | 0.262 | 1.49% |

三轮中位变化均低于20%参考线，但必须保留逐对异常：第二对单读6.256→8.010ms（+1.754ms/+28.04%）、单事务10.294→15.260ms（+4.966ms/+48.24%）。其余两对单事务分别-28.30%/-5.21%，方向不一致；不能根据中位数直接判性能通过，也不能断言附加统计造成确定回归。需要迁移后重新记录环境并延长测量/阶段归因；当前不启动追加测试。固定输入下零漏发，因此完成吞吐为20/s，并非容量测量。

附加统计三轮正式区间pending峰1/0/1；默认组没有同频队列观测，保持null而非零。此结论只覆盖默认单publisher/worker、每秒一条同键业务事件、默认5秒统计加额外1Hz统计的维护连接竞争，不能扩展为大积压或多worker性能。

资源264条，PG内存峰7.492GiB，主机可用内存最低36.10GiB。连续事件max/oom增量0、pgscan/pgsteal增量71613/71247、内存full PSI区间峰0.03993%，存在回收，不称零压力。

分析器首次汇总失败是本地子进程默认stdout缓存不足：420条消息原始payload导致超过1MiB，诊断error=ENOBUFS/status=null/SIGTERM，保存opairf_0930a.analyzer-buffer-diagnostic.json。修正父分析器忽略子stdout、检查退出状态并读取子分析器已写analysis.json，未改证据或断言、未放宽门槛；六轮正式及历史六轮短测分析均通过，node语法检查通过。无需服务器重跑。

用户准备迁移220的Docker数据目录：/var/lib/docker根盘116G已用103G剩7.2G，/data剩1.8T。所有本轮验收已结束且原始证据在本机；没有停止Docker或共享服务，没有新建后续负载。等待用户明确迁移完成并允许恢复后再继续。迁移可能改变IO环境，后续结果必须记录新环境，不直接无条件合并。

## 迁移后恢复复核

用户于北京时间2026-09-30 12:34明确要求继续。只读检查确认DockerRootDir已变为/sas/docker（/dev/md1 ext4剩5.7T），根盘剩84G；PG/两个Redis healthy，PG4CPU/8GiB及原cpuset和/data绑定挂载未变。基础服务UTC04:26:36重启，工作台镜像仍ae3b3d8e17608b277067e3a44ee3e45056a987655b023aad4b025dc0f6811470。迁移前后IO路径/缓存状态不同，结果分组报告，不能简单合并或归因为代码性能变化。环境及服务/源码标识保存在target/server_20260929/opairf_0930b.environment-before.txt。

不升压、保持原工具，追加六轮opairf_0930b于UTC04:34:55（北京时间12:34:55）启动，容器dbproxy-mixed-opairf_0930b，4CPU/16GiB、cpuset20-27,48-55核实。每轮全新库、120秒预热300秒采样，20请求/秒、并发8四连接，默认统计与同维护连接附加1Hz交替。运行中未改挂载源码或启动本任务其他服务器测试。

## 迁移后六轮结果与外部负载干扰

opairf_0930b于UTC2026-09-30 05:19:38（北京时间13:19:38）结束，exit0/OOMfalse，六次实际1 passed及OUTBOX_PAIRS_COMPLETED rounds=6。每轮8400请求、6000正式样本、420同键消息，共50400业务请求/36000正式样本/2520真实Redis消息；全轮full_timing=true、guard_stop=null，错误/未发送/业务核对/消息内容与顺序差异均为0，PG发布全部确认。每轮正式区间统计日志300条。原始六目录、order.jsonl、资源/容器/镜像/源码摘要已拉回target/server_20260929；配对分析为COMPLETE_OUTBOX_PAIRED_MATRIX，资源压力分析也已保存。

| 操作 | 默认组三轮P99 ms | 附加1Hz组三轮P99 ms | 中位差 ms | 中位变化 |
| --- | --- | --- | --- | --- |
| load | 8.713/7.775/8.529 | 8.642/8.023/8.637 | +0.108 | +1.27% |
| load_multi | 11.339/10.173/10.877 | 11.160/10.517/10.959 | +0.082 | +0.75% |
| save | 9.057/8.059/9.036 | 9.165/8.534/8.762 | -0.274 | -3.03% |
| save_multi | 108.764/101.581/101.925 | 107.742/106.600/107.065 | +5.140 | +5.04% |
| transaction | 12.995/9.862/10.613 | 12.191/12.368/11.296 | +1.578 | +14.87% |
| commit_records | 16.828/17.920/19.518 | 16.246/15.471/19.067 | -1.674 | -9.34% |

逐对差保留如下，每项为“附加组减默认组”的绝对差与百分比，第二对虽然执行on→off，比较方向保持一致。

| 操作 | 第一对：ms / % | 第二对：ms / % | 第三对：ms / % |
| --- | --- | --- | --- |
| load | -0.071 / -0.81% | +0.248 / +3.19% | +0.108 / +1.27% |
| load_multi | -0.179 / -1.58% | +0.344 / +3.38% | +0.082 / +0.75% |
| save | +0.108 / +1.19% | +0.475 / +5.89% | -0.274 / -3.03% |
| save_multi | -1.022 / -0.94% | +5.019 / +4.94% | +5.140 / +5.04% |
| transaction | -0.804 / -6.19% | +2.506 / +25.41% | +0.683 / +6.44% |
| commit_records | -0.582 / -3.46% | -2.449 / -13.67% | -0.451 / -2.31% |

各类三轮P99中位变化未超过20%，但第二对单事务9.862→12.368ms仍超过参考线。保留该结果和迁移前a的+28.04%单读/+48.24%单事务；不以中位数掩盖单对变化，不合并迁移前后数据，也不把负差解释为确定改善。

本次存在额外混杂因素：其他任务的goudao-login-soak-20260930-linux220-watch1在UTC04:38:56启动，首轮业务区间为04:35:36–04:42:36，其启动已进入正式采样，之后与其余五轮重叠。结束后UTC05:22:27只读检查显示其8CPU/8GiB配额、cpuset为空，实时CPU815.68%、内存6.063GiB，可能与本任务指定CPU重叠。证据为opairf_0930b.other-load.txt；本轮资源记录中宿主load1峰17.04。没有该任务完整区间CPU序列，不能量化它造成的影响，更不能将本轮异常归因于它或附加统计的某一阶段。结论限于正确性核对通过、测量完成；性能验收和阶段归因仍未完成。暂不开新的服务器编译/负载，也不停止或修改外部任务。

附加统计正式区间pending峰1/1/1，调用P99分别1.672/1.512/1.326ms；默认组队列值为null，默认组日志的空操作耗时不当SQL耗时对比。仍为单publisher/worker、20业务请求/秒及1条同键消息/秒，不代表多worker或积压容量。

资源261条：PG内存峰2.226GiB，宿主可用内存最低34.85GiB，memory.events的max/oom增量均0，pgscan/pgsteal均0，内存full PSI区间峰0.00012614%。基础服务迁移后重启，文件缓存状态不同；PG峰值较旧轮低不能解释为代码改善，也不能解除原容量观测边界。

分析入口为outbox_pairs_opairf_0930b/analysis.json、opairf_0930b.pressure.json，逐对差及时间区间另存opairf_0930b.review.json。下一步先做本地服务等待阶段归因准备；只有外部负载结束或已核实隔离后才运行新的受控诊断，保留本轮而不以追加数据覆盖它。

## 阶段快照诊断准备

验收例子新增显式MIX_STAGE_AUDIT=1，仅支持上述B2 Outbox对照；默认关闭，manifest记录是否启用，六个配对轮次必须一致。正式入口继续传入None，原5秒存储指标轮询和生产计时逻辑均未变。采样复用已有request_stage_snapshot及latency_snapshot，不新增数据库连接、SQL或后台轮询器；在测试探针的1Hz tick中读取内存计数，写stage-snapshots.jsonl，随后执行既有统计探针。记录进程ID、墙钟/单调时间、采样读取耗时、15类操作各三个请求阶段、13个存储阶段的累计互斥桶/总时长、在途与读池占用。采样本身会增加观测成本，必须在新实验的两组都启用，不与旧无采样轮直接合并。

analyze_mixed_stages.mjs由原账本分析器调用，检查完整字段、固定桶/进程、单调计数及时间连续性。只对完全落入正式区间的相邻快照作差，跨预热/结束边界的区间剔除；保存覆盖秒数、每区间增量及汇总于stage-analysis.json。采样间隔超过2.5秒、边界缺失超过1.5秒、正式覆盖不足sample-2.5秒、计数回退/缺失或六类请求没有实际阶段活动均拒绝。历史无stage_audit证据仍按原账本分析，不伪造阶段数据。

P50/P99只报告落在哪个固定桶，尾桶上界null表示无穷；不把桶上界当精确P99。累计最大值仍标为进程生命周期最大值，可能来自预热；读池与在途仅为采样峰。handler包含存储阶段，不能相加阶段分位数/总和；存储计数混合操作且原子读取不是一致的逐请求跟踪，不能用它单独证明SQL、重连或维护连接锁的因果。此工具用于缩小等待范围，缺失的Outbox内部锁/执行分解仍需后续证据。

本地定向Clippy（正式bin、验收例子、fault_process）-D warnings、fmt、Node/bash语法、diff检查通过。五组分析测试覆盖预热与边界、字段重排、无穷桶、缺失/非有限值/无活动、计数回退/时钟跳变/采样缺口；已有opairf_0930b正式及opairs_0930a短测兼容分析通过。此时尚未上传或在220执行新工具，不能称真实采样已通过；待外部构建结束后，以新库、2秒预热/5秒采样先验证六轮，再决定完整诊断。UTC06:07只读确认旧登录长测已结束，但goudao-battle-phase157-build仍运行（cpuset0,1、8GiB），暂不与其并行编译或采样。

## 既有账本的调度与RPC耗时复查

等待外部测量结束期间，对a/b全部12轮的正式成功响应分别重算end_to_end_us、rpc_us和dispatch_us的最近秩P99（升序第ceil(N×0.99)条），每轮6000条业务响应，其中单事务300条。每类还保留最慢五条的同请求三个计时值；派生结果保存于target/server_20260929/opairf_0930{a,b}.rpc-breakdown.json，原始请求账本未改动。端到端P99与原报告逐轮一致。

| 第二对单事务 | 端到端P99：off→on ms | RPC P99：off→on ms | 调度P99：off→on ms |
| --- | --- | --- | --- |
| 迁移前a | 10.294→15.260 | 6.160→13.072 | 5.104→4.587 |
| 迁移后b | 9.862→12.368 | 6.263→9.056 | 4.938→4.947 |

两组异常对照的RPC尾部也有增长，而调度P99未同步明显增长，不能仅以发压调度解释原异常。RPC计时包含SDK、网络、服务与返回路径，仍不能直接归因服务SQL或维护锁；b的外部负载干扰也未消除。这里是各项独立分位数，不能相加或相减以分摊端到端P99，也不把最慢五条当作全分布。继续用双方均启用阶段采样的新实验缩小等待范围，保留a/b原始异常与环境差异。

## 阶段采样真实短测

外部测量结束后，UTC2026-09-30 07:56只读核验220仅基础服务运行，DockerRootDir仍/sas/docker、PG/Redis挂载和资源限制未变，环境存于stgs_0930a.environment-before.txt。上传ab57958中的server_process.rs、acceptance_outbox.rs、mixed_paced.rs和launch_mixed.sh，四个SHA256逐一一致，未改其他源码或生产配置。

新RunId stgs_0930a于UTC07:57:20启动，07:58:54结束exit0/OOMfalse；off→on、on→off、off→on六轮各2秒预热/5秒采样。实际六次1 passed及OUTBOX_PAIRS_COMPLETED，840业务请求、42条同键Redis消息、PG确认及内容/顺序均核对一致，零错误/未发送/业务差异，guard_stop=null。原始六目录、顺序账本、镜像/容器/源码摘要和资源已拉回target/server_20260929；配对分析SMOKE_ONLY，逐轮阶段分析STAGE_INTERVALS_CHECKED。

各轮8条原始阶段快照，剔除跨边界区间后各4个完整增量区间，覆盖3.999621–4.000219秒，满足原5秒短窗口的覆盖要求。六类业务的三个请求阶段均有实际活动，13个存储阶段字段完整；读取计数的采样耗时峰52–75微秒，仅包括读取，不包括序列化/文件写入。没有放宽时间、字段或活动断言。核查摘要stgs_0930a.checked-summary.json与各stage-analysis.json保留全部结果。

资源10条，同cgroup区间PG max/oom增量0、内存full PSI区间峰0.00002914%；只代表该短区间，不解除原升压边界。启动和结束检查未见其他验收容器，但事后读取Docker历史事件仅保留了本轮退出事件，不能用它证明全程无其他短任务；正式诊断须在启动前订阅并连续保存容器start/die事件。下一步仍用20业务请求/秒、并发8、四SDK连接、单publisher/worker，两组都启用阶段采样，另建六库执行120/300秒；新采样开销与a/b无采样轮次分组报告。

正式诊断stgf_0930a于UTC2026-09-30 08:03:17（北京时间16:03:17）启动，源码仍ab57958，镜像未变；环境及上传摘要见target/server_20260929/stgf_0930a.environment-before.txt。容器dbproxy-mixed-stgf_0930a核实4CPU/16GiB、cpuset20-27,48-55，六个全新库stgf_0930a_r{0,1,2}_{off,on}按原顺序120/300秒，双方启用阶段采样。每轮预期8400业务请求、6000正式样本、420同键消息；目前正在执行，预计UTC08:49附近结束，尚无正式结论。

在启动前附加只读Docker事件订阅，连续写/data/dbproxy-test/evidence/stgf_0930a.external-events.jsonl，元数据与错误流同前缀保存；已确认捕获本轮容器start。监听限定UTC08:03:17至09:08:17自动结束，不停止或修改其他服务。结束分析只取正式容器起止时间内事件，并核对本容器start/die及监听错误；它补充容器活动证据，不能单独排除宿主非容器负载。原资源采样继续运行。结束须拉回全部原始目录，核对六次1 passed、exit/OOM、业务与发布账本、阶段覆盖和资源事件，再解释逐对尾延迟；发生外部并行负载时保留干扰，不据此判隔离性能通过。


## 正式阶段诊断 stgf_0930a

UTC2026-09-30 08:03:17至08:48:26正常退出0/OOMfalse，六次1 passed及OUTBOX_PAIRS_COMPLETED。六轮120/300秒共50400业务请求、36000正式样本、2520真实同键Redis消息；业务错误、未发送、内容/顺序/PG确认差异均零，guard_stop=null。原始六目录及容器/镜像/源码/资源/事件记录已拉回target/server_20260929，配对分析COMPLETE_OUTBOX_PAIRED_MATRIX，六轮STAGE_INTERVALS_CHECKED。每轮300正式stats记录，on队列峰均1，off队列缺失为null。完整矩阵不等于性能通过。

| 操作 | off三轮P99 ms | on三轮P99 ms | 中位差ms / % | 逐对差ms（%） |
| --- | --- | --- | --- | --- |
| load | 8.612/7.97/8.216 | 8.417/8.288/7.698 | 0.072 / 0.88% | -0.195 (-2.26%) / 0.318 (3.99%) / -0.518 (-6.30%) |
| load_multi | 9.108/8.114/8.422 | 9.22/9.102/8.466 | 0.680 / 8.07% | 0.112 (1.23%) / 0.988 (12.18%) / 0.044 (0.52%) |
| save | 8.181/7.605/7.828 | 8.361/7.929/7.573 | 0.101 / 1.29% | 0.180 (2.20%) / 0.324 (4.26%) / -0.255 (-3.26%) |
| save_multi | 114.311/125.492/114.097 | 112.36/115.211/113.068 | -1.243 / -1.09% | -1.951 (-1.71%) / -10.281 (-8.19%) / -1.029 (-0.90%) |
| transaction | 16.426/27.484/23.717 | 29.34/17.865/16.206 | -5.852 / -24.67% | 12.914 (78.62%) / -9.619 (-35.00%) / -7.511 (-31.67%) |
| commit_records | 18.51/18.123/18.068 | 17.621/17.35/16.999 | -0.773 / -4.27% | -0.889 (-4.80%) / -0.773 (-4.27%) / -1.069 (-5.92%) |

首对单事务16.426→29.340ms（+78.62%、+12.914ms），其余两对下降，不能以三轮中位下降宣称改善或通过。新采样开销与旧a/b不同，分组报告，原异常全部保留。

阶段完整增量区间每轮约299秒（298.999431–299.000090），各轮单事务299次。单事务handler均值off/on分别4.823/8.381、4.858/5.267、4.887/4.713ms；六轮handler P99均只定位至(10,25]ms桶，不能捏造精确分位数。六轮单事务task_schedule与record_order_wait P99均[0,1]ms，调度均值26.88–30.47us、排序均值0–0.0067us；跨操作的PG连接等待及读池等待P99也均[0,1]ms。首对handler均值上升，说明服务处理内部存在变化，尚不能证明附加stats因果或定位SQL/维护锁；这些累计阶段非逐请求轨迹，不能相加嵌套阶段或独立分位数。采样读取峰135–304us，不含序列化/IO。详细全部存储阶段计数/均值/桶范围保存stgf_0930a.review.json和各stage-analysis.json。

连续Docker事件订阅捕获本容器start与die，错误流为空；截至UTC08:55检查，本容器起止窗口没有其他容器start/die。起止仅基础服务，不能由此排除宿主非容器活动。监听仍按原09:08:17自动截止，不影响后续负载。264条资源中PG峰2.875GiB，主机可用最低39.47GiB，max/oom增量0，pgscan/pgsteal各53889，内存full PSI区间峰0.006495%；存在回收，不声称零压力或解除历史升压边界。

下一步针对单事务handler内部与SDK/RPC等待作代码路径归因和必要的定向观测，不盲目重复同一低速矩阵；尚未证明瓶颈，不先改生产行为。多publisher全分布/并发worker、P06高积压和真正容量仍未完成。

## 调用边界与批量重叠复查

本地新增tools/analyze_mixed_overlap.mjs，仅分析既有账本，不改服务和负载。以intent.scheduled_us+dispatch_us为调用起点，加rpc_us为结束，区间端点相接不算重叠；时间起点在已spawn任务内、调用issue之前，包含调用准备、SDK、网络、服务和返回（发压调度另计dispatch_us），不是SQL执行区间。校验唯一意图/响应、成功状态、完整请求数和非负时间，拒绝提前停止/未发送。边界相接、重复响应和负耗时负例检查通过。每轮派生overlap-analysis.json保留全部300个正式单事务及重叠批次编号。

stgf六轮按r0 off/on、r1 off/on、r2 off/on，单事务与批量调用重叠数分别290/292、299/293、298/293（每轮300条）；全部单事务RPC P99分别14.412/24.625、25.802/16.317、19.104/14.633ms。几乎全部重叠，非重叠仅1–10条，不能以这小组P99推导独立对照或批量阻塞因果。首轮on也有未重叠但22.824ms的调用，重叠本身并不足以解释所有尾部。

代码边界核验：mixed_workload固定n%20的16/17为批量、18为单事务，20/s时相隔100/50ms；四SDK连接按n%4映射，当前单事务与这两个批次分属不同连接。client::exchange在write_message之后drop(attempt)，不在响应等待期间持writer锁；但仍可能等待槽位/写锁。已有ClientObserver::request_attempt_timed能够分别观测queue_wait/exchange，当前混合驱动尚未接入，不猜测其等待为零。服务handler从dispatch_isolated之前到返回之后，早于responses.send，因此不包含响应队列/网络发送。TieredSnapshotStore::apply先PG事务再synchronize_committed_cache；PG operation计时从request_client锁获取之后开始，含ensure_connected、SQL及commit，跨操作汇总，无法以均值差直接分配单事务时间。

后续应复用SDK现有observer补验收调用分阶段观测，并对事务PG/提交后缓存的边界作必要区分。新观测器回调不得阻塞，不把RecordKey/幂等ID作为指标标签，记录开销两组一致；须先新库短测、账本匹配校验，再决定正式诊断，不能把本次离线复查称实测SDK排队或根因已确定。

## SDK逐调用计时短测

cb4049a接入现有ClientObserver::request_attempt_timed，不改生产SDK行为。MIX_SDK_AUDIT=1要求阶段采样同时开启；验收task-local保留最多两次回调，无锁/通道/文件IO或await，不以业务键作指标标签。每条response保存sdk_attempts，包含操作、endpoint、结果、queue_wait_us和exchange_us；种子写入在scope外不记录。严格分析要求每调用恰好一次成功回调、正确操作、endpoint0、非负整数、SDK总量不超过包围调用rpc_us；重试保留原始但不接受为普通无错误性能样本。六轮要求开关一致，历史未启用数据兼容。

本地并发task-local隔离测试、Clippy、fmt、Node负例（缺回调、重复、错误操作、负耗时、超出调用、失败结果）、历史stgf兼容分析通过。确认220仅基础服务后上传三个文件并比对SHA256，新库sdks_0930a六轮2/5秒短测UTC09:27:22至09:28:55 exit0/OOMfalse，六次1 passed及结束标记；840请求、42真实消息核对零差异。全部原始已拉回，配对SMOKE_ONLY，六轮SDK_CALLBACKS_CHECKED及STAGE_INTERVALS_CHECKED。正式5秒样本的SDK queue最大18–52us仅是短测观测，不外推正式尾延迟。记录回调会增加少量开销，后续两组同开且与旧组分开报告。

正式sdkf_0930a于UTC2026-09-30 09:29:47（北京时间17:29:47）启动，容器dbproxy-mixed-sdkf_0930a，预计UTC10:16附近结束。六轮新库120/300秒、20/s并发8四连接、off-on/on-off/off-on；两组SDK和服务阶段采样均启用。源码cb4049a，短测报告70ec7c9。环境文件sdkf_0930a.environment-before.txt；仅基础服务运行，资源限制不变。只读事件订阅已捕获本轮start，PID2357484，UTC10:34:47自动结束。运行中不改挂载源码或并行编译/负载；结束核对六轮完整业务/消息/SDK回调及阶段覆盖，再分析排队与exchange，不据启动或短测宣称性能通过。


## SDK与服务阶段正式对照 sdkf_0930a

UTC2026-09-30 09:29:47至10:14:52，容器exit0/OOMfalse，六次1 passed及OUTBOX_PAIRS_COMPLETED。50400请求/36000正式样本/2520真实同键消息，业务错误、漏发、内容/顺序/PG确认差异零，guard_stop=null。原始六目录、容器/镜像/源码、资源及事件已全部拉回；COMPLETE_OUTBOX_PAIRED_MATRIX、六轮SDK_CALLBACKS_CHECKED与STAGE_INTERVALS_CHECKED。每轮420消息、300正式stats记录，on队列峰1/1/1，off为null。

| 操作 | off三轮P99 ms | on三轮P99 ms | 中位差ms（%） | 逐对差ms（%） |
| --- | --- | --- | --- | --- |
| load | 9.147/7.544/7.231 | 8.44/8.418/8.363 | 0.874 (11.59%) | -0.707 (-7.73%) / 0.874 (11.59%) / 1.132 (15.65%) |
| load_multi | 10.092/8.428/8.375 | 8.998/10.158/8.967 | 0.570 (6.76%) | -1.094 (-10.84%) / 1.730 (20.53%) / 0.592 (7.07%) |
| save | 8.608/8.038/7.743 | 8.44/8.66/8.105 | 0.402 (5.00%) | -0.168 (-1.95%) / 0.622 (7.74%) / 0.362 (4.68%) |
| save_multi | 115.037/112.559/104.997 | 116.192/115.184/117.17 | 3.633 (3.23%) | 1.155 (1.00%) / 2.625 (2.33%) / 12.173 (11.59%) |
| transaction | 13.183/15.865/10.545 | 20.852/18.142/20.235 | 7.052 (53.49%) | 7.669 (58.17%) / 2.277 (14.35%) / 9.690 (91.89%) |
| commit_records | 19.061/18.466/16.368 | 18.1/18.017/18.592 | -0.366 (-1.98%) | -0.961 (-5.04%) / -0.449 (-2.43%) / 2.224 (13.59%) |

单事务中位P99增加53.49%（7.052ms），第一/第三对分别+58.17%/+91.89%，第二对批量读+20.53%也保留。本实验正确性通过，但性能对照超过20%参考线，不能判性能通过，不用历史中位下降抵消。a/b/stgf原始异常继续保留；本轮两组相同SDK/阶段观测开销，仍不直接合并旧无SDK样本。

| 轮次 | 单事务SDK排队P99/最大us | exchange P99 ms | handler均值ms | handler P99桶ms |
| --- | --- | --- | --- | --- |
| sdkf_0930a_r0_off | 29/34 | 10.238 | 4.268 | (10,25] |
| sdkf_0930a_r0_on | 37/197 | 18.508 | 4.974 | (10,25] |
| sdkf_0930a_r1_on | 35/39 | 15.595 | 4.412 | (10,25] |
| sdkf_0930a_r1_off | 30/84 | 14.584 | 5.565 | (10,25] |
| sdkf_0930a_r2_off | 26/35 | 6.290 | 4.048 | (5,10] |
| sdkf_0930a_r2_on | 32/44 | 18.530 | 5.459 | (10,25] |

六轮单事务各300条正式SDK回调，每条只有一次成功attempt，排队P99 26–37us，最大197us。与毫秒级尾部相比，SDK槽位/写锁等待不足以解释本轮差值；exchange包含编解码、网络、服务、返回，不能直接改称SQL时间。逐条最慢五个请求及其对应SDK计时保存在sdkf_0930a.review.json，未相加或相减独立分位数。第三对handler桶从(5,10]ms移至(10,25]ms，为服务内部变化提供线索；无法把累计桶绑定到某一条慢RPC，也不证明附加统计独立造成该变化。

连续事件文件包含本容器start/die且stderr为空，截至UTC10:25仅这两个事件；起止检查仅基础服务，不排除非容器宿主活动。263条资源PG峰3.771GiB、主机可用最低38.81GiB，max/oom增量0，pgscan/pgsteal各85241，full内存PSI区间峰0.010523%。不称零压力、不解除历史升压边界。

下一步停止重复相同低速矩阵，针对单事务PG执行/commit与提交后缓存同步作分解，优先补能够对应同一请求的观测；同时保留服务响应发送未包含于handler的边界。不凭当前数据直接改SQL或锁策略。SDK排队诊断范围已完成，P07尾延迟根因及性能修复、多publisher/并发worker和其他容量范围仍未完成。

## 同事务存储分解短测

bd4e016新增显式acceptance-trace Cargo特性，默认构建不含此代码。只有验收例子在MIX_TX_AUDIT=1时启用；正式入口不启用，不增加生产关闭开关。单事务StorageBackend调用以task-local收集既有存储计时器的相对起止时间，另记录single_transaction_commit；每请求最多64阶段，仅输出operation_id的SHA256及微秒计时，不输出payload/明文业务键。验收日志ACCEPTANCE_TX_TRACE可与账本run-n-0摘要逐条关联；handler不包含响应发送，日志写入也有成本，两组同开并与旧组区分。

analyze_mixed_transaction.mjs严格核对一调用一日志、摘要唯一匹配、schema、合法时间边界、trace总量不超过对应rpc_us，PG连接等待/PG处理/PG写入/commit/提交后cache同步均恰好一次。保存每请求SDK、RPC和存储阶段于transaction-analysis.json，只汇总正式样本。嵌套阶段不相加，PG处理仍含ensure_connected和SQL，commit单独记录；不假称已测SQL内部锁等待。缺失/重复/错摘要/越界/缺阶段负例、本地阶段测试、默认及特性Clippy、fmt、shell/Node语法、sdkf历史兼容通过。

上传storage源码目录、两个Cargo清单及相关server/driver/scripts，以29文件SHA256逐一校验一致（校验清单最初因Windows CRLF路径尾部被拒绝，移除清单行尾CR后全通过，源码未修改）。新库txs_0930a六轮2/5秒UTC10:45:02至10:47:14 exit0/OOMfalse、六次1 passed和结束标记，840请求42真实同键消息零差异。原始全部拉回，SMOKE_ONLY、六轮TRANSACTION_TRACES_CHECKED/SDK_CALLBACKS_CHECKED/STAGE_INTERVALS_CHECKED，每轮7条单事务trace含5条正式样本，合计42条完整关联。该短测用于确认工具，不作为性能结论。

正式txf_0930a于UTC2026-09-30 10:48:48启动，容器dbproxy-mixed-txf_0930a，预计UTC11:35附近（北京时间19:35）结束，以实际状态为准。六轮新库120/300秒，20业务/s、并发8、四连接，off/on、on/off、off/on；双方同时启用MIX_TX_AUDIT、MIX_SDK_AUDIT、MIX_STAGE_AUDIT。代码bd4e016、短测报告a46d10b；每轮预期8400请求6000正式样本420真实消息，420条单事务trace含300正式样本。保持原资源限制，运行中不改挂载源码或并行编译/负载。

启动前仅基础服务，环境证据txf_0930a.environment-before.txt已保存。连续只读Docker事件订阅PID2541266覆盖UTC10:48:48至11:53:48，已捕获本轮start；结束须核对start/die、stderr及窗口内其他容器活动，不排除宿主非容器负载。原始目录outbox_pairs_txf_0930a、fault_process_txf_0930a_r{0,1,2}_{off,on}及txf_0930a.*。结束拉回后要求配对、SDK、阶段和逐事务关联全部核验，再报告同一慢调用的PG等待/处理/commit/缓存阶段；嵌套计时不相加，原性能失败保留，本轮尚无性能结论。


## txf_0930a 正式逐事务分解

UTC2026-09-30 10:48:48至11:34:28，exit0/OOMfalse，六次1 passed及OUTBOX_PAIRS_COMPLETED。六轮50400业务请求、36000正式样本、2520真实同键消息零错误/漏发/内容顺序及PG确认差异，guard_stop=null。原始证据全部拉回；COMPLETE_OUTBOX_PAIRED_MATRIX，六轮SDK、阶段和TRANSACTION_TRACES_CHECKED。各420交易trace含300正式，共2520条完整对应，阶段汇总和每轮最慢五条同请求记录见target/server_20260929/txf_0930a.review.json及transaction-analysis.json。

|操作|off三轮P99 ms|on三轮P99 ms|中位变化%|逐对差ms（%）|
|---|---|---|---|---|
|load|9.057/7.188/8.199|7.151/7.848/7.743|-5.56|-1.906 (-21.04%) / 0.660 (9.18%) / -0.456 (-5.56%)|
|load_multi|9.641/8.555/8.581|8.872/9.204/8.982|4.67|-0.769 (-7.98%) / 0.649 (7.59%) / 0.401 (4.67%)|
|save|8.677/7.73/8.104|7.68/8.327/8.008|-1.18|-0.997 (-11.49%) / 0.597 (7.72%) / -0.096 (-1.18%)|
|save_multi|126.313/109.728/114.79|101.589/113.799/104.51|-8.96|-24.724 (-19.57%) / 4.071 (3.71%) / -10.280 (-8.96%)|
|transaction|29.411/12.999/28.902|19.565/16.802/11.398|-41.87|-9.846 (-33.48%) / 3.803 (29.26%) / -17.504 (-60.56%)|
|commit_records|18.566/16.894/17.826|16.377/18.244/17.628|-1.11|-2.189 (-11.79%) / 1.350 (7.99%) / -0.198 (-1.11%)|

单事务第二对12.999→16.802ms，+29.26%（3.803ms），仍不能判性能通过；中位下降41.87%不抵消逐对越线，也不替换sdkf此前+53.49%的失败。本轮新增日志开销，和旧组分开报告。

|轮次|PG连接等待P99 us|PG处理P99 us|commit P99 us|缓存同步P99 us|
|---|---|---|---|---|
|txf_0930a_r0_off|6436|5621|999|478|
|txf_0930a_r0_on|11607|5167|928|423|
|txf_0930a_r1_on|9846|5458|471|526|
|txf_0930a_r1_off|5361|5020|756|583|
|txf_0930a_r2_off|4933|24274|18929|593|
|txf_0930a_r2_on|12|4919|821|455|

逐请求证据：r1_on最慢n8278，RPC26.695ms，其中PG连接等待19.742ms、PG处理5.859ms（含commit0.451ms）、缓存同步0.323ms，trace总25.936ms。r2_off最慢n4498，RPC32.108ms，PG连接等待5.869ms、PG处理25.016ms（含commit1.846ms）、缓存同步0.327ms，trace31.227ms。r0_off最慢n5598的RPC30.452ms而trace14.573ms，剩余路径仍未覆盖。r2_off的commit最大19.357ms、缓存同步最大19.044ms是各自样本，不把独立极值相加。两组均有不同阶段慢调用；这支持继续核对连接共享及trace外响应路径，不证明附加统计或SQL内部锁是唯一原因。既有跨操作累计桶会稀释单事务尾部，不能据其1ms桶断言每笔事务等待都小于1ms。

267条资源样本，PG峰4.352GiB、主机可用最低38.637GiB；max/oom/oom_kill增量0，pgscan/pgsteal各48997，内存full PSI区间峰0.008335%。有回收，不解除升压限制。连续事件记录已包含自身start/die且stderr空，截至本次收集无其他容器事件；不排除宿主非容器负载。监听按原计划11:53:48自然退出。

下一步先离线核对单事务request_client的共享范围、maintenance连接与批量业务连接关系，并分析同请求trace外剩余时间和批量重叠；不凭猜测改SQL/锁，不重复无新观测的低速矩阵。多publisher并发worker、P06高积压和容量范围仍未完成。

## txf离线连接与计时边界复查

源码核对：server/lib.rs中每个Tiered shard独立连接，maintenance另经PostgresSnapshotStore::connect创建；outbox_stats调用该维护队列stats（outbox.rs独立self.client锁），不直接占用业务shard的request_client互斥锁。单事务按record路由，批量save按首条record选一个shard，克隆保留同一Arc<Mutex>；因此可能竞争同一业务连接，但当前trace没有锁持有者/路由关联，不能断言某次等待由某批次或stats引起。共享数据库CPU/IO等间接影响未排除。

capture的total_us在tracing::info输出之前读取，摘要及部分调用边界也在trace外；server的handler计时结束后才responses.send，独立writer串行write_message。因此同一请求rpc_us-total_us仅为未覆盖路径总差，含日志、调度、响应和网络，不是网络独占时间。六轮每300正式事务的该差P99（执行顺序off/on/on/off/off/on）为15243/1096/1083/1159/1142/892us，最大15879/1177/1507/1860/1424/1308us；r0_off存在显著未覆盖尾部，不能靠PG优化解释所有慢请求。

复用既有overlap分析器核验全部六轮：事务与批量RPC重叠298/270/299/291/298/292条；非重叠仅2/30/1/9/2/8，不是随机或平衡对照，不能从差值推出锁因果。同请求剩余时间和各轮最慢五项保存在target/server_20260929/txf_0930a.boundary-review.json，全部原始交易和请求账本保留。没有运行新负载或更改生产行为。事件订阅结束后文件再次拉回，仍仅本轮start/die且stderr空。

下一项定向观测应优先补trace日志输出耗时及服务响应队列/写出边界的同请求关联，避免把剩余时间归网络；如需确认业务连接等待持有者，应显式记录验收分片索引与锁区间，不能由RPC重叠代替。新观测先本地校验及短测，不直接再跑同一正式矩阵，不增加业务标识指标标签。

## trace输出边界工具（尚未服务器实测）

验收feature内trace schema升级为2：先保存原storage总时长，再测量首条trace的JSON构建、格式化和tracing::info调用耗时，以同SHA256的ACCEPTANCE_TX_OUTPUT第二条记录保存output_us。第二条记录自身的序列化/输出仍未计入，不能把剩余时间解释为网络；异步日志sink下该计时也不代表落盘耗时。默认生产构建不包含此观测。

分析器兼容旧schema1（trace_output_us为null），schema2严格要求唯一、完整、同摘要输出记录及非负整数，storage total+output不得超过同请求RPC；禁止同轮混合版本。缺失/重复/错摘要/负值/类型/越界负例通过，feature与默认storage Clippy通过，fmt及历史txf六轮兼容通过。尚未上传、未运行真实新库短测，不称实测通过。响应队列/写出关联仍待补齐；先用新RunId短测校验新记录完整性，不能直接开展正式对照。

aeba55f计时文件单独上传并核对SHA256 f9b263b86d87e5c5be98c37dac46f64ca2dc0bf15fd0d15f3dba36f4bba92073一致。outs_0930a新库六轮2/5秒于UTC12:12:39至12:14:49 exit0/OOMfalse、六次1 passed及结束标记，840请求42消息零错误/漏发/核对差异。原始全部拉回，SMOKE_ONLY，42条schema2 trace及42条同摘要output日志完整对应，六轮TRANSACTION_TRACES_CHECKED；SDK及阶段检查也通过。每轮7条输出调用耗时峰501/98/114/655/88/104us（含预热），仅为工具短测，不解释此前15ms异常，不代表落盘完成。尚未启动新正式矩阵；下一步补齐响应队列/写出边界再决定是否需要完整对照，避免每加一个计时点就重复约45分钟负载。

## 响应边界短测与正式对照启动

81e8722仅在acceptance-trace构建中给响应队列项附带可选事务摘要与Instant边界；仅显式启用的验收例子记录单事务。schema3新增ACCEPTANCE_TX_RESPONSE：handler_begin、queued、write_begin、write_end均相对服务接收完整请求后的admitted时刻。queued至write_begin包括发送通道背压和等待writer；write范围为write_message调用，不代表客户端收到或磁盘落盘。最终诊断日志本身仍未计时。默认生产构建仍用原ServerFrame队列，无新配置开关。

分析器要求同摘要一对一、非负整数单调边界、storage+首日志耗时不超过handler至queued范围；不要求write_end小于客户端RPC，避免两端完成调度差异误判。缺失/重复/错摘要/反序/范围负例、feature例子和默认生产Clippy、fmt、历史schema2兼容通过。两源码文件上传SHA256一致。

rsps_0930a六轮新库2/5秒UTC12:27:46至12:29:56 exit0/OOMfalse，六次1 passed及结束标记。840请求42真实消息零差异，原始全拉回，SMOKE_ONLY及六轮TX/SDK/STAGE核验通过，42条schema3记录均完整关联。每轮7条交易含预热的响应排队最大10/20/21/10/20/10us，写出调用最大58/57/63/75/70/57us，仅工具短测。

观测边界齐备后正式rspf_0930a于UTC12:30:37（北京时间20:30）启动，六轮120/300秒、20/s并发8四连接、off-on/on-off/off-on，两组TX/SDK/STAGE全开，预计UTC13:17附近（北京时间21:17）结束。容器dbproxy-mixed-rspf_0930a，运行中不得改源码或并行编译/负载。启动前仅基础服务，环境rspf_0930a.environment-before.txt已保存；事件订阅PID2775195从12:30:37至13:35:37自动结束，已捕获本轮start。结束查6次1 passed、exit/OOM、完整原始及三类计时匹配、同请求阶段/日志/响应边界和六类逐对P99；20%失败保留，不以新观测组替换旧组。


## rspf_0930a结束：证据完整性失败，不能验收通过

UTC2026-09-30 12:30:37至13:16:24，exit0/OOMfalse，六次1 passed及OUTBOX_PAIRS_COMPLETED。原始六目录和rspf_0930a.*已全拉回。但严格配对分析在r1_off失败：stage-snapshots.jsonl第445行末尾截断（7693字节、无换行，elapsed_us=444000564）。远端与本地SHA256均7a0294e4211e9b0ccf00f88a3fd21cc15fb762ac15e0aebc37a164296754a22d，排除拉取损坏。原文件和断言保留，不丢弃尾行、不伪造COMPLETE_OUTBOX_PAIRED_MATRIX；状态REJECTED_EVIDENCE_INTEGRITY。其余五轮严格单轮分析通过。

测试result共50400响应、36000正式样本，2520真实同键消息；错误、未发送、核对差异为0，guard_stop=null。六轮消息检查均在阶段解析之前执行且通过；每轮420消息、300正式统计记录。SDK与事务分析独立运行，六轮SDK_CALLBACKS_CHECKED、TRANSACTION_TRACES_CHECKED，每轮420个trace/output/response完整关联（300正式）。这些部分结果不能替代缺失的完整六轮阶段验收。

以下是完整请求账本的诊断性P99，单位ms；因整组证据失败，不作为已通过对照。

|操作|off三轮|on三轮|逐对变化%|中位变化%|
|---|---|---|---|---|
|load|8.514/8.601/7.116|7.960/8.892/8.353|-6.51/3.38/17.38|-1.89|
|load_multi|9.284/8.955/8.889|8.249/9.619/8.586|-11.15/7.41/-3.41|-4.12|
|save|8.302/8.412/7.807|7.626/8.540/8.078|-8.14/1.52/3.47|-2.70|
|save_multi|112.780/115.345/101.676|110.871/112.990/111.987|-1.69/-2.04/10.14|-0.70|
|transaction|16.921/18.672/24.364|26.365/29.813/26.891|55.81/59.67/10.37|44.02|
|commit_records|16.920/17.749/15.944|16.619/18.473/17.320|-1.78/4.08/8.63|2.36|

单事务中位18.672→26.891ms（+44.02%/+8.219ms），第一、第二对分别+55.81%/+9.444ms和+59.67%/+11.141ms，均超20%参考线。全部旧组异常保留，新观测组不替代旧失败。

六轮各300正式事务，执行顺序off/on/on/off/off/on：首条trace输出调用P99为112/114/110/105/113/133us；响应队列P99为13/16/14/15/12/17us；write_message调用P99为64/73/70/71/64/70us。handler范围减去同请求storage及首日志后的余量P99为65/63/59/68/63/84us，仍含第二日志等路径。最终响应诊断日志自身未计时，write完成不代表客户端收到，不相加独立分位数或嵌套阶段。

同请求r1_on n7898：RPC29.273ms，PG连接等待4.113ms、PGoperation24.007ms（含commit0.373ms）、cache0.326ms、storage28.457ms、首日志0.070ms；服务admitted到write_end28.674ms。另r0_on n2418：RPC23.270ms、SDKexchange23.004ms，storage6.417ms、首日志0.068ms、服务write_end6.655ms；r2_on n5618：RPC23.541ms而服务write_end5.970ms。后者说明剩余差不在本次观测到的存储/日志/响应排队写出区间内，但跨端起止不同，不能称为纯网络或精确锁因果。所有同请求数据及每轮最慢五条保存rspf_0930a.review.json和各transaction-analysis.json。

267资源样本，PG峰4.744GiB，主机可用最低38.966GiB，max/oom/oom_kill增量0，pgscan/pgsteal各35986，内存full PSI峰0.002278%。有回收，不解除升压边界。收集时连续事件只有本轮start/die、stderr空，当前只有5基础服务；非容器宿主活动未排除，观察器按13:35:37自然结束，不停止。

退出路径核对：mixed_paced在服务仍运行时读取校验阶段文件；随后Server::drop调用Child::kill/wait，采样器仍每秒writeln并flush。此先读再杀存在验证后继续写入、强杀截断的竞态，与本次尾行损坏一致；尚无写入中断瞬间证据，不能声称已修复。下一步先修验收采样器退出/封存协议，让采样器停止并等待完整flush后再校验文件，继续严格拒绝截断；只作用于验收工具，先本地验证及新RunId短测，不直接重跑45分钟矩阵。剩余多publisher并发worker、P06高积压、容量范围仍缺。

## 采样器封存修复与seals_0930a短测

工具321762a仅修改验收例子、驱动及离线分析：驱动完成请求与核对后创建probe.stop；采样器在下一tick边界停止，完成stats/stage flush，关闭两文件，再以临时文件rename发布probe-sealed.json（schema1及两文件字节数）。驱动最多等待10秒并检查宿主存活，收到确认且核对长度后才读JSONL；之后Server::drop强杀不再中断采样写入。manifest显式probe_seal，离线分析要求确认、准确长度及最终换行，仍逐行严格解析，旧历史无此字段保持原解析规则。没有修改生产服务或旧失败数据。

本地feature例子及fault_process Clippy -D warnings、fmt通过。独立复制历史短测夹具验证封存正例、缺确认、错字节数及缺末尾换行负例；原始文件未改。两上传源SHA256分别1d880ac935e55b922f97490ecd116518f563572ff5f7d30195d0b5148fa5925e和3ad4443e06dca663ec342bfa87f39dc60f2b79fee0d3bb4fac6c92c62054f9fe，远端一致。

seals_0930a六轮新库2/5秒UTC2026-09-30 13:42:28至13:44:40，exit0/OOMfalse，六次1 passed及OUTBOX_PAIRS_COMPLETED。840请求42真实同键消息零错误/未发送/核对差异，guardnull。六份封存确认存在，stats/stage字节数与进程退出后完整原始一致；42条trace/output/response对应，六轮STAGE_INTERVALS_CHECKED/SDK_CALLBACKS_CHECKED/TRANSACTION_TRACES_CHECKED，SMOKE_ONLY。全部原始已拉回。只验证工具协议，不作为性能结论，不修复或覆盖rspf旧证据失败。

rspf事件观察器到期后再次拉回，仍只有自身start/die、stderr空，非容器宿主负载未排除。当前只有5基础服务，无新正式验收。此次没有业务计时新观测，故不自动重跑45分钟矩阵；下一步推进剩余P06工具前置：让修复注入随业务guard停止，并明确已注入/未注入核对语义，先本地验证，不因此授权高压故障或升压。性能诊断仍有跨端未覆盖路径，不能归纯网络；多publisher并发worker和容量缺口继续保留。

## P07并发worker补测边界核对（2026-10-01北京时间）

已核对outbox_poll_costs.rs、outbox_hybrid_plans.rs、outbox_concurrency.rs与run_p07.sh。当前poll工具只有一个领取循环和一个worker名称；两个publisher仅按槽位交替筛选。outbox_queue及其clone共享Arc包裹的同一PG连接互斥锁，直接spawn两个clone不能证明独立PG领取并发。现有8路前缀正确性测试使用各自connect_existing及Barrier，可复用连接建立方式，但该测试没有120/300持续成本记录。九分布诊断是在单事务内执行旧/新SQL并回滚，每个分布只有一次返回验证，不能直接复用为多worker吞吐或公平性结果。run_p07.sh还会无条件重跑已有十万行诊断，新的有界短测不得直接套该入口。

下一步工具明确为独立验收test及独立入口，不修改生产SQL：先两worker各自connect_existing、两个publisher，每500ms一个两任务同步起点，总领取预算仍4次/s；每波两任务筛选同一个publisher，publisher逐波轮换，覆盖同publisher竞争。至少两个可领取partition供每波竞争；同字符串partition跨publisher保留以验证路由隔离。每波需两个claim都返回后再确认，确认结束后才下波，记录实际调用区间而非仅记录spawn/barrier时间；若实际区间未重叠，不能声称本轮验证了重叠领取。不能把每worker各4次/s偷偷翻倍为8次/s。

账本需记录wave/worker/publisher、scheduled/dispatch/claim_begin/claim_end/ack_begin/ack_end、event_id/partition/lease_token及成功/空/未知结果。领取与确认分别5秒有界，超时保留started未知，不重试或当未领取；调度落后和未完成项触发停止后不再发新波，已发项保留。不以全局ready-n返回序列作并发断言：核对唯一event_id、同partition前驱已确认、publisher/destination归属、lease token及最终PG published状态；不要求两个worker平均分配消息冒充公平性证明。

阶段顺序：先新库、小数据（1000级）、2/5秒无stats验证基础并发/有界停止/账本，分析器正负例通过后再扩展九分布。九分布须每场景新库独立保留，ready/dense-ready应保留足够连续样本，backoff/leased/head-blocked应有按publisher分隔的独立可领取组，none/all-blocked必须持续空领取且确认数0。不要从单次100001行探针推断持续分布不变：正式窗口应保存各类队列计数、耗尽/分布漂移则拒绝。统计对照后续再加，明确统计共享哪一worker连接，不能隐式变更为独立连接。此次只是代码路径核对和具体工具设计，尚未实现/实测，不标多worker性能或全分布完成。

rhdf_0930a事件观察器到期后已重拉完整文件：仍仅该工作台start/die两条，stderr空；非容器活动仍不排除。当前实际只有5基础服务，没有新增验收负载，原资源与不升压限制不变。

## P07 双独立连接首次短测 p7ps_0930a（2026-10-01北京时间）

工具990530d新增独立outbox_parallel_poll测试、run/launch_p07_parallel入口与严格JSONL分析器，不调用旧十万行诊断、不修改生产SQL。两worker分别connect_existing，每500ms两次claim（总4次/s），逐波交替两个publisher；每publisher两个FIFO分区，分区名跨publisher相同。两个claim都返回后才ack，两个ack都完成后才下波。每次操作先同步started日志，保存实际调用起止；5秒timeout/error写unknown、不重试，join等待双方结果后才拒绝。100ms调度guard停止新波。同步账本IO有成本，本组不用于生产性能结论；当前硬编码2/5秒、1000行、无stats。

本地目标Clippy -D warnings、fmt、Node语法及一个正夹具/11个负例通过（缺失、重复、unknown、归属、FIFO、token、最终PG、ack边界、guard、重叠计数、末尾换行）。三个上传文件SHA256核对一致。新库p7ps_0930a容器UTC2026-09-30 16:48:04.948至16:48:20.560，exit0/OOMfalse，1 passed及P07_PARALLEL_SMOKE_COMPLETED。14波28次claim/ack，短采样窗口20次claim；14波实际调用区间全部重叠。1000行最终核对，28条唯一事件已发布、972条仍未发布；两个publisher归属、同key前驱确认、token及PG状态全部严格通过，离线PARALLEL_CLAIMS_CHECKED/SMOKE_ONLY。完整原始在target/server_20260929/parallel_p7ps_0930a及p7ps_0930a.*。

启动前/结束后实际均只有5基础服务，启动前进程检索无cargo/rustc/fault_process/outbox_poll；PG限制仍4CPU/8GiB及14-17,42-45。仅两条资源采样、一个观察区间，PG max/oom/oom_kill增量0、scan/steal各5776；采样稀疏，不能排除未采样峰值或宿主非容器活动。本轮未建立连续外部Docker事件订阅，不能声称整个窗口无其他活动。资源回收仍在，不解除不升压边界。

尚未实测新工具guard停止或claim/ack真实未知路径，也未完成九分布/统计开销/多worker正式性能。下一步先补此工具有界停止与双方未知结果留档的异步测试及严格分析负例，再分步扩展小数据九分布，显式核对可领取/受阻分布与样本不耗尽。不要重复本轮正常短测或直接扩大到十万/容量矩阵。所有旧失败和P06/P09缺口保留。

## P07 并发工具停止与未知结果本地验证

在990530d基础上把100ms调度判定提取为WaveGuard，保持阈值；一旦拒绝即保持停止，后续即使时隙恢复也不会重新领取。新增三个异步本地测试：100ms精确边界接受/150ms实际等待后拒绝/后续不恢复；claim与ack各模拟两个永久pending，实际走原5秒期限并保存双方started与unknown；一worker立即错误时，另一worker延迟完成仍保存completed，证明join不会因首个错误取消同波另一项。共3 passed，真实数据库短测仍ignored，没有启动或重复远端负载。这是future模拟，不代表真实PG已提交后断连或超时故障已覆盖；文件同步IO和预置也未取得硬期限，不称全流程有界。

严格分析器补充ack的started不得早于双方claim结束。Node正夹具与18个负例通过，新增guard记录、ack unknown、缺另一worker完成、过早ack started、错误时间类型、零实际重叠、缺结果等。原p7ps_0930a原始账本用新分析器复查通过；原始未改。目标Clippy -D warnings、fmt通过。此次仅本地工具验证，未上传新源码，不据此声称九分布/正式性能或真实超时完成；下一步实际扩展小数据分布夹具与空领取账本，优先leased/backoff及none/all-blocked，再覆盖头阻塞，保持两worker总4次/s、各场景新库、不升压。

## P07 空领取两分布短测（工具eab6aea）

新增schema2 fixture及before/after分布计数，显式区分claim_calls与返回事件数；空结果单独记录empty且禁止生成ack，仍严格检查双方实际调用区间、publisher、14波时序及1000行最终PG。none将1000行lease_until置一天后；all-blocked只将两publisher各两分区的头（共4行）置死信，996后继保持非死信。没有删数据或停worker，不改生产SQL。分布起止严格要求租约/死信/未来available/owner计数符合夹具；这只是边界快照，不冒充全窗口连续分布采样。目标Clippy/fmt通过，原18负例加两种空场景正例及12负例通过。两上传文件SHA256一致。

p7none_0930a新库UTC17:12:58.752至17:13:14.357，p7block_0930a新库UTC17:13:35.997至17:13:51.791，均exit0/OOMfalse、1 passed/P07_PARALLEL_SMOKE_COMPLETED。每个1000行、2/5秒、两worker共4调用/s，无stats；每场景14波均实际重叠、28调用全部空（短采样20调用）、0ack、0published，最终1000行一致。none起止1000future_leased；all-blocked起止4dead；其他未来available/owner均0。严格PARALLEL_CLAIMS_CHECKED/SMOKE_ONLY，原始目录及容器/镜像/源码/日志/资源均已回收至target/server_20260929。

实际启动前、两场景之间、结束后只有5基础服务；启动前进程检索无其他编译/验收。两场景各2条资源样本、单观察区间，PG max/oom/oom_kill及scan/steal增量0，不能排除未采样峰值、不解除既有回收导致的不升压边界。连续只读Docker事件订阅在第一场景前启动，timeout180秒自然结束（约UTC17:15:58）；截至收集只含本次两个容器start/die共4条、stderr空。到期后补拉完整文件，不停止观察器，非容器活动未排除。

不重复这两个正常短测。下一步实现leased/backoff的多数受阻行加独立可领取FIFO分区（防耗尽、起止分布计数），随后补头租约/退避/死信混合和dense-ready；仍新库、小数据、总4调用/s，先严格本地正负例再短测。当前九分布及多worker正式性能尚未完成，真实超时故障仍未覆盖，原失败和P06/P09缺口保留。

## P07 leased/backoff 混合分布短测及预置失败

工具68860a4先实现900受阻行、100独立可领取行（每publisher450/50），schema3增加每publisher起止reserve，完整FIFO与最终PG核对。首轮p7lease_0930a UTC17:27:34.270至17:27:42.669，exit101/OOMfalse：预置UPDATE partition_key触发生产dbproxy_reject_outbox_content_mutation拒绝，尚未发负载。失败新库、原始日志完整保留；只有1条资源样本，资源分析器拒绝need at least two samples，不给该轮区间压力结论。

b89072c将分区直接确定在INSERT，保留生产不可变触发器，未绕过或修改生产约束；仅之后设置可变lease_until/available_at。源码上传SHA256 b17338c126427578a9e3f36f2d2f360ec2c88e17db0c60855d0b0122a1695a87远端一致。目标Clippy/fmt、旧18+空12+混合10分析负例及各正例通过。修正后新库p7lease_0930b UTC17:28:25.752至17:28:41.716，p7back_0930a UTC17:29:00.094至17:29:16.052，均exit0/OOMfalse、1 passed/结束标记。各1000行、2/5秒、两worker总4调用/s，无stats；14波全部实际区间重叠、28次唯一领取及确认（短采样20），最终1000PG行一致。

leased起止900future_leased，backoff起止900future_available；受阻发布0，owner0，各publisher独立可领取余量50→36。防耗尽与publisher/FIFO/最终28published严格通过，PARALLEL_CLAIMS_CHECKED/SMOKE_ONLY；边界快照不代表连续分布采样，不是九分布正式性能。全部三轮原始已回收target/server_20260929。成功两轮各2资源样本、单区间PG max/oom0，scan/steal分别7843和14262，有回收不升压，不排除未采样峰值。

前中后实际均仅5基础服务；连续事件p7mixed_0930a.events在首失败轮前启动、timeout180秒约17:30:34自然退出，收集时保留本组容器事件，需到期后补拉，不停止。上一p7empty事件窗口到期后已重拉仍4条自身start/die，stderr空。非容器活动不排除。下一步补小数据leased-heads/backoff-heads/dead-heads及明确dense-ready与分散ready差别，继续新库/有界预算/严格账本，不重复这两正常短测。保留首轮失败，真实数据库超时、多worker正式性能、P06高积压/P09容量仍缺。

## P07 三种头阻塞混合短测（5a554cd）

小数据leased-heads/backoff-heads/dead-heads各1000行，两publisher各450行受阻FIFO（各2个头）加50行独立可领取FIFO；只有4个头设置未来租约、未来available或死信，896后继不设该状态。分区仍在INSERT时确定，保留不可变约束；schema3每publisher reserve起止50→36、blocked_published0，28领取确认事件的FIFO/归属及最终PG完整核对。工具目标Clippy/fmt及原18+空12+混合25负例通过；上传SHA256 a86000340786ff2a8041efa317d2a70001ced8de491ec5d1690f81581ad8941b一致。

三个独立新库串行：p7hl_0930a UTC17:41:59.067→17:42:15.235，p7hb_0930a17:42:20.440→17:42:37.375，p7hd_0930a17:42:41.881→17:42:57.637。各exit0/OOMfalse、1 passed/结束标记，2/5秒两worker共4调用/s无stats，14波均实际重叠、28返回/确认、短采样20调用，1000最终PG一致；各900受阻分区记录无发布、两publisher可领余量各36，4个受阻头对应状态起止计数保持。严格PARALLEL_CLAIMS_CHECKED/SMOKE_ONLY，所有原始及容器/镜像/源码/资源已回收，不作为正式性能或高积压结论。

各仅2资源样本，PG max/oom增量0，scan/steal分别9461、4841、8404，有回收不升压，不排除采样外峰值。前/中/后实际均5基础服务；连续事件p7heads_0930a在首轮前开始、240秒约17:45:59自然结束，结束后补拉，不停止。旧p7mixed观察器到期文件已重拉，只有3轮自身start/die共6条、stderr空，宿主非容器负载不排除。

现有工具名ready实际每publisher仅2个FIFO分区、每分区250行，对应密集ready而不是旧hybrid的独立key ready；不能把名字当覆盖两分布。下一步实际补独立key ready夹具与严格合法返回顺序核对，明确保留历史ready=密集语义，再评估正式统计对照所需防耗尽/分布观测。三种头阻塞短测不重复；九分布正式性能、真实超时、P06高积压/P09容量仍缺，全部旧失败保留。

## P07 独立key ready短测（3c46528）

新增spread-ready而保留历史ready密集语义。INSERT时每publisher生成500个独立分区，跨publisher同名key；schema4起止核对每publisher总数/分区数500、未发布500→486。领取只要求合法key与event严格对应、唯一、publisher/destination/token正确及最终PG匹配，不对独立key施加不存在的全局FIFO；同波两worker交换合法返回顺序的正例通过，5个新增错误分区/耗尽/事件等负例严格拒绝，原18+12+25负例仍通过。目标Clippy/fmt通过，上传SHA256543321bb58f2a4bfdcbfe8063087967723b5434100b5c32963dca26c34657f79一致。

p7spread_0930a新库UTC17:58:06.011至17:58:21.816，exit0/OOMfalse、1 passed/结束标记。1000行2/5秒、两worker总4调用/s无stats，14波全部实际重叠，28唯一领取/确认（短采样20）、1000最终PG一致，两publisher各486未发布独立key。严格PARALLEL_CLAIMS_CHECKED/SMOKE_ONLY，原始及容器/镜像/源码/资源全回收。前后实际5基础服务，启动前进程无其他编译/验收。2资源样本单区间PGmax/oom/reclaim0，不排除未采样峰值，不解除旧升压边界。p7spread事件订阅首轮前120秒、约18:00:06自然结束，下一次补拉到期文件；p7heads已到期补拉仍6条自身start/die、stderr空，不排除非容器活动。

九类分布现各有小数据短测证据：密集ready=p7ps，独立ready=p7spread，none=p7none，all-blocked=p7block，leased=p7lease_b，backoff=p7back，三头阻塞=p7hl/hb/hd。它们分步开发、schema/观测不同，不能合并为同版本正式性能矩阵，原p7lease_a预置失败保留。当前测试仍硬编码2/5秒且正式ready需求1680调用超过1000行，混合可领100行也不足；不得直接把秒数改成120/300启动。

下一步先实际补参数与预算校验、充分但有界的新库行数/独立可领余量、统一分布/封存账本，并明确stats共享worker0现有连接（两组观测相同，非独立连接）。为保持受阻比例与防耗尽需明确正式夹具规模，不能暗增速率或默认十万行。先本地边界/负例和必要新工具短测，再决定120/300三对统计对照；九类正式性能、真实超时、P06高积压/P09容量未完成，不重复已完成同工具正常短测。
