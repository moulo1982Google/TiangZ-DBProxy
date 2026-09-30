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
