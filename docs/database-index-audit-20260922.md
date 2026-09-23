# DBProxy 全库索引与查询审查（2026-09-22）

2026-09-22 普通回执：迁移 015 新增数据库记录时间及清理索引，每租户独立连接分批清理超过 7 天的普通回执，并保护并发重试；事务回执和业务数据不删除。规则、升级与验证见[回执保留](record-deletion-and-receipt-retention.md)。

2026-09-22 Outbox 后续：已采用少量候选优先、按组查找兜底的领取查询，无新增表或索引；九种积压分布和顺序并发通过实测，成本及剩余限制见[Outbox 查询验证](outbox-hybrid-validation-20260922.md)。


状态：全库静态审查与第一批修复完成；用户准备 Docker 并授权后，已使用手册的 PostgreSQL 18.4 / Redis 8.8.1 镜像完成 35 项真实依赖复验，见[验证记录](database-index-validation-20260922.md)。基线为本地 `e54b545`；远端 `bdc278e` 的回执保留期文档已阅读，普通回执 7 天清理已由后续迁移 015 实现，见本文顶部链接。本文不代表线上数据库实际具有这些索引，也不代表容量测试通过。

## 结论与优先级

第二批已在 PG 18.6 上完成批量快照读取和队列到期筛选改写，新增查询分布测试，见[第二批验证](index-query-validation-20260922.md)。批量全分区扫描及大量延后任务的范围查找已改善；全部可领取、有效租约积压和死信头阻塞仍有扫描成本。

维护范围必须包含全部表、SQL、后台统计、迁移及启动检查，不能只为回执清理增加一个索引。当前不是“所有表都没索引”：18 张逻辑表都有主键声明，快照另有 32 个叶子分区；但已有索引并未覆盖所有实际访问路径。

第一批已处理以下三类明确问题，再落地已确定的普通回执保留 7 天：

1. 旧 Trade 回执恢复按 operation_id 查询账本、Outbox，缺少对应索引。
2. Outbox 全局统计没有外层过滤，每轮都会处理历史数据。已拆分活跃集合和死信集合；统计与队列仍共用维护连接，隔离和超时属于后续工作。
3. 启动检查没有统一验证关键索引的存在、定义和有效状态。

批量快照读取、队列领取顺序与索引匹配、重复索引是否值得保留，需要执行计划证据后决定。不能见到一个 WHERE 字段就增加一个单列索引，也不能通过禁用顺序扫描掩盖 SQL 问题。

## 逐表核对

以下名称均省略 `dbproxy_` 前缀。主键也会建立索引；“覆盖”仅指声明的结构能支持查询，并不保证优化器在所有数据分布下都选它。

| 表 | 当前访问与索引 | 判断 / 后续工作 |
| --- | --- | --- |
| snapshots | 完整 `(namespace, record_key)` 主键与 HASH 分区；单条读取、条件更新按完整键定位 | 批量读取已改为逐键 LATERAL 查询；十万条样本的 1/30/64 条参数验证通过 |
| idempotency | 按 request_id 查询、去重、更新；request_id 主键 | 请求定位有主键；迁移 015 已增加 recorded_at 与 (recorded_at, request_id) 清理索引，覆盖 7 天分批清理 |
| transactions | 按 operation_id 提交、恢复；operation_id 主键 | 当前按号查询有覆盖；未实现按时间清理，不预加时间索引 |
| multi_transactions | 按 operation_id 查询结果、记录数；operation_id 主键 | 当前查询有覆盖；表增长与保留策略另行设计 |
| multi_transaction_records | 按 operation_id 查询，按 namespace、record_key 排序；三列复合主键，另有 operation_id 单列索引 | 复合主键覆盖这条路径；单列索引覆盖重叠但更小，验证收益后再决定删除 |
| multi_transaction_effects | 按 operation_id 比较完整效果；operation_id 主键 | 当前查询有覆盖；不可变内容保留策略不属于普通回执清理 |
| operation_claims | 按 operation_id 注册、校验操作种类；operation_id 主键 | 当前查询有覆盖；claimed_at 不参与当前定时清理，无依据为其加索引 |
| trades | 按 trade_id 查询、加锁与更新；trade_id 主键 | 当前查询有覆盖 |
| trade_operations | 按 operation_id 恢复；operation_id 主键及 `(trade_id, new_trade_version)` 索引 | 当前按号查询有覆盖；额外索引不未经验证就移除 |
| trade_operation_records | 按 operation_id 查询并按 namespace、record_key 排序；三列复合主键 | 当前查询有覆盖 |
| ledger_postings | posting_id 主键；trade_id、account_id 相关复合索引；实际恢复按 operation_id 查询并按 posting_id 排序 | 迁移 013 已增加 `(operation_id, posting_id)`，十万条历史验证通过 |
| outbox | event_id 主键，ready、dead-letter、未发布消息组顺序索引 | 迁移 013 已增加 `(operation_id, event_id)`；统计已拆分；领取复杂度待专项验证 |
| cache_repairs | RecordKey 主键；`(available_at, requested_at)` 活跃索引和死信索引 | 按键修改有覆盖；领取按 requested_at 排序，和索引首列不一致；积压时验证排序/扫描成本 |
| append_records | RecordKey 主键，operation_id 索引 | 当前追加冲突检查有覆盖；尚无通用扫描查询 API，不按假想条件补索引 |
| outbox_publishers | publisher_id 主键，注册与读取按此定位 | 当前查询有覆盖 |
| outbox_routes | route_key 主键，`(producer, route_version)` 唯一约束 | 按路由定位有覆盖；启动收集 publisher 是小配置表扫描，不因全扫描就判错 |
| outbox_admin_audit | id 主键，当前运行代码仅追加审计 | 未实现按事件/日期检索；以后新增查询时同步设计对应索引 |
| schema_migrations | version 主键，按版本检查、登记 | 小规模元数据表，现有访问有覆盖 |

审查时表定义依据：`crates/dbproxy-storage/migrations/000` 至 `012`，本轮新增 `013`；实际 SQL 依据：storage 的 lib、trade、commit、cache_repair、outbox、relay、outbox_admin 模块，以及 server 的统计轮询入口。

## 审查时的问题及修复依据

### 1. 交易回执恢复缺少两条访问路径

`trade.rs::load_receipt_parts` 执行：

```sql
SELECT ... FROM dbproxy_ledger_postings
WHERE operation_id = $1 ORDER BY posting_id;

SELECT ... FROM dbproxy_outbox
WHERE operation_id = $1 ORDER BY event_id;
```

现有账本的 trade/account 索引、Outbox 的队列索引都不以 operation_id 开头。外键声明也没有额外建立这两条索引。这影响 LoadTradeTransaction，也影响重复提交时的完整内容比对。缺少索引可能表现为扫全表，也可能沿 posting/event 主键读取大量无关记录，并非只能出现一种执行计划。

整改候选固定为 `(operation_id, posting_id)` 和 `(operation_id, event_id)` 普通复合索引，不把 payload 放进索引。Outbox 索引不能限于未发布消息，否则无法恢复包含已发布事件的旧回执。使用新增迁移安装，不篡改已执行的历史迁移；迁移号依最终合并顺序分配，不能与计划中的保留期迁移冲突。

### 2. Outbox 监控处理完整历史

`outbox.rs::stats` 把过滤条件写在各个 COUNT/MIN 的 FILTER 中，外层 `FROM dbproxy_outbox` 没有 WHERE。`main.rs` 以 5 秒间隔启动统计轮询，单轮执行时间还会增加实际周期。结果只要少数计数，但查询工作量仍可能随全部历史消息增长。

同轮还执行 `source_stats` 的未发布消息分组统计，存在重复统计成本；`cache_repairs::stats` 也需要遍历当前修复/死信集合。专用维护连接避免了直接占用请求连接，但并未消除数据库 CPU/I/O 成本，而且统计会与使用同一连接的队列领取/确认相互等待。

整改应先把需要的集合表达在可用索引的外层条件中，并合并能复用的统计。保留现有死信计数语义：不得简单添加 `published_at IS NULL` 就漏掉原来会计入的历史死信；可拆分活跃集合与死信集合。活跃集合即使有索引，精确 COUNT 仍需处理集合成员，因此还要设计统计超时、采样频率和连接隔离。精确统计超时不应直接等同于 PG 不可达；健康探测须与昂贵统计区分。

### 3. 缺少全库关键索引校验

现有 `migrate` 在迁移锁下执行未登记迁移，并验证快照分区布局；迁移登记不证明后来没有人删除或错误重建索引。现有测试只额外检查少量 Outbox 索引名称，不能覆盖全库。

维护应建立“表—实际查询—必需索引”的明确清单。新库先完成建表和索引，再允许服务及 worker 工作；已有迁移记录的库仍应检查主键、唯一约束、关键辅助索引和 32 个叶子分区索引。检查列顺序、唯一性、部分索引条件、`indisvalid`/`indisready`，不能只认名称或 `IF NOT EXISTS`。

清理索引缺失时不得启动清理；业务必需主键或恢复索引不满足时启动失败并指出对象。不得在业务运行中隐式重建大索引。巡检低频读取系统目录，不通过业务全表扫描验证索引存在。

## 需要测量后决定的事项

- **批量恢复**：第二批已确认原 SQL 在十万条样本、30/64 个参数时扫描全部分区，完成逐键查找改写并验证结果顺序、不同 namespace、缺失位置及 payload。单语句视图和服务端重复键拒绝规则不变；其他数据规模、冷盘和容量结论仍需单独测量。
- **Outbox 领取**：已有[专项记录](outbox-claim-index-review.md)显示，新排序索引在正常分布下加速，却让大量死信阻塞场景退化。复用这些反例，不恢复迁移 011 已删除的旧索引，也不直接补 enqueue_order 单列索引。
- **缓存修复领取**：第二批已测可领取、延后重试、已租用、大量死信；仅调整到期筛选以使用现有日期索引，没有改变请求顺序。大量可领取任务仍排序，已租用积压仍需过滤，尚未解决这些成本。
- **重叠索引**：多记录明细的 operation_id 单列索引与主键前缀重叠。先比较读计划、索引大小和写入成本，不仅凭结构重叠就删除。
- **容量维护**：记录表/索引大小、死行、自动清理进度与查询耗时。索引不能解决无限增长，也不应给所有 revision、日期、payload 一律建索引。

## 实施次序与验收边界

1. 修复交易回执索引缺口和 Outbox 历史扫描；引入全库关键结构校验。
2. 在隔离测试库验证所有业务与后台查询，结果回填本文；只有证据支持的候选索引才进入迁移。
3. 再实现普通回执 7 天保留，使用同一套索引检查、查询验证、后台超时和监控约定。事务回执与业务事实不随之删除。

验收覆盖：空库初始化、迁移后重复启动、删失索引、同名错误索引、分区索引失效、多租户独立数据库、索引存在但执行计划不合适，以及查询结果/事务语义回归。隔离性能样本覆盖“大量无关历史＋少量目标记录”、大量活跃消息、延后重试和死信阻塞。对比执行计划实际扫描量、BUFFERS、耗时和新增索引的写入成本；不承诺有 LIMIT 就只读指定行数，也不禁止小表合理顺序扫描。

第一批已增加两项查询索引、全库启动检查及只读巡检命令，并改写 Outbox 全局统计。真实测试覆盖空库初始化、重复启动、缺失/错误/无效索引、分区脱离、十万条历史查询，以及现有存储、缓存修复并发和网络回归。多个隔离测试库分别完成初始化；多租户服务整体压力、所有查询的数据分布组合与写入成本仍需后续测量。详细证据及复现命令见[验证记录](database-index-validation-20260922.md)。

第三批已完成缓存修复领取查询和迁移 014，包含七种十万条积压分布、顺序/锁验证及新增索引写入成本；见[缓存修复验证](cache-repair-index-validation-20260922.md)。Outbox 查询改写及普通回执清理已在后续批次落地，见本文顶部验证链接。
