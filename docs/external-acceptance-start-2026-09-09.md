# 缓存修复与客户端切换加固：外网验收启动记录

本记录仅覆盖北京时间 2026-09-09 00:47 启动时的部署和验证事实，**不代表 12 小时验收已通过**。后续结论必须依据本轮最终对账和故障恢复证据。

## 固定版本与切换

- DBProxy：`b6977ffd4c41ab468725e0a3dcd5447056c0a4f3`。
- TiangZ：`97f6f7c1a0c5bd489afa614c18166aefe729dbc2`，重新编译并链接本次 DBProxy Rust 客户端；服务端、故障负载和 Relay 工具也重新编译。
- 两仓库使用冻结提交的 Git archive，远端源码树、上传哈希和部署二进制校验通过。未混入主工程正在进行的开发。
- 旧轮次 `validation-20260908-12h-194205` 人工中止，日志和 `operator-stop.json` 保留。停止时动态地图探针已经通过，但原角色的新鲜业务恢复轮次尚未收齐；控制器收到 SIGTERM 后记录 `action_failed`，再记录 `baseline_recovered` 和 `orchestrator_stopped`。此前通过 6 次故障动作，旧轮次不能计为完整通过。
- 先停止旧定时器、故障注入和验收负载，再停止全部游戏进程及两个 DBProxy 实例；确认旧写入进程退出后，备份数据库、二进制和配置，再执行部署。
- 迁移 010 `cache-repair-leases`、011 `outbox-index-cleanup` 成功。租约全局序列为 BIGINT / NO CYCLE，旧 `dbproxy_outbox_partition_order` 索引已不存在。
- 两实例 `/ready` 成功，连接上限指标均为 256，观测端口仍绑定本机。短验观察到 54 条连接，超限拒绝为 0。

## 启动前验证

- Linux DBProxy workspace release binaries 构建通过。
- TiangZ `npm run verify` 完整矩阵 9/9；内部 quick 24/24、check 15/15。
- 首轮构建遗漏生成 TypeScript SDK，导致类型检查失败；补充 `npm ci` 的 SDK prepare 步骤后完整重跑通过。失败日志保留为 `build-missing-sdk.log`，未修改代码或放宽断言。
- 100 名玩家在两个地图完成共 6 个健康轮次，保持原账号和角色，无账号替换。
- DBProxy 短验完成 21,560 次读取、3,500 次 enqueue、1,780 次事务、280 次交易。读取、写入、交易及一致性错误均为 0，最终对账通过。
- Relay 完成 89 次提交，89 条 Outbox 全部发布、0 死信；178 条快照、89 条事实记录与 Redis Stream 唯一事件数和顺序核对通过。
- Node 10 玩家探针通过；短验综合 12 项检查全部通过。短验未注入故障，不能替代正式长时间验收。

## 正式轮次与证据

轮次为 `validation-20260909-12h-004723`，开始时间为北京时间 **2026-09-09 00:47:23**，计划结束 **12:47:25**，综合汇总定时器为 **12:47:55**。启动观察时，玩家长跑、DBProxy soak、Relay、故障注入和日志审计五个服务均为 active/running，重启次数为 0；两个收尾定时器已启用，指标读取和日志捕获无错误。

正式负载为 100 玩家及独立 DBProxy 负载。故障计划覆盖缓存 Redis、持久 Redis、PostgreSQL、DBProxy 切换、地图进程和联合存储故障；预热 15 分钟，间隔 25–35 分钟，联合故障在 3 小时后允许执行。动态地图丢失允许恢复至安全静态地图，仍要求验证原角色、持久状态和可玩性。

远端证据位于 `/var/log/tiangz-chaos/validation-20260909-12h-004723`，部署证据位于 `/srv/tiangz-build/20260909-dbproxy-hardening/evidence`。最终查看本轮 `acceptance-final.json` 及其引用的游戏、故障、soak、Relay 和日志审计结果；不复用旧轮次的通过结论。

启动证据包 `dbproxy-external-20260909-start-evidence.tar.gz` 的 SHA-256 为 `232a6393d2240276fb626bb81f1f5891922e37e8a87d0a319ffc595052702ff9`。证据包、配置凭据及数据库备份不纳入 Git；数据库与敏感配置备份仅保存在远端受限目录。
