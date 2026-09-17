# 角色恢复的权威读取（2026-09-17）

> 后续决策已改为全namespace默认权威读取；下文保留此前按namespace选择方案及其测试历史，不再代表当前默认行为。当前契约见[默认读取契约](default-read-contract.md)。历史测试结果不能替代新版本验收。


## 本轮选择

不移除Redis、不拆新连接池、不改网络协议。新增启动配置`storage.authoritativeReadNamespaces`，按精确namespace选择PG权威读取。默认空数组保持旧行为；未配置的读取仍可能命中过期/落后的缓存，本轮没有修复所有缓存一致性问题。

```json
"storage": {
  "backend": "postgresRedis",
  "postgresUrlEnv": "DBPROXY_POSTGRES_URL",
  "redisUrlEnv": "DBPROXY_REDIS_URL",
  "authoritativeReadNamespaces": ["player"],
  "shards": 2,
  "postgresConnectionWaitTimeoutMs": 2000,
  "cacheFallbackTimeoutMs": 2000
}
```

完整文件为`configs/authoritative-player.example.json`。字段最多64个名字，名字非空、不带首尾空白、最多256个UTF-8字节。不使用通配符或前缀匹配；业务namespace在部署配置中声明，DBProxy没有内置玩家/游戏特例。

单条Load命中策略后不访问Redis；LoadMulti中任一记录命中，则整批经首个记录所属请求分片，在一个PG语句快照内读取，保留顺序和缺失位置。混合批次不会分成缓存半份+PG半份，也不拆成多个分片查询跨越原子提交。未回填缓存：读取成功不依赖Redis，写入和原有repair机制仍保持原样。多次独立Load不提供跨请求事务快照。

PG失败或读取超时必须返回存储错误，禁止降级旧缓存、将失败变成None、创建默认资产。它保证的是语句开始时可见的已提交记录，不等待普通Enqueue backlog排空，不承诺读取请求之后发生的新提交。

## 生命周期结论

MMORPG已存在正确下线路径：保活时通过原Actor恢复；保活过期后，Location进入removing → `unit.Offline` → `SaveOnOffline/SaveDomains/SaveMulti`确认PG提交 → 删除Location → 删除权威玩家索引 → 定时器延迟销毁Actor。

最终保存未返回或失败时不移除角色；失败恢复后允许重试，持久化组件保留原pending请求ID。当前Repository不使用Enqueue，因此不必增加队列排空机制。SaveMulti仍允许领域级部分成功，不能把它称为整组原子保存；关键经济事务继续走原来的事务接口。进程在周期保存前崩溃的内存变化不在此保证内。

未来业务如使用Enqueue，必须另设计已提交检查点/恢复顺序，不能把AOF入队ACK当成PG落库。此改动没有提供任意队列写入的read-your-writes保证。

## 并发与预算

- 复用已有固定shards连接、PG锁等待预算；没有额外连接池/无限重试。
- 权威读的外层`cacheFallbackTimeoutMs`覆盖取得连接、重连和查询，内部锁等待仍受`postgresConnectionWaitTimeoutMs`约束。复用旧字段避免增加一组旋钮，名字中的cache并不代表该分支访问缓存。
- 客户端池等待和网络另有预算，不能把上述时间当完整登录上限；这些预算应小于业务RPC剩余期限。读超时取消future不等于PG立即中断已发送SQL。
- 活跃请求连接上限受shards限制；维护连接另计。多DBProxy实例、多租户共享同一PG时要合并预算并留后台任务余量。本次不把存储基准的4写死成部署默认。
- 复用`postgres_connection_wait`/`postgres_operation`观测，不计为缓存命中，也不虚增缓存miss/fallback计数。尚未新增独立authority模式吞吐指标。

## 接入与升级

1. 构建新DBProxy：`cargo build --release -p tiangz-dbproxy-server --bin tiangz-dbproxy-server`。
2. 离线校验：`target/release/tiangz-dbproxy-server --check-config configs/authoritative-player.example.json`，Windows可加.exe。
3. 所有共享存储的DBProxy节点（含failover）部署相同namespace策略，维护窗口重启。只改一台会在切换后重新出现旧读；不可称滚动期间已具有完整保证。
4. Examples的本机game故障组生成配置已启用`player`；dbproxy故障组保留原缓存语义，避免掩盖七天测试缺口。三组全量验收尚未运行。
5. SLG配置源码启用`slg.demo.player.v1`和`slg.demo.world.v1`；须重新构建镜像后生效。没有操作当前运行容器。当前SLG Demo持久模式每次命令/定时结算都Load，并非正式保活内存Actor架构；这个配置会让那些读取也走PG，不仅是登录。其性能需另测，不能复用MMORPG保活结论。

协议和SDK接口未改，旧二进制会拒绝新JSON字段，不能删除配置字段掩盖版本不匹配。TypeScript SDK标准测试会执行现有协议锁生成器，本轮协议产物没有变更。

## 测试与复现

```powershell
# DBProxy根目录；只创建专用临时容器，结束后停止并保留容器
node tools/test_authoritative_reads.mjs
cargo test --workspace
npm.cmd run test:typescript

# Examples根目录
npx.cmd --no-install vitest run tests/unit/final_offline_storage_boundary.test.ts tests/unit/retained_player_storage_boundary.test.ts tests/unit/gate_recovery_destination.test.ts tests/unit/map_host_disposal_race.test.ts tests/legacy/player_persistence_self_test.test.ts
```

最终真实TCP测试`target/authority-read-T1yI40`，同一隔离PG/Redis上连续3轮通过，两个容器停止退出0。二进制SHA256 `8d727958e92a764237644a4bd9c01d2d9f877685ed47c7d3be2cd3585879d2bd`。原始报告、diff、测试源码副本保留在目录；target不进Git，换机需另行复制。此前三轮在`target/authority-read-UgcB5e`（调整配置集合布局前）与`target/authority-read-FbJ3lC`（未加混合批次断言），均不替代最终版本。

覆盖：人为构造PG新/缓存旧；PG存在/缓存负值；缺失记录；顺序；无已知revision读取；默认缓存路径不变；混合批次一个PG操作；PG排他锁阻塞时不回退缓存、解锁后恢复。通过真实Rust客户端和TCP服务，不是纯mock。

Examples相关22项测试通过（包含旧持久化self-test内部多断言）；最终保存等待/失败/重试与保活边界为控制流替身测试，不是游戏端到端耗时。DBProxy默认workspace测试通过，真实存储ignored项不算通过；本轮单独运行了上述权威读测试。SDK 18项、演练控制器10项通过。最终严格Clippy、全workspace格式检查、release服务端构建通过；配置集合布局调整后重跑配置17项及真实TCP三轮。新release二进制的示例配置和SLG配置离线检查通过，未开启业务监听或读取凭据。

未覆盖：完整登录三路径耗时、SLG新镜像强杀回归、500玩家长稳、24小时验收、真实Redis写暂停的整链路复跑。旧缓存回归仍保留为已知缺口；不要宣称所有namespace的旧读已经解决。

开发期两次编译错误已修正：ConfigError不是From<&str>，需显式构造；SnapshotWrite.updated_at_unix_ms为u64，测试不能按i64填写。编译失败时没有执行旧二进制冒充新测试。

严格Clippy首次发现配置enum增加Vec后超过大小阈值，以及上轮基准多余async包装；改为固定Box切片及直接spawn连接future，未抑制告警。随后重建服务端并重跑上述配置/真实读取验收。没有提交或推送代码。
