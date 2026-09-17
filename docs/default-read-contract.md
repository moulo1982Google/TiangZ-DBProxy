# 默认权威读取 / Authoritative reads by default

## 契约

| 入口 | 语义 |
| --- | --- |
| `load/load_multi`；TS `Load/LoadMulti` | 读取PG主库数据库快照中可见的已提交状态；失败返回错误，不使用缓存兜底 |
| `load_cached/load_cached_multi`；TS `LoadCached/LoadCachedMulti` | 显式允许旧缓存；可指定版本下限，不满足时回源PG |
| 保活玩家复用 | 使用原权威Server的玩家内存，不调用存储恢复接口 |

默认LoadMulti通过一个请求分片连接执行一条SQL，整个批次共享语句快照，保持输入顺序及缺失位置，不按分片拆成多次查询。多次独立Load不提供共同快照。数据库连接必须指向同一个PG主库，不能指向异步只读副本。

严格读取不包括未保存的Server内存、尚未落库的Enqueue，以及数据库语句快照建立后才提交的更新；也不保证读取返回之后永远是最新。角色仍驻留时重连复用内存；从存储恢复时才读取PG。跨服接管仍须完成所有权交接、旧所有者隔离和必要保存，读PG不代替这些步骤。

PG已确认提交后，缓存失败不会将写入改为失败。缓存修复沿用持久队列、有界重试、死信及观测；revision-aware写入与租约token继续防止旧任务覆盖新版本。PG提交结果未知时保留原操作号和完整请求重试，不能直接宣称成功或换号重做。

## 显式缓存读取

```rust
let snapshot = client.load(&record).await?;
let relaxed = client.load_cached(&record, None).await?;
let fenced = client.load_cached(&record, Some(Revision(101))).await?;
let batch = client.load_cached_multi(&records, &minima).await?;
```

线协议保持原RPC，新增`allow_stale`，默认false。单条支持`min_revision`；批量`min_revisions`为空或逐项对应，0表示不设下限。负缓存/缺失不能满足正下限。任一下限不满足时整批回源PG，PG仍未达到下限时返回可重试的`STORAGE_UNAVAILABLE`，不无限轮询。下限只用于知道版本的调用方，不是登录恢复的前提。显式缓存读取本身不承诺跨记录同一快照。

TS Transport新增可选`loadCached/loadCachedMulti`适配入口；宿主尚未实现时SDK明确拒绝，禁止忽略下限或退回旧接口伪装支持。现有TiangZ Host继续使用默认Load，恢复路径不需要接入缓存入口。

兼容保留`authoritativeReadNamespaces`：默认读取已经全量权威，该字段只额外阻止匹配namespace的显式缓存读取；批量任一匹配则整批读PG。空列表不会恢复旧的默认缓存行为。

## 升级与调用方检查

这是默认语义变更，必须重建并重启所有DBProxy节点，包括备用endpoint及多租户实例。旧客户端可连接升级后的服务端，省略新字段仍得到权威读取。服务端接受明确列出的旧指纹；新客户端要求新指纹，拒绝旧服务端，不能放宽握手。全部节点升级前不能宣称整个部署已满足新契约。协议代码/SDK指纹必须由生成器更新。

TiangZ Repository与MMORPG恢复路径使用Load/LoadMulti及直接Save/事务；本次检查未发现这些恢复路径依赖Enqueue立即可读。保活与最终保存仍由业务生命周期管理。故障soak的queued记录先直接保存种子，再异步Enqueue；运行期间检查存在，最终对账等待落库，没有将Enqueue的序号当作已提交revision。

任何新增`Enqueue → Load`流程只能看到查询时已提交状态；入队ACK不能作为立即读取成功的依据。需要立即恢复的状态使用直接保存/事务，或由业务另设明确的持久检查点。不得把读取到旧的已提交状态误判为数据丢失，也不能因入队成功就返回尚未落库的新状态。

## 验证与容量

默认workspace测试不包含需真实PG/Redis的ignored项。真实测试须在隔离容器中串行执行，不能暂停业务Redis。回归覆盖旧正缓存、负缓存、缺失、PG失败不退缓存、单条与批量、版本下限、独立节点及新建后端、缓存写超时后仍成功写入且权威读取正确。测试结果另附，不能继承旧namespace方案的历史验收结论。

容量看冷登录、进程恢复、批量接管的读取峰值，分别记录PG连接等待、查询耗时、恢复总时长、P50/P95/P99和错误。在线玩家数不能直接换算为PG查询数，保活玩家复用不应产生快照读取。已有存储暖读微基准不代表完整登录能力；新默认契约的端到端容量与外网长稳需单独验收。


## 本轮实测证据（2026-09-17）

`node tools/test_authoritative_reads.mjs`在专用临时PG/Redis完成：真实TCP权威读取三轮通过，每轮包含缓存写暂停、版本栅栏、PG锁阻塞，以及100次双记录原子更新与100次批量读取并发校验；随后五个storage回归通过，含原`acknowledged_write_timeout_must_not_expose_old_cache`。该测试仍标为ignored以隔离真实服务依赖，但已经由控制器显式执行并通过，不再是未修复的已知缺口。

证据目录`target/authority-read-W2M1FK`，报告`report.json`；TCP测试二进制SHA256为`8c5071e0f4ad76bf9d85aa4cb72d9db10ee66807975d7290615f5158a81867a4`，storage测试二进制为`cc38e8d8e517f1d2bd995efd2eacda9c3b3d838bbe98f942840377f98ff6b7ef`。两容器正常停止，退出0，数据未删除。target不纳入Git，迁机须另存证据。新建后端连接验证没有进程内版本记忆仍能正确读取；这不等于已完成操作系统级进程强杀验收。

SDK新增默认路径隔离与显式缓存栅栏回归，21项通过。生成器已更新协议指纹；旧指纹列为服务端显式兼容别名，新客户端不会连入旧服务端。未部署外网、未启动长稳，也没有测定冷登录/进程恢复/批量接管的生产容量。

最终检查：`cargo test --workspace`通过（日志`target/default-read-workspace-tests.log`）；`cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings`及`cargo build --release -p tiangz-dbproxy-server --bin tiangz-dbproxy-server`均通过。TiangZ仅更新三份契约文档，没有更改其业务源码或执行其Runtime长稳。
