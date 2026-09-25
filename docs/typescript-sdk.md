# TypeScript SDK

`@tiangz/dbproxy-sdk`只提供与运行时无关的持久化契约，不直接打开TCP连接。每种宿主只需要实现一次`DbProxyTransport`，领域Repository不应知道Transport如何连接DBProxy。

## 固定调用层次

```text
领域Repository
  -> DbProxyClient
  -> DbProxyTransport
  -> DBProxy TCP服务
```

`DbProxyClient`负责：

- 校验RecordKey、Schema、Revision和幂等ID；
- 在跨Transport边界前复制Payload，防止调用方继续修改缓冲；
- 保持`Load`、`Save`、`EnqueueSnapshot`、`ApplyTransaction`、`LoadTransaction`的稳定语义；
- 提供`LoadMulti`批量恢复1至64条不重复记录，并保持缺失记录的响应位置；
- 提供`SaveMulti`和`EnqueueMultiSnapshot`批量写入1至64条不重复记录，并保持逐记录结果顺序；
- 提供`ApplyMultiTransaction`和`LoadMultiTransaction`，并拒绝重复RecordKey和超过256条记录的请求；
- 提供`LoadTrade`、`ApplyTradeTransaction`和`LoadTradeTransaction`，在进入Transport前校验状态迁移、重复ID、int64 Posting和按asset零和；
- 明确使用`bigint`表示uint64，避免JavaScript number精度丢失。

Transport负责：

- 协议版本、协议指纹和内部令牌握手；
- TCP连接池、超时与失效连接回收；
- 把服务端错误转换为`DbProxyRemoteError`；
- 原样返回DBProxy确认的Revision和事务结果。

TiangZ主仓库的`HostDbProxyTransport`是首个真实宿主实现：TCP连接池和网络I/O由Rust Host Runtime驱动，业务V8只等待Promise，不直接打开Socket，也不导入`dbproxy-storage`。玩家Payload编码、恢复顺序与Repository重试策略仍由TiangZ拥有。

## 重试约束

### 0.7 候选：一个逻辑操作共用预算

`const operation = client.WithRequestBudget(5000)` 返回独立客户端，不改变原客户端或其他并发操作。范围内的所有读取、保存、业务退避与同 ID 重试消耗同一期限；`GetRemainingRequestBudgetMs()` 可用于判断后续退避是否还放得下。默认使用 Transport 的 `requestTimeoutMs`（缺省 5000），允许整数 1..120000ms，嵌套范围只能缩短父范围。剩余毫秒向下取整，不足 1ms 不再准入。

Transport 必须声明 `supportsRequestTimeout: true` 并实现各方法最后一个可选参数 `DbProxyRequestOptions { timeoutMs }`，让真实 I/O/任务在该剩余预算内结束或取消；仅用 Promise.race 停止等待不符合此能力。自定义宿主通过 `monotonicNowMs()` 提供单调时钟，Node 默认使用 performance.now，裸 V8 必须显式提供时钟，不能用会跳变的 Date.now 替代。普通未建立范围的调用继续兼容旧 Transport，建立预算范围时缺失能力明确失败。

SDK 的校验和防御性复制也消耗预算；旧调用不自动产生幂等号或新增重试。预算耗尽保守抛出 `DbProxyRemoteError(StorageUnavailable)`，先前写入仍可能已提交，必须保留原请求号与同一载荷恢复。作用域不抢占同步 CPU；物理任务终结和迟到响应处理仍属于 Transport。TiangZ 候选 Host 使用 Rust Instant 绝对期限覆盖 TS 转换、Host 排队和 SDK I/O，Repository 在入口创建一次范围。

验证：`npm run test:typescript` 29 条通过，新增 8 条包含并发/嵌套、16 类调用传递、过期拒绝、单调时钟及防御性副本；6 个初始接口红测与绿色输出在 `target/test-results/v0.7-ts-budget-api-{red,green}.log`。版本号暂未发布为 0.7；候选由 npm pack 正规构建供明确选择的宿主联调，不能将此状态写成已经发布。

兼容性补充：未创建预算范围时，Transport 的参数个数也保持原状，不附加一个可观察的 undefined 参数；宿主 CommitRecords 的既有断言在联合测试中检出了该差异。原断言保留，增加普通 SDK 调用参数形状检查，29 条复测通过（`target/test-results/v0.7-ts-budget-legacy-args.log`）。

SDK不会自动生成或替换`requestId`、`operationId`。超时表示请求结果未知，重试必须复用原ID和完全相同的Payload。Transport可以重连后重放同一个请求，但不能创建新幂等ID。

如果调用方进程在事务提交后、应用内存状态或返回RPC前崩溃，恢复路径可以用`LoadTransaction(operationId, record)`或`LoadMultiTransaction(operationId, records)`读取第一次提交保存的`newRevision/result`。多记录查询必须提供原始完整记录集合；同一个operationId不能换一组记录读取，也不能用回执查询替代正常的业务校验。

跨记录事务示例：

```ts
const result = await client.ApplyMultiTransaction({
  operationId: "trade:request-1001",
  writes: [buyerWrite, sellerWrite],
  result: new TextEncoder().encode("trade-committed"),
});
```

业务 Repository 负责先在内存中校验并构造 `buyerWrite/sellerWrite`，DBProxy 只负责整组 Revision/CAS 和原子落库。不要在 DBProxy 中追加业务步骤，也不要因为 Endpoint 切换而更换 `operationId`。

需要交易单、托管、账本和事件原子提交时使用交易接口：

```ts
const result = await client.ApplyTradeTransaction({
  operationId: "trade:1001:escrow",
  transition: {
    tradeId: "trade:1001",
    expectedVersion: 0n,
    nextState: "escrowed",
    payload: encodedOrder,
    updatedAtUnixMs: now,
  },
  writes: [buyerWallet, sellerInventory],
  ledgerPostings: [
    { postingId: "p1", accountId: "buyer", asset: "gold", amount: -100n, metadata: empty },
    { postingId: "p2", accountId: "escrow:1001", asset: "gold", amount: 100n, metadata: empty },
  ],
  outboxEvents: [{
    eventId: "e1",
    topic: "trade.escrowed",
    partitionKey: "trade:1001",
    payload: encodedEvent,
    occurredAtUnixMs: now,
  }],
  result: encodedReceipt,
});
```

所有 `Uint8Array` 会在跨 Transport 边界和返回 SDK 前防御性复制。SDK 的校验是尽早发现调用错误，不能替代服务端业务规则和 PostgreSQL CAS；Outbox 消费者仍必须按 event ID 去重。

`EnqueueSnapshot`禁止携带`expectedRevision`。它成功只表示Redis AOF backlog已经接收，不能向业务报告PostgreSQL事务已经提交。

`SaveMulti`不是事务。调用方收到部分成功结果时，必须保存成功条目的新revision，再处理失败条目；不能因为一个领域失败就把其余领域当成未提交。`EnqueueMultiSnapshot`同样只适用于允许小范围回退的普通状态。

## 协议锁

运行：

```powershell
npm run codegen:typescript
npm run test:typescript
```

生成器从`crates/dbproxy-protocol/proto/dbproxy.proto`计算SHA-256，并从Rust协议crate读取权威`PROTOCOL_VERSION`后更新`protocol-lock.ts`。Rust与TypeScript必须使用同一版本和指纹；手工修改生成文件会在后续生成时被覆盖。
