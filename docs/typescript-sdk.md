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
- 保持`Load`、`Save`、`EnqueueSnapshot`、`ApplyTransaction`的稳定语义；
- 明确使用`bigint`表示uint64，避免JavaScript number精度丢失。

Transport负责：

- 协议版本、协议指纹和内部令牌握手；
- TCP连接池、超时与失效连接回收；
- 把服务端错误转换为`DbProxyRemoteError`；
- 原样返回DBProxy确认的Revision和事务结果。

TiangZ主仓库的`HostDbProxyTransport`是首个真实宿主实现：TCP连接池和网络I/O由Rust Host Runtime驱动，业务V8只等待Promise，不直接打开Socket，也不导入`dbproxy-storage`。玩家Payload编码、恢复顺序与Repository重试策略仍由TiangZ拥有。

## 重试约束

SDK不会自动生成或替换`requestId`、`operationId`。超时表示请求结果未知，重试必须复用原ID和完全相同的Payload。Transport可以重连后重放同一个请求，但不能创建新幂等ID。

`EnqueueSnapshot`禁止携带`expectedRevision`。它成功只表示Redis AOF backlog已经接收，不能向业务报告PostgreSQL事务已经提交。

## 协议锁

运行：

```powershell
npm run codegen:typescript
npm run test:typescript
```

生成器从`crates/dbproxy-protocol/proto/dbproxy.proto`计算SHA-256并更新`protocol-lock.ts`。Rust与TypeScript必须使用同一版本和指纹；手工修改生成文件会在后续生成时被覆盖。
