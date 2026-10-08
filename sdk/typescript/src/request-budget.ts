/** 传输必须在此剩余期限内释放/丢弃 I/O，不能只停止等待 Promise。 / Transports must reclaim I/O within this remaining budget, not merely stop awaiting it. */
export interface DbProxyRequestOptions {
  readonly timeoutMs: number;
}

/** 一次逻辑操作的单调时钟期限；子范围共用时钟并且只能缩短期限。 / A monotonic deadline for one operation; children share its clock and may only shorten it. */
export class RequestBudget {
  private readonly clock: { read: () => number; last: number };
  private readonly deadline: number;

  /** 参数校验与时钟读取均发生在准入前。 / Validates duration and clock before admitting work. */
  constructor(timeoutMs: number, read?: () => number, parent?: RequestBudget) {
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 120_000) {
      throw new RangeError("DBProxy request timeout must be an integer from 1 to 120000 ms");
    }
    if (!parent && !read) {
      if (typeof globalThis.performance?.now !== "function") {
        throw new Error("DBProxy request budget requires a monotonic clock");
      }
      read = () => globalThis.performance.now();
    }
    this.clock = parent?.clock ?? { read: read!, last: -Infinity };
    this.deadline = Math.min(this.now() + timeoutMs, parent?.deadline ?? Infinity);
  }

  /** 向下取整以避免给下游续期；不足一毫秒时停止新请求。 / Rounds down without extending downstream time; less than one millisecond admits no new work. */
  remainingMs(): number {
    return Math.max(0, Math.floor(this.deadline - this.now()));
  }

  /** 拒绝无效或倒退的时钟，不用可能跳变的墙钟替代。 / Rejects invalid or reversed clocks instead of falling back to wall time. */
  private now(): number {
    const now = this.clock.read();
    if (!Number.isFinite(now) || now < this.clock.last) {
      throw new Error("DBProxy request budget requires a finite monotonic clock");
    }
    this.clock.last = now;
    return now;
  }
}
