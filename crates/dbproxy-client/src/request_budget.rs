//! 单次逻辑请求的墙钟期限和结果确定性，跨排队、重连与重试保持同一所有者。 / Owns one logical request's deadline and outcome certainty across queues, reconnects and retries.

use std::time::Duration;
use tokio::time::Instant;

use super::ClientError;

pub(super) struct RequestBudget {
    pub(super) deadline: Instant,
    may_have_sent: bool,
}

impl RequestBudget {
    /// 在 API 开始执行时创建一次期限；零值和无法表示的期限属于配置错误。 / Starts one deadline when the API is polled and rejects zero or unrepresentable durations.
    pub(super) fn new(duration: Duration) -> Result<Self, ClientError> {
        if duration.is_zero() {
            return Err(ClientError::InvalidConfig(
                "request timeout must be positive",
            ));
        }
        let deadline = Instant::now()
            .checked_add(duration)
            .ok_or(ClientError::InvalidConfig("request timeout is too large"))?;
        Ok(Self {
            deadline,
            may_have_sent: false,
        })
    }

    /// 已到期时禁止立即 ready 的许可、锁或写操作继续发包。 / Prevents an immediately-ready lock or write from sending after expiry.
    pub(super) fn check(&self) -> Result<(), ClientError> {
        if Instant::now() >= self.deadline {
            Err(self.timeout_error())
        } else {
            Ok(())
        }
    }

    /// 进入帧写入后保守视为可能已发送；后续重试不抹掉这段历史。 / Treats starting a write as possibly sent, preserving that fact across retries.
    pub(super) fn start_write(&mut self) {
        self.may_have_sent = true;
    }

    /// 仅整个逻辑操作从未写入时，才报告确定未发送。 / Reports definitely unsent only if no attempt of the logical operation started writing.
    pub(super) fn timeout_error(&self) -> ClientError {
        if self.may_have_sent {
            ClientError::RequestTimeout
        } else {
            ClientError::RequestNotSentTimeout
        }
    }
}
