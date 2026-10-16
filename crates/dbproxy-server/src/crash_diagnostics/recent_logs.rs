//! 最近日志环形缓冲：与标准输出写入同一份格式化文本，崩溃时取最后 N 行写进报告。
//! Recent-log ring buffer: keeps the exact text written to stdout so a crash report can include the last N lines.

use std::{
    collections::VecDeque,
    io::{self, Write},
    sync::{Arc, Mutex, PoisonError, TryLockError},
    time::Duration,
};

use tracing_subscriber::fmt::MakeWriter;

/// `diagnostics.crash.recentLogLines` 的上限。 / Upper bound of `diagnostics.crash.recentLogLines`.
pub const MAX_RECENT_LOG_LINES: usize = 2_000;
/// 单行保留的最大字节数，超出部分截断，保证缓冲内存有界（约 2000 × 4 KiB）。
/// Bytes kept per line; longer lines are truncated so the buffer stays bounded (about 2000 × 4 KiB).
pub const MAX_RECENT_LOG_LINE_BYTES: usize = 4_096;
const TRUNCATED_SUFFIX: &str = " …[truncated]";

/// 固定容量的最近日志行。 / Fixed-capacity buffer of recent log lines.
#[derive(Debug)]
pub struct RecentLogs {
    capacity: usize,
    lines: Mutex<VecDeque<String>>,
}

/// 崩溃路径取快照时缓冲正被占用。 / The buffer was busy when the crash path tried to read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecentLogsBusy;

impl RecentLogs {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.min(MAX_RECENT_LOG_LINES);
        Self {
            capacity,
            lines: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// 写入一段日志文本；按换行拆成多行，每行单独计数。
    /// Record one chunk of log text; it is split on newlines and every line counts separately.
    pub fn push_text(&self, text: &str) {
        if self.capacity == 0 {
            return;
        }
        let mut lines = self.lines.lock().unwrap_or_else(PoisonError::into_inner);
        for line in text.lines() {
            if lines.len() == self.capacity {
                lines.pop_front();
            }
            lines.push_back(truncate_line(line));
        }
    }

    /// 崩溃路径使用：不阻塞等待，最多重试约 100 ms，拿不到锁就返回 `RecentLogsBusy`。
    /// For crash paths: never blocks indefinitely; retries for about 100 ms, then reports `RecentLogsBusy`.
    pub fn try_snapshot(&self) -> Result<Vec<String>, RecentLogsBusy> {
        for _ in 0..100 {
            match self.lines.try_lock() {
                Ok(lines) => return Ok(lines.iter().cloned().collect()),
                Err(TryLockError::Poisoned(poisoned)) => {
                    return Ok(poisoned.into_inner().iter().cloned().collect());
                }
                Err(TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(1)),
            }
        }
        Err(RecentLogsBusy)
    }
}

fn truncate_line(line: &str) -> String {
    let line = line.trim_end_matches('\r');
    if line.len() <= MAX_RECENT_LOG_LINE_BYTES {
        return line.to_owned();
    }
    let mut end = MAX_RECENT_LOG_LINE_BYTES;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    let mut kept = String::with_capacity(end + TRUNCATED_SUFFIX.len());
    kept.push_str(&line[..end]);
    kept.push_str(TRUNCATED_SUFFIX);
    kept
}

/// 包装原有的日志输出：照常写出，同时把同一份文本记进 `RecentLogs`。
/// Wraps the original log writer: output is unchanged and the same text is copied into `RecentLogs`.
#[derive(Clone)]
pub struct TeeMakeWriter<M> {
    inner: M,
    recent: Arc<RecentLogs>,
}

impl TeeMakeWriter<fn() -> io::Stdout> {
    /// 与 `tracing_subscriber::fmt()` 默认相同的标准输出。 / Same stdout target as the `fmt()` default.
    pub fn stdout(recent: Arc<RecentLogs>) -> Self {
        Self::new(io::stdout, recent)
    }
}

impl<M> TeeMakeWriter<M> {
    pub fn new(inner: M, recent: Arc<RecentLogs>) -> Self {
        Self { inner, recent }
    }
}

impl<'a, M> MakeWriter<'a> for TeeMakeWriter<M>
where
    M: MakeWriter<'a>,
{
    type Writer = TeeWriter<'a, M::Writer>;

    fn make_writer(&'a self) -> Self::Writer {
        TeeWriter::new(self.inner.make_writer(), &self.recent)
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        TeeWriter::new(self.inner.make_writer_for(meta), &self.recent)
    }
}

/// 一次日志事件的写入器；事件写完（Drop）时把文本记入缓冲。
/// Writer for one log event; the text is recorded when the writer is dropped.
pub struct TeeWriter<'a, W: Write> {
    inner: W,
    recent: &'a RecentLogs,
    buffer: Vec<u8>,
}

impl<'a, W: Write> TeeWriter<'a, W> {
    fn new(inner: W, recent: &'a RecentLogs) -> Self {
        Self {
            inner,
            recent,
            buffer: Vec::new(),
        }
    }
}

impl<W: Write> Write for TeeWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        if let Some(copied) = bytes.get(..written) {
            self.buffer.extend_from_slice(copied);
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> Drop for TeeWriter<'_, W> {
    fn drop(&mut self) {
        if !self.buffer.is_empty() {
            self.recent
                .push_text(&String::from_utf8_lossy(&self.buffer));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_only_the_newest_lines_up_to_capacity() {
        let recent = RecentLogs::new(3);
        for index in 0..10 {
            recent.push_text(&format!("line {index}\n"));
        }
        assert_eq!(
            recent.try_snapshot().unwrap(),
            vec!["line 7", "line 8", "line 9"]
        );
    }

    #[test]
    fn capacity_is_clamped_and_zero_records_nothing() {
        assert_eq!(
            RecentLogs::new(MAX_RECENT_LOG_LINES + 1).capacity(),
            MAX_RECENT_LOG_LINES
        );
        let recent = RecentLogs::new(0);
        recent.push_text("ignored\n");
        assert!(recent.try_snapshot().unwrap().is_empty());
    }

    #[test]
    fn multi_line_text_counts_each_line_and_long_lines_are_truncated_on_char_boundary() {
        let recent = RecentLogs::new(10);
        recent.push_text("first\r\nsecond\n");
        let long = "界".repeat(MAX_RECENT_LOG_LINE_BYTES);
        recent.push_text(&long);
        let lines = recent.try_snapshot().unwrap();
        assert_eq!(&lines[..2], ["first", "second"]);
        assert!(lines[2].ends_with(TRUNCATED_SUFFIX));
        assert!(lines[2].len() <= MAX_RECENT_LOG_LINE_BYTES + TRUNCATED_SUFFIX.len());
    }

    #[test]
    fn snapshot_reports_busy_instead_of_blocking() {
        let recent = RecentLogs::new(2);
        let _held = recent.lines.lock().unwrap();
        assert_eq!(recent.try_snapshot(), Err(RecentLogsBusy));
    }

    #[test]
    fn tee_writer_forwards_bytes_and_records_the_event_text() {
        #[derive(Clone, Default)]
        struct Sink(Arc<Mutex<Vec<u8>>>);
        impl Write for Sink {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let sink = Sink::default();
        let recent = Arc::new(RecentLogs::new(4));
        let make = {
            let sink = sink.clone();
            TeeMakeWriter::new(move || sink.clone(), Arc::clone(&recent))
        };
        {
            let mut writer = make.make_writer();
            writer.write_all(b"2026-10-16 INFO started\n").unwrap();
        }
        assert_eq!(
            sink.0.lock().unwrap().as_slice(),
            b"2026-10-16 INFO started\n"
        );
        assert_eq!(
            recent.try_snapshot().unwrap(),
            vec!["2026-10-16 INFO started"]
        );
    }

    #[test]
    fn tracing_events_reach_the_buffer_through_the_fmt_layer() {
        let recent = Arc::new(RecentLogs::new(8));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(TeeMakeWriter::new(io::sink, Arc::clone(&recent)))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(answer = 42, "recent log probe");
        });
        let lines = recent.try_snapshot().unwrap();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("INFO"), "{lines:?}");
        assert!(lines[0].contains("recent log probe answer=42"), "{lines:?}");
    }
}
