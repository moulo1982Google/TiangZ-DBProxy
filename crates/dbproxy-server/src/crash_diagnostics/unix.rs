//! Unix：启动时放开 core 文件大小上限。core 文件写到哪里由宿主机的 `kernel.core_pattern` 决定，进程改不了。
//! Unix: raise the core-file size limit at startup. Where cores go is decided by the host's `kernel.core_pattern`.

use std::io;

/// 调整后的 core 上限。 / The core limit after adjustment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreLimit {
    Unlimited,
    /// 硬上限不允许无限，只能把软上限提到硬上限（字节）。
    /// The hard limit forbids unlimited; the soft limit was raised to the hard limit (bytes).
    CappedAtHardLimit(libc::rlim_t),
}

pub fn raise_core_limit() -> io::Result<CoreLimit> {
    let mut current = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `current` 是有效的可写 rlimit。 / `current` is a valid, writable rlimit.
    if unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut current) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let unlimited = libc::rlimit {
        rlim_cur: libc::RLIM_INFINITY,
        rlim_max: libc::RLIM_INFINITY,
    };
    // SAFETY: 传入有效的 rlimit 指针。 / A valid rlimit pointer is passed.
    if unsafe { libc::setrlimit(libc::RLIMIT_CORE, &unlimited) } == 0 {
        return Ok(CoreLimit::Unlimited);
    }
    let capped = libc::rlimit {
        rlim_cur: current.rlim_max,
        rlim_max: current.rlim_max,
    };
    // SAFETY: 传入有效的 rlimit 指针。 / A valid rlimit pointer is passed.
    if unsafe { libc::setrlimit(libc::RLIMIT_CORE, &capped) } != 0 {
        return Err(io::Error::last_os_error());
    }
    if current.rlim_max == libc::RLIM_INFINITY {
        Ok(CoreLimit::Unlimited)
    } else {
        Ok(CoreLimit::CappedAtHardLimit(current.rlim_max))
    }
}

/// Linux 的 core_pattern，只用于启动日志；其他 Unix 没有这个文件时返回 None。
/// Linux core_pattern, for the startup log only; None where the file does not exist.
pub fn core_pattern() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/core_pattern")
        .ok()
        .map(|pattern| pattern.trim().to_owned())
}
