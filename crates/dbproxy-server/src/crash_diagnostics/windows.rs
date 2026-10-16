//! Windows：未处理异常过滤器。异常线程只登记请求并等待，转储和文本记录由预先创建的 `crash-dump` 线程完成，
//! 这样栈溢出等栈空间不足的情况也能写出转储。写完后交回原过滤器（或系统默认处理），进程照常以原异常码结束。
//! Windows: unhandled-exception filter. The faulting thread only records the request and waits; a pre-spawned
//! `crash-dump` thread writes the dump and text record, so it also works when the faulting stack is nearly exhausted.
//! Afterwards the previous filter (or the system default) runs and the process ends with the original exception code.

use std::{
    fs, io,
    os::windows::io::AsRawHandle,
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicU32, Ordering},
    },
    thread::{self, Thread},
    time::{Duration, Instant},
};

use windows_sys::Win32::System::{
    Diagnostics::Debug::{
        EXCEPTION_POINTERS, LPTOP_LEVEL_EXCEPTION_FILTER, MINIDUMP_EXCEPTION_INFORMATION,
        MINIDUMP_TYPE, MiniDumpNormal, MiniDumpWithHandleData,
        MiniDumpWithIndirectlyReferencedMemory, MiniDumpWithThreadInfo,
        MiniDumpWithUnloadedModules, MiniDumpWriteDump, SetUnhandledExceptionFilter,
    },
    Threading::{GetCurrentProcess, GetCurrentThreadId},
};

use super::report::access_operation;

const EXCEPTION_CONTINUE_SEARCH: i32 = 0;
const EXCEPTION_ACCESS_VIOLATION: u32 = 0xC000_0005;
/// 异常线程最多等待转储线程多久；超时后照常交回系统处理。
/// How long the faulting thread waits for the dump thread before handing over to the system anyway.
const DUMP_WAIT: Duration = Duration::from_secs(120);

/// 小型转储：线程、栈、间接引用内存、句柄和卸载模块；不含完整堆，体积通常为几 MB。
/// Mini dump: threads, stacks, indirectly referenced memory, handles and unloaded modules; no full heap.
const DUMP_TYPE: MINIDUMP_TYPE = MiniDumpNormal
    | MiniDumpWithThreadInfo
    | MiniDumpWithIndirectlyReferencedMemory
    | MiniDumpWithUnloadedModules
    | MiniDumpWithHandleData;

const IDLE: u8 = 0;
const REQUESTED: u8 = 1;
const DONE: u8 = 2;

static ENTERED: AtomicBool = AtomicBool::new(false);
static PHASE: AtomicU8 = AtomicU8::new(IDLE);
static REQUEST: AtomicPtr<EXCEPTION_POINTERS> = AtomicPtr::new(std::ptr::null_mut());
static THREAD_ID: AtomicU32 = AtomicU32::new(0);
static WORKER: OnceLock<Thread> = OnceLock::new();
static PREVIOUS: OnceLock<LPTOP_LEVEL_EXCEPTION_FILTER> = OnceLock::new();

/// 一次未处理异常的摘要；只在转储线程里存在，期间异常线程阻塞，指针保持有效。
/// Summary of one unhandled exception; lives only on the dump thread while the faulting thread is blocked.
pub struct NativeException {
    pub thread_id: u32,
    pub code: u32,
    pub address: usize,
    pub access: Option<(&'static str, usize)>,
    pointers: *mut EXCEPTION_POINTERS,
}

impl NativeException {
    /// # Safety
    /// `pointers` 为空或指向异常线程仍在使用的有效 `EXCEPTION_POINTERS`。
    /// `pointers` is null or points to valid `EXCEPTION_POINTERS` owned by the still-blocked faulting thread.
    unsafe fn from_pointers(pointers: *mut EXCEPTION_POINTERS, thread_id: u32) -> Self {
        // SAFETY: 见函数说明。 / See the function contract.
        let record = unsafe { pointers.as_ref() }
            .and_then(|pointers| unsafe { pointers.ExceptionRecord.as_ref() });
        let Some(record) = record else {
            return Self {
                thread_id,
                code: 0,
                address: 0,
                access: None,
                pointers,
            };
        };
        let code = record.ExceptionCode as u32;
        let access =
            (code == EXCEPTION_ACCESS_VIOLATION && record.NumberParameters >= 2).then(|| {
                (
                    access_operation(record.ExceptionInformation[0]),
                    record.ExceptionInformation[1],
                )
            });
        Self {
            thread_id,
            code,
            address: record.ExceptionAddress as usize,
            access,
            pointers,
        }
    }

    /// 用 `MiniDumpWriteDump` 把本进程写入 `file`。 / Write this process to `file` with `MiniDumpWriteDump`.
    pub fn write_minidump(&self, file: &fs::File) -> io::Result<()> {
        let information = MINIDUMP_EXCEPTION_INFORMATION {
            ThreadId: self.thread_id,
            ExceptionPointers: self.pointers,
            ClientPointers: 0,
        };
        let exception = if self.pointers.is_null() {
            std::ptr::null()
        } else {
            &information as *const MINIDUMP_EXCEPTION_INFORMATION
        };
        // SAFETY: 句柄来自打开的可写文件；异常参数在调用期间有效。
        // The handle comes from an open writable file; the exception parameter stays valid during the call.
        let written = unsafe {
            MiniDumpWriteDump(
                GetCurrentProcess(),
                std::process::id(),
                file.as_raw_handle(),
                DUMP_TYPE,
                exception,
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        if written == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// 预先创建转储线程并安装过滤器；只处理第一次未处理异常。
/// Spawn the dump thread and install the filter; only the first unhandled exception is handled.
pub fn install(handler: impl FnOnce(&NativeException) + Send + 'static) -> io::Result<()> {
    let worker = thread::Builder::new()
        .name("crash-dump".into())
        .spawn(move || {
            while PHASE.load(Ordering::Acquire) != REQUESTED {
                thread::park();
            }
            let pointers = REQUEST.load(Ordering::Acquire);
            // SAFETY: 异常线程在 DONE 之前一直阻塞在过滤器里。 / The faulting thread stays in the filter until DONE.
            let exception = unsafe {
                NativeException::from_pointers(pointers, THREAD_ID.load(Ordering::Acquire))
            };
            handler(&exception);
            PHASE.store(DONE, Ordering::Release);
        })?;
    if WORKER.set(worker.thread().clone()).is_err() {
        return Err(io::Error::other(
            "the Windows crash filter is already installed",
        ));
    }
    // SAFETY: 过滤器是满足签名的 extern "system" 函数。 / The filter is an extern "system" function of the right type.
    let previous = unsafe { SetUnhandledExceptionFilter(Some(filter)) };
    let _ = PREVIOUS.set(previous);
    Ok(())
}

unsafe extern "system" fn filter(pointers: *const EXCEPTION_POINTERS) -> i32 {
    if !ENTERED.swap(true, Ordering::AcqRel)
        && let Some(worker) = WORKER.get()
    {
        REQUEST.store(pointers.cast_mut(), Ordering::Release);
        // SAFETY: 无参数的系统调用。 / Parameterless system call.
        THREAD_ID.store(unsafe { GetCurrentThreadId() }, Ordering::Release);
        PHASE.store(REQUESTED, Ordering::Release);
        worker.unpark();
        let deadline = Instant::now() + DUMP_WAIT;
        while PHASE.load(Ordering::Acquire) != DONE && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if PHASE.load(Ordering::Acquire) != DONE {
            // 只写固定文本，不分配内存。转储线程可能仍在写，文件可能不完整。
            // Fixed text only, no allocation. The dump thread may still be writing; files may be incomplete.
            let _ = io::Write::write_all(
                &mut io::stderr(),
                b"TiangZ DBProxy crash dump timed out after 120 s; the minidump and native record may be missing or incomplete\n",
            );
        }
    }
    match PREVIOUS.get().copied().flatten() {
        // SAFETY: 原过滤器由系统返回，按原约定调用。 / The previous filter came from the system; call it as intended.
        Some(previous) => unsafe { previous(pointers) },
        None => EXCEPTION_CONTINUE_SEARCH,
    }
}
