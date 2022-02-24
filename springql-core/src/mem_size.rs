// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

/// This trait requires estimation on how much heap memory [bytes] used by impl queue.
///
/// Rust does not provide standard way to calculate heap memory size (Box, Vec, for example).
/// Since Memory State Machine uses total memory size of queues, we need to impl this trait by ourselves.
pub(crate) trait MemSize {
    fn mem_size(&self) -> usize;
}

const RAW_POINTER_SIZE: usize = 8;

/// Note: assuming many objects share the same Arc so interior object size is negligible.
#[inline]
pub(crate) fn arc_overhead_size() -> usize {
    RAW_POINTER_SIZE
}

#[inline]
pub(crate) fn chrono_naive_date_time_overhead_size() -> usize {
    4 * 3
}
