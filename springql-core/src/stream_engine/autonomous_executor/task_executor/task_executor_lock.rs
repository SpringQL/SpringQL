// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use anyhow::Context;

// Fixes: <https://github.com/SpringQL/SpringQL/issues/61#issuecomment-1082615502>
//
// `std::sync::RwLock` uses `pthread_rwlock_wrlock`, which might cause writer starvation without setting `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP` attribute.
// `parking_lot::RwLock` avoids writer starvation.
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Task executor is responsible for queues' cleanup on pipeline update.
///
/// This lock is to assure for workers to safely execute tasks while acquiring TaskExecuteLockGuard,
/// while it also gives PipelineUpdateLockGuard to autonomous_executor to dominate task executor and safely update pipeline.
#[derive(Debug, Default)]
pub struct TaskExecutorLock(RwLock<TaskExecutorLockToken>);

impl TaskExecutorLock {
    pub fn task_execution_barrier(&self) -> TaskExecutionBarrierGuard {
        let write_lock = self.0.write();
        TaskExecutionBarrierGuard(write_lock)
    }

    /// # Returns
    ///
    /// Ok on successful lock, Err on write lock.
    pub fn try_task_execution(&self) -> Result<TaskExecutionLockGuard, anyhow::Error> {
        self.0
            .try_read()
            .map(TaskExecutionLockGuard)
            .context("write lock may be taken")
    }
}

#[derive(Debug, Default)]
pub struct TaskExecutorLockToken;

#[derive(Debug)]
pub struct TaskExecutionBarrierGuard<'a>(RwLockWriteGuard<'a, TaskExecutorLockToken>);

#[derive(Debug)]
pub struct TaskExecutionLockGuard<'a>(RwLockReadGuard<'a, TaskExecutorLockToken>);
