// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use anyhow::{anyhow, Context};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Task executor is responsible for queues' cleanup on pipeline update.
///
/// This lock is to assure for workers to safely execute tasks while acquiring TaskExecuteLockGuard,
/// while it also gives PipelineUpdateLockGuard to autonomous_executor to dominate task executor and safely update pipeline.
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct TaskExecutorLock(
    RwLock<TaskExecutorLockToken>,
);

impl TaskExecutorLock {
    pub(in crate::stream_engine::autonomous_executor) fn task_execution_barrier(
        &self,
    ) -> TaskExecutionBarrierGuard {
        let write_lock = self.0.write();
        TaskExecutionBarrierGuard(write_lock)
    }

    /// # Returns
    ///
    /// Ok on successful lock, Err on write lock.
    pub(in crate::stream_engine::autonomous_executor) fn try_task_execution(
        &self,
    ) -> Result<TaskExecutionLockGuard, anyhow::Error> {
        self.0
            .try_read()
            .map(TaskExecutionLockGuard)
            .context("write lock may be taken")
    }
}

#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct TaskExecutorLockToken;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct TaskExecutionBarrierGuard<'a>(
    RwLockWriteGuard<'a, TaskExecutorLockToken>,
);

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct TaskExecutionLockGuard<'a>(
    RwLockReadGuard<'a, TaskExecutorLockToken>,
);
