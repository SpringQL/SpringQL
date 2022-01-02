use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Task executor is responsible for queues' cleanup on pipeline update.
///
/// This lock is to assure for workers to safely execute tasks while acquiring TaskExecuteLockGuard,
/// while it also gives PipelineUpdateLockGuard to autonomous_executor to dominate task executor and safely update pipeline.
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct TaskExecutorLock(
    RwLock<TaskExecutorLockToken>,
);

impl TaskExecutorLock {
    pub(in crate::stream_engine::autonomous_executor) fn pipeline_update(
        &self,
    ) -> PipelineUpdateLockGuard {
        let write_lock = self
            .0
            .write()
            .expect("another thread sharing the same TaskExecutorLock must not panic");
        PipelineUpdateLockGuard(write_lock)
    }

    pub(in crate::stream_engine::autonomous_executor) fn task_execution(
        &self,
    ) -> TaskExecutionLockGuard {
        let read_lock = self
            .0
            .read()
            .expect("another thread sharing the same TaskExecutorLock must not panic");
        TaskExecutionLockGuard(read_lock)
    }
}

#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct TaskExecutorLockToken;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct PipelineUpdateLockGuard<'a>(
    RwLockWriteGuard<'a, TaskExecutorLockToken>,
);

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct TaskExecutionLockGuard<'a>(
    RwLockReadGuard<'a, TaskExecutorLockToken>,
);
