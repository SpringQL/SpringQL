// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine) mod task;

pub(crate) mod row;

mod current_pipeline;
mod task_executor;

use crate::error::Result;
use crate::pipeline::Pipeline;
use std::sync::Arc;

pub(crate) use row::ForeignSinkRow;
pub(in crate::stream_engine) use row::{
    CurrentTimestamp, NaiveRowRepository, RowRepository, Timestamp,
};
pub(in crate::stream_engine) use task_executor::{FlowEfficientScheduler, Scheduler};

use self::{current_pipeline::CurrentPipeline, task_executor::TaskExecutor};

use super::dependency_injection::DependencyInjection;

#[cfg(test)]
pub(super) mod test_support;

/// Automatically executes the latest task graph (uniquely deduced from the latest pipeline).
///
/// This also has PerformanceMonitor and MemoryStateMachine to dynamically switch task execution policies.
///
/// All interface methods are called from main thread, while `new()` spawns worker threads.
#[derive(Debug)]
pub(in crate::stream_engine) struct AutonomousExecutor<DI>
where
    DI: DependencyInjection,
{
    latest_pipeline: Arc<CurrentPipeline>,

    task_executor: TaskExecutor<DI>,
}

impl<DI> AutonomousExecutor<DI>
where
    DI: DependencyInjection,
{
    pub(in crate::stream_engine) fn new(n_worker_threads: usize) -> Self {
        let latest_pipeline = Arc::new(CurrentPipeline::default());
        let task_executor = TaskExecutor::new(n_worker_threads, latest_pipeline.clone());
        Self {
            latest_pipeline,
            task_executor,
        }
    }

    pub(in crate::stream_engine) fn notify_pipeline_update(
        &self,
        pipeline: Pipeline,
    ) -> Result<()> {
        let lock = self.task_executor.pipeline_update_lock();
        self.task_executor.cleanup(&lock)?;
        self.latest_pipeline.update(pipeline);
        Ok(())
    }
}
