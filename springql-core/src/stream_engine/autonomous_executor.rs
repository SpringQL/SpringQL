// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine) mod task;

pub(crate) mod row;

mod pipeline_derivatives;
mod queue;
mod task_executor;
mod task_graph;

use crate::error::Result;
use crate::pipeline::Pipeline;
use std::sync::Arc;

pub(crate) use row::SinkRow;
pub(in crate::stream_engine) use row::Timestamp;

use self::{pipeline_derivatives::PipelineDerivatives, task_executor::TaskExecutor};

#[cfg(test)]
pub(super) mod test_support;

/// Automatically executes the latest task graph (uniquely deduced from the latest pipeline).
///
/// This also has PerformanceMonitor and MemoryStateMachine to dynamically switch task execution policies.
///
/// All interface methods are called from main thread, while `new()` spawns worker threads.
#[derive(Debug)]
pub(in crate::stream_engine) struct AutonomousExecutor {
    task_executor: TaskExecutor,
}

impl AutonomousExecutor {
    pub(in crate::stream_engine) fn new(n_worker_threads: usize) -> Self {
        let pipeline_derivatives = Arc::new(PipelineDerivatives::new(Pipeline::default()));
        let task_executor = TaskExecutor::new(n_worker_threads, pipeline_derivatives);
        Self { task_executor }
    }

    pub(in crate::stream_engine) fn notify_pipeline_update(
        &mut self,
        pipeline: Pipeline,
    ) -> Result<()> {
        let task_executor = &self.task_executor;

        let lock = task_executor.pipeline_update_lock();

        let pipeline_derivatives = Arc::new(PipelineDerivatives::new(pipeline));

        task_executor.cleanup(&lock, pipeline_derivatives.task_graph());
        task_executor.update_pipeline(&lock, pipeline_derivatives)?;

        Ok(())
    }
}
