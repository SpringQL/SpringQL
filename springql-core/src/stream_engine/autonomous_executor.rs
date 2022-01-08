// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine) mod event_queue;
pub(in crate::stream_engine) mod task;

pub(crate) mod row;

pub(in crate::stream_engine::autonomous_executor) mod worker;

mod performance_metrics;
mod performance_monitor_worker;
mod pipeline_derivatives;
mod queue;
mod repositories;
mod task_executor;
mod task_graph;

use crate::error::Result;
use crate::pipeline::Pipeline;
use std::sync::Arc;

pub(crate) use row::SinkRow;

use self::{
    event_queue::{event::Event, EventQueue},
    performance_monitor_worker::PerformanceMonitorWorker,
    pipeline_derivatives::PipelineDerivatives,
    task_executor::TaskExecutor,
};

#[cfg(test)]
pub(super) mod test_support;

/// Automatically executes the latest task graph (uniquely deduced from the latest pipeline).
///
/// This also has PerformanceMonitorWorker and MemoryStateMachineWorker to dynamically switch task execution policies.
///
/// All interface methods are called from main thread, while `new()` spawns worker threads.
#[derive(Debug)]
pub(in crate::stream_engine) struct AutonomousExecutor {
    event_queue: Arc<EventQueue>,

    task_executor: TaskExecutor,
    performance_monitor_worker: PerformanceMonitorWorker,
}

impl AutonomousExecutor {
    pub(in crate::stream_engine) fn new(n_worker_threads: usize) -> Self {
        let event_queue = Arc::new(EventQueue::default());
        let performance_monitor_worker = PerformanceMonitorWorker::new(event_queue.clone());
        let task_executor = TaskExecutor::new(
            n_worker_threads,
            event_queue.clone(),
            performance_monitor_worker.metrics(),
        );
        Self {
            event_queue,
            task_executor,
            performance_monitor_worker,
        }
    }

    pub(in crate::stream_engine) fn notify_pipeline_update(
        &self,
        pipeline: Pipeline,
    ) -> Result<()> {
        let task_executor = &self.task_executor;
        let lock = task_executor.pipeline_update_lock();

        let pipeline_derivatives = Arc::new(PipelineDerivatives::new(pipeline));

        task_executor.cleanup(&lock, pipeline_derivatives.task_graph());
        task_executor.update_pipeline(&lock, pipeline_derivatives.clone())?;

        let event = Event::UpdatePipeline {
            pipeline_derivatives,
        };
        self.event_queue.publish(event);

        Ok(())
    }
}
