// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod row;
pub(crate) mod task;

pub(in crate::stream_engine) mod event_queue;

pub(in crate::stream_engine::autonomous_executor) mod worker;

mod memory_state_machine;
mod memory_state_machine_worker;
mod performance_metrics;
mod performance_monitor_worker;
mod pipeline_derivatives;
mod purger_worker;
mod queue;
mod repositories;
mod task_executor;
mod task_graph;

use crate::error::{Result, SpringError};
use crate::low_level_rs::SpringConfig;
use crate::pipeline::Pipeline;
use std::sync::Arc;

pub(crate) use row::SinkRow;

use self::memory_state_machine_worker::MemoryStateMachineWorker;
use self::purger_worker::purger_worker_thread::PurgerWorkerThreadArg;
use self::purger_worker::PurgerWorker;
use self::repositories::Repositories;
use self::task_executor::task_executor_lock::TaskExecutorLock;
use self::worker::worker_handle::WorkerStopCoordinate;
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
    memory_state_machine_worker: MemoryStateMachineWorker,
    performance_monitor_worker: PerformanceMonitorWorker,
    purger_worker: PurgerWorker,
}

impl AutonomousExecutor {
    pub(in crate::stream_engine) fn new(config: &SpringConfig) -> Self {
        let repos = Arc::new(Repositories::new(config));
        let task_executor_lock = Arc::new(TaskExecutorLock::default());
        let event_queue = Arc::new(EventQueue::default());
        let worker_stop_coordinate = Arc::new(WorkerStopCoordinate::default());

        let task_executor = TaskExecutor::new(
            config,
            repos.clone(),
            task_executor_lock.clone(),
            event_queue.clone(),
            worker_stop_coordinate.clone(),
        );
        let memory_state_machine_worker = MemoryStateMachineWorker::new(
            &config.memory,
            event_queue.clone(),
            worker_stop_coordinate.clone(),
        );
        let performance_monitor_worker = PerformanceMonitorWorker::new(
            config,
            event_queue.clone(),
            worker_stop_coordinate.clone(),
        );
        let purger_worker = PurgerWorker::new(
            event_queue.clone(),
            worker_stop_coordinate,
            PurgerWorkerThreadArg::new(repos, task_executor_lock),
        );
        Self {
            event_queue,
            task_executor,
            memory_state_machine_worker,
            performance_monitor_worker,
            purger_worker,
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

    /// Workers in autonomous executor may get SpringError but it must continue their work.
    /// This method provides common way, like logging, to handle an error and then continue their work.
    fn handle_error(e: SpringError) {
        match e {
            SpringError::ForeignSourceTimeout { .. } | SpringError::InputTimeout { .. } => {
                log::trace!("{:?}", e)
            }

            SpringError::ForeignIo { .. }
            | SpringError::SpringQlCoreIo(_)
            | SpringError::Unavailable { .. } => log::warn!("{:?}", e),

            SpringError::InvalidOption { .. }
            | SpringError::InvalidFormat { .. }
            | SpringError::Sql(_)
            | SpringError::ThreadPoisoned(_) => log::error!("{:?}", e),

            SpringError::InvalidConfig { .. } => unreachable!("must be handled on startup"),
        }
    }
}
