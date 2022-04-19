// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod row;
pub(crate) mod task;

pub(in crate::stream_engine) mod event_queue;

pub(in crate::stream_engine::autonomous_executor) mod main_job_lock;
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
use crate::stream_engine::autonomous_executor::main_job_lock::MainJobLock;
use crate::stream_engine::autonomous_executor::worker::worker_handle::WorkerSetupCoordinator;
use std::sync::Arc;

pub(crate) use row::SinkRow;

use self::event_queue::non_blocking_event_queue::NonBlockingEventQueue;
use self::memory_state_machine_worker::MemoryStateMachineWorker;
use self::purger_worker::purger_worker_thread::PurgerWorkerThreadArg;
use self::purger_worker::PurgerWorker;
use self::repositories::Repositories;
use self::task_executor::task_executor_lock::TaskExecutorLock;
use self::worker::worker_handle::WorkerStopCoordinator;
use self::{
    event_queue::event::Event, performance_monitor_worker::PerformanceMonitorWorker,
    pipeline_derivatives::PipelineDerivatives, task_executor::TaskExecutor,
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
    event_queue: Arc<NonBlockingEventQueue>,

    task_executor: TaskExecutor,

    // just holds these ownership
    _memory_state_machine_worker: MemoryStateMachineWorker,
    _performance_monitor_worker: PerformanceMonitorWorker,
    _purger_worker: PurgerWorker,
}

impl AutonomousExecutor {
    pub(in crate::stream_engine) fn new(config: &SpringConfig) -> Self {
        let repos = Arc::new(Repositories::new(config));
        let main_job_lock = Arc::new(MainJobLock::default());
        let task_executor_lock = Arc::new(TaskExecutorLock::default());
        let event_queue = Arc::new(NonBlockingEventQueue::default());
        let worker_setup_coordinator = Arc::new(WorkerSetupCoordinator::new(config));
        let worker_stop_coordinator = Arc::new(WorkerStopCoordinator::default());

        let task_executor = TaskExecutor::new(
            config,
            repos.clone(),
            main_job_lock.clone(),
            task_executor_lock.clone(),
            event_queue.clone(),
            worker_setup_coordinator.clone(),
            worker_stop_coordinator.clone(),
        );
        let memory_state_machine_worker = MemoryStateMachineWorker::new(
            &config.memory,
            main_job_lock.clone(),
            event_queue.clone(),
            worker_setup_coordinator.clone(),
            worker_stop_coordinator.clone(),
        );
        let performance_monitor_worker = PerformanceMonitorWorker::new(
            config,
            main_job_lock.clone(),
            event_queue.clone(),
            worker_setup_coordinator.clone(),
            worker_stop_coordinator.clone(),
        );
        let purger_worker = PurgerWorker::new(
            main_job_lock,
            event_queue.clone(),
            worker_setup_coordinator.clone(),
            worker_stop_coordinator,
            PurgerWorkerThreadArg::new(repos, task_executor_lock),
        );

        worker_setup_coordinator.sync_wait_all_workers();
        log::info!("[AutonomousExecutor] All workers started");

        Self {
            event_queue,
            task_executor,
            _memory_state_machine_worker: memory_state_machine_worker,
            _performance_monitor_worker: performance_monitor_worker,
            _purger_worker: purger_worker,
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
