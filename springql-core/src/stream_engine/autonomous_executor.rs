// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod args;
mod event_queue;
mod main_job_lock;
mod memory_state_machine;
mod memory_state_machine_worker;
mod performance_metrics;
mod performance_monitor_worker;
mod pipeline_derivatives;
mod purger_worker;
mod queue;
mod repositories;
mod row;
mod task;
mod task_executor;
mod task_graph;
mod worker;

#[cfg(test)]
pub mod test_support;

pub use row::SpringValue;
pub use row::{
    ColumnValues, JsonObject, NnSqlValue, RowTime, SourceRow, SqlCompareResult, SqlValue,
    SqlValueHashKey, StreamColumns, StreamRow,
};
pub use task::{
    NetClientSourceReader, NetServerSourceReader, SinkWriterRepository, SourceReader,
    SourceReaderRepository, SourceTask, Task, TaskContext, Tuple, Window,
};

use std::sync::Arc;

use crate::{
    api::{
        error::{Result, SpringError},
        SpringConfig,
    },
    pipeline::Pipeline,
    stream_engine::autonomous_executor::{
        args::{Coordinators, EventQueues, Locks},
        event_queue::{BlockingEventQueue, Event, NonBlockingEventQueue},
        main_job_lock::MainJobLock,
        memory_state_machine_worker::MemoryStateMachineWorker,
        performance_monitor_worker::PerformanceMonitorWorker,
        pipeline_derivatives::PipelineDerivatives,
        purger_worker::{PurgerWorker, PurgerWorkerThreadArg},
        repositories::Repositories,
        task_executor::{TaskExecutor, TaskExecutorLock},
        worker::{WorkerSetupCoordinator, WorkerStopCoordinator},
    },
};

/// Automatically executes the latest task graph (uniquely deduced from the latest pipeline).
///
/// This also has PerformanceMonitorWorker and MemoryStateMachineWorker to dynamically switch task execution policies.
///
/// All interface methods are called from main thread, while `new()` spawns worker threads.
#[derive(Debug)]
pub struct AutonomousExecutor {
    b_event_queue: Arc<BlockingEventQueue>,

    main_job_lock: Arc<MainJobLock>,
    task_executor: TaskExecutor,

    // just holds these ownership
    _memory_state_machine_worker: MemoryStateMachineWorker,
    _performance_monitor_worker: PerformanceMonitorWorker,
    _purger_worker: PurgerWorker,
}

impl AutonomousExecutor {
    pub fn new(config: &SpringConfig) -> Self {
        let repos = Arc::new(Repositories::new(config));
        let locks = Locks::new(
            Arc::new(MainJobLock::default()),
            Arc::new(TaskExecutorLock::default()),
        );
        let event_queues = EventQueues::new(
            Arc::new(BlockingEventQueue::default()),
            Arc::new(NonBlockingEventQueue::default()),
        );
        let coordinators = Coordinators::new(
            Arc::new(WorkerSetupCoordinator::new(config)),
            Arc::new(WorkerStopCoordinator::default()),
        );

        let task_executor = TaskExecutor::new(
            config,
            repos.clone(),
            locks.clone(),
            event_queues.clone(),
            coordinators.clone(),
        );
        let memory_state_machine_worker = MemoryStateMachineWorker::new(
            &config.memory,
            locks.main_job_lock.clone(),
            event_queues.clone(),
            coordinators.clone(),
        );
        let performance_monitor_worker = PerformanceMonitorWorker::new(
            config,
            locks.main_job_lock.clone(),
            event_queues.clone(),
            coordinators.clone(),
        );
        let purger_worker = PurgerWorker::new(
            locks.main_job_lock.clone(),
            event_queues.clone(),
            coordinators.clone(),
            PurgerWorkerThreadArg::new(repos, locks.task_executor_lock.clone()),
        );

        coordinators
            .worker_setup_coordinator
            .sync_wait_all_workers();
        log::info!("[AutonomousExecutor] All workers started");

        Self {
            b_event_queue: event_queues.blocking,
            main_job_lock: locks.main_job_lock,
            task_executor,
            _memory_state_machine_worker: memory_state_machine_worker,
            _performance_monitor_worker: performance_monitor_worker,
            _purger_worker: purger_worker,
        }
    }

    pub fn notify_pipeline_update(&self, pipeline: Pipeline) -> Result<()> {
        let main_job_lock = &self.main_job_lock;
        let lock = main_job_lock.main_job_barrier();

        let pipeline_derivatives = Arc::new(PipelineDerivatives::new(pipeline));

        let task_executor = &self.task_executor;
        task_executor.cleanup(&lock, pipeline_derivatives.task_graph());
        task_executor.update_pipeline(&lock, pipeline_derivatives.clone())?;

        let event = Event::UpdatePipeline {
            pipeline_derivatives,
        };
        self.b_event_queue.publish_blocking(event);

        Ok(())
    }

    /// Workers in autonomous executor may get SpringError but it must continue their work.
    /// This method provides common way, like logging, to handle an error and then continue their work.
    fn handle_error(e: SpringError) {
        match e {
            SpringError::ForeignSourceTimeout { .. } | SpringError::InputTimeout { .. } => {
                log::trace!("{:?}", e)
            }

            // IO Error / Value Error
            SpringError::ForeignIo { .. }
            | SpringError::SpringQlCoreIo(_)
            | SpringError::Unavailable { .. }
            | SpringError::Time(_) => log::warn!("{:?}", e),

            SpringError::InvalidOption { .. }
            | SpringError::InvalidFormat { .. }
            | SpringError::Sql(_)
            | SpringError::ThreadPoisoned(_) => log::error!("{:?}", e),

            SpringError::InvalidConfig { .. } => unreachable!("must be handled on startup"),
            SpringError::Null { .. } => unreachable!("must be handled on startup"),
        }
    }
}
