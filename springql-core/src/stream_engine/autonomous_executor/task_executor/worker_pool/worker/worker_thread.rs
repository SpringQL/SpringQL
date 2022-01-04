// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use crate::{
    error::SpringError,
    stream_engine::{
        autonomous_executor::{
            current_pipeline::CurrentPipeline,
            task::{
                sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
                source_task::source_reader::source_reader_repository::SourceReaderRepository,
                task_context::TaskContext,
            },
            task_executor::{
                scheduler::{flow_efficient_scheduler::FlowEfficientScheduler, Scheduler},
                task_executor_lock::TaskExecutorLock,
            },
        },
        dependency_injection::DependencyInjection,
    },
};

use super::worker_id::WorkerId;

// TODO config
const TASK_WAIT_MSEC: u64 = 100;

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct WorkerThread;

impl WorkerThread {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn run<DI: DependencyInjection>(
        id: WorkerId,
        task_executor_lock: Arc<TaskExecutorLock>,
        current_pipeline: Arc<CurrentPipeline>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
        pipeline_update_receiver: mpsc::Receiver<Arc<CurrentPipeline>>,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        let _ = thread::spawn(move || {
            Self::main_loop::<DI>(
                id,
                task_executor_lock.clone(),
                current_pipeline,
                row_repo,
                source_reader_repo,
                sink_writer_repo,
                pipeline_update_receiver,
                stop_receiver,
            )
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn main_loop<DI: DependencyInjection>(
        id: WorkerId,
        task_executor_lock: Arc<TaskExecutorLock>,
        current_pipeline: Arc<CurrentPipeline>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
        pipeline_update_receiver: mpsc::Receiver<Arc<CurrentPipeline>>,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        let mut current_pipeline = current_pipeline;
        let mut scheduler = FlowEfficientScheduler::default();
        let mut cur_worker_state = <FlowEfficientScheduler as Scheduler>::W::default();

        log::debug!("[Worker#{}] Started", id);

        while stop_receiver.try_recv().is_err() {
            current_pipeline = Self::handle_interruption(
                id,
                &pipeline_update_receiver,
                current_pipeline,
                &mut scheduler,
            );

            let _lock = task_executor_lock.task_execution();

            if let Some((task, next_worker_state)) = scheduler.next_task(cur_worker_state) {
                log::debug!("[Worker#{}] Scheduled task:{}", id, task.id());

                cur_worker_state = next_worker_state;

                let context = TaskContext::<DI>::new(
                    task.id().clone(),
                    current_pipeline.clone(),
                    row_repo.clone(),
                    source_reader_repo.clone(),
                    sink_writer_repo.clone(),
                );

                task.run(&context).unwrap_or_else(Self::handle_error)
            } else {
                thread::sleep(Duration::from_millis(TASK_WAIT_MSEC))
            }
        }
    }

    /// May re-create CurrentPipeline and update scheduler state.
    fn handle_interruption(
        id: WorkerId,
        pipeline_update_receiver: &mpsc::Receiver<Arc<CurrentPipeline>>,
        current_pipeline: Arc<CurrentPipeline>,
        scheduler: &mut FlowEfficientScheduler,
    ) -> Arc<CurrentPipeline> {
        if let Ok(current_pipeline) = pipeline_update_receiver.try_recv() {
            log::debug!("[Worker#{}] got interruption", id);

            scheduler
                .notify_pipeline_update(current_pipeline.as_ref())
                .expect("failed to update scheduler's state");
            current_pipeline
        } else {
            current_pipeline
        }
    }

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
        }
    }
}
