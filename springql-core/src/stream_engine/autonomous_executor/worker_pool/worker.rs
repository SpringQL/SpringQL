// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod worker_id;

// TODO config
const TASK_WAIT_MSEC: u64 = 100;

use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use crate::{
    error::SpringError,
    pipeline::Pipeline,
    stream_engine::{
        autonomous_executor::{
            scheduler::scheduler_read::SchedulerRead,
            task::{
                sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
                source_task::source_reader::source_reader_repository::SourceReaderRepository,
                task_context::TaskContext,
            },
            Scheduler,
        },
        dependency_injection::DependencyInjection,
    },
};

use self::worker_id::WorkerId;

#[derive(Debug)]
pub(super) struct Worker {
    stop_button: mpsc::SyncSender<()>,
}

impl Worker {
    pub(super) fn new<DI: DependencyInjection>(
        id: WorkerId,
        scheduler_read: SchedulerRead<DI>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
    ) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let _ = thread::spawn(move || {
            Self::main_loop::<DI>(
                id,
                scheduler_read.clone(),
                row_repo,
                source_reader_repo,
                sink_writer_repo,
                stop_receiver,
            )
        });
        Self { stop_button }
    }

    fn main_loop<DI: DependencyInjection>(
        id: WorkerId,
        scheduler: SchedulerRead<DI>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        let mut cur_worker_state =
            <<DI as DependencyInjection>::SchedulerType as Scheduler>::W::default();

        log::debug!("[Worker#{}] Started", id);

        while stop_receiver.try_recv().is_err() {
            let scheduler = scheduler.read_lock();

            if let Some((task, next_worker_state)) = scheduler.next_task(cur_worker_state.clone()) {
                log::debug!("[Worker#{}] Scheduled task:{}", id, task.id());

                cur_worker_state = next_worker_state;

                let context = TaskContext::<DI>::new(
                    scheduler.task_graph().as_ref(),
                    task.id().clone(),
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

impl Drop for Worker {
    fn drop(&mut self) {
        self.stop_button
            .send(())
            .expect("failed to wait for worker thread to finish its job");
    }
}
