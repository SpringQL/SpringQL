// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use crate::{
    error::SpringError,
    pipeline::Pipeline,
    stream_engine::autonomous_executor::{
        event_queue::{
            event::{Event, EventTag},
            EventPoll, EventQueue,
        },
        pipeline_derivatives::PipelineDerivatives,
        repositories::Repositories,
        task::task_context::TaskContext,
        task_executor::{
            scheduler::{flow_efficient_scheduler::FlowEfficientScheduler, Scheduler},
            task_executor_lock::TaskExecutorLock,
        },
    },
};

use super::generic_worker_id::GenericWorkerId;

// TODO config
const TASK_WAIT_MSEC: u64 = 100;

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct GenericWorkerThread;

impl GenericWorkerThread {
    pub(super) fn run(
        id: GenericWorkerId,
        task_executor_lock: Arc<TaskExecutorLock>,
        event_queue: Arc<EventQueue>,
        repos: Arc<Repositories>,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        let event_poll = event_queue.subscribe(EventTag::UpdatePipeline);

        let _ = thread::spawn(move || {
            Self::main_loop(
                id,
                task_executor_lock.clone(),
                event_poll,
                repos,
                stop_receiver,
            )
        });
    }

    fn main_loop(
        id: GenericWorkerId,
        task_executor_lock: Arc<TaskExecutorLock>,
        event_poll: EventPoll,
        repos: Arc<Repositories>,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        let mut pipeline_derivatives = Arc::new(PipelineDerivatives::new(Pipeline::default()));
        let mut scheduler = FlowEfficientScheduler::default();
        let mut cur_worker_state = <FlowEfficientScheduler as Scheduler>::W::default();

        log::debug!("[GenericWorker#{}] Started", id);

        while stop_receiver.try_recv().is_err() {
            if let Ok(_lock) = task_executor_lock.try_task_execution() {
                if let Some((task_id, next_worker_state)) = scheduler.next_task(cur_worker_state) {
                    log::debug!("[GenericWorker#{}] Scheduled task:{}", id, task_id);

                    cur_worker_state = next_worker_state;

                    let context = TaskContext::new(
                        task_id.clone(),
                        pipeline_derivatives.clone(),
                        repos.clone(),
                    );

                    let task = pipeline_derivatives
                        .get_task(&task_id)
                        .expect("task id got from scheduler");

                    task.run(&context).unwrap_or_else(Self::handle_error)
                } else {
                    thread::sleep(Duration::from_millis(TASK_WAIT_MSEC))
                }
            } else {
                pipeline_derivatives = Self::handle_interruption(
                    id,
                    &event_poll,
                    pipeline_derivatives,
                    &mut scheduler,
                );
            }
        }
    }

    /// May re-create CurrentPipeline and update scheduler state.
    ///
    /// # Returns
    ///
    /// Some on interruption.
    fn handle_interruption(
        id: GenericWorkerId,
        event_poll: &EventPoll,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        scheduler: &mut FlowEfficientScheduler,
    ) -> Arc<PipelineDerivatives> {
        if let Some(Event::UpdatePipeline {
            pipeline_derivatives,
        }) = event_poll.poll()
        {
            log::debug!("[GenericWorker#{}] got UpdatePipeline event", id);

            scheduler
                .notify_pipeline_update(pipeline_derivatives.as_ref())
                .expect("failed to update scheduler's state");
            pipeline_derivatives
        } else {
            pipeline_derivatives
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
