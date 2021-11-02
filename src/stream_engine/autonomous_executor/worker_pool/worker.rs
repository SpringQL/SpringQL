pub(in crate::stream_engine::autonomous_executor) mod worker_id;

// TODO config
const TASK_WAIT_MSEC: u64 = 100;

use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use crate::stream_engine::{
    autonomous_executor::{
        scheduler::scheduler_read::SchedulerRead, task::task_context::TaskContext, Scheduler,
    },
    dependency_injection::DependencyInjection,
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
    ) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let _ = thread::spawn(move || {
            Self::main_loop::<DI>(id, scheduler_read.clone(), row_repo.clone(), stop_receiver)
        });
        Self { stop_button }
    }

    fn main_loop<DI: DependencyInjection>(
        id: WorkerId,
        scheduler: SchedulerRead<DI>,
        row_repo: Arc<DI::RowRepositoryType>,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        let mut cur_worker_state =
            <<DI as DependencyInjection>::SchedulerType as Scheduler>::W::default();

        log::debug!("[Worker#{}] Started", id);

        while stop_receiver.try_recv().is_err() {
            if let Some((task, next_worker_state)) =
                scheduler.read_lock().next_task(cur_worker_state.clone())
            {
                log::debug!("[Worker#{}] Scheduled task:{}", id, task.id());

                cur_worker_state = next_worker_state;

                let context = TaskContext::<DI>::new(
                    scheduler.read_lock().task_graph().as_ref(),
                    task.id().clone(),
                    row_repo.clone(),
                );
                task.run(&context).unwrap_or_else(|e| {
                    log::error!("Error while executing task: {}", e);
                })
            } else {
                thread::sleep(Duration::from_millis(TASK_WAIT_MSEC))
            }
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
