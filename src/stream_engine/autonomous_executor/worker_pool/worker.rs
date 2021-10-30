pub(in crate::stream_engine::autonomous_executor) mod worker_id;

// TODO config
const TASK_WAIT_MSEC: u64 = 100;

use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use crate::stream_engine::{
    autonomous_executor::{scheduler::scheduler_read::SchedulerRead, Scheduler},
    dependency_injection::DependencyInjection,
};

use self::worker_id::WorkerId;

#[derive(Debug)]
pub(super) struct Worker {
    id: WorkerId,
    stop_button: mpsc::SyncSender<()>,
}

impl Worker {
    pub(super) fn new<DI: DependencyInjection>(
        id: WorkerId,
        scheduler_read: SchedulerRead<DI>,
    ) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let _ =
            thread::spawn(move || Self::main_loop::<DI>(id, scheduler_read.clone(), stop_receiver));
        Self { id, stop_button }
    }

    fn id(&self) -> WorkerId {
        self.id
    }

    fn main_loop<DI: DependencyInjection>(
        id: WorkerId,
        scheduler: SchedulerRead<DI>,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        while stop_receiver.try_recv().is_err() {
            if let Some(task) = scheduler.read_lock().next_task(id) {
                task.run().unwrap_or_else(|e| {
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
