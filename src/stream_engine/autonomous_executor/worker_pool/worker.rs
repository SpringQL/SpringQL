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

#[derive(Debug)]
pub(super) struct Worker {
    stop_button: mpsc::SyncSender<()>,
}

impl Worker {
    pub(super) fn new<DI: DependencyInjection>(scheduler_read: SchedulerRead<DI>) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let _ = thread::spawn(move || Self::main_loop::<DI>(scheduler_read.clone(), stop_receiver));
        Self { stop_button }
    }

    fn main_loop<DI: DependencyInjection>(
        scheduler: SchedulerRead<DI>,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        let mut cur_ws = <<DI as DependencyInjection>::SchedulerType as Scheduler>::W::default();

        while stop_receiver.try_recv().is_err() {
            if let Some((task, next_ws)) = scheduler.read_lock().next_task(cur_ws.clone()) {
                cur_ws = next_ws;

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
