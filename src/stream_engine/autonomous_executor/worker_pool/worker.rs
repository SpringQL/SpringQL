// TODO config
const TASK_WAIT_MSEC: u64 = 100;

use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

#[derive(Debug)]
pub(super) struct Worker {
    stop_button: mpsc::SyncSender<()>,
}

impl Worker {
    pub(super) fn new(scheduler: Arc<Mutex<Scheduler>>) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let thread = thread::spawn(|| Self::main_loop(scheduler.clone(), stop_receiver));
        Self { stop_button }
    }

    fn main_loop(scheduler: Arc<Mutex<Scheduler>>, stop_receiver: mpsc::Receiver<()>) {
        while stop_receiver.try_recv().is_err() {
            if let Some(task) = scheduler
                .lock()
                .expect("worker threads sharing the same scheduler must not panic")
                .next_task()
            {
                task.run().map_err(|e| {
                    log::error!("Error while executing task: {}", e);
                });
            } else {
                thread::sleep(Duration::from_millis(TASK_WAIT_MSEC));
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
