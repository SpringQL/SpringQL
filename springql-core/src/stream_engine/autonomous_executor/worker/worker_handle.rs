use std::sync::{mpsc, Arc};

use crate::stream_engine::autonomous_executor::event_queue::EventQueue;

use super::worker_thread::WorkerThread;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct WorkerHandle {
    stop_button: mpsc::SyncSender<()>,
}

impl WorkerHandle {
    pub(super) fn new<T: WorkerThread>(
        event_queue: Arc<EventQueue>,
        thread_arg: T::ThreadArg,
    ) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);
        let _ = T::run(event_queue, stop_receiver, thread_arg);
        Self { stop_button }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.stop_button
            .send(())
            .expect("failed to wait for worker thread to finish its job");
    }
}
