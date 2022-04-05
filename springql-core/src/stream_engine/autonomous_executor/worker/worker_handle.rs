// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    sync::{mpsc, Arc, Mutex, MutexGuard},
    thread,
    time::Duration,
};

use crate::stream_engine::autonomous_executor::event_queue::EventQueue;

use super::worker_thread::WorkerThread;

/// Handler to run worker thread.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct WorkerHandle {
    stop_button: mpsc::SyncSender<()>,
}

impl WorkerHandle {
    pub(in crate::stream_engine::autonomous_executor) fn new<T: WorkerThread>(
        event_queue: Arc<EventQueue>,
        worker_stop_coordinate: Arc<WorkerStopCoordinate>,
        thread_arg: T::ThreadArg,
    ) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);
        worker_stop_coordinate.join();

        let _ = T::run(
            event_queue,
            stop_receiver,
            worker_stop_coordinate,
            thread_arg,
        );
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

/// Coordinator to stop worker threads.
///
/// Since worker threads publish and subscribe event, a worker cannot be dropped before waiting for other workers to finish
/// (event publisher might fail to send event to channel if the receiver is already dropped).
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct WorkerStopCoordinate {
    live_worker_count: Mutex<u16>,
}

impl WorkerStopCoordinate {
    pub(in crate::stream_engine::autonomous_executor) fn sync_wait_all_workers(&self) {
        self.leave();
        while *self.locked_count() > 0 {
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn join(&self) {
        let mut live_worker_count = self
            .live_worker_count
            .lock()
            .expect("live_worker_count mutex got poisoned");
        *live_worker_count += 1;
    }

    fn leave(&self) {
        let mut live_worker_count = self.locked_count();
        *live_worker_count -= 1;
    }

    fn locked_count(&self) -> MutexGuard<u16> {
        self.live_worker_count
            .lock()
            .expect("live_worker_count mutex got poisoned")
    }
}
