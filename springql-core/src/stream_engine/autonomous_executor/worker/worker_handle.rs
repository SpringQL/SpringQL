// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use parking_lot::{Mutex, MutexGuard};

use crate::{
    api::SpringConfig,
    stream_engine::autonomous_executor::{
        args::{Coordinators, EventQueues},
        main_job_lock::MainJobLock,
        worker::worker_thread::WorkerThread,
    },
};

/// Handler to run worker thread.
#[derive(Debug)]
pub struct WorkerHandle {
    stop_button: mpsc::SyncSender<()>,
}

impl WorkerHandle {
    pub fn new<T: WorkerThread>(
        main_job_lock: Arc<MainJobLock>,
        event_queues: EventQueues,
        coordinators: Coordinators,
        thread_arg: T::ThreadArg,
    ) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);
        coordinators.worker_stop_coordinator.join();

        T::run(
            main_job_lock,
            event_queues,
            stop_receiver,
            coordinators,
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

/// Coordinator to setup worker threads.
///
/// Any worker cannot start its main job before the whole setup sequence finish.
#[derive(Debug)]
pub struct WorkerSetupCoordinator {
    n_generic_workers: Mutex<i64>, // do not use usize to detect < 0
    n_source_workers: Mutex<i64>,

    has_setup_memory_state_machine_worker: Mutex<bool>,
    has_setup_performance_monitor_worker: Mutex<bool>,
    has_setup_purger_worker: Mutex<bool>,
}

impl WorkerSetupCoordinator {
    const SYNC_SLEEP: Duration = Duration::from_millis(1);

    pub fn new(config: &SpringConfig) -> Self {
        let n_generic_workers = config.worker.n_generic_worker_threads as i64;
        let n_source_workers = config.worker.n_source_worker_threads as i64;
        Self {
            n_generic_workers: Mutex::new(n_generic_workers),
            n_source_workers: Mutex::new(n_source_workers),
            has_setup_memory_state_machine_worker: Mutex::new(false),
            has_setup_performance_monitor_worker: Mutex::new(false),
            has_setup_purger_worker: Mutex::new(false),
        }
    }

    pub fn sync_wait_all_workers(&self) {
        log::info!("[{:?}] sync_wait_all_workers", thread::current().id());

        self.sync_i64(&self.n_generic_workers);
        self.sync_i64(&self.n_source_workers);

        self.sync_bool(&self.has_setup_memory_state_machine_worker);
        self.sync_bool(&self.has_setup_performance_monitor_worker);
        self.sync_bool(&self.has_setup_purger_worker);
    }

    pub fn ready_generic_worker(&self) {
        self.ready_i64(&self.n_generic_workers)
    }
    pub fn ready_source_worker(&self) {
        self.ready_i64(&self.n_source_workers)
    }

    pub fn ready_memory_state_machine_worker(&self) {
        self.ready_bool(&self.has_setup_memory_state_machine_worker);
    }
    pub fn ready_performance_monitor_worker_worker(&self) {
        self.ready_bool(&self.has_setup_performance_monitor_worker);
    }
    pub fn ready_purger_worker(&self) {
        self.ready_bool(&self.has_setup_purger_worker);
    }

    fn sync_i64(&self, n: &Mutex<i64>) {
        self.sync(n, |n_| *n_ == 0);
    }

    fn sync_bool(&self, b: &Mutex<bool>) {
        self.sync(b, |b_| *b_);
    }

    fn sync<T, F: Fn(&T) -> bool>(&self, v: &Mutex<T>, is_sync: F) {
        loop {
            match v.try_lock() {
                Some(v_) => {
                    if is_sync(&v_) {
                        break;
                    } else {
                        thread::sleep(Self::SYNC_SLEEP);
                    }
                }
                None => thread::sleep(Self::SYNC_SLEEP),
            }
        }
    }

    fn ready_i64(&self, n: &Mutex<i64>) {
        let mut n_ = n.lock();
        assert!(*n_ > 0);
        *n_ -= 1;
    }

    fn ready_bool(&self, b: &Mutex<bool>) {
        let mut b_ = b.lock();
        assert!(!*b_);
        *b_ = true;
    }
}

/// Coordinator to stop worker threads.
///
/// Since worker threads publish and subscribe event, a worker cannot be dropped before waiting for other workers to finish
/// (event publisher might fail to send event to channel if the receiver is already dropped).
#[derive(Debug, Default)]
pub struct WorkerStopCoordinator {
    live_worker_count: Mutex<u16>,
}

impl WorkerStopCoordinator {
    pub fn sync_wait_all_workers(&self) {
        self.leave();
        while *self.locked_count() > 0 {
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn join(&self) {
        let mut live_worker_count = self.live_worker_count.lock();
        *live_worker_count += 1;
    }

    fn leave(&self) {
        let mut live_worker_count = self.locked_count();
        *live_worker_count -= 1;
    }

    fn locked_count(&self) -> MutexGuard<u16> {
        self.live_worker_count.lock()
    }
}
