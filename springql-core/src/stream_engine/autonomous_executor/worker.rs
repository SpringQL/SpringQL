// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Worker framework.

mod worker_handle;
mod worker_thread;

pub use worker_handle::{WorkerHandle, WorkerSetupCoordinator, WorkerStopCoordinator};
pub use worker_thread::{WorkerThread, WorkerThreadLoopState};
