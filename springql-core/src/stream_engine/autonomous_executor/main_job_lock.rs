// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use anyhow::Context;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Lock object to barrier main jobs in worker threads.
///
/// This lock is to assure for workers to safely execute tasks while acquiring MainJobLockGuard,
/// while it also gives MainJobBarrierGuard for publishers of strongly-consistent events to block main jobs in other worker threads.
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct MainJobLock(RwLock<MainJobLockToken>);

impl MainJobLock {
    pub(in crate::stream_engine::autonomous_executor) fn main_job_barrier(
        &self,
    ) -> MainJobBarrierGuard {
        let write_lock = self.0.write();
        MainJobBarrierGuard(write_lock)
    }

    /// # Returns
    ///
    /// Ok on successful lock, Err on write lock.
    pub(in crate::stream_engine::autonomous_executor) fn try_main_job(
        &self,
    ) -> Result<MainJobLockGuard, anyhow::Error> {
        self.0
            .try_read()
            .map(MainJobLockGuard)
            .context("write lock may be taken")
    }
}

#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct MainJobLockToken;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct MainJobBarrierGuard<'a>(
    RwLockWriteGuard<'a, MainJobLockToken>,
);

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct MainJobLockGuard<'a>(
    RwLockReadGuard<'a, MainJobLockToken>,
);
