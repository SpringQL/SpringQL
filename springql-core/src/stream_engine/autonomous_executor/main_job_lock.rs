// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use anyhow::Context;

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Lock object to barrier main jobs in worker threads.
///
/// This lock is to assure for workers to safely execute tasks while acquiring MainJobLockGuard,
/// while it also gives MainJobBarrierGuard for publishers of strongly-consistent events to block main jobs in other worker threads.
///
/// See [sequence diagram](https://github.com/SpringQL/SpringQL/issues/100#issuecomment-1101732796) to understand the strongly-consistent events flow.
#[derive(Debug, Default)]
pub struct MainJobLock(RwLock<MainJobLockToken>);

impl MainJobLock {
    pub fn main_job_barrier(&self) -> MainJobBarrierGuard {
        let write_lock = self.0.write().unwrap();
        MainJobBarrierGuard(write_lock)
    }

    /// # Returns
    ///
    /// Ok on successful lock, Err on write lock.
    pub fn try_main_job(&self) -> Result<MainJobLockGuard, anyhow::Error> {
        self.0
            .try_read()
            .ok()
            .map(MainJobLockGuard)
            .context("write lock may be taken")
    }
}

#[derive(Debug, Default)]
pub struct MainJobLockToken;

#[derive(Debug)]
pub struct MainJobBarrierGuard<'a>(RwLockWriteGuard<'a, MainJobLockToken>);

#[derive(Debug)]
pub struct MainJobLockGuard<'a>(RwLockReadGuard<'a, MainJobLockToken>);
