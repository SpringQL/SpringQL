// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::Result;
use crate::{error::SpringError, stream_engine::StreamEngine};
use anyhow::anyhow;

use super::spring_config::SpringConfig;

#[derive(Clone, Debug)]
pub(crate) struct EngineMutex(Arc<Mutex<StreamEngine>>);

impl EngineMutex {
    pub(crate) fn new(config: &SpringConfig) -> Self {
        let engine = StreamEngine::new(config);
        Self(Arc::new(Mutex::new(engine)))
    }

    /// # Failure
    ///
    /// - [SpringError::ThreadPoisoned](crate::error::SpringError::ThreadPoisoned)
    pub(crate) fn get(&self) -> Result<MutexGuard<'_, StreamEngine>> {
        self.0
            .lock()
            .map_err(|e| {
                anyhow!(
                    "another thread sharing the same stream-engine got panic: {:?}",
                    e
                )
            })
            .map_err(SpringError::SpringQlCoreIo)
    }
}
