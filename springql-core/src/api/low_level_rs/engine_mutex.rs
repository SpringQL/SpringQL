use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::Result;
use crate::{error::SpringError, stream_engine::StreamEngine};
use anyhow::anyhow;

#[derive(Clone, Debug)]
pub(super) struct EngineMutex(Arc<Mutex<StreamEngine>>);

impl EngineMutex {
    pub(super) fn new(n_worker_threads: usize) -> Self {
        let engine = StreamEngine::new(n_worker_threads);
        Self(Arc::new(Mutex::new(engine)))
    }

    /// # Failure
    ///
    /// - [SpringError::ThreadPoisoned](crate::error::SpringError::ThreadPoisoned)
    pub(super) fn get(&self) -> Result<MutexGuard<'_, StreamEngine>> {
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
