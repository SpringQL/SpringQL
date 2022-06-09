// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod in_memory_queue;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::anyhow;
use once_cell::sync::Lazy;

use crate::{
    api::error::{Result, SpringError},
    pipeline::name::QueueName,
    stream_engine::in_memory_queue_repository::in_memory_queue::InMemoryQueue,
};

static INSTANCE: Lazy<Arc<InMemoryQueueRepository>> =
    Lazy::new(|| Arc::new(InMemoryQueueRepository::empty()));

/// Singleton
#[derive(Debug)]
pub(in crate::stream_engine) struct InMemoryQueueRepository(
    Mutex<HashMap<QueueName, Arc<InMemoryQueue>>>, // TODO faster (lock-free?) queue
);

impl InMemoryQueueRepository {
    fn empty() -> Self {
        Self(Mutex::new(HashMap::new()))
    }

    pub(in crate::stream_engine) fn instance() -> Arc<Self> {
        INSTANCE.clone()
    }

    /// # Failure
    ///
    /// - `SpringError::Unavailable` when:
    ///   - queue named `queue_name` does not exist.
    pub(in crate::stream_engine) fn get(
        &self,
        queue_name: &QueueName,
    ) -> Result<Arc<InMemoryQueue>> {
        self.lock()
            .get(queue_name)
            .cloned()
            .ok_or_else(|| SpringError::Unavailable {
                resource: queue_name.to_string(),
                source: anyhow!("queue not found"),
            })
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - queue named `queue_name` already exists.
    pub(in crate::stream_engine) fn create(&self, queue_name: QueueName) -> Result<()> {
        let r = self
            .lock()
            .insert(queue_name.clone(), Arc::new(InMemoryQueue::default()));

        if r.is_none() {
            Ok(())
        } else {
            Err(SpringError::Sql(anyhow!(
                "queue ({}) already exists",
                queue_name
            )))
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<QueueName, Arc<InMemoryQueue>>> {
        self.0
            .lock()
            .expect("another thread sharing the same InMemoryQueueRepository internal got panic")
    }
}
