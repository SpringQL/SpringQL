mod in_memory_queue;

use anyhow::anyhow;
use once_cell::sync::Lazy;

use crate::error::{Result, SpringError};
use crate::pipeline::name::QueueName;
use std::collections::HashMap;
use std::sync::Arc;

use self::in_memory_queue::InMemoryQueue;

static INSTANCE: Lazy<Arc<InMemoryQueueRepository>> =
    Lazy::new(|| Arc::new(InMemoryQueueRepository::empty()));

/// Singleton
#[derive(Debug)]
pub(in crate::stream_engine) struct InMemoryQueueRepository(HashMap<QueueName, Arc<InMemoryQueue>> 

);

impl InMemoryQueueRepository {
    fn empty() -> Self {
        Self(HashMap::new())
    }

    pub(in crate::stream_engine) fn instance() -> Arc<Self> {
        INSTANCE.clone()
    }

    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
    ///   - queue named `queue_name` does not exist.
    pub(in crate::stream_engine) fn get(
        &self,
        queue_name: &QueueName,
    ) -> Result<Arc<InMemoryQueue>> {
        self.0
            .get(queue_name)
            .cloned()
            .ok_or_else(|| SpringError::Unavailable {
                resource: queue_name.to_string(),
                source: anyhow!("queue not found"),
            })
    }
}
