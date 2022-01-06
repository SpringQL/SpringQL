use std::collections::{HashMap, HashSet};

use crate::stream_engine::autonomous_executor::task_graph::queue_id::row_queue_id::RowQueueId;

use super::row_queue::RowQueue;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct RowQueueRepository {
    repo: HashMap<RowQueueId, RowQueue>,
}

impl RowQueueRepository {
    pub(in crate::stream_engine::autonomous_executor) fn get(
        &self,
        row_queue_id: &RowQueueId,
    ) -> &RowQueue {
        self.repo
            .get(row_queue_id)
            .unwrap_or_else(|| panic!("row queue id {} is not in RowQueueRepository", row_queue_id))
    }

    /// Removes all currently existing queues and creates new empty ones.
    pub(in crate::stream_engine::autonomous_executor) fn reset(
        &mut self,
        queue_ids: HashSet<RowQueueId>,
    ) -> Self {
        let repo = queue_ids
            .into_iter()
            .map(|queue_id| (queue_id, RowQueue::default()))
            .collect();
        Self { repo }
    }
}
