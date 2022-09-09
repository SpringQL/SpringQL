// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use std::sync::RwLock;

use crate::stream_engine::autonomous_executor::{
    queue::window_queue::WindowQueue, task_graph::WindowQueueId,
};

#[derive(Debug, Default)]
pub struct WindowQueueRepository {
    repo: RwLock<HashMap<WindowQueueId, Arc<WindowQueue>>>,
}

impl WindowQueueRepository {
    pub fn get(&self, window_queue_id: &WindowQueueId) -> Arc<WindowQueue> {
        let repo = self.repo.read().unwrap();
        repo.get(window_queue_id)
            .unwrap_or_else(|| {
                panic!(
                    "Window queue id {} is not in WindowQueueRepository",
                    window_queue_id
                )
            })
            .clone()
    }

    /// Removes all currently existing queues and creates new empty ones.
    pub fn reset(&self, queue_ids: HashSet<WindowQueueId>) {
        let mut repo = self.repo.write().unwrap();
        repo.clear();

        queue_ids.into_iter().for_each(|queue_id| {
            repo.insert(queue_id, Arc::new(WindowQueue::default()));
        });
    }

    pub fn purge(&self) {
        let mut repo = self.repo.write().unwrap();
        repo.iter_mut().for_each(|(_, queue)| {
            queue.purge();
        });
    }
}
