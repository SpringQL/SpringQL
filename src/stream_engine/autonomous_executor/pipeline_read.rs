use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::stream_engine::pipeline::Pipeline;

/// Reader of Pipeline
#[derive(Clone, Debug, new)]
pub(in crate::stream_engine) struct PipelineRead(Arc<RwLock<Pipeline>>);

impl PipelineRead {
    pub(super) fn read_lock(&self) -> RwLockReadGuard<'_, Pipeline> {
        self.0
            .read()
            .expect("another thread sharing the same Pipeline must not panic")
    }
}
