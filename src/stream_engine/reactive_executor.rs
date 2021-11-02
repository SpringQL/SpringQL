use std::sync::Arc;

use super::{
    command::alter_pipeline_command::AlterPipelineCommand,
    dependency_injection::DependencyInjection,
    pipeline::{self, Pipeline},
};
use crate::error::Result;

/// Executor of pipeline management.
///
/// All methods (recursive) are called from main thread.
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct ReactiveExecutor {
    pipeline: Arc<Pipeline>,
}

impl ReactiveExecutor {
    pub(in crate::stream_engine) fn alter_pipeline(
        &mut self,
        command: AlterPipelineCommand,
    ) -> Result<Arc<Pipeline>> {
        let new_pipeline = todo!();
        self.pipeline = Arc::new(new_pipeline);
        Ok(self.pipeline.clone())
    }
}
