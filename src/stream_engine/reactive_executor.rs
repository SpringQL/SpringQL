use super::{
    command::alter_pipeline_command::AlterPipelineCommand,
    dependency_injection::DependencyInjection,
    pipeline::{self, server_model::ServerModel, Pipeline},
};
use crate::error::Result;

/// Executor of pipeline management.
///
/// All methods (recursive) are called from main thread.
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct ReactiveExecutor {
    pipeline: Pipeline,
}

impl ReactiveExecutor {
    pub(in crate::stream_engine) fn alter_pipeline(
        &mut self,
        command: AlterPipelineCommand,
    ) -> Result<Pipeline> {
        let new_pipeline = Self::new_pipeline(self.pipeline.clone(), command)?;
        self.pipeline = new_pipeline;
        Ok(self.pipeline.clone())
    }

    fn new_pipeline(pipeline: Pipeline, command: AlterPipelineCommand) -> Result<Pipeline> {
        match command {
            AlterPipelineCommand::CreateStream(_) => todo!(),
            AlterPipelineCommand::CreateForeignStream(server) => {
                Self::create_foreign_stream(pipeline, server)
            }
            AlterPipelineCommand::CreatePump(_) => todo!(),
            AlterPipelineCommand::AlterPump(_) => todo!(),
        }
    }

    fn create_foreign_stream(mut pipeline: Pipeline, server: ServerModel) -> Result<Pipeline> {
        let fst = server.serving_foreign_stream();
        pipeline.add_foreign_stream(fst)?;
        pipeline.add_server(server)?;
        Ok(pipeline)
    }
}
