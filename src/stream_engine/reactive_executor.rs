use super::{
    command::alter_pipeline_command::AlterPipelineCommand,
    dependency_injection::DependencyInjection,
    pipeline::{
        self,
        pump_model::{pump_state::PumpState, PumpModel},
        server_model::ServerModel,
        Pipeline,
    },
};
use crate::{error::Result, model::name::PumpName};

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
            AlterPipelineCommand::CreatePump(pump) => Self::create_pump(pipeline, pump),
            AlterPipelineCommand::AlterPump { name, state } => {
                Self::alter_pump(pipeline, name, state)
            }
        }
    }

    fn create_foreign_stream(mut pipeline: Pipeline, server: ServerModel) -> Result<Pipeline> {
        let fst = server.serving_foreign_stream();
        pipeline.add_foreign_stream(fst)?;
        pipeline.add_server(server)?;
        Ok(pipeline)
    }

    fn create_pump(mut pipeline: Pipeline, pump: PumpModel) -> Result<Pipeline> {
        pipeline.add_pump(pump)?;
        Ok(pipeline)
    }

    fn alter_pump(mut pipeline: Pipeline, name: PumpName, state: PumpState) -> Result<Pipeline> {
        let pump = pipeline.get_pump(&name)?;
        let new_pump = pump.started();
        pipeline.remove_pump(&name)?;
        pipeline.add_pump(new_pump)?;
        Ok(pipeline)
    }
}
