use super::command::alter_pipeline_command::AlterPipelineCommand;
use crate::{
    error::Result,
    model::name::PumpName,
    pipeline::{
        pump_model::{pump_state::PumpState, PumpModel},
        server_model::ServerModel,
        Pipeline,
    },
};

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

    fn new_pipeline(mut pipeline: Pipeline, command: AlterPipelineCommand) -> Result<Pipeline> {
        match command {
            AlterPipelineCommand::CreateStream(_) => todo!(),
            AlterPipelineCommand::CreateForeignStream(server) => {
                Self::create_foreign_stream(pipeline, server)
            }
            AlterPipelineCommand::CreatePump(pump) => Self::create_pump(pipeline, pump),
            AlterPipelineCommand::AlterPump { name, state } => {
                Self::alter_pump(&mut pipeline, name, state)?;
                Ok(pipeline)
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

    fn alter_pump(pipeline: &mut Pipeline, name: PumpName, state: PumpState) -> Result<()> {
        let pump = pipeline.get_pump(&name)?.clone();
        match (pump.state(), state) {
            (PumpState::Stopped, PumpState::Started) => Self::_alter_pump_start(pipeline, &pump),
            (PumpState::Started, PumpState::Stopped) => todo!(),
            _ => Ok(()),
        }
    }
    fn _alter_pump_start(pipeline: &mut Pipeline, pump: &PumpModel) -> Result<()> {
        let new_pump = pump.started();
        pipeline.remove_pump(pump.name())?;
        pipeline.add_pump(new_pump)?;
        Ok(())
    }
}
