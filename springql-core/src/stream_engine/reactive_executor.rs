// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use super::command::alter_pipeline_command::AlterPipelineCommand;
use crate::{
    error::Result,
    pipeline::{
        name::PumpName,
        pump_model::{pump_state::PumpState, PumpModel},
        sink_writer_model::SinkWriterModel,
        source_reader_model::SourceReaderModel,
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
            AlterPipelineCommand::CreateSourceStream(source) => {
                Self::create_foreign_source_stream(pipeline, source)
            }
            AlterPipelineCommand::CreateForeignSinkStream(sink) => {
                Self::create_foreign_sink_stream(pipeline, sink)
            }
            AlterPipelineCommand::CreatePump(pump) => Self::create_pump(pipeline, pump),
            AlterPipelineCommand::AlterPump { name, state } => {
                Self::alter_pump(&mut pipeline, name, state)?;
                Ok(pipeline)
            }
        }
    }

    fn create_foreign_source_stream(
        mut pipeline: Pipeline,
        source: SourceReaderModel,
    ) -> Result<Pipeline> {
        let fst = source.dest_source_stream();
        pipeline.add_foreign_stream(fst)?;
        pipeline.add_source_reader(source)?;
        Ok(pipeline)
    }
    fn create_foreign_sink_stream(
        mut pipeline: Pipeline,
        sink: SinkWriterModel,
    ) -> Result<Pipeline> {
        let fst = sink.from_foreign_stream();
        pipeline.add_foreign_stream(fst)?;
        pipeline.add_sink_writer(sink)?;
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
