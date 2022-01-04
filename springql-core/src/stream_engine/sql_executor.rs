// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use super::command::alter_pipeline_command::AlterPipelineCommand;
use crate::{
    error::Result,
    pipeline::{
        foreign_stream_model::ForeignStreamModel, pump_model::PumpModel,
        sink_writer_model::SinkWriterModel, source_reader_model::SourceReaderModel, Pipeline,
    },
};

/// Executor of SQL.
///
/// All methods (recursive) are called from main thread.
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct SqlExecutor {
    pipeline: Pipeline,
}

impl SqlExecutor {
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
            AlterPipelineCommand::CreateSourceStream(source_stream) => {
                Self::create_foreign_source_stream(pipeline, source_stream)
            }
            AlterPipelineCommand::CreateSourceReader(source_reader) => {
                Self::create_foreign_source_reader(pipeline, source_reader)
            }
            AlterPipelineCommand::CreateForeignSinkStream(sink) => {
                Self::create_foreign_sink_stream(pipeline, sink)
            }
            AlterPipelineCommand::CreatePump(pump) => Self::create_pump(pipeline, pump),
        }
    }

    fn create_foreign_source_stream(
        mut pipeline: Pipeline,
        source_stream: ForeignStreamModel,
    ) -> Result<Pipeline> {
        pipeline.add_foreign_stream(Arc::new(source_stream))?;
        Ok(pipeline)
    }
    fn create_foreign_source_reader(
        mut pipeline: Pipeline,
        source_reader: SourceReaderModel,
    ) -> Result<Pipeline> {
        pipeline.add_source_reader(source_reader)?;
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
}
