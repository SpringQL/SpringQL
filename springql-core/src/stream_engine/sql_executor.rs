// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::{
    api::error::Result,
    pipeline::{
        pump_model::PumpModel, sink_writer_model::SinkWriterModel,
        source_reader_model::SourceReaderModel, stream_model::StreamModel, Pipeline,
        PipelineVersion,
    },
    stream_engine::command::alter_pipeline_command::AlterPipelineCommand,
};

/// Executor of SQL.
///
/// All methods (recursive) are called from main thread.
#[derive(Debug)]
pub(in crate::stream_engine) struct SqlExecutor {
    pipeline: Pipeline,
}

impl Default for SqlExecutor {
    fn default() -> Self {
        Self {
            pipeline: Pipeline::new(PipelineVersion::new()),
        }
    }
}

impl SqlExecutor {
    pub(crate) fn current_pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

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
                Self::create_source_stream(pipeline, source_stream)
            }
            AlterPipelineCommand::CreateSourceReader(source_reader) => {
                Self::create_source_reader(pipeline, source_reader)
            }
            AlterPipelineCommand::CreateStream(stream) => Self::create_stream(pipeline, stream),
            AlterPipelineCommand::CreateSinkStream(sink_stream) => {
                Self::create_sink_stream(pipeline, sink_stream)
            }
            AlterPipelineCommand::CreateSinkWriter(sink_writer) => {
                Self::create_sink_writer(pipeline, sink_writer)
            }
            AlterPipelineCommand::CreatePump(pump) => Self::create_pump(pipeline, *pump),
        }
    }

    fn create_source_stream(
        mut pipeline: Pipeline,
        source_stream: StreamModel,
    ) -> Result<Pipeline> {
        pipeline.add_stream(Arc::new(source_stream))?;
        Ok(pipeline)
    }
    fn create_source_reader(
        mut pipeline: Pipeline,
        source_reader: SourceReaderModel,
    ) -> Result<Pipeline> {
        pipeline.add_source_reader(source_reader)?;
        Ok(pipeline)
    }

    fn create_stream(mut pipeline: Pipeline, stream: StreamModel) -> Result<Pipeline> {
        pipeline.add_stream(Arc::new(stream))?;
        Ok(pipeline)
    }

    fn create_sink_stream(mut pipeline: Pipeline, sink_stream: StreamModel) -> Result<Pipeline> {
        pipeline.add_stream(Arc::new(sink_stream))?;
        Ok(pipeline)
    }
    fn create_sink_writer(
        mut pipeline: Pipeline,
        sink_writer: SinkWriterModel,
    ) -> Result<Pipeline> {
        pipeline.add_sink_writer(sink_writer)?;
        Ok(pipeline)
    }

    fn create_pump(mut pipeline: Pipeline, pump: PumpModel) -> Result<Pipeline> {
        pipeline.add_pump(pump)?;
        Ok(pipeline)
    }
}
