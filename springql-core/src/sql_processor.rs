// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod sql_parser;

mod query_planner;

use self::{
    query_planner::QueryPlanner,
    sql_parser::{syntax::SelectStreamSyntax, SqlParser},
};
use crate::{
    error::Result,
    pipeline::{
        name::PumpName, pump_model::PumpModel, sink_writer_model::SinkWriterModel,
        source_reader_model::SourceReaderModel, stream_model::StreamModel, Pipeline,
    },
    sql_processor::sql_parser::parse_success::ParseSuccess,
    stream_engine::command::{
        alter_pipeline_command::AlterPipelineCommand, insert_plan::InsertPlan,
        query_plan::QueryPlan, Command,
    },
};

#[derive(Debug, Default)]
pub(crate) struct SqlProcessor(SqlParser);

impl SqlProcessor {
    /// # Failures
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) on syntax and semantics error.
    pub(crate) fn compile<S: Into<String>>(&self, sql: S, pipeline: &Pipeline) -> Result<Command> {
        let command = match self.0.parse(sql)? {
            ParseSuccess::CreateSourceStream(source_stream_model) => {
                self.compile_create_source_stream(source_stream_model, pipeline)?
            }
            ParseSuccess::CreateSourceReader(source_reader_model) => {
                self.compile_create_source_reader(source_reader_model, pipeline)?
            }
            ParseSuccess::CreateStream(stream_model) => {
                self.compile_create_stream(stream_model, pipeline)?
            }
            ParseSuccess::CreateSinkStream(sink_stream_model) => {
                self.compile_create_sink_stream(sink_stream_model, pipeline)?
            }
            ParseSuccess::CreateSinkWriter(sink_writer_model) => {
                self.compile_create_sink_writer(sink_writer_model, pipeline)?
            }
            ParseSuccess::CreatePump {
                pump_name,
                select_stream_syntax,
                insert_plan,
            } => {
                self.compile_create_pump(pump_name, select_stream_syntax, insert_plan, pipeline)?
            }
        };
        Ok(command)
    }

    fn compile_create_source_stream(
        &self,
        source_stream_model: StreamModel,
        _pipeline: &Pipeline,
    ) -> Result<Command> {
        // TODO semantic check
        Ok(Command::AlterPipeline(
            AlterPipelineCommand::CreateSourceStream(source_stream_model),
        ))
    }

    fn compile_create_source_reader(
        &self,
        source_reader_model: SourceReaderModel,
        _pipeline: &Pipeline,
    ) -> Result<Command> {
        // TODO semantic check
        Ok(Command::AlterPipeline(
            AlterPipelineCommand::CreateSourceReader(source_reader_model),
        ))
    }

    fn compile_create_stream(
        &self,
        stream_model: StreamModel,
        _pipeline: &Pipeline,
    ) -> Result<Command> {
        // TODO semantic check
        Ok(Command::AlterPipeline(AlterPipelineCommand::CreateStream(
            stream_model,
        )))
    }

    fn compile_create_sink_stream(
        &self,
        sink_stream_model: StreamModel,
        _pipeline: &Pipeline,
    ) -> Result<Command> {
        // TODO semantic check
        Ok(Command::AlterPipeline(
            AlterPipelineCommand::CreateSinkStream(sink_stream_model),
        ))
    }

    fn compile_create_sink_writer(
        &self,
        sink_writer_model: SinkWriterModel,
        _pipeline: &Pipeline,
    ) -> Result<Command> {
        // TODO semantic check
        Ok(Command::AlterPipeline(
            AlterPipelineCommand::CreateSinkWriter(sink_writer_model),
        ))
    }

    fn compile_create_pump(
        &self,
        pump_name: PumpName,
        select_stream_syntax: SelectStreamSyntax,
        insert_plan: InsertPlan,
        pipeline: &Pipeline,
    ) -> Result<Command> {
        let query_plan = self.compile_select_stream(select_stream_syntax, pipeline)?;
        let pump = PumpModel::new(pump_name, query_plan, insert_plan);
        Ok(Command::AlterPipeline(AlterPipelineCommand::CreatePump(
            Box::new(pump),
        )))
    }

    fn compile_select_stream(
        &self,
        select_stream_syntax: SelectStreamSyntax,
        pipeline: &Pipeline,
    ) -> Result<QueryPlan> {
        let planner = QueryPlanner::new(select_stream_syntax);
        planner.plan(pipeline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pipeline::{
            name::{SinkWriterName, SourceReaderName, StreamName},
            option::options_builder::OptionsBuilder,
            pipeline_version::PipelineVersion,
            sink_writer_model::{sink_writer_type::SinkWriterType, SinkWriterModel},
            source_reader_model::{source_reader_type::SourceReaderType, SourceReaderModel},
            stream_model::{stream_shape::StreamShape, StreamModel},
        },
        stream_engine::command::alter_pipeline_command::AlterPipelineCommand,
    };
    use pretty_assertions::assert_eq;

    #[test]
    fn test_create_source_stream() {
        let processor = SqlProcessor::default();
        let pipeline = Pipeline::new(PipelineVersion::new());

        let sql = "
            CREATE SOURCE STREAM source_trade (
              ts TIMESTAMP NOT NULL ROWTIME,    
              ticker TEXT NOT NULL,
              amount INTEGER NOT NULL
            );
            ";
        let command = processor.compile(sql, &pipeline).unwrap();

        let expected_shape = StreamShape::fx_trade();
        let expected_stream =
            StreamModel::new(StreamName::new("source_trade".to_string()), expected_shape);

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreateSourceStream(expected_stream))
        );
    }

    #[test]
    fn test_create_source_reader() {
        let processor = SqlProcessor::default();
        let pipeline = Pipeline::fx_source_only();

        let sql = "
            CREATE SOURCE READER tcp_source FOR st_1
              TYPE NET_CLIENT OPTIONS (
                REMOTE_PORT '17890'
              );
            ";
        let command = processor.compile(sql, &pipeline).unwrap();

        let expected_name = SourceReaderName::new("tcp_source".to_string());

        let expected_options = OptionsBuilder::default()
            .add("REMOTE_PORT", "17890")
            .build();
        let expected_dest_source_stream = StreamName::new("st_1".to_string());
        let expected_source = SourceReaderModel::new(
            expected_name,
            SourceReaderType::NetClient,
            expected_dest_source_stream,
            expected_options,
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreateSourceReader(expected_source))
        );
    }

    #[test]
    fn test_create_sink_stream() {
        let processor = SqlProcessor::default();
        let pipeline = Pipeline::new(PipelineVersion::new());

        let sql = "
            CREATE SINK STREAM sink_trade (
              ts TIMESTAMP NOT NULL ROWTIME,    
              ticker TEXT NOT NULL,
              amount INTEGER NOT NULL
            );
            ";
        let command = processor.compile(sql, &pipeline).unwrap();

        let expected_shape = StreamShape::fx_trade();
        let expected_stream =
            StreamModel::new(StreamName::new("sink_trade".to_string()), expected_shape);

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreateSinkStream(expected_stream))
        );
    }

    #[test]
    fn test_create_sink_writer() {
        let processor = SqlProcessor::default();
        let pipeline = Pipeline::fx_sink_only();

        let sql = "
            CREATE SINK WRITER tcp_sink_trade FOR sink_1
              TYPE NET_CLIENT OPTIONS (
                REMOTE_PORT '17890'
              );
            ";
        let command = processor.compile(sql, &pipeline).unwrap();

        let expected_options = OptionsBuilder::default()
            .add("REMOTE_PORT", "17890")
            .build();
        let expected_sink = SinkWriterModel::new(
            SinkWriterName::new("tcp_sink_trade".to_string()),
            SinkWriterType::Net,
            StreamName::new("sink_1".to_string()),
            expected_options,
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreateSinkWriter(expected_sink))
        );
    }
}
