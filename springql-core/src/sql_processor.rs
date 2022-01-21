// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod sql_parser;

use self::sql_parser::{syntax::SelectStreamSyntax, SqlParser};
use crate::{
    error::Result,
    pipeline::{
        name::{ColumnName, PumpName, StreamName},
        pump_model::PumpModel,
        sink_stream_model::SinkStreamModel,
        sink_writer_model::SinkWriterModel,
        source_reader_model::SourceReaderModel,
        source_stream_model::SourceStreamModel,
        Pipeline,
    },
    sql_processor::sql_parser::parse_success::ParseSuccess,
    stream_engine::command::{
        alter_pipeline_command::AlterPipelineCommand,
        insert_plan::InsertPlan,
        query_plan::{query_plan_operation::QueryPlanOperation, QueryPlan},
        Command,
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
        source_stream_model: SourceStreamModel,
        pipeline: &Pipeline,
    ) -> Result<Command> {
        // TODO semantic check
        Ok(Command::AlterPipeline(
            AlterPipelineCommand::CreateSourceStream(source_stream_model),
        ))
    }

    fn compile_create_source_reader(
        &self,
        source_reader_model: SourceReaderModel,
        pipeline: &Pipeline,
    ) -> Result<Command> {
        // TODO semantic check
        Ok(Command::AlterPipeline(
            AlterPipelineCommand::CreateSourceReader(source_reader_model),
        ))
    }

    fn compile_create_sink_stream(
        &self,
        sink_stream_model: SinkStreamModel,
        pipeline: &Pipeline,
    ) -> Result<Command> {
        // TODO semantic check
        Ok(Command::AlterPipeline(
            AlterPipelineCommand::CreateSinkStream(sink_stream_model),
        ))
    }

    fn compile_create_sink_writer(
        &self,
        sink_writer_model: SinkWriterModel,
        pipeline: &Pipeline,
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
            pump,
        )))
    }

    fn compile_select_stream(
        &self,
        select_stream_syntax: SelectStreamSyntax,
        pipeline: &Pipeline,
    ) -> Result<QueryPlan> {
        let mut plan = QueryPlan::default();

        let from_op = self.compile_from(select_stream_syntax.from_stream);
        let projection_op = self.compile_projection(select_stream_syntax.column_names);

        plan.add_root(projection_op.clone());
        plan.add_left(&projection_op, from_op);
        Ok(plan)
    }

    fn compile_from(&self, from_stream: StreamName) -> QueryPlanOperation {
        QueryPlanOperation::Collect {
            stream: from_stream,
        }
    }

    fn compile_projection(&self, column_names: Vec<ColumnName>) -> QueryPlanOperation {
        QueryPlanOperation::Projection { column_names }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::SpringError,
        pipeline::{
            name::{PumpName, SinkWriterName, SourceReaderName, StreamName},
            option::options_builder::OptionsBuilder,
            pipeline_version::PipelineVersion,
            pump_model::PumpModel,
            sink_stream_model::SinkStreamModel,
            sink_writer_model::{sink_writer_type::SinkWriterType, SinkWriterModel},
            source_reader_model::{source_reader_type::SourceReaderType, SourceReaderModel},
            source_stream_model::SourceStreamModel,
            stream_model::{stream_shape::StreamShape, StreamModel},
        },
        stream_engine::command::{
            alter_pipeline_command::AlterPipelineCommand, insert_plan::InsertPlan,
            query_plan::QueryPlan,
        },
    };
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

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
        let expected_stream = SourceStreamModel::new(StreamModel::new(
            StreamName::new("source_trade".to_string()),
            Arc::new(expected_shape),
        ));

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
              TYPE NET_SERVER OPTIONS (
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
            SourceReaderType::Net,
            expected_dest_source_stream,
            expected_options,
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreateSourceReader(expected_source))
        );
    }
    #[test]
    fn test_create_source_reader_for_invalid_stream() {
        let processor = SqlProcessor::default();
        let pipeline = Pipeline::new(PipelineVersion::new());

        let sql = "
            CREATE SOURCE READER tcp_source FOR st_404
              TYPE NET_SERVER OPTIONS (
                REMOTE_PORT '17890'
              );
            ";
        assert!(matches!(
            processor.compile(sql, &pipeline).unwrap_err(),
            SpringError::Sql(_)
        ));
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
        let expected_stream = SinkStreamModel::new(StreamModel::new(
            StreamName::new("sink_trade".to_string()),
            Arc::new(expected_shape),
        ));

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
              TYPE NET_SERVER OPTIONS (
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

    #[test]
    fn test_create_pump() {
        let processor = SqlProcessor::default();
        let pipeline = Pipeline::fx_source_sink_no_pump();

        let sql = "
            CREATE PUMP pu_passthrough AS
              INSERT INTO st_2 (ts, ticker, amount)
              SELECT STREAM ts, ticker, amount FROM st_1;
            ";
        let command = processor.compile(sql, &pipeline).unwrap();

        let expected_pump = PumpModel::new(
            PumpName::new("pu_passthrough".to_string()),
            QueryPlan::fx_collect_projection(
                StreamName::new("st_1".to_string()),
                vec![
                    ColumnName::fx_timestamp(),
                    ColumnName::fx_ticker(),
                    ColumnName::fx_amount(),
                ],
            ),
            InsertPlan::fx_trade(StreamName::new("st_2".to_string())),
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreatePump(expected_pump))
        );
    }
}
