// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod sql_parser;

use self::sql_parser::{syntax::SelectStreamSyntax, SqlParser};
use crate::{
    error::Result,
    pipeline::{
        name::{ColumnName, StreamName},
        pump_model::PumpModel,
    },
    sql_processor::sql_parser::parse_success::ParseSuccess,
    stream_engine::command::{
        alter_pipeline_command::AlterPipelineCommand,
        query_plan::{query_plan_operation::QueryPlanOperation, QueryPlan},
        Command,
    },
};

#[derive(Debug, Default)]
pub(crate) struct SqlProcessor(SqlParser);

impl SqlProcessor {
    pub(crate) fn compile<S: Into<String>>(&self, sql: S) -> Result<Command> {
        let command = match self.0.parse(sql)? {
            ParseSuccess::CreatePump {
                pump_name,
                select_stream_syntax,
                insert_plan,
            } => {
                let query_plan = self.compile_select_stream(select_stream_syntax);
                let pump = PumpModel::new(pump_name, query_plan, insert_plan);
                Command::AlterPipeline(AlterPipelineCommand::CreatePump(pump))
            }
            ParseSuccess::CommandWithoutQuery(command) => command,
        };
        Ok(command)
    }

    fn compile_select_stream(&self, select_stream_syntax: SelectStreamSyntax) -> QueryPlan {
        let mut plan = QueryPlan::default();

        let from_op = self.compile_from(select_stream_syntax.from_stream);
        let projection_op = self.compile_projection(select_stream_syntax.column_names);

        plan.add_root(projection_op.clone());
        plan.add_left(&projection_op, from_op);
        plan
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
        pipeline::{
            name::{PumpName, SinkWriterName, SourceReaderName, StreamName},
            option::options_builder::OptionsBuilder,
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

        let sql = "
            CREATE SOURCE STREAM source_trade (
              ts TIMESTAMP NOT NULL ROWTIME,    
              ticker TEXT NOT NULL,
              amount INTEGER NOT NULL
            );
            ";
        let command = processor.compile(sql).unwrap();

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

        let sql = "
            CREATE SOURCE READER tcp_source FOR source_trade
              TYPE NET_SERVER OPTIONS (
                REMOTE_PORT '17890'
              );
            ";
        let command = processor.compile(sql).unwrap();

        let expected_name = SourceReaderName::new("tcp_source".to_string());

        let expected_options = OptionsBuilder::default()
            .add("REMOTE_PORT", "17890")
            .build();
        let expected_dest_source_stream = StreamName::new("source_trade".to_string());
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
    fn test_create_sink_stream() {
        let processor = SqlProcessor::default();

        let sql = "
            CREATE SINK STREAM sink_trade (
              ts TIMESTAMP NOT NULL ROWTIME,    
              ticker TEXT NOT NULL,
              amount INTEGER NOT NULL
            );
            ";
        let command = processor.compile(sql).unwrap();

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

        let sql = "
            CREATE SINK WRITER tcp_sink_trade FOR sink_trade
              TYPE NET_SERVER OPTIONS (
                REMOTE_PORT '17890'
              );
            ";
        let command = processor.compile(sql).unwrap();

        let expected_options = OptionsBuilder::default()
            .add("REMOTE_PORT", "17890")
            .build();
        let expected_sink = SinkWriterModel::new(
            SinkWriterName::new("tcp_sink_trade".to_string()),
            SinkWriterType::Net,
            StreamName::new("sink_trade".to_string()),
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

        let sql = "
            CREATE PUMP pu_passthrough AS
              INSERT INTO sink_trade (ts, ticker, amount)
              SELECT STREAM ts, ticker, amount FROM source_trade;
            ";
        let command = processor.compile(sql).unwrap();

        let expected_pump = PumpModel::new(
            PumpName::new("pu_passthrough".to_string()),
            QueryPlan::fx_collect_projection(
                StreamName::new("source_trade".to_string()),
                vec![
                    ColumnName::fx_timestamp(),
                    ColumnName::fx_ticker(),
                    ColumnName::fx_amount(),
                ],
            ),
            InsertPlan::fx_trade(StreamName::new("sink_trade".to_string())),
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreatePump(expected_pump))
        );
    }
}
