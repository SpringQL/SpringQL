// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod sql_parser;

use self::sql_parser::{syntax::SelectStreamSyntax, SqlParser};
use crate::{
    error::Result,
    pipeline::{
        name::{ColumnName, StreamName},
        pump_model::{pump_state::PumpState, PumpModel},
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
                let pump = PumpModel::new(pump_name, PumpState::Stopped, query_plan, insert_plan);
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
            foreign_stream_model::ForeignStreamModel,
            name::{PumpName, StreamName},
            option::options_builder::OptionsBuilder,
            pump_model::{pump_state::PumpState, PumpModel},
            sink_writer::{sink_writer_type::SinkWriterType, SinkWriter},
            source_reader_model::{source_reader_type::SourceReaderType, SourceReaderModel},
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
            ) SERVER NET_SERVER OPTIONS (
              REMOTE_PORT '17890'
            );
            ";
        let command = processor.compile(sql).unwrap();

        let expected_shape = StreamShape::fx_trade();
        let expected_options = OptionsBuilder::default()
            .add("REMOTE_PORT", "17890")
            .build();
        let expected_stream = ForeignStreamModel::new(StreamModel::new(
            StreamName::new("source_trade".to_string()),
            Arc::new(expected_shape),
        ));
        let expected_source = SourceReaderModel::new(
            SourceReaderType::Net,
            Arc::new(expected_stream),
            expected_options,
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreateForeignSourceStream(
                expected_source
            ))
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
            ) SERVER NET_SERVER OPTIONS (
              REMOTE_PORT '17890'
            );
            ";
        let command = processor.compile(sql).unwrap();

        let expected_shape = StreamShape::fx_trade();
        let expected_options = OptionsBuilder::default()
            .add("REMOTE_PORT", "17890")
            .build();
        let expected_stream = ForeignStreamModel::new(StreamModel::new(
            StreamName::new("sink_trade".to_string()),
            Arc::new(expected_shape),
        ));
        let expected_sink = SinkWriter::new(
            SinkWriterType::Net,
            Arc::new(expected_stream),
            expected_options,
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreateForeignSinkStream(expected_sink))
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
            PumpState::Stopped,
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

    #[test]
    fn test_alter_pump_start() {
        let processor = SqlProcessor::default();

        let sql = "
            ALTER PUMP pu_passthrough START;
            ";
        let command = processor.compile(sql).unwrap();

        let expected_pump = PumpName::new("pu_passthrough".to_string());

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::AlterPump {
                name: expected_pump,
                state: PumpState::Started,
            })
        );
    }
}
