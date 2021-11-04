mod pest_parser_impl;

use crate::error::Result;
use crate::stream_engine::command::Command;

use self::pest_parser_impl::PestParserImpl;

#[derive(Debug, Default)]
pub(crate) struct SqlParser(PestParserImpl);

impl SqlParser {
    pub(crate) fn parse<S: Into<String>>(&self, sql: S) -> Result<Command> {
        self.0.parse(sql)
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
            server_model::{server_type::ServerType, ServerModel},
            stream_model::{stream_shape::StreamShape, StreamModel},
        },
        stream_engine::command::alter_pipeline_command::AlterPipelineCommand,
    };
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    #[test]
    fn test_create_source_stream() {
        let parser = SqlParser::default();

        let sql = "
            CREATE SOURCE STREAM source_trade (
              ts TIMESTAMP NOT NULL ROWTIME,    
              ticker TEXT NOT NULL,
              amount INTEGER NOT NULL
            ) SERVER NET_SERVER OPTIONS (
              REMOTE_PORT '17890'
            );
            ";
        let command = parser.parse(sql).unwrap();

        let expected_shape = StreamShape::fx_trade();
        let expected_options = OptionsBuilder::default()
            .add("REMOTE_PORT", "17890")
            .build();
        let expected_stream = ForeignStreamModel::new(StreamModel::new(
            StreamName::new("source_trade".to_string()),
            Arc::new(expected_shape),
        ));
        let expected_server = ServerModel::new(
            ServerType::SourceNet,
            Arc::new(expected_stream),
            expected_options,
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreateForeignStream(expected_server))
        );
    }

    #[test]
    fn test_create_sink_stream() {
        let parser = SqlParser::default();

        let sql = "
            CREATE SINK STREAM sink_trade (
              ts TIMESTAMP NOT NULL ROWTIME,    
              ticker TEXT NOT NULL,
              amount INTEGER NOT NULL
            ) SERVER NET_SERVER OPTIONS (
              REMOTE_PORT '17890'
            );
            ";
        let command = parser.parse(sql).unwrap();

        let expected_shape = StreamShape::fx_trade();
        let expected_options = OptionsBuilder::default()
            .add("REMOTE_PORT", "17890")
            .build();
        let expected_stream = ForeignStreamModel::new(StreamModel::new(
            StreamName::new("sink_trade".to_string()),
            Arc::new(expected_shape),
        ));
        let expected_server = ServerModel::new(
            ServerType::SinkNet,
            Arc::new(expected_stream),
            expected_options,
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreateForeignStream(expected_server))
        );
    }

    #[test]
    fn test_create_pump() {
        let parser = SqlParser::default();

        let sql = "
            CREATE PUMP pu_passthrough AS
              INSERT INTO sink_trade (ts, ticker, amount)
              SELECT STREAM ts, ticker, amount FROM source_trade;
            ";
        let command = parser.parse(sql).unwrap();

        let expected_pump = PumpModel::new(
            PumpName::new("pu_passthrough".to_string()),
            PumpState::Stopped,
            StreamName::new("source_trade".to_string()),
            StreamName::new("sink_trade".to_string()),
        );

        assert_eq!(
            command,
            Command::AlterPipeline(AlterPipelineCommand::CreatePump(expected_pump))
        );
    }

    #[test]
    fn test_alter_pump_start() {
        let parser = SqlParser::default();

        let sql = "
            ALTER PUMP pu_passthrough START;
            ";
        let command = parser.parse(sql).unwrap();

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
