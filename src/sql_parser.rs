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
            name::StreamName,
            option::options_builder::OptionsBuilder,
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
}
