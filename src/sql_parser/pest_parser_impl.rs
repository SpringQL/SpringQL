use crate::error::Result;
use crate::stream_engine::command::Command;

#[derive(Debug, Default)]
pub(in crate::sql_parser) struct PestParserImpl;

impl PestParserImpl {
    pub(crate) fn parse<S: Into<String>>(&self, sql: S) -> Result<Command> {
        todo!()
    }
}
