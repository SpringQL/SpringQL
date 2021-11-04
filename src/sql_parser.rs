use crate::error::Result;
use crate::stream_engine::command::Command;

#[derive(Debug, Default)]
pub(crate) struct SqlParser;

impl SqlParser {
    pub(crate) fn parse<S: Into<String>>(&self, sql: S) -> Result<Command> {
        todo!()
    }
}
