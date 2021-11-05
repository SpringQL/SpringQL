mod sql_parser;

use self::sql_parser::SqlParser;
use crate::{error::Result, stream_engine::command::Command};

#[derive(Debug, Default)]
pub(crate) struct SqlProcessor(SqlParser);

impl SqlProcessor {
    pub(crate) fn compile<S: Into<String>>(&self, sql: S) -> Result<Command> {
        self.0.parse(sql)
    }
}
