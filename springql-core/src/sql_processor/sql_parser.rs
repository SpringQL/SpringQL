mod pest_parser_impl;

use crate::error::Result;
use crate::stream_engine::command::Command;

use self::pest_parser_impl::PestParserImpl;

#[derive(Debug, Default)]
pub(in crate::sql_processor) struct SqlParser(PestParserImpl);

impl SqlParser {
    pub(in crate::sql_processor) fn parse<S: Into<String>>(&self, sql: S) -> Result<Command> {
        self.0.parse(sql)
    }
}
