pub(in crate::sql_processor) mod parse_success;
pub(in crate::sql_processor) mod syntax;

mod pest_parser_impl;

use crate::error::Result;

use self::parse_success::ParseSuccess;
use self::pest_parser_impl::PestParserImpl;

#[derive(Debug, Default)]
pub(in crate::sql_processor) struct SqlParser(PestParserImpl);

impl SqlParser {
    pub(in crate::sql_processor) fn parse<S: Into<String>>(&self, sql: S) -> Result<ParseSuccess> {
        self.0.parse(sql)
    }
}
