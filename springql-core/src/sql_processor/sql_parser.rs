// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod syntax;

pub(in crate::sql_processor) mod parse_success;

mod pest_parser_impl;

use crate::error::Result;

use self::parse_success::ParseSuccess;
use self::pest_parser_impl::PestParserImpl;

#[derive(Debug, Default)]
pub(in crate::sql_processor) struct SqlParser(PestParserImpl);

impl SqlParser {
    pub(in crate::sql_processor) fn parse<S: Into<String>>(&self, sql: S) -> Result<ParseSuccess> {
        let sql = sql.into();
        log::debug!("start parsing SQL: {}", &sql);
        self.0.parse(sql)
    }
}
