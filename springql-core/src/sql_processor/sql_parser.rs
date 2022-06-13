// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod parse_success;
mod pest_parser_impl;
mod syntax;

pub use parse_success::{CreatePump, ParseSuccess};
pub use pest_parser_impl::PestParserImpl;
pub use syntax::*;

use crate::api::error::Result;

#[derive(Debug, Default)]
pub struct SqlParser(PestParserImpl);

impl SqlParser {
    pub fn parse<S: Into<String>>(&self, sql: S) -> Result<ParseSuccess> {
        let sql = sql.into();
        log::debug!("start parsing SQL: {}", &sql);
        self.0.parse(sql)
    }
}
