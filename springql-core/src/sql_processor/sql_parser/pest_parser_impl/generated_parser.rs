// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use pest_derive::Parser;

/// The parser generated from `springql.pest`.
///
/// pest_derive::Parser macro puts `pub enum Rule` at this level.
#[derive(Parser)]
#[grammar = "sql_processor/sql_parser/pest_grammar/springql.pest"]
pub(super) struct GeneratedParser;
