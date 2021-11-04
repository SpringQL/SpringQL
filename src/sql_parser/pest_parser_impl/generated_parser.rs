use pest_derive::Parser;

/// The parser generated from `springql.pest`.
///
/// pest_derive::Parser macro puts `pub enum Rule` at this level.
#[derive(Parser)]
#[grammar = "sql_parser/pest_grammar/springql.pest"]
pub(super) struct GeneratedParser;
