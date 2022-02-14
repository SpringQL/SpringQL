use crate::sql_processor::sql_parser::syntax::SelectStreamSyntax;

mod field;
mod from_item;
mod group_aggregate;
mod window;

#[derive(Clone, Debug, new)]
pub(in crate::sql_processor) struct SelectSyntaxAnalyzer {
    select_syntax: SelectStreamSyntax,
}
