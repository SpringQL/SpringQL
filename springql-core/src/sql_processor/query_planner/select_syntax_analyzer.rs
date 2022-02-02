use crate::{expression::Expression, sql_processor::sql_parser::syntax::SelectStreamSyntax};

mod field;
mod from_item;

#[derive(Clone, Debug, new)]
pub(in crate::sql_processor) struct SelectSyntaxAnalyzer {
    select_syntax: SelectStreamSyntax,
}

impl SelectSyntaxAnalyzer {
    pub(in crate::sql_processor) fn all_expressions(&self) -> Vec<Expression> {
        let mut expressions = self.field_expressions();
        expressions.append(&mut self.from_item_expressions());
        expressions
    }
}
