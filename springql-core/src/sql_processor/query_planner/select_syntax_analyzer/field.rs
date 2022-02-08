use super::SelectSyntaxAnalyzer;
use crate::sql_processor::sql_parser::syntax::SelectFieldSyntax;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn select_list(&self) -> &[SelectFieldSyntax] {
        &self.select_syntax.fields
    }
}
