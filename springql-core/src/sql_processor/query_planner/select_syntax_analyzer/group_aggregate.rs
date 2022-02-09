use super::SelectSyntaxAnalyzer;
use crate::sql_processor::sql_parser::syntax::GroupingElementSyntax;

impl SelectSyntaxAnalyzer {
    /// TODO multiple GROUP BY
    pub(in super::super) fn grouping_element(&self) -> Option<GroupingElementSyntax> {
        self.select_syntax.grouping_element.clone()
    }
}
