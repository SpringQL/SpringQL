// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use super::SelectSyntaxAnalyzer;
use crate::sql_processor::sql_parser::syntax::GroupingElementSyntax;

impl SelectSyntaxAnalyzer {
    /// TODO multiple GROUP BY
    pub(in super::super) fn grouping_element(&self) -> Option<GroupingElementSyntax> {
        self.select_syntax.grouping_element.clone()
    }
}
