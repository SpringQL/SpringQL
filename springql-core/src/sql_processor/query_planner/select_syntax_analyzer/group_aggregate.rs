// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use super::SelectSyntaxAnalyzer;
use crate::sql_processor::sql_parser::syntax::GroupingElementSyntax;

impl SelectSyntaxAnalyzer {
    /// TODO multiple GROUP BY
    pub(in super::super) fn grouping_element(&self) -> Option<GroupingElementSyntax> {
        self.select_syntax.grouping_element.clone()
    }
}
