// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use super::SelectSyntaxAnalyzer;
use crate::sql_processor::sql_parser::syntax::SelectFieldSyntax;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn select_list(&self) -> &[SelectFieldSyntax] {
        &self.select_syntax.fields
    }
}
