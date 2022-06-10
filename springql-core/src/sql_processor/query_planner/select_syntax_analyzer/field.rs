// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::sql_processor::{
    query_planner::select_syntax_analyzer::SelectSyntaxAnalyzer, sql_parser::SelectFieldSyntax,
};

impl SelectSyntaxAnalyzer {
    pub fn select_list(&self) -> &[SelectFieldSyntax] {
        &self.select_syntax.fields
    }
}
