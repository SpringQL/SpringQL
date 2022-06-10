// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{pipeline::WindowParameter, sql_processor::query_planner::SelectSyntaxAnalyzer};

impl SelectSyntaxAnalyzer {
    pub fn window_parameter(&self) -> Option<WindowParameter> {
        self.select_syntax.window_clause.clone()
    }
}
