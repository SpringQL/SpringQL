// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use super::SelectSyntaxAnalyzer;

use crate::pipeline::pump_model::window_parameter::WindowParameter;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn window_parameter(&self) -> Option<WindowParameter> {
        self.select_syntax.window_clause.clone()
    }
}
