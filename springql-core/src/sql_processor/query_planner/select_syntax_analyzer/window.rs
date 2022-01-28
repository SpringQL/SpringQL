use super::SelectSyntaxAnalyzer;

use crate::pipeline::pump_model::window_parameter::WindowParameter;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn window_parameter(&self) -> Option<WindowParameter> {
        self.select_syntax.window_clause.clone()
    }
}
