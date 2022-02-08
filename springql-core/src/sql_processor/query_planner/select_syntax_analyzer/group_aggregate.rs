use super::SelectSyntaxAnalyzer;
use crate::error::Result;
use crate::expression::{AggrExpr, ValueExpr};
use crate::pipeline::pump_model::window_operation_parameter::aggregate::GroupAggregateParameter;
use crate::sql_processor::sql_parser::syntax::SelectFieldSyntax;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn aggr_expr_select_list(&self) -> Vec<AggrExpr> {
        self.select_syntax
            .fields
            .iter()
            .filter_map(|field| match field {
                SelectFieldSyntax::ValueExpr { .. } => None,
                SelectFieldSyntax::AggrExpr { aggr_expr, .. } => Some(aggr_expr.clone()),
            })
            .collect()
    }

    /// TODO multiple GROUP BY
    pub(in super::super) fn grouping_element(&self) -> Option<ValueExpr> {
        self.select_syntax.grouping_element.clone()
    }
}
