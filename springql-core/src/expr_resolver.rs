pub(crate) mod expr_label;

use crate::error::Result;
use crate::sql_processor::sql_parser::syntax::SelectFieldSyntax;
use crate::stream_engine::{SqlValue, Tuple};

use self::expr_label::{ExprLabel, ExprLabelGenerator};

/// ExprResolver is to:
///
/// 1. register ValueExpr / AggrExpr in select_list with their (optional) alias and get new ExprLabel.
/// 2. resolve alias in ValueExprOrAlias / AggrExprAlias and get existing ExprLabel.
/// 3. evaluate expression or get cached SqlValue from ExprLabel.
#[derive(Debug)]
pub(crate) struct ExprResolver {
    label_gen: ExprLabelGenerator,
}

impl ExprResolver {
    pub(crate) fn new(select_list: Vec<SelectFieldSyntax>) -> (Self, Vec<ExprLabel>) {
        unimplemented!()
    }

    /// Register expression if it's not in select_list
    pub(crate) fn resolve_value_alias(
        &mut self,
        value_expr_or_alias: ValueExprOrAlias,
    ) -> ExprLabel {
        unimplemented!()
    }

    /// label -> (internal) expression + tuple (for ColumnReference) -> SqlValue.
    ///
    /// SqlValue is cached and not calculated twice.
    pub(crate) fn eval(label: ExprLabel, tuple: &Tuple) -> Result<SqlValue> {
        unimplemented!()
    }
}
