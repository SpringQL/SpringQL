// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::error::Result;
use crate::expr_resolver::expr_label::ValueExprLabel;
use crate::expr_resolver::ExprResolver;
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;

use super::SqlValues;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct ValueProjectionSubtask {
    value_exprs: Vec<ValueExprLabel>,
}

impl ValueProjectionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        expr_resolver: &ExprResolver,
        tuple: &Tuple,
    ) -> Result<SqlValues> {
        let values = self
            .value_exprs
            .iter()
            .map(|label| expr_resolver.eval_value_expr(*label, tuple))
            .collect::<Result<Vec<_>>>()?;

        Ok(SqlValues::new(values))
    }
}
