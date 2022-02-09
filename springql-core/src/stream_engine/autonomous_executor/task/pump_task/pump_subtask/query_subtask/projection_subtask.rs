// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::expr_resolver::expr_label::{AggrExprLabel, ValueExprLabel};
use crate::expr_resolver::ExprResolver;
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;

use super::SqlValues;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct ProjectionSubtask {
    value_exprs: Vec<ValueExprLabel>,
    aggr_exprs: Vec<AggrExprLabel>,
}

impl ProjectionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        expr_resolver: &ExprResolver,
        tuple: &Tuple,
    ) -> Result<SqlValues> {
        let values_from_value_exprs = self
            .value_exprs
            .iter()
            .map(|label| expr_resolver.eval_value_expr(*label, tuple))
            .collect::<Result<Vec<_>>>()?;

        let mut values_from_aggr_exprs = self
            .aggr_exprs
            .iter()
            .map(|label| expr_resolver.get_aggr_expr_result(*label))
            .collect();

        let mut values = values_from_value_exprs;
        values.append(&mut values_from_aggr_exprs);

        Ok(SqlValues::new(values))
    }
}
