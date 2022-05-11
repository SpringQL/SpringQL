// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::error::Result;
use crate::expr_resolver::expr_label::ExprLabel;
use crate::expr_resolver::ExprResolver;
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;
use crate::stream_engine::autonomous_executor::task::window::aggregate::AggregatedAndGroupingValues;

use super::SqlValues;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct ProjectionSubtask {
    exprs: Vec<ExprLabel>,
}

impl ProjectionSubtask {
    /// Projection for SELECT without aggregate.
    pub(in crate::stream_engine::autonomous_executor) fn run_without_aggr(
        &self,
        expr_resolver: &ExprResolver,
        tuple: &Tuple,
    ) -> Result<SqlValues> {
        let values = self
            .exprs
            .iter()
            .map(|label| match label {
                ExprLabel::Value(group_by_value_label) => {
                    expr_resolver.eval_value_expr(*group_by_value_label, tuple)
                }
                ExprLabel::Aggr(_) => unreachable!("aggregate must not be in select_list"),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(SqlValues::new(values))
    }

    /// Projection for SELECT with aggregate.
    /// select_list must only have GROUP BY elements or aggregate expressions.
    /// (Column reference without aggregate is not allowed.)
    pub(in crate::stream_engine::autonomous_executor) fn run_with_aggr(
        &self,
        aggregated_and_grouping_values: AggregatedAndGroupingValues,
    ) -> Result<SqlValues> {
        let values = self
            .exprs
            .iter()
            .map(|label| {
                match label {
                    ExprLabel::Value(group_by_value_label) => {
                        aggregated_and_grouping_values.get_group_by_value(group_by_value_label)
                    }
                    ExprLabel::Aggr(aggr_label) => {
                        aggregated_and_grouping_values.get_aggregated_value(aggr_label)
                    }
                }
                .cloned()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(SqlValues::new(values))
    }
}
