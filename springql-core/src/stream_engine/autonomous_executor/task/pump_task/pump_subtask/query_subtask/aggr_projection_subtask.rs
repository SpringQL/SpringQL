// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::error::Result;
use crate::expr_resolver::expr_label::AggrExprLabel;
use crate::pipeline::pump_model::window_operation_parameter::aggregate::GroupByLabels;
use crate::stream_engine::autonomous_executor::task::window::aggregate::GroupAggrOut;

use super::SqlValues;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct AggrProjectionSubtask {
    group_by_labels: GroupByLabels,
    aggr_expr: AggrExprLabel,
}

impl AggrProjectionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        group_aggr_out: GroupAggrOut,
    ) -> Result<SqlValues> {
        let aggr_label = self.aggr_expr;
        let group_by_labels = &self.group_by_labels;

        let (aggr_result, group_by_values) =
            group_aggr_out.into_results(aggr_label, group_by_labels.clone())?;

        let mut values = group_by_values.into_sql_values();
        values.push(aggr_result);
        Ok(SqlValues::new(values)) // FIXME keep select list order: <https://gh01.base.toyota-tokyo.tech/SpringQL-internal/SpringQL/issues/174>
    }
}
