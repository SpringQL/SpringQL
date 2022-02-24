// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::expr_resolver::expr_label::{AggrExprLabel, ValueExprLabel};
use crate::stream_engine::autonomous_executor::task::window::aggregate::GroupAggrOut;

use super::SqlValues;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct AggrProjectionSubtask {
    group_by_expr: ValueExprLabel,
    aggr_expr: AggrExprLabel,
}

impl AggrProjectionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        group_aggr_out: GroupAggrOut,
    ) -> Result<SqlValues> {
        let aggr_label = self.aggr_expr;
        let group_by_label = self.group_by_expr;

        let (aggr_result, group_by_result) =
            group_aggr_out.into_results(aggr_label, group_by_label)?;

        Ok(SqlValues::new(vec![group_by_result, aggr_result])) // FIXME keep select list order: <https://gh01.base.toyota-tokyo.tech/SpringQL-internal/SpringQL/issues/174>
    }
}
