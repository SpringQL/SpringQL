mod aggregate_state;

use std::collections::HashMap;

use crate::{
    expr_resolver::ExprResolver,
    pipeline::pump_model::window_operation_parameter::aggregate::GroupAggregateParameter,
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::WindowInFlowByWindowTask,
            task::{tuple::Tuple, window::GroupAggrOut},
        },
        time::timestamp::Timestamp,
        NnSqlValue, SqlValue,
    },
};

use self::aggregate_state::{AggregateState, AvgState};

use super::Pane;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct AggrPane {
    open_at: Timestamp,
    close_at: Timestamp,

    group_aggregation_parameter: GroupAggregateParameter,

    inner: AggrPaneInner,
}

impl Pane for AggrPane {
    type CloseOut = Vec<GroupAggrOut>;

    fn open_at(&self) -> Timestamp {
        self.open_at
    }

    fn close_at(&self) -> Timestamp {
        self.close_at
    }

    fn dispatch(
        &mut self,
        expr_resolver: &ExprResolver,
        tuple: &Tuple,
    ) -> WindowInFlowByWindowTask {
        let group_by_value = expr_resolver
            .eval_value_expr(self.group_aggregation_parameter.group_by, tuple)
            .expect("TODO Result");
        let group_by_value = if let SqlValue::NotNull(v) = group_by_value {
            v
        } else {
            unimplemented!("group by NULL is not supported ")
        };

        let aggregated_value = expr_resolver
            .eval_aggr_expr_inner(self.group_aggregation_parameter.aggr_expr, tuple)
            .expect("TODO Result");
        let aggregated_value = if let SqlValue::NotNull(v) = aggregated_value {
            v
        } else {
            unimplemented!("aggregation with NULL value is not supported")
        };

        match &mut self.inner {
            AggrPaneInner::Avg { states } => {
                let state = states
                    .entry(group_by_value)
                    .or_insert_with(AvgState::default);

                state.next(
                    aggregated_value
                        .unpack::<i64>()
                        .expect("only i64 is supported currently"),
                );

                WindowInFlowByWindowTask::zero()
            }
        }
    }

    fn close(self) -> (Self::CloseOut, WindowInFlowByWindowTask) {
        let aggr_label = self.group_aggregation_parameter.aggr_expr;
        let group_by_label = self.group_aggregation_parameter.group_by;

        match self.inner {
            AggrPaneInner::Avg { states } => {
                let group_aggr_out_seq = states
                    .into_iter()
                    .map(|(group_by, state)| {
                        let aggr_value = SqlValue::NotNull(NnSqlValue::BigInt(state.finalize()));
                        GroupAggrOut::new(
                            aggr_label,
                            aggr_value,
                            group_by_label,
                            SqlValue::NotNull(group_by),
                        )
                    })
                    .collect();

                (group_aggr_out_seq, WindowInFlowByWindowTask::zero())
            }
        }
    }
}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) enum AggrPaneInner {
    Avg {
        states: HashMap<NnSqlValue, AvgState>,
    },
}

impl AggrPaneInner {
    pub(in crate::stream_engine::autonomous_executor) fn new() -> Self {
        // TODO branch by AggregateFunctionParameter in aggr_expr

        AggrPaneInner::Avg {
            states: HashMap::new(),
        }
    }
}
