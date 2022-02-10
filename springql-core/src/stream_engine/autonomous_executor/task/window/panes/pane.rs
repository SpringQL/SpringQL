mod aggregate_state;

use std::collections::HashMap;

use crate::{
    expr_resolver::ExprResolver,
    pipeline::pump_model::window_operation_parameter::aggregate::GroupAggregateParameter,
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::WindowInFlowByWindowTask,
            task::{
                tuple::Tuple,
                window::{watermark::Watermark, GroupAggrOut},
            },
        },
        time::timestamp::Timestamp,
        NnSqlValue, SqlValue,
    },
};

use self::aggregate_state::{AggregateState, AvgState};

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct Pane {
    open_at: Timestamp,
    close_at: Timestamp,

    inner: PaneInner,
}

impl Pane {
    pub(in crate::stream_engine::autonomous_executor) fn open_at(&self) -> Timestamp {
        self.open_at
    }

    pub(in crate::stream_engine::autonomous_executor) fn is_acceptable(
        &self,
        rowtime: &Timestamp,
    ) -> bool {
        &self.open_at <= rowtime && rowtime < &self.close_at
    }

    pub(in crate::stream_engine::autonomous_executor) fn should_close(
        &self,
        watermark: &Watermark,
    ) -> bool {
        self.close_at <= watermark.as_timestamp()
    }

    pub(in crate::stream_engine::autonomous_executor) fn dispatch(
        &mut self,
        expr_resolver: &ExprResolver,
        tuple: &Tuple,
    ) -> WindowInFlowByWindowTask {
        match &mut self.inner {
            PaneInner::Avg {
                group_aggregation_parameter,
                states,
            } => {
                let group_by_value = expr_resolver
                    .eval_value_expr(group_aggregation_parameter.group_by, tuple)
                    .expect("TODO Result");
                let group_by_value = if let SqlValue::NotNull(v) = group_by_value {
                    v
                } else {
                    unimplemented!("group by NULL is not supported ")
                };

                let aggregated_value = expr_resolver
                    .eval_aggr_expr_inner(group_aggregation_parameter.aggr_expr, tuple)
                    .expect("TODO Result");
                let aggregated_value = if let SqlValue::NotNull(v) = aggregated_value {
                    v
                } else {
                    unimplemented!("aggregation with NULL value is not supported")
                };

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

    pub(in crate::stream_engine::autonomous_executor) fn close(
        self,
    ) -> (Vec<GroupAggrOut>, WindowInFlowByWindowTask) {
        match self.inner {
            PaneInner::Avg {
                group_aggregation_parameter,
                states,
            } => {
                let group_aggr_out_seq = states
                    .into_iter()
                    .map(|(group_by, state)| {
                        let aggr_label = group_aggregation_parameter.aggr_expr;
                        let aggr_value = SqlValue::NotNull(NnSqlValue::BigInt(state.finalize()));
                        let group_by_label = group_aggregation_parameter.group_by;
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
pub(in crate::stream_engine::autonomous_executor) enum PaneInner {
    Avg {
        group_aggregation_parameter: GroupAggregateParameter,
        states: HashMap<NnSqlValue, AvgState>,
    },
}

impl PaneInner {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        group_aggregation_parameter: GroupAggregateParameter,
    ) -> Self {
        // TODO branch by AggregateFunctionParameter in aggr_expr

        let states = HashMap::new();
        PaneInner::Avg {
            group_aggregation_parameter,
            states,
        }
    }
}