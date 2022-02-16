mod aggregate_state;

use std::collections::HashMap;

use ordered_float::OrderedFloat;

use crate::{
    expr_resolver::ExprResolver,
    pipeline::pump_model::window_operation_parameter::{
        aggregate::{AggregateFunctionParameter, GroupAggregateParameter},
        WindowOperationParameter,
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::WindowInFlowByWindowTask,
            task::{tuple::Tuple, window::aggregate::GroupAggrOut},
        },
        time::timestamp::Timestamp,
        NnSqlValue, SqlValue,
    },
};

use self::aggregate_state::AvgState;

use super::Pane;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct AggrPane {
    open_at: Timestamp,
    close_at: Timestamp,

    group_aggregation_parameter: GroupAggregateParameter,

    inner: AggrPaneInner,
}

impl Pane for AggrPane {
    type CloseOut = GroupAggrOut;
    type DispatchArg = ();

    /// # Panics
    ///
    /// if `op_param` is not `GroupAggregateParameter`
    fn new(open_at: Timestamp, close_at: Timestamp, op_param: WindowOperationParameter) -> Self {
        if let WindowOperationParameter::GroupAggregation(group_aggregation_parameter) = op_param {
            let inner = match group_aggregation_parameter.aggr_func {
                AggregateFunctionParameter::Avg => AggrPaneInner::Avg {
                    states: HashMap::new(),
                },
            };

            Self {
                open_at,
                close_at,
                group_aggregation_parameter,
                inner,
            }
        } else {
            panic!("op_param {:?} is not GroupAggregateParameter", op_param)
        }
    }

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
        _arg: (),
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
                        .unpack::<f32>()
                        .expect("only f32 is supported currently"),
                );

                WindowInFlowByWindowTask::zero()
            }
        }
    }

    fn close(
        self,
        _expr_resolver: &ExprResolver,
    ) -> (Vec<Self::CloseOut>, WindowInFlowByWindowTask) {
        let aggr_label = self.group_aggregation_parameter.aggr_expr;
        let group_by_label = self.group_aggregation_parameter.group_by;

        match self.inner {
            AggrPaneInner::Avg { states } => {
                let group_aggr_out_seq = states
                    .into_iter()
                    .map(|(group_by, state)| {
                        let aggr_value =
                            SqlValue::NotNull(NnSqlValue::Float(OrderedFloat(state.finalize())));
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
