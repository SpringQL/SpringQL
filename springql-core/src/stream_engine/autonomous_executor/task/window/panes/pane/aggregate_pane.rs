// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod aggregate_state;

use std::collections::HashMap;

use ordered_float::OrderedFloat;

use crate::{
    api::error::Result,
    expr_resolver::ExprResolver,
    pipeline::{
        AggregateFunctionParameter, AggregateParameter, GroupByLabels, WindowOperationParameter,
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::WindowInFlowByWindowTask,
            task::{
                tuple::Tuple,
                window::{
                    aggregate::AggregatedAndGroupingValues,
                    panes::pane::{aggregate_pane::aggregate_state::AvgState, Pane},
                },
            },
        },
        time::timestamp::SpringTimestamp,
        NnSqlValue, SqlValue,
    },
};

#[derive(Debug)]
pub struct AggrPane {
    open_at: SpringTimestamp,
    close_at: SpringTimestamp,

    aggregate_parameter: AggregateParameter,

    inner: AggrPaneInner,
}

impl Pane for AggrPane {
    type CloseOut = AggregatedAndGroupingValues;
    type DispatchArg = ();

    /// # Panics
    ///
    /// if `op_param` is not `GroupAggregateParameter`
    fn new(
        open_at: SpringTimestamp,
        close_at: SpringTimestamp,
        op_param: WindowOperationParameter,
    ) -> Self {
        if let WindowOperationParameter::Aggregate(aggregate_parameter) = op_param {
            let inner = match aggregate_parameter.aggr_func {
                AggregateFunctionParameter::Avg => AggrPaneInner::Avg {
                    states: HashMap::new(),
                },
            };

            Self {
                open_at,
                close_at,
                aggregate_parameter,
                inner,
            }
        } else {
            panic!("op_param {:?} is not GroupAggregateParameter", op_param)
        }
    }

    fn open_at(&self) -> SpringTimestamp {
        self.open_at
    }

    fn close_at(&self) -> SpringTimestamp {
        self.close_at
    }

    fn dispatch(
        &mut self,
        expr_resolver: &ExprResolver,
        tuple: &Tuple,
        _arg: (),
    ) -> WindowInFlowByWindowTask {
        let group_by_values = GroupByValues::from_group_by_labels(
            self.aggregate_parameter.group_by.clone(),
            expr_resolver,
            tuple,
        )
        .expect("TODO handle Result");

        let aggregated_value = expr_resolver
            .eval_aggr_expr_inner(self.aggregate_parameter.aggr_expr, tuple)
            .expect("TODO Result");
        let aggregated_value = if let SqlValue::NotNull(v) = aggregated_value {
            v
        } else {
            unimplemented!("aggregation with NULL value is not supported")
        };

        match &mut self.inner {
            AggrPaneInner::Avg { states } => {
                let state = states
                    .entry(group_by_values)
                    .or_insert_with(AvgState::default);

                state.next(
                    aggregated_value
                        .unpack::<f32>()
                        .expect("only f32 is supported currently"),
                );

                WindowInFlowByWindowTask::zero() // state in AVG is constant
            }
        }
    }

    fn close(
        self,
        _expr_resolver: &ExprResolver,
    ) -> (Vec<Self::CloseOut>, WindowInFlowByWindowTask) {
        let aggr_label = self.aggregate_parameter.aggr_expr;
        let group_by_labels = self.aggregate_parameter.group_by;

        match self.inner {
            AggrPaneInner::Avg { states } => {
                let aggregated_and_grouping_values_seq = states
                    .into_iter()
                    .map(|(group_by_values, state)| {
                        let aggr_value =
                            SqlValue::NotNull(NnSqlValue::Float(OrderedFloat(state.finalize())));

                        let group_bys = group_by_labels
                            .as_labels()
                            .iter()
                            .cloned()
                            .zip(group_by_values.into_sql_values())
                            .collect();

                        AggregatedAndGroupingValues::new(vec![(aggr_label, aggr_value)], group_bys)
                    })
                    .collect();

                (
                    aggregated_and_grouping_values_seq,
                    WindowInFlowByWindowTask::zero(),
                )
            }
        }
    }
}

#[derive(Debug)]
pub enum AggrPaneInner {
    Avg {
        states: HashMap<GroupByValues, AvgState>,
    },
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct GroupByValues(
    /// TODO support NULL in GROUP BY elements
    Vec<NnSqlValue>,
);

impl GroupByValues {
    /// Order of elements in GROUP BY clause is preserved.
    fn from_group_by_labels(
        group_by_labels: GroupByLabels,
        expr_resolver: &ExprResolver,
        tuple: &Tuple,
    ) -> Result<Self> {
        let values = group_by_labels
            .as_labels()
            .iter()
            .map(|group_by_label| {
                let group_by_value = expr_resolver.eval_value_expr(*group_by_label, tuple)?;

                if let SqlValue::NotNull(v) = group_by_value {
                    Ok(v)
                } else {
                    unimplemented!("group by NULL is not supported ")
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self(values))
    }

    pub fn into_sql_values(self) -> Vec<SqlValue> {
        self.0.into_iter().map(SqlValue::NotNull).collect()
    }
}
