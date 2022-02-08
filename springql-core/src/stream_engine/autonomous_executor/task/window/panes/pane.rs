mod aggregate_state;

use std::collections::HashMap;

use crate::{
    pipeline::{
        field::{field_name::ColumnReference, Field},
        name::{ColumnName, StreamName},
        pump_model::window_operation_parameter::aggregate::{
            AggregateFunctionParameter, GroupAggregateParameter,
        },
    },
    stream_engine::{
        autonomous_executor::task::{tuple::Tuple, window::watermark::Watermark},
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

    pub(in crate::stream_engine::autonomous_executor) fn dispatch(&mut self, tuple: Tuple) {
        match &mut self.inner {
            PaneInner::Avg {
                group_aggregation_parameter,
                states,
            } => {
                let group_by_pointer = &group_aggregation_parameter.group_by;
                let aggregated_pointer =
                    &group_aggregation_parameter.aggregation_parameter.aggregated;

                let group_by_value = tuple
                    .get_value(group_by_pointer)
                    .expect("field pointer for GROUP BY must be checked before");
                let group_by_value = if let SqlValue::NotNull(v) = group_by_value {
                    v
                } else {
                    unimplemented!("group by NULL is not supported ")
                };

                let aggregated_value = tuple
                    .get_value(aggregated_pointer)
                    .expect("field pointer for aggregated value must be checked before");
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
            }
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn close(self) -> Vec<Tuple> {
        match self.inner {
            PaneInner::Avg {
                group_aggregation_parameter,
                states,
            } => states
                .into_iter()
                .map(|(group_by, state)| {
                    let rowtime = self.close_at; // FIXME tuple.rowtime is always close_at
                    let avg_field = {
                        let avg_value = SqlValue::NotNull(NnSqlValue::BigInt(state.finalize()));
                        let fixme_colref = ColumnReference::new(
                            StreamName::new("_".to_string()),
                            ColumnName::new(
                                group_aggregation_parameter
                                    .aggregation_parameter
                                    .aggregated_alias
                                    .to_string(),
                            ),
                        );
                        Field::new(
                            fixme_colref, // TODO use label instead of `stream.alias`
                            avg_value,
                        )
                    };
                    let group_by_field = {
                        let group_by_value = SqlValue::NotNull(group_by);
                        Field::new(group_aggregation_parameter.group_by.clone(), group_by_value)
                    };
                    Tuple::new(rowtime, vec![avg_field, group_by_field])
                })
                .collect(),
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
        match group_aggregation_parameter
            .aggregation_parameter
            .aggregate_function
        {
            AggregateFunctionParameter::Avg => {
                let states = HashMap::new();
                PaneInner::Avg {
                    group_aggregation_parameter,
                    states,
                }
            }
        }
    }
}
