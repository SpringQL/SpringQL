mod aggregate_state;

use std::collections::HashMap;

use crate::{
    error::Result,
    pipeline::{
        field::{field_pointer::FieldPointer, Field},
        pump_model::window_operation_parameter::AggregateParameter,
    },
    stream_engine::{
        autonomous_executor::task::tuple::Tuple, time::timestamp::Timestamp, NnSqlValue, SqlValue,
    },
};

use self::aggregate_state::{AggregateState, AvgState};

#[derive(Debug)]
pub(super) struct Pane {
    open_at: Timestamp,
    close_at: Timestamp,

    inner: PaneInner,
}

#[derive(Debug)]
pub(super) enum PaneInner {
    Avg {
        aggregation_parameter: AggregateParameter,
        states: HashMap<NnSqlValue, AvgState>,
    },
}

impl Pane {
    pub(super) fn is_acceptable(&self, rowtime: &Timestamp) -> bool {
        &self.open_at <= rowtime && rowtime < &self.close_at
    }

    pub(super) fn should_close(&self, watermark: &Watermark) -> bool {
        &self.close_at <= watermark.as_timestamp()
    }

    pub(super) fn dispatch(&mut self, tuple: Tuple) -> Result<()> {
        match &mut self.inner {
            PaneInner::Avg {
                aggregation_parameter,
                states,
            } => {
                let group_by_pointer = FieldPointer::from(&aggregation_parameter.group_by);
                let aggregated_pointer = FieldPointer::from(&aggregation_parameter.aggregated);

                let group_by_value = tuple.get_value(&group_by_pointer)?;
                let group_by_value = if let SqlValue::NotNull(v) = group_by_value {
                    v
                } else {
                    unimplemented!("group by NULL is not supported ")
                };

                let aggregated_value = tuple.get_value(&aggregated_pointer)?;
                let aggregated_value = if let SqlValue::NotNull(v) = aggregated_value {
                    v
                } else {
                    unimplemented!("aggregation with NULL value is not supported")
                };

                let state = states
                    .entry(group_by_value)
                    .or_insert_with(|| AvgState::default());

                state.next(
                    aggregated_value
                        .unpack::<i64>()
                        .expect("only i64 is supported currently"),
                );
            }
        }
        Ok(())
    }

    pub(super) fn close(self) -> Vec<Tuple> {
        match self.inner {
            PaneInner::Avg {
                aggregation_parameter,
                states,
            } => states
                .into_iter()
                .map(|(group_by, state)| {
                    let rowtime = self.close_at; // FIXME tuple.rowtime is always close_at
                    let avg_field = {
                        let avg_value = SqlValue::NotNull(NnSqlValue::BigInt(state.finalize()));
                        Field::new(aggregation_parameter.aggregated, avg_value)
                    };
                    let group_by_field = {
                        let group_by_value = SqlValue::NotNull(group_by);
                        Field::new(aggregation_parameter.group_by, group_by_value)
                    };
                    Tuple::new(rowtime, vec![avg_field, group_by_field])
                })
                .collect(),
        }
    }
}
