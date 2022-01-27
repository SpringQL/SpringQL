use crate::{
    pipeline::pump_model::{
        window_operation_parameter::WindowOperationParameter, window_parameter::WindowParameter,
    },
    stream_engine::time::{duration::SpringDuration, timestamp::Timestamp},
};

use self::pane::{Pane, PaneInner};

use super::watermark::Watermark;

mod pane;

#[derive(Debug)]
pub(super) struct Panes {
    /// FIXME want to use `LinkedList::drain_filter` but it's unstable.
    ///
    /// Sorted by `Pane::open_at`.
    panes: Vec<Pane>,

    window_param: WindowParameter,
    op_param: WindowOperationParameter,
}

impl Panes {
    pub(super) fn new(window_param: WindowParameter, op_param: WindowOperationParameter) -> Self {
        Self {
            panes: vec![],
            window_param,
            op_param,
        }
    }

    /// Generate new panes if not exists.
    /// Then, return all panes to get a tuple with the `rowtime`.
    ///
    /// Caller must assure rowtime is not smaller than watermark.
    pub(super) fn panes_to_dispatch(
        &mut self,
        rowtime: Timestamp,
    ) -> impl Iterator<Item = &mut Pane> {
        // log::error!(
        //     "[start] panes_to_dispatch: rowtime: {:?}, panes(open_at): {:#?}",
        //     rowtime,
        //     self.panes.iter().map(|p| p.open_at()).collect::<Vec<_>>()
        // );

        self.generate_panes_if_not_exist(rowtime);

        // log::error!(
        //     "[after generate] panes_to_dispatch: rowtime: {:?}, panes(open_at): {:#?}",
        //     rowtime,
        //     self.panes.iter().map(|p| p.open_at()).collect::<Vec<_>>()
        // );

        self.panes
            .iter_mut()
            .filter(move |pane| pane.is_acceptable(&rowtime))
    }

    pub(super) fn remove_panes_to_close(&mut self, watermark: &Watermark) -> Vec<Pane> {
        let mut panes_to_close = vec![];

        // `0..self.panes.len()` is wrong since length changes in the loop.
        for idx in 0.. {
            if idx >= self.panes.len() {
                break;
            }

            let pane = &mut self.panes[idx];

            if pane.should_close(watermark) {
                let pane = self.panes.remove(idx);
                panes_to_close.push(pane);
            }
        }

        log::error!(
            "watermark: {:?}\npanes(open_at): {:?}\npanes_to_close(open_at): {:?}\n",
            watermark.as_timestamp(),
            self.panes.iter().map(|p| p.open_at()).collect::<Vec<_>>(),
            panes_to_close
                .iter()
                .map(|p| p.open_at())
                .collect::<Vec<_>>(),
        );

        panes_to_close
    }

    fn generate_panes_if_not_exist(&mut self, rowtime: Timestamp) {
        // Sort-Merge Join like algorithm
        let mut pane_idx = 0;
        for open_at in self.valid_open_at_s(rowtime) {
            if pane_idx >= self.panes.len() {
                self.panes.push(self.generate_pane(open_at));
            } else if open_at < self.panes[pane_idx].open_at() {
                self.panes.insert(pane_idx, self.generate_pane(open_at));
            } else if open_at == self.panes[pane_idx].open_at() {
                // Pane already exists. Do nothing
            } else {
                // next pane may have the open_at
                pane_idx += 1;
            }
        }
    }

    fn valid_open_at_s(&self, rowtime: Timestamp) -> Vec<Timestamp> {
        let mut ret = vec![];

        let leftmost_open_at = {
            let l = (rowtime - self.window_param.length().to_chrono())
                .ceil(self.window_param.period().to_chrono());

            // edge case
            if l == rowtime - self.window_param.length().to_chrono() {
                l + self.window_param.period().to_chrono()
            } else {
                l
            }
        };
        let rightmost_open_at = rowtime.floor(self.window_param.period().to_chrono());

        let mut open_at = leftmost_open_at;
        while open_at <= rightmost_open_at {
            ret.push(open_at);
            open_at = open_at + self.window_param.period().to_chrono();
        }

        ret
    }

    fn generate_pane(&self, open_at: Timestamp) -> Pane {
        let close_at = open_at + self.window_param.length().to_chrono();
        let pane_inner = match &self.op_param {
            WindowOperationParameter::Aggregation(param) => PaneInner::new(param.clone()),
        };
        Pane::new(open_at, close_at, pane_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        pipeline::{
            field::aliased_field_name::AliasedFieldName,
            name::FieldAlias,
            pump_model::window_operation_parameter::{
                AggregateFunctionParameter, AggregateParameter,
            },
        },
        stream_engine::time::duration::event_duration::EventDuration,
    };

    use super::*;
    use pretty_assertions::assert_eq;

    fn dont_care_window_operation_parameter() -> WindowOperationParameter {
        WindowOperationParameter::Aggregation(AggregateParameter::new(
            AliasedFieldName::factory("", ""),
            AliasedFieldName::factory("", ""),
            FieldAlias::new("".to_string()),
            AggregateFunctionParameter::Avg,
        ))
    }

    #[test]
    fn test_valid_open_at_s() {
        fn sliding_window_panes(length: EventDuration, period: EventDuration) -> Panes {
            Panes::new(
                WindowParameter::TimedSlidingWindow {
                    length,
                    period,
                    allowed_delay: EventDuration::from_secs(0),
                },
                dont_care_window_operation_parameter(),
            )
        }

        let panes = sliding_window_panes(EventDuration::from_secs(10), EventDuration::from_secs(5));
        assert_eq!(
            panes.valid_open_at_s(Timestamp::from_str("2020-01-01 00:00:05.000000000").unwrap()),
            vec![
                Timestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                Timestamp::from_str("2020-01-01 00:00:05.000000000").unwrap()
            ]
        );
        assert_eq!(
            panes.valid_open_at_s(Timestamp::from_str("2020-01-01 00:00:09.999999999").unwrap()),
            vec![
                Timestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                Timestamp::from_str("2020-01-01 00:00:05.000000000").unwrap()
            ]
        );

        let panes =
            sliding_window_panes(EventDuration::from_secs(10), EventDuration::from_secs(10));
        assert_eq!(
            panes.valid_open_at_s(Timestamp::from_str("2020-01-01 00:00:00.000000000").unwrap()),
            vec![Timestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),]
        );
        assert_eq!(
            panes.valid_open_at_s(Timestamp::from_str("2020-01-01 00:00:09.999999999").unwrap()),
            vec![Timestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),]
        );
    }
}
