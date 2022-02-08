use std::cmp::Ordering;

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
        self.generate_panes_if_not_exist(rowtime);

        self.panes
            .iter_mut()
            .filter(move |pane| pane.is_acceptable(&rowtime))
    }

    pub(super) fn remove_panes_to_close(&mut self, watermark: &Watermark) -> Vec<Pane> {
        let mut panes_to_close = vec![];

        let mut idx = 0;
        while idx < self.panes.len() {
            let pane = &mut self.panes[idx];

            if pane.should_close(watermark) {
                let pane = self.panes.remove(idx);
                panes_to_close.push(pane);
            } else {
                idx += 1;
            }
        }

        panes_to_close
    }

    fn generate_panes_if_not_exist(&mut self, rowtime: Timestamp) {
        // Sort-Merge Join like algorithm
        let mut pane_idx = 0;
        for open_at in self.valid_open_at_s(rowtime) {
            loop {
                if pane_idx < self.panes.len() {
                    match open_at.cmp(&self.panes[pane_idx].open_at()) {
                        Ordering::Less => unreachable!("watermark must kick this rowtime"),
                        Ordering::Equal => {
                            // Pane already exists.
                            break; // next open_at
                        }
                        Ordering::Greater => {
                            // next pane may have the open_at
                            pane_idx += 1;
                        }
                    }
                } else {
                    // no pane has the open_at
                    self.panes.push(self.generate_pane(open_at));
                    break; // next open_at
                }
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
            WindowOperationParameter::GroupAggregation(param) => PaneInner::new(param.clone()),
        };
        Pane::new(open_at, close_at, pane_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        expr_resolver::ExprResolver,
        expression::ValueExpr,
        pipeline::{
            field::field_name::ColumnReference,
            name::ValueAlias,
            pump_model::window_operation_parameter::aggregate::{
                AggregateFunctionParameter, GroupAggregateParameter,
            },
        },
        stream_engine::time::duration::event_duration::EventDuration, sql_processor::sql_parser::syntax::SelectFieldSyntax,
    };

    use super::*;
    use pretty_assertions::assert_eq;

    fn dont_care_window_operation_parameter() -> WindowOperationParameter {
        let aggregated_expr = ValueExpr::factory_colref("dontcare", "dontcare");
        let group_by_expr = ValueExpr::factory_colref("dontcare", "dontcare");

        let select_list = vec![SelectFieldSyntax::ValueExpr {
            value_expr: aggregated_expr,
            alias: None,
        }];
        let (mut expr_resolver, labels_select_list) = ExprResolver::new(select_list);

        let group_by_label = expr_resolver.register_value_expr(group_by_expr);

        WindowOperationParameter::GroupAggregation(GroupAggregateParameter {
            aggr_expr: labels_select_list[0],
            group_by: group_by_label,
        })
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
