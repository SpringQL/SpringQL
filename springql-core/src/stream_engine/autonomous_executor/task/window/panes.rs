// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(in crate::stream_engine::autonomous_executor) mod pane;

use std::cmp::Ordering;

use crate::{
    pipeline::{WindowOperationParameter, WindowParameter},
    stream_engine::{
        autonomous_executor::task::window::{panes::pane::Pane, watermark::Watermark},
        time::{duration::SpringDuration, timestamp::SpringTimestamp},
    },
};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct Panes<P>
where
    P: Pane,
{
    /// FIXME want to use `LinkedList::drain_filter` but it's unstable.
    ///
    /// Sorted by `Pane::open_at`.
    panes: Vec<P>,

    window_param: WindowParameter,
    op_param: WindowOperationParameter,
}

impl<P> Panes<P>
where
    P: Pane,
{
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
        rowtime: SpringTimestamp,
    ) -> impl Iterator<Item = &mut P> {
        self.generate_panes_if_not_exist(rowtime);

        self.panes
            .iter_mut()
            .filter(move |pane| pane.is_acceptable(&rowtime))
    }

    pub(super) fn remove_panes_to_close(&mut self, watermark: &Watermark) -> Vec<P> {
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

    pub(super) fn purge(&mut self) {
        self.panes.clear()
    }

    fn generate_panes_if_not_exist(&mut self, rowtime: SpringTimestamp) {
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

    fn valid_open_at_s(&self, rowtime: SpringTimestamp) -> Vec<SpringTimestamp> {
        let mut ret = vec![];

        let leftmost_open_at = {
            let l = (rowtime - self.window_param.length().to_duration())
                .ceil(self.window_param.period().to_duration());

            // edge case
            if l == rowtime - self.window_param.length().to_duration() {
                l + self.window_param.period().to_duration()
            } else {
                l
            }
        };
        let rightmost_open_at = rowtime.floor(self.window_param.period().to_duration());

        let mut open_at = leftmost_open_at;
        while open_at <= rightmost_open_at {
            ret.push(open_at);
            open_at = open_at + self.window_param.period().to_duration();
        }

        ret
    }

    fn generate_pane(&self, open_at: SpringTimestamp) -> P {
        let close_at = open_at + self.window_param.length().to_duration();
        P::new(open_at, close_at, self.op_param.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        expr_resolver::{expr_label::ExprLabel, ExprResolver},
        expression::{AggrExpr, ValueExpr},
        pipeline::{AggregateFunctionParameter, AggregateParameter, GroupByLabels},
        sql_processor::sql_parser::syntax::SelectFieldSyntax,
        stream_engine::{
            autonomous_executor::task::window::panes::pane::aggregate_pane::AggrPane,
            time::duration::event_duration::SpringEventDuration,
        },
    };

    use super::*;
    use pretty_assertions::assert_eq;

    fn dont_care_window_operation_parameter() -> WindowOperationParameter {
        let aggr_expr = AggrExpr {
            func: AggregateFunctionParameter::Avg,
            aggregated: ValueExpr::factory_colref("dontcare", "dontcare"),
        };
        let group_by_expr = ValueExpr::factory_colref("dontcare", "dontcare");

        let select_list = vec![SelectFieldSyntax::AggrExpr {
            aggr_expr,
            alias: None,
        }];
        let (mut expr_resolver, labels) = ExprResolver::new(select_list);

        let group_by_labels =
            GroupByLabels::new(vec![expr_resolver.register_value_expr(group_by_expr)]);

        WindowOperationParameter::Aggregate(AggregateParameter {
            aggr_func: AggregateFunctionParameter::Avg,
            aggr_expr: if let ExprLabel::Aggr(l) = labels[0] {
                l
            } else {
                unreachable!()
            },
            group_by: group_by_labels,
        })
    }

    #[test]
    fn test_valid_open_at_s() {
        fn sliding_window_panes(
            length: SpringEventDuration,
            period: SpringEventDuration,
        ) -> Panes<AggrPane> {
            Panes::new(
                WindowParameter::TimedSlidingWindow {
                    length,
                    period,
                    allowed_delay: SpringEventDuration::from_secs(0),
                },
                dont_care_window_operation_parameter(),
            )
        }

        let panes = sliding_window_panes(
            SpringEventDuration::from_secs(10),
            SpringEventDuration::from_secs(5),
        );
        assert_eq!(
            panes.valid_open_at_s(
                SpringTimestamp::from_str("2020-01-01 00:00:05.000000000").unwrap()
            ),
            vec![
                SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                SpringTimestamp::from_str("2020-01-01 00:00:05.000000000").unwrap()
            ]
        );
        assert_eq!(
            panes.valid_open_at_s(
                SpringTimestamp::from_str("2020-01-01 00:00:09.999999999").unwrap()
            ),
            vec![
                SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                SpringTimestamp::from_str("2020-01-01 00:00:05.000000000").unwrap()
            ]
        );

        let panes = sliding_window_panes(
            SpringEventDuration::from_secs(10),
            SpringEventDuration::from_secs(10),
        );
        assert_eq!(
            panes.valid_open_at_s(
                SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap()
            ),
            vec![SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),]
        );
        assert_eq!(
            panes.valid_open_at_s(
                SpringTimestamp::from_str("2020-01-01 00:00:09.999999999").unwrap()
            ),
            vec![SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),]
        );
    }
}
