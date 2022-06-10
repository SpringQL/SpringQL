// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::collections::HashMap;

use anyhow::anyhow;

use crate::{
    api::error::{Result, SpringError},
    expr_resolver::expr_label::{AggrExprLabel, ValueExprLabel},
    pipeline::pump_model::{WindowOperationParameter, WindowParameter},
    stream_engine::{
        autonomous_executor::task::window::{
            panes::{pane::aggregate_pane::AggrPane, Panes},
            watermark::Watermark,
            Window,
        },
        SqlValue,
    },
};

/// for aggregate expressions: AggrExprLabel -> SqlValue,
/// for GROUP BY expressions: ValueExprLabel -> SqlValue
///
/// Projection operation for SELECT with aggregate completes with this instance (without Tuple).
#[derive(Clone, PartialEq, Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct AggregatedAndGroupingValues {
    aggr: HashMap<AggrExprLabel, SqlValue>,
    group_by: HashMap<ValueExprLabel, SqlValue>,
}
impl AggregatedAndGroupingValues {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        aggregates: Vec<(AggrExprLabel, SqlValue)>,
        group_bys: Vec<(ValueExprLabel, SqlValue)>,
    ) -> Self {
        Self {
            aggr: aggregates.into_iter().collect(),
            group_by: group_bys.into_iter().collect(),
        }
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - `label` is not included in aggregation result
    pub(in crate::stream_engine::autonomous_executor) fn get_aggregated_value(
        &self,
        label: &AggrExprLabel,
    ) -> Result<&SqlValue> {
        self.aggr
            .get(label)
            .ok_or_else(|| SpringError::Sql(anyhow!("aggregate label not found: {:?}", label)))
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - `label` is not included in aggregation result
    pub(in crate::stream_engine::autonomous_executor) fn get_group_by_value(
        &self,
        label: &ValueExprLabel,
    ) -> Result<&SqlValue> {
        self.group_by
            .get(label)
            .ok_or_else(|| SpringError::Sql(anyhow!("GROUP BY label not found: {:?}", label)))
    }
}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct AggrWindow {
    watermark: Watermark,
    panes: Panes<AggrPane>,
}

impl Window for AggrWindow {
    type Pane = AggrPane;

    fn watermark(&self) -> &Watermark {
        &self.watermark
    }

    fn watermark_mut(&mut self) -> &mut Watermark {
        &mut self.watermark
    }

    fn panes(&self) -> &Panes<Self::Pane> {
        &self.panes
    }

    fn panes_mut(&mut self) -> &mut Panes<Self::Pane> {
        &mut self.panes
    }

    fn purge(&mut self) {
        self.panes.purge()
    }
}

impl AggrWindow {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        window_param: WindowParameter,
        op_param: WindowOperationParameter,
    ) -> Self {
        let watermark = Watermark::new(window_param.allowed_delay());
        Self {
            watermark,
            panes: Panes::new(window_param, op_param),
        }
    }
}

#[cfg(test)]
mod tests {
    use springql_test_logger::setup_test_logger;

    use super::*;

    use std::str::FromStr;

    use crate::{
        expr_resolver::{expr_label::ExprLabel, ExprResolver},
        expression::{AggrExpr, ValueExpr},
        pipeline::{
            pump_model::{AggregateFunctionParameter, AggregateParameter, GroupByLabels},
            AggrAlias, ColumnName, StreamName,
        },
        sql_processor::sql_parser::syntax::SelectFieldSyntax,
        stream_engine::{
            autonomous_executor::task::tuple::Tuple,
            time::{
                duration::{event_duration::SpringEventDuration, SpringDuration},
                timestamp::SpringTimestamp,
            },
        },
    };

    fn t_expect(
        aggr_label: AggrExprLabel,
        group_by_label: ValueExprLabel,
        aggregated_and_grouping_values: AggregatedAndGroupingValues,
        expected_ticker: &str,
        expected_avg_amount: i16,
    ) {
        let ticker = aggregated_and_grouping_values
            .get_group_by_value(&group_by_label)
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(
            ticker.unpack::<String>().unwrap(),
            expected_ticker.to_string()
        );

        let avg_amount = aggregated_and_grouping_values
            .get_aggregated_value(&aggr_label)
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(
            avg_amount.unpack::<f32>().unwrap().round() as i16,
            expected_avg_amount
        );
    }

    fn sort_key(
        group_by_label: &ValueExprLabel,
        aggregated_and_grouping_values: &AggregatedAndGroupingValues,
    ) -> String {
        aggregated_and_grouping_values
            .get_group_by_value(group_by_label)
            .unwrap()
            .clone()
            .unwrap()
            .unpack::<String>()
            .unwrap()
    }

    #[test]
    fn test_timed_sliding_window_aggregation() {
        setup_test_logger();

        // SELECT ticker, AVG(amount) AS avg_amount
        //   FROM trade
        //   SLIDING WINDOW duration_secs(10), duration_secs(5), duration_secs(1)
        //   GROUP BY ticker;

        let ticker_expr = ValueExpr::factory_colref(
            StreamName::fx_trade().as_ref(),
            ColumnName::fx_ticker().as_ref(),
        );
        let avg_amount_expr = AggrExpr {
            func: AggregateFunctionParameter::Avg,
            aggregated: ValueExpr::factory_colref(
                StreamName::fx_trade().as_ref(),
                ColumnName::fx_amount().as_ref(),
            ),
        };

        let select_list = vec![
            SelectFieldSyntax::ValueExpr {
                value_expr: ticker_expr,
                alias: None,
            },
            SelectFieldSyntax::AggrExpr {
                aggr_expr: avg_amount_expr,
                alias: Some(AggrAlias::new("avg_amount".to_string())),
            },
        ];

        let (expr_resolver, labels) = ExprResolver::new(select_list);
        match &labels[..] {
            &[ExprLabel::Value(group_by_label), ExprLabel::Aggr(aggr_label)] => {
                let mut window = AggrWindow::new(
                    WindowParameter::TimedSlidingWindow {
                        length: SpringEventDuration::from_secs(10),
                        period: SpringEventDuration::from_secs(5),
                        allowed_delay: SpringEventDuration::from_secs(1),
                    },
                    WindowOperationParameter::Aggregate(AggregateParameter {
                        aggr_func: AggregateFunctionParameter::Avg,
                        aggr_expr: aggr_label,
                        group_by: GroupByLabels::new(vec![group_by_label]),
                    }),
                );

                // [:55, :05): ("GOOGL", 100)
                // [:00, :10): ("GOOGL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                        "GOOGL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:55, :05): ("GOOGL", 100), ("ORCL", 100)
                // [:00, :10): ("GOOGL", 100), ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:04.999999999").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:55, :05): -> "GOOGL" AVG = 100; "ORCL" AVG = 100
                //
                // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
                // [:05, :15):                                ("ORCL", 400)
                let (mut out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:06.000000000").unwrap(),
                        "ORCL",
                        400,
                    ),
                    (),
                );
                assert_eq!(out.len(), 2);
                out.sort_by_key(|aggregated_and_grouping_values| {
                    sort_key(&group_by_label, aggregated_and_grouping_values)
                });
                t_expect(
                    aggr_label,
                    group_by_label,
                    out.get(0).cloned().unwrap(),
                    "GOOGL",
                    100,
                );
                t_expect(
                    aggr_label,
                    group_by_label,
                    out.get(1).cloned().unwrap(),
                    "ORCL",
                    100,
                );
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400) <-- !!NOT CLOSED YET (within delay)!!
                // [:05, :15):                                ("ORCL", 400), ("ORCL", 100)
                // [:10, :20):                                               ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:10.999999999").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // too late data to be ignored
                //
                // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
                // [:05, :15):                                ("ORCL", 400), ("ORCL", 100)
                // [:10, :20):                                               ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:09.999999998").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400),                ("ORCL", 100) <-- !!LATE DATA!!
                // [:05, :15):                                ("ORCL", 400), ("ORCL", 100), ("ORCL", 100)
                // [:10, :20):                                               ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:09.9999999999").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:00, :10): -> "GOOGL" AVG = 100; "ORCL" AVG = 200
                //
                // [:05, :15):                                ("ORCL", 400), ("ORCL", 100), ("ORCL", 100), ("ORCL", 100)
                // [:10, :20):                                               ("ORCL", 100),                ("ORCL", 100)
                let (mut out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:11.000000000").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert_eq!(out.len(), 2);
                out.sort_by_key(|aggregated_and_grouping_values| {
                    sort_key(&group_by_label, aggregated_and_grouping_values)
                });
                t_expect(
                    aggr_label,
                    group_by_label,
                    out.get(0).cloned().unwrap(),
                    "GOOGL",
                    100,
                );
                t_expect(
                    aggr_label,
                    group_by_label,
                    out.get(1).cloned().unwrap(),
                    "ORCL",
                    200,
                );
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:05, :15): -> "ORCL" = 175
                // [:10, :20): -> "ORCL" = 100
                //
                // [:15, :25):                                                                                           ("ORCL", 100)
                // [:20, :30):                                                                                           ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:21.000000000").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert_eq!(out.len(), 2);
                t_expect(
                    aggr_label,
                    group_by_label,
                    out.get(0).cloned().unwrap(),
                    "ORCL",
                    175,
                );
                t_expect(
                    aggr_label,
                    group_by_label,
                    out.get(1).cloned().unwrap(),
                    "ORCL",
                    100,
                );
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_timed_fixed_window_aggregation() {
        setup_test_logger();

        // SELECT ticker, AVG(amount) AS avg_amount
        //   FROM trade
        //   FIXED WINDOW duration_secs(10), duration_secs(1)
        //   GROUP BY ticker;

        let ticker_expr = ValueExpr::factory_colref(
            StreamName::fx_trade().as_ref(),
            ColumnName::fx_ticker().as_ref(),
        );
        let avg_amount_expr = AggrExpr {
            func: AggregateFunctionParameter::Avg,
            aggregated: ValueExpr::factory_colref(
                StreamName::fx_trade().as_ref(),
                ColumnName::fx_amount().as_ref(),
            ),
        };

        let select_list = vec![
            SelectFieldSyntax::ValueExpr {
                value_expr: ticker_expr,
                alias: None,
            },
            SelectFieldSyntax::AggrExpr {
                aggr_expr: avg_amount_expr,
                alias: Some(AggrAlias::new("avg_amount".to_string())),
            },
        ];

        let (expr_resolver, labels) = ExprResolver::new(select_list);
        match &labels[..] {
            &[ExprLabel::Value(group_by_label), ExprLabel::Aggr(aggr_label)] => {
                let mut window = AggrWindow::new(
                    WindowParameter::TimedFixedWindow {
                        length: SpringEventDuration::from_secs(10),
                        allowed_delay: SpringEventDuration::from_secs(1),
                    },
                    WindowOperationParameter::Aggregate(AggregateParameter {
                        aggr_func: AggregateFunctionParameter::Avg,
                        aggr_expr: aggr_label,
                        group_by: GroupByLabels::new(vec![group_by_label]),
                    }),
                );

                // [:00, :10): ("GOOGL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                        "GOOGL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:00, :10): ("GOOGL", 100), ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:09.000000000").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:09.999999999").unwrap(),
                        "ORCL",
                        400,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400) <-- !!NOT CLOSED YET (within delay)!!
                // [:10, :20):                                               ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:10.999999999").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // too late data to be ignored
                //
                // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
                // [:10, :20):                                               ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:09.999999998").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400),                ("ORCL", 100) <-- !!LATE DATA!!
                // [:10, :20):                                               ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:09.9999999999").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert!(out.is_empty());
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:00, :10): -> "GOOGL" AVG = 100; "ORCL" AVG = 200
                //
                // [:10, :20):                                               ("ORCL", 100),                ("ORCL", 100)
                let (mut out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:11.000000000").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert_eq!(out.len(), 2);
                out.sort_by_key(|aggregated_and_grouping_values| {
                    sort_key(&group_by_label, aggregated_and_grouping_values)
                });
                t_expect(
                    aggr_label,
                    group_by_label,
                    out.get(0).cloned().unwrap(),
                    "GOOGL",
                    100,
                );
                t_expect(
                    aggr_label,
                    group_by_label,
                    out.get(1).cloned().unwrap(),
                    "ORCL",
                    200,
                );
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);

                // [:10, :20): -> "ORCL" = 100
                //
                // [:20, :30):                                                                                           ("ORCL", 100)
                let (out, window_in_flow) = window.dispatch(
                    &expr_resolver,
                    Tuple::factory_trade(
                        SpringTimestamp::from_str("2020-01-01 00:00:21.000000000").unwrap(),
                        "ORCL",
                        100,
                    ),
                    (),
                );
                assert_eq!(out.len(), 1);
                t_expect(
                    aggr_label,
                    group_by_label,
                    out.get(0).cloned().unwrap(),
                    "ORCL",
                    100,
                );
                assert_eq!(window_in_flow.window_gain_bytes_states, 0);
                assert_eq!(window_in_flow.window_gain_bytes_rows, 0);
            }
            _ => unreachable!(),
        }
    }
}
