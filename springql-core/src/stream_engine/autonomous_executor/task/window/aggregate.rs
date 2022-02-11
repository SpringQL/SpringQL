use crate::{
    error::{Result, SpringError},
    expr_resolver::{
        expr_label::{AggrExprLabel, ValueExprLabel},
        ExprResolver,
    },
    pipeline::pump_model::{
        window_operation_parameter::WindowOperationParameter, window_parameter::WindowParameter,
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::WindowInFlowByWindowTask,
            task::window::panes::pane::Pane,
        },
        SqlValue, Tuple,
    },
};

use anyhow::anyhow;

use super::{panes::Panes, watermark::Watermark};

#[derive(Clone, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct GroupAggrOut {
    aggr_label: AggrExprLabel,
    aggr_result: SqlValue,

    group_by_label: ValueExprLabel,
    group_by_result: SqlValue,
}
impl GroupAggrOut {
    /// # Returns
    ///
    /// (aggregate result, group by result)
    ///
    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - `label` is not included in aggregation result
    pub(in crate::stream_engine::autonomous_executor) fn into_results(
        self,
        aggr_label: AggrExprLabel,
        group_by_label: ValueExprLabel,
    ) -> Result<(SqlValue, SqlValue)> {
        if self.aggr_label != aggr_label {
            Err(SpringError::Sql(anyhow!(
                "aggregate labeled {:?} is not calculated",
                aggr_label
            )))
        } else if self.group_by_label != group_by_label {
            Err(SpringError::Sql(anyhow!(
                "GROUP BY expression {:?} is not calculated",
                group_by_label
            )))
        } else {
            Ok((self.aggr_result, self.group_by_result))
        }
    }
}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct AggrWindow {
    watermark: Watermark,
    panes: Panes,
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

    /// A task dispatches a tuple from waiting queue.
    /// This window returns output tuples from panes inside if they are closed.
    ///
    /// TODO return Vec<Tuple> for JOIN window
    pub(in crate::stream_engine::autonomous_executor) fn dispatch_aggregate(
        &mut self,
        expr_resolver: &ExprResolver,
        tuple: Tuple,
    ) -> (Vec<GroupAggrOut>, WindowInFlowByWindowTask) {
        let rowtime = *tuple.rowtime();

        if rowtime < self.watermark.as_timestamp() {
            // too late tuple does not have any chance to be dispatched nor to close a pane.
            (Vec::new(), WindowInFlowByWindowTask::zero())
        } else {
            let window_in_flow_dispatch = self
                .panes
                .panes_to_dispatch(rowtime)
                .map(|pane| pane.dispatch(expr_resolver, &tuple))
                .fold(WindowInFlowByWindowTask::zero(), |acc, window_in_flow| {
                    acc + window_in_flow
                });

            self.watermark.update(rowtime);

            let (group_aggr_out_seq, window_in_flow_close) = self
                .panes
                .remove_panes_to_close(&self.watermark)
                .into_iter()
                .fold(
                    (Vec::new(), WindowInFlowByWindowTask::zero()),
                    |(mut group_aggr_out_acc, window_in_flow_acc), pane| {
                        let (mut group_aggr_out_seq, window_in_flow) = pane.close();
                        group_aggr_out_acc.append(&mut group_aggr_out_seq);
                        (group_aggr_out_acc, window_in_flow_acc + window_in_flow)
                    },
                );

            (
                group_aggr_out_seq,
                window_in_flow_dispatch + window_in_flow_close,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use springql_test_logger::setup_test_logger;

    use super::*;

    use std::str::FromStr;

    use crate::{
        expr_resolver::ExprResolver,
        expression::{AggrExpr, ValueExpr},
        pipeline::{
            name::{AggrAlias, ColumnName, StreamName},
            pump_model::window_operation_parameter::aggregate::{
                AggregateFunctionParameter, GroupAggregateParameter,
            },
        },
        sql_processor::sql_parser::syntax::SelectFieldSyntax,
        stream_engine::{
            autonomous_executor::task::tuple::Tuple,
            time::{
                duration::{event_duration::EventDuration, SpringDuration},
                timestamp::Timestamp,
            },
        },
    };

    fn t_expect(group_aggr_out: GroupAggrOut, expected_ticker: &str, expected_avg_amount: i16) {
        let ticker = group_aggr_out.group_by_result.unwrap();
        assert_eq!(
            ticker.unpack::<String>().unwrap(),
            expected_ticker.to_string()
        );

        let avg_amount = group_aggr_out.aggr_result.unwrap();
        assert_eq!(avg_amount.unpack::<i16>().unwrap(), expected_avg_amount);
    }

    #[test]
    fn test_timed_sliding_window_aggregation() {
        setup_test_logger();

        // SELECT ticker AS tick, AVG(amount) AS avg_amount
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

        let (mut expr_resolver, _, aggr_labels_select_list) = ExprResolver::new(select_list);

        let group_by_expr = ValueExpr::factory_colref(
            StreamName::fx_trade().as_ref(),
            ColumnName::fx_ticker().as_ref(),
        );
        let group_by_label = expr_resolver.register_value_expr(group_by_expr);

        let mut window = AggrWindow::new(
            WindowParameter::TimedSlidingWindow {
                length: EventDuration::from_secs(10),
                period: EventDuration::from_secs(5),
                allowed_delay: EventDuration::from_secs(1),
            },
            WindowOperationParameter::GroupAggregation(GroupAggregateParameter {
                aggr_func: AggregateFunctionParameter::Avg,
                aggr_expr: aggr_labels_select_list[0],
                group_by: group_by_label,
            }),
        );

        // [:55, :05): ("GOOGL", 100)
        // [:00, :10): ("GOOGL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                "GOOGL",
                100,
            ),
        );
        assert!(out.is_empty());

        // [:55, :05): ("GOOGL", 100), ("ORCL", 100)
        // [:00, :10): ("GOOGL", 100), ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:04.999999999").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert!(out.is_empty());

        // [:55, :05): -> "GOOGL" AVG = 100; "ORCL" AVG = 100
        //
        // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
        // [:05, :15):                                ("ORCL", 400)
        let (mut out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:06.000000000").unwrap(),
                "ORCL",
                400,
            ),
        );
        assert_eq!(out.len(), 2);
        out.sort_by_key(|group_aggr_out| {
            group_aggr_out
                .group_by_result
                .clone()
                .unwrap()
                .unpack::<String>()
                .unwrap()
        });
        t_expect(out.get(0).cloned().unwrap(), "GOOGL", 100);
        t_expect(out.get(1).cloned().unwrap(), "ORCL", 100);

        // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400) <-- !!NOT CLOSED YET (within delay)!!
        // [:05, :15):                                ("ORCL", 400), ("ORCL", 100)
        // [:10, :20):                                               ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:10.999999999").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert!(out.is_empty());

        // too late data to be ignored
        //
        // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
        // [:05, :15):                                ("ORCL", 400), ("ORCL", 100)
        // [:10, :20):                                               ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:09.999999998").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert!(out.is_empty());

        // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400),                ("ORCL", 100) <-- !!LATE DATA!!
        // [:05, :15):                                ("ORCL", 400), ("ORCL", 100), ("ORCL", 100)
        // [:10, :20):                                               ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:09.9999999999").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert!(out.is_empty());

        // [:00, :10): -> "GOOGL" AVG = 100; "ORCL" AVG = 200
        //
        // [:05, :15):                                ("ORCL", 400), ("ORCL", 100), ("ORCL", 100), ("ORCL", 100)
        // [:10, :20):                                               ("ORCL", 100),                ("ORCL", 100)
        let (mut out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:11.000000000").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert_eq!(out.len(), 2);
        out.sort_by_key(|group_aggr_out| {
            group_aggr_out
                .group_by_result
                .clone()
                .unwrap()
                .unpack::<String>()
                .unwrap()
        });
        t_expect(out.get(0).cloned().unwrap(), "GOOGL", 100);
        t_expect(out.get(1).cloned().unwrap(), "ORCL", 200);

        // [:05, :15): -> "ORCL" = 175
        // [:10, :20): -> "ORCL" = 100
        //
        // [:15, :25):                                                                                           ("ORCL", 100)
        // [:20, :30):                                                                                           ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:21.000000000").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert_eq!(out.len(), 2);
        t_expect(out.get(0).cloned().unwrap(), "ORCL", 175);
        t_expect(out.get(1).cloned().unwrap(), "ORCL", 100);
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

        let (mut expr_resolver, _, aggr_labels_select_list) = ExprResolver::new(select_list);

        let group_by_expr = ValueExpr::factory_colref(
            StreamName::fx_trade().as_ref(),
            ColumnName::fx_ticker().as_ref(),
        );
        let group_by_label = expr_resolver.register_value_expr(group_by_expr);

        let mut window = AggrWindow::new(
            WindowParameter::TimedFixedWindow {
                length: EventDuration::from_secs(10),
                allowed_delay: EventDuration::from_secs(1),
            },
            WindowOperationParameter::GroupAggregation(GroupAggregateParameter {
                aggr_func: AggregateFunctionParameter::Avg,
                aggr_expr: aggr_labels_select_list[0],
                group_by: group_by_label,
            }),
        );

        // [:00, :10): ("GOOGL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                "GOOGL",
                100,
            ),
        );
        assert!(out.is_empty());

        // [:00, :10): ("GOOGL", 100), ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:09.000000000").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert!(out.is_empty());

        // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:09.999999999").unwrap(),
                "ORCL",
                400,
            ),
        );
        assert!(out.is_empty());

        // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400) <-- !!NOT CLOSED YET (within delay)!!
        // [:10, :20):                                               ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:10.999999999").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert!(out.is_empty());

        // too late data to be ignored
        //
        // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
        // [:10, :20):                                               ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:09.999999998").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert!(out.is_empty());

        // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400),                ("ORCL", 100) <-- !!LATE DATA!!
        // [:10, :20):                                               ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:09.9999999999").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert!(out.is_empty());

        // [:00, :10): -> "GOOGL" AVG = 100; "ORCL" AVG = 200
        //
        // [:10, :20):                                               ("ORCL", 100),                ("ORCL", 100)
        let (mut out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:11.000000000").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert_eq!(out.len(), 2);
        out.sort_by_key(|group_aggr_out| {
            group_aggr_out
                .group_by_result
                .clone()
                .unwrap()
                .unpack::<String>()
                .unwrap()
        });
        t_expect(out.get(0).cloned().unwrap(), "GOOGL", 100);
        t_expect(out.get(1).cloned().unwrap(), "ORCL", 200);

        // [:10, :20): -> "ORCL" = 100
        //
        // [:20, :30):                                                                                           ("ORCL", 100)
        let (out, _) = window.dispatch_aggregate(
            &expr_resolver,
            Tuple::factory_trade(
                Timestamp::from_str("2020-01-01 00:00:21.000000000").unwrap(),
                "ORCL",
                100,
            ),
        );
        assert_eq!(out.len(), 1);
        t_expect(out.get(0).cloned().unwrap(), "ORCL", 100);
    }
}
