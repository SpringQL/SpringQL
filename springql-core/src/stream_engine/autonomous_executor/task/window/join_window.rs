// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    pipeline::{JoinParameter, WindowOperationParameter, WindowParameter},
    stream_engine::autonomous_executor::task::window::{
        panes::{JoinPane, Panes},
        watermark::Watermark,
        Window,
    },
};

#[derive(Debug)]
pub struct JoinWindow {
    watermark: Watermark,
    panes: Panes<JoinPane>,
}

impl Window for JoinWindow {
    type Pane = JoinPane;

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

impl JoinWindow {
    pub fn new(window_param: WindowParameter, join_param: JoinParameter) -> Self {
        let watermark = Watermark::new(window_param.allowed_delay());
        Self {
            watermark,
            panes: Panes::new(window_param, WindowOperationParameter::Join(join_param)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use springql_test_logger::setup_test_logger;

    use crate::{
        expr_resolver::ExprResolver,
        expression::ValueExpr,
        pipeline::{ColumnName, ColumnReference, JoinParameter, JoinType, StreamName},
        sql_processor::SelectFieldSyntax,
        stream_engine::{
            autonomous_executor::task::window::panes::JoinDir,
            time::{SpringDuration, SpringEventDuration, SpringTimestamp},
            SqlValue, Tuple,
        },
    };

    use super::*;

    fn t_expect(
        tuple: Tuple,
        expected_timestamp: SpringTimestamp,
        expected_amount: i32,
        expected_temperature: Option<i32>,
    ) {
        let timestamp = tuple
            .get_value(&ColumnReference::fx_trade_timestamp())
            .unwrap()
            .unwrap();
        assert_eq!(
            timestamp.unpack::<SpringTimestamp>().unwrap(),
            expected_timestamp
        );

        let amount = tuple
            .get_value(&ColumnReference::fx_trade_amount())
            .unwrap()
            .unwrap();
        assert_eq!(amount.unpack::<i32>().unwrap(), expected_amount);

        let temperature = tuple
            .get_value(&ColumnReference::fx_city_temperature_temperature())
            .unwrap();
        match temperature {
            SqlValue::Null => assert!(expected_temperature.is_none()),
            SqlValue::NotNull(t) => assert_eq!(
                t.unpack::<i32>().unwrap(),
                expected_temperature.expect("joined tuple has non-NULL temperature")
            ),
        }
    }

    #[test]
    fn test_timed_fixed_window_left_out_join() {
        setup_test_logger();

        // SELECT trade.timestamp, trade.amount, city_temperature.temperature
        //   FROM trade
        //   LEFT OUTER JOIN city_temperature
        //   ON trade.timestamp = city_temperature.timestamp
        //   FIXED WINDOW duration_secs(10), duration_secs(1);

        let trade_timestamp_expr = ValueExpr::factory_colref(
            StreamName::fx_trade().as_ref(),
            ColumnName::fx_timestamp().as_ref(),
        );
        let trade_amount_expr = ValueExpr::factory_colref(
            StreamName::fx_trade().as_ref(),
            ColumnName::fx_amount().as_ref(),
        );
        let city_temperature_temperature_expr = ValueExpr::factory_colref(
            StreamName::fx_city_temperature().as_ref(),
            ColumnName::fx_temperature().as_ref(),
        );

        let city_temperature_timestamp_expr = ValueExpr::factory_colref(
            StreamName::fx_city_temperature().as_ref(),
            ColumnName::fx_timestamp().as_ref(),
        );
        let on_expr = ValueExpr::factory_eq(
            trade_timestamp_expr.clone(),
            city_temperature_timestamp_expr,
        );

        let select_list = vec![
            SelectFieldSyntax::ValueExpr {
                value_expr: trade_timestamp_expr,
                alias: None,
            },
            SelectFieldSyntax::ValueExpr {
                value_expr: trade_amount_expr,
                alias: None,
            },
            SelectFieldSyntax::ValueExpr {
                value_expr: city_temperature_temperature_expr,
                alias: None,
            },
        ];

        let (mut expr_resolver, _) = ExprResolver::new(select_list);

        let on_expr_label = expr_resolver.register_value_expr(on_expr);

        let mut window = JoinWindow::new(
            WindowParameter::TimedFixedWindow {
                length: SpringEventDuration::from_secs(10),
                allowed_delay: SpringEventDuration::from_secs(1),
            },
            JoinParameter {
                join_type: JoinType::LeftOuter,
                left_colrefs: vec![
                    ColumnReference::fx_trade_timestamp(),
                    ColumnReference::fx_trade_ticker(),
                    ColumnReference::fx_trade_amount(),
                ],
                right_colrefs: vec![
                    ColumnReference::fx_city_temperature_timestamp(),
                    ColumnReference::fx_city_temperature_city(),
                    ColumnReference::fx_city_temperature_temperature(),
                ],
                on_expr: on_expr_label,
            },
        );

        // [:00, :10): t(:00, 100)
        let (out, window_in_flow) = window
            .dispatch(
                &expr_resolver,
                Tuple::factory_trade(
                    SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                    "",
                    100,
                ),
                JoinDir::Left,
            )
            .unwrap();
        assert!(out.is_empty());
        assert!(window_in_flow.window_gain_bytes_rows > 0);
        assert_eq!(window_in_flow.window_gain_bytes_states, 0);

        // [:00, :10): t(:00, 100), c(:00, 10)
        let (out, window_in_flow) = window
            .dispatch(
                &expr_resolver,
                Tuple::factory_city_temperature(
                    SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
                    "",
                    10,
                ),
                JoinDir::Right,
            )
            .unwrap();
        assert!(out.is_empty());
        assert!(window_in_flow.window_gain_bytes_rows > 0);
        assert_eq!(window_in_flow.window_gain_bytes_states, 0);

        // [:00, :10): t(:00, 100), c(:00, 10), t(:09.9, 200)
        let (out, window_in_flow) = window
            .dispatch(
                &expr_resolver,
                Tuple::factory_trade(
                    SpringTimestamp::from_str("2020-01-01 00:00:09.999999999").unwrap(),
                    "",
                    200,
                ),
                JoinDir::Left,
            )
            .unwrap();
        assert!(out.is_empty());
        assert!(window_in_flow.window_gain_bytes_rows > 0);
        assert_eq!(window_in_flow.window_gain_bytes_states, 0);

        // [:00, :10): t(:00, 100), c(:00, 10), t(:09.9, 200) <-- !!NOT CLOSED YET (within delay)!!
        // [:10, :20):                                         t(:10.9, 300)
        let (out, window_in_flow) = window
            .dispatch(
                &expr_resolver,
                Tuple::factory_trade(
                    SpringTimestamp::from_str("2020-01-01 00:00:10.999999999").unwrap(),
                    "",
                    300,
                ),
                JoinDir::Left,
            )
            .unwrap();
        assert!(out.is_empty());
        assert!(window_in_flow.window_gain_bytes_rows > 0);
        assert_eq!(window_in_flow.window_gain_bytes_states, 0);

        // too late data to be ignored
        //
        // [:00, :10): t(:00, 100), c(:00, 10), t(:09.9, 200)
        // [:10, :20):                                         t(:10.9, 300)
        let (out, window_in_flow) = window
            .dispatch(
                &expr_resolver,
                Tuple::factory_trade(
                    SpringTimestamp::from_str("2020-01-01 00:00:09.999999998").unwrap(),
                    "",
                    400,
                ),
                JoinDir::Left,
            )
            .unwrap();
        assert!(out.is_empty());
        assert_eq!(window_in_flow.window_gain_bytes_rows, 0);
        assert_eq!(window_in_flow.window_gain_bytes_states, 0);

        // [:00, :10): t(:00, 100), c(:00, 10), t(:09.9, 200),               t(:09.9, 500) <-- !!LATE DATA!!
        // [:10, :20):                                         t(:10.9, 300)
        let (out, window_in_flow) = window
            .dispatch(
                &expr_resolver,
                Tuple::factory_trade(
                    SpringTimestamp::from_str("2020-01-01 00:00:09.999999999").unwrap(),
                    "",
                    500,
                ),
                JoinDir::Left,
            )
            .unwrap();
        assert!(out.is_empty());
        assert!(window_in_flow.window_gain_bytes_rows > 0);
        assert_eq!(window_in_flow.window_gain_bytes_states, 0);

        // [:00, :10): -> tc(:00, 100, 10), tc(:09.9, 200, NULL), tc(:09.9, 500, NULL)
        //
        // [:10, :20):                                          t(:10.9, 300),            t(:11, 600)
        let (out, window_in_flow) = window
            .dispatch(
                &expr_resolver,
                Tuple::factory_trade(
                    SpringTimestamp::from_str("2020-01-01 00:00:11.000000000").unwrap(),
                    "",
                    600,
                ),
                JoinDir::Left,
            )
            .unwrap();
        assert_eq!(out.len(), 3);
        t_expect(
            out.get(0).cloned().unwrap(),
            SpringTimestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
            100,
            Some(10),
        );
        t_expect(
            out.get(1).cloned().unwrap(),
            SpringTimestamp::from_str("2020-01-01 00:00:09.999999999").unwrap(),
            200,
            None,
        );
        t_expect(
            out.get(2).cloned().unwrap(),
            SpringTimestamp::from_str("2020-01-01 00:00:09.999999999").unwrap(),
            500,
            None,
        );
        assert!(window_in_flow.window_gain_bytes_rows < 0);
        assert_eq!(window_in_flow.window_gain_bytes_states, 0);
    }
}
