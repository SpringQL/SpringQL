mod panes;
mod watermark;

use crate::pipeline::pump_model::{
    window_operation_parameter::WindowOperationParameter, window_parameter::WindowParameter,
};

use self::{panes::Panes, watermark::Watermark};

use super::tuple::Tuple;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct Window {
    watermark: Watermark,
    panes: Panes,
}

impl Window {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        window_param: WindowParameter,
        op_param: WindowOperationParameter,
    ) -> Self {
        match window_param {
            WindowParameter::TimedSlidingWindow { allowed_delay, .. } => {
                let watermark = Watermark::new(allowed_delay);
                Self {
                    watermark,
                    panes: Panes::new(window_param, op_param),
                }
            }
        }
    }

    /// A task dispatches a tuple from waiting queue.
    /// This window returns output tuples from panes inside if they are closed.
    pub(in crate::stream_engine::autonomous_executor) fn dispatch(
        &mut self,
        tuple: Tuple,
    ) -> Vec<Tuple> {
        let rowtime = *tuple.rowtime();

        if rowtime < self.watermark.as_timestamp() {
            // too late tuple does not have any chance to be dispatched nor to close a pane.
            Vec::new()
        } else {
            self.panes
                .panes_to_dispatch(rowtime)
                .for_each(|pane| pane.dispatch(tuple.clone()));

            self.watermark.update(rowtime);

            self.panes
                .remove_panes_to_close(&self.watermark)
                .into_iter()
                .map(|pane| pane.close())
                .flatten()
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use springql_test_logger::setup_test_logger;

    use super::*;

    use std::str::FromStr;

    use crate::{
        pipeline::{
            field::{aliased_field_name::AliasedFieldName, field_pointer::FieldPointer},
            name::{ColumnName, FieldAlias, StreamName},
            pump_model::window_operation_parameter::{
                AggregateFunctionParameter, AggregateParameter,
            },
        },
        stream_engine::{
            autonomous_executor::task::tuple::Tuple,
            time::{
                duration::{event_duration::EventDuration, SpringDuration},
                timestamp::Timestamp,
            },
            SqlValue,
        },
    };

    #[test]
    fn test_timed_sliding_window_aggregation() {
        setup_test_logger();

        // SELECT ticker, AVG(amount) AS avg_amount
        //   FROM trade
        //   SLIDING WINDOW duration_secs(10), duration_secs(5), duration_secs(1)
        //   GROUP BY ticker;

        fn t_expect(tuple: &Tuple, expected_ticker: &str, expected_avg_amount: i16) {
            let ticker = tuple.get_value(&FieldPointer::from("ticker")).unwrap();
            if let SqlValue::NotNull(ticker) = ticker {
                assert_eq!(
                    ticker.unpack::<String>().unwrap(),
                    expected_ticker.to_string()
                );
            } else {
                unreachable!("not null")
            }

            let avg_amount = tuple.get_value(&FieldPointer::from("avg_amount")).unwrap();
            if let SqlValue::NotNull(avg_amount) = avg_amount {
                assert_eq!(avg_amount.unpack::<i16>().unwrap(), expected_avg_amount);
            } else {
                unreachable!("not null")
            }
        }

        let mut window = Window::new(
            WindowParameter::TimedSlidingWindow {
                length: EventDuration::from_secs(10),
                period: EventDuration::from_secs(5),
                allowed_delay: EventDuration::from_secs(1),
            },
            WindowOperationParameter::Aggregation(AggregateParameter {
                group_by: AliasedFieldName::factory(
                    StreamName::fx_trade().as_ref(),
                    ColumnName::fx_ticker().as_ref(),
                ),
                aggregated: AliasedFieldName::factory(
                    StreamName::fx_trade().as_ref(),
                    ColumnName::fx_amount().as_ref(),
                ),
                aggregated_alias: FieldAlias::new("avg_amount".to_string()),
                aggregate_function: AggregateFunctionParameter::Avg,
            }),
        );

        // [:55, :05): ("GOOGL", 100)
        // [:00, :10): ("GOOGL", 100)
        let out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01 00:00:00.000000000").unwrap(),
            "GOOGL",
            100,
        ));
        assert!(out.is_empty());

        // [:55, :05): ("GOOGL", 100), ("ORCL", 100)
        // [:00, :10): ("GOOGL", 100), ("ORCL", 100)
        let out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01 00:00:04.999999999").unwrap(),
            "ORCL",
            100,
        ));
        assert!(out.is_empty());

        // [:55, :05): -> "GOOGL" AVG = 100; "ORCL" AVG = 100
        //
        // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
        // [:05, :15):                                ("ORCL", 400)
        let mut out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01 00:00:06.000000000").unwrap(),
            "ORCL",
            400,
        ));
        assert_eq!(out.len(), 2);
        out.sort_by_key(|tuple| {
            tuple
                .get_value(&FieldPointer::from("ticker"))
                .unwrap()
                .unwrap()
                .unpack::<String>()
                .unwrap()
        });
        t_expect(out.get(0).unwrap(), "GOOGL", 100);
        t_expect(out.get(1).unwrap(), "ORCL", 100);

        // // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400) <-- !!NOT CLOSED YET (within delay)!!
        // // [:05, :15):                                ("ORCL", 400), ("ORCL", 100)
        // // [:10, :20):                                               ("ORCL", 100)
        // let out = window.dispatch(Tuple::factory_trade(
        //     Timestamp::from_str("2020-01-01 00:00:10.999999999").unwrap(),
        //     "ORCL",
        //     100,
        // ));
        // assert!(out.is_empty());

        // // [:00, :10): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400),                ("ORCL", 100) <-- !!LATE DATA!!
        // // [:05, :15):                                ("ORCL", 400), ("ORCL", 100), ("ORCL", 100)
        // // [:10, :20):                                               ("ORCL", 100)
        // let out = window.dispatch(Tuple::factory_trade(
        //     Timestamp::from_str("2020-01-01 00:00:08.000000000").unwrap(),
        //     "ORCL",
        //     100,
        // ));
        // assert!(out.is_empty());

        // // [:00, :10): -> "GOOGL" AVG = 100; "ORCL" AVG = 200
        // //
        // // [:05, :15):                                ("ORCL", 400), ("ORCL", 100), ("ORCL", 100), ("ORCL", 100)
        // // [:10, :20):                                               ("ORCL", 100),                ("ORCL", 100)
        // let mut out = window.dispatch(Tuple::factory_trade(
        //     Timestamp::from_str("2020-01-01 00:00:11.000000000").unwrap(),
        //     "ORCL",
        //     100,
        // ));
        // assert_eq!(out.len(), 2);
        // out.sort_by_key(|tuple| {
        //     tuple
        //         .get_value(&FieldPointer::from("ticker"))
        //         .unwrap()
        //         .unwrap()
        //         .unpack::<String>()
        //         .unwrap()
        // });
        // t_expect(out.get(0).unwrap(), "GOOGL", 100);
        // t_expect(out.get(1).unwrap(), "ORCL", 200);

        // // [:05, :15): -> "ORCL" = 175
        // // [:10, :20): -> "ORCL" = 100
        // //
        // // [:15, :25):                                                                                           ("ORCL", 100)
        // // [:20, :30):                                                                                           ("ORCL", 100)
        // let out = window.dispatch(Tuple::factory_trade(
        //     Timestamp::from_str("2020-01-01 00:00:21.000000000").unwrap(),
        //     "ORCL",
        //     100,
        // ));
        // assert_eq!(out.len(), 2);
        // t_expect(out.get(0).unwrap(), "ORCL", 175);
        // t_expect(out.get(1).unwrap(), "ORCL", 100);
    }
}
