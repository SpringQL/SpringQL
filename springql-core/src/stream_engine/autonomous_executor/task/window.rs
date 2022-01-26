use self::{
    window_operation_parameter::WindowOperationParameter, window_parameter::WindowParameter,
};

use super::tuple::Tuple;

mod window_operation_parameter;
mod window_parameter;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct Window {
    window_param: WindowParameter,
    op_param: WindowOperationParameter,
}

impl Window {
    /// A task dispatches a tuple from waiting queue.
    /// This window returns output tuples from panes inside if they are closed.
    pub(in crate::stream_engine::autonomous_executor) fn dispatch(
        &self,
        tuple: Tuple,
    ) -> Vec<Tuple> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        pipeline::{field::field_pointer::FieldPointer, name::FieldAlias},
        stream_engine::{
            autonomous_executor::task::tuple::Tuple, time::timestamp::Timestamp, SqlValue,
        },
    };

    use super::{
        window_operation_parameter::{AggregateFunctionParameter, WindowOperationParameter},
        window_parameter::WindowParameter,
        *,
    };

    #[test]
    fn test_timed_sliding_window_aggregation() {
        // SELECT ticker, AVG(amount) AS avg_amount
        //   FROM trade
        //   SLIDING WINDOW duration_millis(1000), duration_mills(500)
        //   GROUP BY ticker;

        fn t_expect(tuple: Tuple, expected_ticker: &str, expected_avg_amount: i16) {
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

        let window = Window::new(
            WindowParameter::TimedSlidingWindow {
                length: EventDuration::from_millis(1000),
                period: EventDuration::from_mills(500),
                allowed_delay: EventDuration::from_mills(100),
            },
            WindowOperationParameter::Aggregation {
                group_by: FieldPointer::from("ticker"),
                aggregated: FieldPointer::from("amount"),
                aggregated_alias: FieldAlias::new("avg_amount".to_string()),
                aggregate_function: AggregateFunctionParameter::Avg,
            },
        );

        // [0.000, 1.000): ("GOOGL", 100)
        let out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01T00:00:00.001").unwrap(),
            "GOOGL",
            100,
        ));
        assert!(out.is_empty());

        // [0.000, 1.000): ("GOOGL", 100), ("ORCL", 100)
        let out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01T00:00:00.499").unwrap(),
            "ORCL",
            100,
        ));
        assert!(out.is_empty());

        // [0.000, 1.000): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400)
        // [0.500, 1.500):                                ("ORCL", 400)
        let out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01T00:00:00.500").unwrap(),
            "ORCL",
            400,
        ));
        assert!(out.is_empty());

        // [0.000, 1.000): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400) <-- !!NOT CLOSED YET (within delay)!!
        // [0.500, 1.500):                                ("ORCL", 400), ("ORCL", 100)
        // [1.000, 2.000):                                               ("ORCL", 100)
        let out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01T00:00:01.099").unwrap(),
            "ORCL",
            100,
        ));
        assert!(out.is_empty());

        // [0.000, 1.000): ("GOOGL", 100), ("ORCL", 100), ("ORCL", 400),                ("ORCL", 100) <-- !!LATE DATA!!
        // [0.500, 1.500):                                ("ORCL", 400), ("ORCL", 100), ("ORCL", 100)
        // [1.000, 2.000):                                               ("ORCL", 100)
        let out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01T00:00:00.800").unwrap(),
            "ORCL",
            100,
        ));
        assert!(out.is_empty());

        // [0.000, 1.000): -> "GOOGL" AVG = 100; "ORCL" AVG = 200
        //
        // [0.500, 1.500):                                ("ORCL", 400), ("ORCL", 100), ("ORCL", 100), ("ORCL", 100)
        // [1.000, 2.000):                                               ("ORCL", 100),                ("ORCL", 100)
        let out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01T00:00:01.100").unwrap(),
            "ORCL",
            100,
        ));
        assert_eq!(out.len(), 2);
        t_expect(out[0], "GOOGL", 100);
        t_expect(out[1], "ORCL", 200);

        // [0.500, 1.500): -> "ORCL" = 175
        // [1.000, 2.000): -> "ORCL" = 100
        //
        // [1.500, 2.500):                                                                                           ("ORCL", 100)
        // [2.000, 3.000):                                                                                           ("ORCL", 100)
        let out = window.dispatch(Tuple::factory_trade(
            Timestamp::from_str("2020-01-01T00:00:02.100").unwrap(),
            "ORCL",
            100,
        ));
        assert_eq!(out.len(), 2);
        t_expect(out[0], "ORCL", 175);
        t_expect(out[1], "ORCL", 100);
    }
}
