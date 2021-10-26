use crate::{
    error::Result, model::query_plan::operation::SlidingWindowOperation,
    stream_engine::executor::data::row::Row,
};
use chrono::Duration;
use std::{collections::VecDeque, rc::Rc};

use super::{interm_row::PreservedRow, row_window::RowWindow};

#[derive(Debug)]
pub(super) struct SlidingWindowExecutor {
    window: RowWindow,
    window_width: Duration, // TODO row-based sliding window
}

impl SlidingWindowExecutor {
    pub(super) fn register(op: &SlidingWindowOperation) -> Self {
        let window_width = match op {
            SlidingWindowOperation::TimeBased { lower_bound } => *lower_bound,
        };

        Self {
            window: RowWindow::default(),
            window_width,
        }
    }

    /// Mutates internal window state.
    pub(super) fn run(&mut self, input: Rc<Row>) -> Result<&RowWindow> {
        let input_ts = input.rowtime();
        let lower_bound_ts = input_ts - self.window_width;

        let mut new_window_fifo = self
            .window
            .inner()
            .iter()
            .filter(|r| {
                let ts = r.as_ref().rowtime();
                lower_bound_ts < ts && ts <= input_ts
            })
            .cloned()
            .collect::<VecDeque<PreservedRow>>();

        new_window_fifo.push_front(PreservedRow::new(input));

        self.window = RowWindow::new(new_window_fifo);
        Ok(&self.window)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        dependency_injection::{test_di::TestDI, DependencyInjection},
        model::name::{ColumnName, PumpName},
        stream_engine::{
            executor::data::{row::Row, value::sql_value::SqlValue},
            RowRepository, Timestamp,
        },
    };

    use super::*;

    /// Using example from: <https://docs.sqlstream.com/sql-reference-guide/select-statement/window-clause/#time-based-sliding-windows>
    #[test]
    fn test_sliding_window() {
        struct TestCase {
            input: Row,               // PK: timestamp
            expected: Vec<Timestamp>, // PKs; FIFO (left is the latest pushed)
        }

        let di = TestDI::default();
        let row_repo = di.row_repository();

        let pump = PumpName::fx_trade_window();
        let downstream_pumps = vec![pump.clone()];

        let op = SlidingWindowOperation::TimeBased {
            lower_bound: Duration::minutes(5),
        };
        let mut executor = SlidingWindowExecutor::register(&op);

        let t_03_02_00 = Timestamp::from_str("2019-03-30 03:02:00.000000000").unwrap();
        let t_03_02_10 = Timestamp::from_str("2019-03-30 03:02:10.000000000").unwrap();
        let t_03_03_00 = Timestamp::from_str("2019-03-30 03:03:00.000000000").unwrap();
        let t_03_04_00 = Timestamp::from_str("2019-03-30 03:04:00.000000000").unwrap();
        let t_03_04_30 = Timestamp::from_str("2019-03-30 03:04:30.000000000").unwrap();
        let t_03_04_45 = Timestamp::from_str("2019-03-30 03:04:45.000000000").unwrap();
        let t_03_05_00 = Timestamp::from_str("2019-03-30 03:05:00.000000000").unwrap();
        let t_03_05_30 = Timestamp::from_str("2019-03-30 03:05:30.000000000").unwrap();
        let t_03_59_45 = Timestamp::from_str("2019-03-30 03:59:45.000000000").unwrap();
        let t_04_02_00 = Timestamp::from_str("2019-03-30 04:02:00.000000000").unwrap();
        let t_04_04_00 = Timestamp::from_str("2019-03-30 04:04:00.000000000").unwrap();
        let t_04_06_00 = Timestamp::from_str("2019-03-30 04:06:00.000000000").unwrap();
        let t_04_08_00 = Timestamp::from_str("2019-03-30 04:08:00.000000000").unwrap();
        let t_04_18_00 = Timestamp::from_str("2019-03-30 04:18:00.000000000").unwrap();
        let t_04_43_00 = Timestamp::from_str("2019-03-30 04:43:00.000000000").unwrap();
        let t_04_44_00 = Timestamp::from_str("2019-03-30 04:44:00.000000000").unwrap();
        let t_05_46_00 = Timestamp::from_str("2019-03-30 05:46:00.000000000").unwrap();

        let test_cases = vec![
            TestCase {
                input: Row::factory_trade(t_03_02_00, "ORCL", 20),
                expected: vec![t_03_02_00],
            },
            TestCase {
                input: Row::factory_trade(t_03_02_10, "ORCL", 20),
                expected: vec![t_03_02_10, t_03_02_00],
            },
            TestCase {
                input: Row::factory_trade(t_03_03_00, "IBM", 30),
                expected: vec![t_03_03_00, t_03_02_10, t_03_02_00],
            },
            TestCase {
                input: Row::factory_trade(t_03_04_00, "ORCL", 15),
                expected: vec![t_03_04_00, t_03_03_00, t_03_02_10, t_03_02_00],
            },
            TestCase {
                input: Row::factory_trade(t_03_04_30, "IBM", 40),
                expected: vec![t_03_04_30, t_03_04_00, t_03_03_00, t_03_02_10, t_03_02_00],
            },
            TestCase {
                input: Row::factory_trade(t_03_04_45, "IBM", 10),
                expected: vec![
                    t_03_04_45, t_03_04_30, t_03_04_00, t_03_03_00, t_03_02_10, t_03_02_00,
                ],
            },
            TestCase {
                input: Row::factory_trade(t_03_05_00, "MSFT", 15),
                expected: vec![
                    t_03_05_00, t_03_04_45, t_03_04_30, t_03_04_00, t_03_03_00, t_03_02_10,
                    t_03_02_00,
                ],
            },
            TestCase {
                input: Row::factory_trade(t_03_05_30, "MSFT", 55),
                expected: vec![
                    t_03_05_30, t_03_05_00, t_03_04_45, t_03_04_30, t_03_04_00, t_03_03_00,
                    t_03_02_10, t_03_02_00,
                ],
            },
            TestCase {
                input: Row::factory_trade(t_03_59_45, "IBM", 20),
                expected: vec![t_03_59_45],
            },
            TestCase {
                input: Row::factory_trade(t_04_02_00, "GOOGL", 100),
                expected: vec![t_04_02_00, t_03_59_45],
            },
            TestCase {
                input: Row::factory_trade(t_04_04_00, "GOOGL", 100),
                expected: vec![t_04_04_00, t_04_02_00, t_03_59_45],
            },
            TestCase {
                input: Row::factory_trade(t_04_06_00, "ORCL", 5),
                expected: vec![t_04_06_00, t_04_04_00, t_04_02_00],
            },
            TestCase {
                input: Row::factory_trade(t_04_08_00, "IBM", 15),
                expected: vec![t_04_08_00, t_04_06_00, t_04_04_00],
            },
            TestCase {
                input: Row::factory_trade(t_04_18_00, "IBM", 40),
                expected: vec![t_04_18_00],
            },
            TestCase {
                input: Row::factory_trade(t_04_18_00, "GOOGL", 100),
                expected: vec![t_04_18_00, t_04_18_00],
            },
            TestCase {
                input: Row::factory_trade(t_04_18_00, "GOOGL", 100),
                expected: vec![t_04_18_00, t_04_18_00, t_04_18_00],
            },
            TestCase {
                input: Row::factory_trade(t_04_18_00, "IBM", 15),
                expected: vec![t_04_18_00, t_04_18_00, t_04_18_00, t_04_18_00],
            },
            TestCase {
                input: Row::factory_trade(t_04_43_00, "IBM", 60),
                expected: vec![t_04_43_00],
            },
            TestCase {
                input: Row::factory_trade(t_04_44_00, "ORCL", 1000),
                expected: vec![t_04_44_00, t_04_43_00],
            },
            TestCase {
                input: Row::factory_trade(t_05_46_00, "ORCL", 3000),
                expected: vec![t_05_46_00],
            },
        ];

        // (ForeignInputServer ->) row1 -> Stream[ticker] -> ref. row1 -> Window[ticker.pump1]

        for TestCase {
            input: row,
            expected,
        } in test_cases
        {
            row_repo.emit_owned(row, &downstream_pumps).unwrap();
            let in_row = row_repo.collect_next(&pump).unwrap();
            let window = executor.run(in_row).unwrap();

            let got_pks = window
                .inner()
                .iter()
                .map(|got_row| {
                    let got_sql_value = got_row
                        .as_ref()
                        .get(&ColumnName::new("timestamp".to_string()))
                        .unwrap();
                    if let SqlValue::NotNull(got_nn_sql_value) = got_sql_value {
                        got_nn_sql_value.unpack()
                    } else {
                        unreachable!()
                    }
                })
                .collect::<Result<Vec<Timestamp>>>()
                .unwrap();

            assert_eq!(got_pks, expected);
        }
    }
}
