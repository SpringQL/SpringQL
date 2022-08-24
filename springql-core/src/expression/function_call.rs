// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::expression::ValueExprType;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FunctionCall<E>
where
    E: ValueExprType,
{
    /// ```text
    /// DURATION_MILLIS(1) -> EventDuration::from_millis(1)
    /// ```
    DurationMillis { duration_millis: Box<E> },

    /// ```text
    /// DURATION_SECS(1) -> EventDuration::from_secs(1)
    /// ```
    DurationSecs { duration_secs: Box<E> },

    /// ```text
    /// FLOOR_TIME("2020-01-01 01:11:11.000000000", DURATION_SECS(10 * 60)) -> "2020-01-01 01:10:00.000000000"
    /// ```
    FloorTime { target: Box<E>, resolution: Box<E> },
}
