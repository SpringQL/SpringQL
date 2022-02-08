use super::ValueExprType;

#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum FunctionCall<E>
where
    E: ValueExprType,
{
    /// ```text
    /// DURATION_SECS(1) -> EventDuration::from_secs(1)
    /// ```
    DurationSecs { duration_secs: Box<E> },

    /// ```text
    /// FLOOR_TIME("2020-01-01 01:11:11.000000000", DURATION_SECS(10 * 60)) -> "2020-01-01 01:10:00.000000000"
    /// ```
    FloorTime { target: Box<E>, resolution: Box<E> },
}
