use super::ValueExprPh1;

#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum FunctionCall {
    /// ```text
    /// DURATION_SECS(1) -> EventDuration::from_secs(1)
    /// ```
    DurationSecs { duration_secs: Box<ValueExprPh1> },

    /// ```text
    /// FLOOR_TIME("2020-01-01 01:11:11.000000000", DURATION_SECS(10 * 60)) -> "2020-01-01 01:10:00.000000000"
    /// ```
    FloorTime {
        target: Box<ValueExprPh1>,
        resolution: Box<ValueExprPh1>,
    },
}
