use crate::stream_engine::time::duration::event_duration::EventDuration;

/// Window parameters
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum WindowParameter {
    /// Time-based sliding window
    ///
    /// ```text
    /// length = 10sec, period = 5sec, allowed_delay = 0;
    ///
    /// pane1 |        |
    /// pane2      |         |
    /// pane3          |          |
    ///
    /// -----------------------------------> t
    ///      :00  :05  :10  :15  :20
    /// ```
    TimedSlidingWindow {
        length: EventDuration,
        period: EventDuration,
        allowed_delay: EventDuration,
    },
}
