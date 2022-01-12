use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::PerformanceMetrics;

/// Summary of [PerformanceMetrics](super::PerformanceMetrics).
///
/// From this summary, [TaskExecutor](crate::stream_processor::autonomous_executor::task_executor::TaskExecutor):
/// - transits memory state diagram
/// - changes task scheduler
/// - launches purger
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(in crate::stream_engine::autonomous_executor) struct PerformanceMetricsSummary {
    /// might be negative value if `used` event is subscribed earlier than `put` event.
    pub(in crate::stream_engine::autonomous_executor) queue_total_bytes: i64,
}

impl From<&PerformanceMetrics> for PerformanceMetricsSummary {
    fn from(pm: &PerformanceMetrics) -> Self {
        let queue_total_bytes = Self::queue_total_bytes(pm);
        Self { queue_total_bytes }
    }
}

impl Display for PerformanceMetricsSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.queue_total_bytes)
    }
}

impl PerformanceMetricsSummary {
    fn queue_total_bytes(pm: &PerformanceMetrics) -> i64 {
        let window = pm
            .get_window_queues()
            .iter()
            .fold(0, |acc, (_, met)| acc + met.bytes());

        let row = pm
            .get_row_queues()
            .iter()
            .fold(0, |acc, (_, met)| acc + met.bytes());

        window + row
    }
}
