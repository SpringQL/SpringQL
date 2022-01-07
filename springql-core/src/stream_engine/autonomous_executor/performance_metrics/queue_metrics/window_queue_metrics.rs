use serde::{Deserialize, Serialize};

use crate::stream_engine::autonomous_executor::{
    performance_metrics::metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
    task_graph::queue_id::window_queue_id::WindowQueueId,
};

/// Stock monitor of a window queue.
#[derive(Clone, Eq, PartialEq, Debug, Default, Serialize, Deserialize)]
pub(in crate::stream_engine::autonomous_executor) struct WindowQueueMetrics {
    rows_waiting: u64,
    bytes: u64,
}

impl WindowQueueMetrics {
    pub(in crate::stream_engine::autonomous_executor::performance_metrics) fn update_by_task_execution(
        &mut self,
        id: &WindowQueueId,
        command: &MetricsUpdateByTaskExecution,
    ) {
        self.rows_waiting =
            (self.rows_waiting as i64 + command.window_queue_waiting_gain_rows(id)) as u64;
        self.bytes = (self.bytes as i64 + command.window_queue_gain_bytes(id)) as u64;
    }

    pub(in crate::stream_engine::autonomous_executor::performance_metrics) fn update_by_purge(
        &mut self,
        id: &WindowQueueId,
    ) {
        self.rows_waiting = 0;
        self.bytes = 0;
    }

    /// Current number of rows in the queue.
    pub(in crate::stream_engine::autonomous_executor) fn rows_waiting(&self) -> u64 {
        self.rows_waiting
    }

    /// Current bytes consumed in the queue.
    pub(in crate::stream_engine::autonomous_executor) fn bytes(&self) -> u64 {
        self.bytes
    }
}
