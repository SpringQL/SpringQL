use serde::{Deserialize, Serialize};

use crate::stream_engine::autonomous_executor::{
    performance_metrics::metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
    task_graph::queue_id::row_queue_id::RowQueueId,
};

/// Stock monitor of a row queue (including in-memory queue sink).
#[derive(Clone, Eq, PartialEq, Debug, Default, Serialize, Deserialize)]
pub(in crate::stream_engine::autonomous_executor) struct RowQueueMetrics {
    rows: u64,
    bytes: u64,
}

impl RowQueueMetrics {
    pub(in crate::stream_engine::autonomous_executor) fn update_by_task_execution(
        &mut self,
        id: &RowQueueId,
        command: &MetricsUpdateByTaskExecution,
    ) {
        self.rows = (self.rows as i64 + command.row_queue_gain_rows(id)) as u64;
        self.bytes = (self.bytes as i64 + command.row_queue_gain_bytes(id)) as u64;
    }

    pub(in crate::stream_engine::autonomous_executor) fn update_by_purge(
        &mut self,
        id: &RowQueueId,
    ) {
        self.rows = 0;
        self.bytes = 0;
    }

    /// Current number of rows in the queue.
    pub(in crate::stream_engine::autonomous_executor) fn rows(&self) -> u64 {
        self.rows
    }

    /// Current bytes consumed in the queue.
    pub(in crate::stream_engine::autonomous_executor) fn bytes(&self) -> u64 {
        self.bytes
    }
}
