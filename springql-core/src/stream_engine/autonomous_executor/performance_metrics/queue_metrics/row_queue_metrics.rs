// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::stream_engine::autonomous_executor::{
    performance_metrics::{
        calculation::floor0, metrics_update_command::MetricsUpdateByTaskExecution,
    },
    task_graph::RowQueueId,
};

/// Stock monitor of a row queue (including in-memory queue sink).
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct RowQueueMetrics {
    /// might be negative value if `used` event is subscribed earlier than `put` event.
    rows: i64,
    /// ditto
    bytes: i64,
}

impl RowQueueMetrics {
    pub fn update_by_task_execution(
        &mut self,
        id: &RowQueueId,
        command: &MetricsUpdateByTaskExecution,
    ) {
        self.rows += command.row_queue_gain_rows(id);
        self.bytes += command.row_queue_gain_bytes(id);
    }

    pub fn update_by_purge(&mut self) {
        self.rows = 0;
        self.bytes = 0;
    }

    /// Current number of rows in the queue.
    pub fn rows(&self) -> u64 {
        floor0(self.rows)
    }

    /// Current bytes consumed in the queue.
    pub fn bytes(&self) -> u64 {
        floor0(self.bytes)
    }
}
