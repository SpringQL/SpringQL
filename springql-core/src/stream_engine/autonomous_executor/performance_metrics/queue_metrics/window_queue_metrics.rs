// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::stream_engine::autonomous_executor::{
    performance_metrics::{
        calculation::floor0, metrics_update_command::MetricsUpdateByTaskExecution,
    },
    task_graph::queue_id::window_queue_id::WindowQueueId,
};

/// Stock monitor of a window queue.
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct WindowQueueMetrics {
    /// might be negative value if `dispatched` event is subscribed earlier than `put` event.
    rows_waiting: i64,
    /// ditto
    bytes: i64,
}

impl WindowQueueMetrics {
    pub fn update_by_task_execution(
        &mut self,
        id: &WindowQueueId,
        command: &MetricsUpdateByTaskExecution,
    ) {
        self.rows_waiting += command.window_queue_waiting_gain_rows(id);
        self.bytes += command.window_queue_gain_bytes(id);
    }

    pub fn update_by_purge(&mut self) {
        self.rows_waiting = 0;
        self.bytes = 0;
    }

    /// Current number of rows in the queue.
    pub fn rows_waiting(&self) -> u64 {
        floor0(self.rows_waiting)
    }

    /// Current bytes consumed in the queue.
    pub fn bytes(&self) -> u64 {
        floor0(self.bytes)
    }
}
