// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use serde::{Deserialize, Serialize};

use crate::stream_engine::autonomous_executor::{
    performance_metrics::{
        calculation::floor0,
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
    },
    task_graph::queue_id::row_queue_id::RowQueueId,
};

/// Stock monitor of a row queue (including in-memory queue sink).
#[derive(Clone, Eq, PartialEq, Debug, Default, Serialize, Deserialize)]
pub(in crate::stream_engine::autonomous_executor) struct RowQueueMetrics {
    /// might be negative value if `used` event is subscribed earlier than `put` event.
    rows: i64,
    /// ditto
    bytes: i64,
}

impl RowQueueMetrics {
    pub(in crate::stream_engine::autonomous_executor) fn update_by_task_execution(
        &mut self,
        id: &RowQueueId,
        command: &MetricsUpdateByTaskExecution,
    ) {
        self.rows += command.row_queue_gain_rows(id);
        self.bytes += command.row_queue_gain_bytes(id);
    }

    pub(in crate::stream_engine::autonomous_executor) fn update_by_purge(&mut self) {
        self.rows = 0;
        self.bytes = 0;
    }

    /// Current number of rows in the queue.
    pub(in crate::stream_engine::autonomous_executor) fn rows(&self) -> u64 {
        floor0(self.rows)
    }

    /// Current bytes consumed in the queue.
    pub(in crate::stream_engine::autonomous_executor) fn bytes(&self) -> u64 {
        floor0(self.bytes)
    }
}
