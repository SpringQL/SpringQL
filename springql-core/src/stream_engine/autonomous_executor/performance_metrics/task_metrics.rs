// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::stream_engine::autonomous_executor::performance_metrics::{
    calculation::next_avg, metrics_update_command::MetricsUpdateByTaskExecution,
};

/// Flow monitor of a task (including in-memory queue sink) execution.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct TaskMetrics {
    avg_gain_bytes_per_sec: f32,
    n_executions: u64,
}

impl TaskMetrics {
    pub fn update_by_task_execution(&mut self, command: &MetricsUpdateByTaskExecution) {
        let n = self.n_executions;

        self.n_executions += 1;
        self.avg_gain_bytes_per_sec = next_avg(
            self.avg_gain_bytes_per_sec,
            n,
            command.task_gain_bytes_per_sec(),
        );
    }

    pub fn avg_gain_bytes_per_sec(&self) -> f32 {
        self.avg_gain_bytes_per_sec
    }
}
