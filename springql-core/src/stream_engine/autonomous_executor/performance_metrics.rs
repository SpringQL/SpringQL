// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod calculation;
mod metrics_update_command;
mod performance_metrics_summary;
mod queue_metrics;
mod task_metrics;

pub use metrics_update_command::{
    InQueueMetricsUpdateByCollect, InQueueMetricsUpdateByTask, MetricsUpdateByTaskExecution,
    MetricsUpdateByTaskExecutionOrPurge, OutQueueMetricsUpdateByTask, TaskMetricsUpdateByTask,
    WindowInFlowByWindowTask,
};
pub use performance_metrics_summary::PerformanceMetricsSummary;
pub use queue_metrics::{RowQueueMetrics, WindowQueueMetrics};
pub use task_metrics::TaskMetrics;

use std::collections::HashMap;

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    pipeline::PipelineVersion,
    stream_engine::autonomous_executor::task_graph::{
        QueueId, RowQueueId, TaskGraph, TaskId, WindowQueueId,
    },
};

/// Performance metrics of task execution. It has the same lifetime as a TaskGraph (i.e. a Pipeline).
///
/// It is monitored by `PerformanceMonitorWorker`,
/// and it is updated by `TaskExecutor`.
///
/// `PerformanceMonitorWorker` does not frequently read from `RwLock<*Metrics>`, and schedulers in `TaskExecutor` are not expected to
/// execute consequent tasks (sharing the same queue as input or output) by different workers at the same time.
/// Therefore, not much contention for `RwLock<*Metrics>` occurs.
///
/// Note that `PerformanceMetrics` does not hold precise value in 2 means:
///
/// 1. Memory consumption (bytes) is calculated from estimation value. Rust does not provide ways to calculate `struct`'s exact memory size. See: <https://stackoverflow.com/a/68255583>
/// 2. Rows are `put` and `used` from different tasks, and possibly from different threads. These thread publish `IncrementalUpdateMetrics` events to change the queue's size and number of rows but the event might be out-of-order.
///
/// Number of rows and bytes are stock value and they might be negative value temporarily.
/// In such cases, stock getters in PerformanceMetrics return 0.
#[derive(Debug)]
pub struct PerformanceMetrics {
    /// From which version this metrics constructed
    pipeline_version: PipelineVersion,

    tasks: HashMap<TaskId, RwLock<TaskMetrics>>,
    row_queues: HashMap<RowQueueId, RwLock<RowQueueMetrics>>,
    window_queues: HashMap<WindowQueueId, RwLock<WindowQueueMetrics>>,
}

impl PerformanceMetrics {
    fn new(
        pipeline_version: PipelineVersion,
        task_ids: Vec<TaskId>,
        row_queue_ids: Vec<RowQueueId>,
        window_queue_ids: Vec<WindowQueueId>,
    ) -> Self {
        let tasks = task_ids
            .into_iter()
            .map(|id| (id, RwLock::new(TaskMetrics::default())))
            .collect();

        let row_queues = row_queue_ids
            .into_iter()
            .map(|id| (id, RwLock::new(RowQueueMetrics::default())))
            .collect();

        let window_queues = window_queue_ids
            .into_iter()
            .map(|id| (id, RwLock::new(WindowQueueMetrics::default())))
            .collect();

        Self {
            pipeline_version,
            tasks,
            row_queues,
            window_queues,
        }
    }

    pub fn from_task_graph(graph: &TaskGraph) -> Self {
        Self::new(
            *graph.pipeline_version(),
            graph.tasks(),
            graph.row_queues(),
            graph.window_queues(),
        )
    }

    pub fn pipeline_version(&self) -> &PipelineVersion {
        &self.pipeline_version
    }

    pub fn update_by_task_execution(&self, command: &MetricsUpdateByTaskExecution) {
        let task_id = command.updated_task();
        let mut task_metrics = self.get_task_write(task_id);
        task_metrics.update_by_task_execution(command);

        command
            .updated_queues()
            .iter()
            .for_each(|queue_id| match queue_id {
                QueueId::Row(row_queue_id) => {
                    let mut row_queue_metrics = self.get_row_queue_write(row_queue_id);
                    row_queue_metrics.update_by_task_execution(row_queue_id, command);
                }
                QueueId::Window(window_queue_id) => {
                    let mut window_queue_metrics = self.get_window_queue_write(window_queue_id);
                    window_queue_metrics.update_by_task_execution(window_queue_id, command);
                }
            })
    }

    pub fn update_by_purge(&self) {
        for row_queue_id in self.row_queues.keys() {
            let mut row_queue_metrics = self.get_row_queue_write(row_queue_id);
            row_queue_metrics.update_by_purge();
        }

        for window_queue_id in self.window_queues.keys() {
            let mut window_queue_metrics = self.get_window_queue_write(window_queue_id);
            window_queue_metrics.update_by_purge();
        }
    }

    pub fn rows_for_task_input(&self, queue_id: &QueueId) -> u64 {
        match queue_id {
            QueueId::Row(id) => {
                let q = self.get_row_queue_read(id);
                q.rows()
            }
            QueueId::Window(id) => {
                let q = self.get_window_queue_read(id);
                q.rows_waiting()
            }
        }
    }

    pub fn avg_gain_bytes_per_sec(&self, task_id: &TaskId) -> f32 {
        let t = self.get_task_read(task_id);
        t.avg_gain_bytes_per_sec()
    }

    pub fn get_window_queues(
        &self,
    ) -> Vec<(&WindowQueueId, RwLockReadGuard<'_, WindowQueueMetrics>)> {
        self.window_queues
            .iter()
            .map(|(id, q)| (id, q.read().unwrap()))
            .collect()
    }

    pub fn get_row_queues(&self) -> Vec<(&RowQueueId, RwLockReadGuard<'_, RowQueueMetrics>)> {
        self.row_queues
            .iter()
            .map(|(id, q)| (id, q.read().unwrap()))
            .collect()
    }

    pub fn get_tasks(&self) -> Vec<(&TaskId, RwLockReadGuard<'_, TaskMetrics>)> {
        self.tasks
            .iter()
            .map(|(id, t)| (id, t.read().unwrap()))
            .collect()
    }

    fn get_task_read(&self, id: &TaskId) -> RwLockReadGuard<'_, TaskMetrics> {
        self.tasks
            .get(id)
            .unwrap_or_else(|| panic!("task_id {} not found", id))
            .read()
            .unwrap()
    }
    fn get_window_queue_read(&self, id: &WindowQueueId) -> RwLockReadGuard<'_, WindowQueueMetrics> {
        self.window_queues
            .get(id)
            .unwrap_or_else(|| panic!("queue_id {} not found", id))
            .read()
            .unwrap()
    }
    fn get_row_queue_read(&self, id: &RowQueueId) -> RwLockReadGuard<'_, RowQueueMetrics> {
        self.row_queues
            .get(id)
            .unwrap_or_else(|| panic!("queue_id {} not found", id))
            .read()
            .unwrap()
    }

    fn get_task_write(&self, id: &TaskId) -> RwLockWriteGuard<'_, TaskMetrics> {
        self.tasks
            .get(id)
            .unwrap_or_else(|| panic!("task id {} not found", id))
            .write()
            .unwrap()
    }
    fn get_row_queue_write(&self, id: &RowQueueId) -> RwLockWriteGuard<'_, RowQueueMetrics> {
        self.row_queues
            .get(id)
            .expect("queue_id not found")
            .write()
            .unwrap()
    }
    fn get_window_queue_write(
        &self,
        id: &WindowQueueId,
    ) -> RwLockWriteGuard<'_, WindowQueueMetrics> {
        self.window_queues
            .get(id)
            .expect("queue_id not found")
            .write()
            .unwrap()
    }
}
