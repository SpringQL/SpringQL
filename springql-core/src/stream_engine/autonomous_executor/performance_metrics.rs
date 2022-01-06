pub(in crate::stream_engine::autonomous_executor) mod performance_metrics_summary;

pub(in crate::stream_engine::autonomous_executor) mod queue_metrics;
pub(in crate::stream_engine::autonomous_executor) mod task_metrics;

pub(in crate::stream_engine::autonomous_executor) mod metrics_update_command;

mod calculation;

use std::{
    collections::HashMap,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use self::{
    metrics_update_command::metrics_update_command_by_task_execution::MetricsUpdateCommandByTaskExecution,
    queue_metrics::{row_queue_metrics::RowQueueMetrics, window_queue_metrics::WindowQueueMetrics},
    task_metrics::TaskMetrics,
};
use super::task_graph::{
    queue_id::{row_queue_id::RowQueueId, window_queue_id::WindowQueueId, QueueId},
    task_id::TaskId,
    TaskGraph,
};

/// Performance metrics of task execution.
///
/// It is monitored by [PerformanceMonitorWorker](crate::stream_processor::autonomous_executor::worker::performance_monitor_worker::PerformanceMonitoRworker),
/// and it is updated by [TaskExecutor](crate::stream_processor::autonomous_executor::task_executor::TaskExecutor).
///
/// `PerformanceMonitorWorker` does not frequently read from `RwLock<*Metrics>`, and schedulers in `TaskExecutor` are not expected to
/// execute consequent tasks (sharing the same queue as input or output) by different workers at the same time.
/// Therefore, not much contention for `RwLock<*Metrics>` occurs.
#[derive(Debug)]
pub(super) struct PerformanceMetrics {
    tasks: HashMap<TaskId, RwLock<TaskMetrics>>,

    row_queues: HashMap<RowQueueId, RwLock<RowQueueMetrics>>,
    window_queues: HashMap<WindowQueueId, RwLock<WindowQueueMetrics>>,
}

impl PerformanceMetrics {
    pub(super) fn new(
        task_ids: Vec<TaskId>,
        row_queue_ids: Vec<RowQueueId>,
        window_queue_ids: Vec<WindowQueueId>,
    ) -> Self {
        let window_queues = window_queue_ids
            .into_iter()
            .map(|id| (id, RwLock::new(WindowQueueMetrics::default())))
            .collect();
        let row_queues = row_queue_ids
            .into_iter()
            .map(|id| (id, RwLock::new(RowQueueMetrics::default())))
            .collect();
        let tasks = task_ids
            .into_iter()
            .map(|id| (id, RwLock::new(TaskMetrics::default())))
            .collect();

        Self {
            window_queues,
            row_queues,
            tasks,
        }
    }

    pub(super) fn from_task_graph(graph: &TaskGraph) -> Self {
        Self::new(graph.tasks(), graph.row_queues(), graph.window_queues())
    }

    pub(super) fn update_by_task_execution(&self, command: &MetricsUpdateCommandByTaskExecution) {
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

    pub(super) fn rows_for_task_input(&self, queue_id: &QueueId) -> u64 {
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

    pub(super) fn avg_gain_bytes_per_sec(&self, task_id: &TaskId) -> f32 {
        let t = self.get_task_read(task_id);
        t.avg_gain_bytes_per_sec()
    }

    pub(super) fn get_window_queues(
        &self,
    ) -> Vec<(&WindowQueueId, RwLockReadGuard<'_, WindowQueueMetrics>)> {
        self.window_queues
            .iter()
            .map(|(id, q)| (id, q.read().expect("not poisoned")))
            .collect()
    }

    pub(super) fn get_row_queues(
        &self,
    ) -> Vec<(&RowQueueId, RwLockReadGuard<'_, RowQueueMetrics>)> {
        self.row_queues
            .iter()
            .map(|(id, q)| (id, q.read().expect("not poisoned")))
            .collect()
    }

    pub(super) fn get_tasks(&self) -> Vec<(&TaskId, RwLockReadGuard<'_, TaskMetrics>)> {
        self.tasks
            .iter()
            .map(|(id, t)| (id, t.read().expect("not poisoned")))
            .collect()
    }

    fn get_task_read(&self, id: &TaskId) -> RwLockReadGuard<'_, TaskMetrics> {
        self.tasks
            .get(id)
            .expect("task_id not found")
            .read()
            .expect("poisoned")
    }
    fn get_window_queue_read(&self, id: &WindowQueueId) -> RwLockReadGuard<'_, WindowQueueMetrics> {
        self.window_queues
            .get(id)
            .expect("queue_id not found")
            .read()
            .expect("poisoned")
    }
    fn get_row_queue_read(&self, id: &RowQueueId) -> RwLockReadGuard<'_, RowQueueMetrics> {
        self.row_queues
            .get(id)
            .expect("queue_id not found")
            .read()
            .expect("poisoned")
    }

    fn get_task_write(&self, id: &TaskId) -> RwLockWriteGuard<'_, TaskMetrics> {
        self.tasks
            .get(id)
            .expect("queue_id not found")
            .write()
            .expect("poisoned")
    }
    fn get_row_queue_write(&self, id: &RowQueueId) -> RwLockWriteGuard<'_, RowQueueMetrics> {
        self.row_queues
            .get(id)
            .expect("queue_id not found")
            .write()
            .expect("poisoned")
    }
    fn get_window_queue_write(
        &self,
        id: &WindowQueueId,
    ) -> RwLockWriteGuard<'_, WindowQueueMetrics> {
        self.window_queues
            .get(id)
            .expect("queue_id not found")
            .write()
            .expect("poisoned")
    }
}
