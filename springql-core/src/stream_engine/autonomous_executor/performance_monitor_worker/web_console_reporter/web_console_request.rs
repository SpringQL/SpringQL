// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use serde_json::json;

use crate::stream_engine::autonomous_executor::{
    performance_metrics::{
        queue_metrics::{RowQueueMetrics, WindowQueueMetrics},
        task_metrics::TaskMetrics,
        PerformanceMetrics,
    },
    task_graph::{
        queue_id::{row_queue_id::RowQueueId, window_queue_id::WindowQueueId},
        task_id::TaskId,
        TaskGraph,
    },
};

#[derive(Clone, PartialEq, Debug)]
pub struct WebConsoleRequest {
    tasks: Vec<TaskRequest>,
    queues: Vec<QueueRequest>,
}

impl WebConsoleRequest {
    pub fn from_metrics(metrics: &PerformanceMetrics, graph: &TaskGraph) -> Self {
        let tasks = metrics
            .get_tasks()
            .iter()
            .map(|(id, metrics)| TaskRequest::from_metrics(id, &*metrics))
            .collect();

        let queues = metrics
            .get_row_queues()
            .iter()
            .map(|(id, metrics)| QueueRequest::from_row(id, &*metrics, graph))
            .chain(
                metrics
                    .get_window_queues()
                    .iter()
                    .map(|(id, metrics)| QueueRequest::from_window(id, &*metrics, graph)),
            )
            .collect();

        Self { tasks, queues }
    }
}

impl WebConsoleRequest {
    pub fn to_json(&self) -> serde_json::Value {
        json!(
            {
                "tasks": self.tasks.iter().map(TaskRequest::to_json).collect::<Vec<_>>(),
                "queues": self.queues.iter().map(QueueRequest::to_json).collect::<Vec<_>>(),
            }
        )
    }
}

#[derive(Clone, PartialEq, Debug)]
struct TaskRequest {
    id: String,
    type_: String,
    avg_gain_bytes_per_sec: f32,
}

impl TaskRequest {
    fn from_metrics(id: &TaskId, metrics: &TaskMetrics) -> Self {
        Self {
            id: id.to_string(),
            type_: match id {
                TaskId::Source { .. } => "source-task",
                TaskId::Pump { .. } => "pump-task",
                TaskId::Sink { .. } => "sink-task",
            }
            .to_string(),
            avg_gain_bytes_per_sec: metrics.avg_gain_bytes_per_sec(),
        }
    }

    fn to_json(&self) -> serde_json::Value {
        json!(
            {
                "id": self.id.clone(),
                "type": self.type_.clone(),
                "avg-gain-bytes-per-sec": self.avg_gain_bytes_per_sec,
            }
        )
    }
}

#[derive(Clone, PartialEq, Debug)]
struct QueueRequest {
    id: String,
    upstream_task_id: String,
    downstream_task_id: String,
    queue: QueueInnerRequest,
}

impl QueueRequest {
    fn from_row(id: &RowQueueId, metrics: &RowQueueMetrics, graph: &TaskGraph) -> Self {
        let upstream_task_id = graph.upstream_task(&id.clone().into()).to_string();
        let downstream_task_id = graph.downstream_task(&id.clone().into()).to_string();
        let queue = QueueInnerRequest::Row {
            num_rows: metrics.rows(),
            total_bytes: metrics.bytes(),
        };
        Self {
            id: id.to_string(),
            upstream_task_id,
            downstream_task_id,
            queue,
        }
    }

    fn from_window(id: &WindowQueueId, metrics: &WindowQueueMetrics, graph: &TaskGraph) -> Self {
        let upstream_task_id = graph.upstream_task(&id.clone().into()).to_string();
        let downstream_task_id = graph.downstream_task(&id.clone().into()).to_string();
        let queue = QueueInnerRequest::Window {
            num_rows_waiting: metrics.rows_waiting(),
            total_bytes: metrics.bytes(),
        };
        Self {
            id: id.to_string(),
            upstream_task_id,
            downstream_task_id,
            queue,
        }
    }

    fn to_json(&self) -> serde_json::Value {
        json!(
            {
                "id": self.id.clone(),
                "upstream-task-id": self.upstream_task_id.clone(),
                "downstream-task-id": self.downstream_task_id.clone(),
                "row-queue": match &self.queue {
                    row_queue @ QueueInnerRequest::Row {..} => Some(row_queue.to_json()),
                    _ => None,
                },
                "window-queue": match &self.queue {
                    window_queue @ QueueInnerRequest::Window {..} => Some(window_queue.to_json()),
                    _ => None,
                },
            }
        )
    }
}

#[derive(Clone, PartialEq, Debug)]
enum QueueInnerRequest {
    Row {
        num_rows: u64,
        total_bytes: u64,
    },
    Window {
        num_rows_waiting: u64,
        total_bytes: u64,
    },
}

impl QueueInnerRequest {
    fn to_json(&self) -> serde_json::Value {
        match self {
            QueueInnerRequest::Row {
                num_rows,
                total_bytes,
            } => json!(
                {
                    "num-rows": num_rows,
                    "total-bytes": total_bytes,
                }
            ),
            QueueInnerRequest::Window {
                num_rows_waiting,
                total_bytes,
            } => json!({
                "num-rows-waiting": num_rows_waiting,
                "total-bytes": total_bytes,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::PipelineVersion;

    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn test_to_json() {
        fn t(metrics: PerformanceMetrics, graph: TaskGraph, expected: serde_json::Value) {
            let request = WebConsoleRequest::from_metrics(&metrics, &graph);
            assert_eq!(request.to_json(), expected);
        }

        let task_graph = TaskGraph::new(PipelineVersion::new());

        t(
            PerformanceMetrics::from_task_graph(&task_graph),
            task_graph,
            json!({
                "tasks": [],
                "queues": [],
            }),
        );
    }
}
