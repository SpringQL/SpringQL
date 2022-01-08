// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! A task graph is a DAG where nodes are `TaskId`s and edges are `QueueId`s.
//!
//! ![Task graph concept diagram](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/pipeline-and-task-graph.svg)
//!
//! A scheduler generates series of TaskId which a GenericWorker executes at a time.

pub(in crate::stream_engine::autonomous_executor) mod flow_efficient_scheduler;

use crate::stream_engine::autonomous_executor::performance_metrics::PerformanceMetrics;
use crate::stream_engine::autonomous_executor::task_graph::task_id::TaskId;
use crate::stream_engine::autonomous_executor::task_graph::TaskGraph;

/// All scheduler implementation must be stateless because MemoryStateMachine replace scheduler implementation
/// dynamically.
pub(in crate::stream_engine::autonomous_executor) trait Scheduler {
    /// Called from worker threads.
    fn next_task_series(&self, graph: &TaskGraph, metrics: &PerformanceMetrics) -> Vec<TaskId>;
}
