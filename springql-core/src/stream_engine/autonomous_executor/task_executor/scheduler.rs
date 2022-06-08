// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! A task graph is a DAG where nodes are `TaskId`s and edges are `QueueId`s.
//!
//! ![Task graph concept diagram](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/pipeline-and-task-graph.svg)
//!
//! A scheduler generates series of TaskId which a GenericWorker executes at a time.

pub(in crate::stream_engine::autonomous_executor) mod flow_efficient_scheduler;
pub(in crate::stream_engine::autonomous_executor) mod memory_reducing_scheduler;
pub(in crate::stream_engine::autonomous_executor) mod source_scheduler;

/// Max length of task series a scheduler calculates.
/// FlowEfficientScheduler does not care this value to achieve _collector-to-stopper_ policy.
///
/// Too long series may badly affect on scheduler change (e.g. memory state change from severe to moderate).
const MAX_TASK_SERIES: u16 = 20;

use std::fmt::Debug;

use crate::stream_engine::autonomous_executor::{
    performance_metrics::PerformanceMetrics,
    task_graph::{task_id::TaskId, TaskGraph},
};

/// All scheduler implementation must be stateless because MemoryStateMachine replace scheduler implementation
/// dynamically.
pub(in crate::stream_engine::autonomous_executor) trait Scheduler:
    Debug + Default
{
    /// Called from worker threads.
    fn next_task_series(&self, graph: &TaskGraph, metrics: &PerformanceMetrics) -> Vec<TaskId>;
}
