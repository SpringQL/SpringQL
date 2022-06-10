// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::stream_engine::autonomous_executor::{
    performance_metrics::PerformanceMetrics,
    task_executor::scheduler::{FlowEfficientScheduler, MemoryReducingScheduler, Scheduler},
    task_graph::{TaskGraph, TaskId},
};

#[derive(Debug)]
pub enum GenericWorkerScheduler {
    FlowEfficient(FlowEfficientScheduler),
    MemoryReducing(MemoryReducingScheduler),
}

impl Default for GenericWorkerScheduler {
    fn default() -> Self {
        Self::FlowEfficient(FlowEfficientScheduler::default())
    }
}

impl GenericWorkerScheduler {
    pub fn flow_efficient_scheduler() -> Self {
        Self::FlowEfficient(FlowEfficientScheduler::default())
    }
    pub fn memory_reducing_scheduler() -> Self {
        Self::MemoryReducing(MemoryReducingScheduler::default())
    }
}

impl Scheduler for GenericWorkerScheduler {
    fn next_task_series(&self, graph: &TaskGraph, metrics: &PerformanceMetrics) -> Vec<TaskId> {
        match self {
            GenericWorkerScheduler::FlowEfficient(sched) => sched.next_task_series(graph, metrics),
            GenericWorkerScheduler::MemoryReducing(sched) => sched.next_task_series(graph, metrics),
        }
    }
}
