use crate::stream_engine::autonomous_executor::{
    performance_metrics::PerformanceMetrics,
    task_executor::scheduler::{
        flow_efficient_scheduler::FlowEfficientScheduler,
        memory_reducing_scheduler::MemoryReducingScheduler, Scheduler,
    },
    task_graph::{task_id::TaskId, TaskGraph},
};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) enum GenericWorkerScheduler {
    FlowEfficient(FlowEfficientScheduler),
    MemoryReducing(MemoryReducingScheduler),
}

impl Default for GenericWorkerScheduler {
    fn default() -> Self {
        Self::FlowEfficient(FlowEfficientScheduler::default())
    }
}

impl GenericWorkerScheduler {
    pub(in crate::stream_engine::autonomous_executor) fn flow_efficient_scheduler() -> Self {
        Self::FlowEfficient(FlowEfficientScheduler::default())
    }
    pub(in crate::stream_engine::autonomous_executor) fn memory_reducing_scheduler() -> Self {
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
