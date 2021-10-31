pub(super) mod scheduler_read;
pub(super) mod scheduler_write;

mod flow_efficient_scheduler;

pub(crate) use flow_efficient_scheduler::FlowEfficientScheduler;

use crate::stream_engine::pipeline::Pipeline;

use super::{
    task::{task_graph::TaskGraph, Task},
    worker_pool::worker::worker_id::WorkerId,
};

pub(in crate::stream_engine) trait Scheduler {
    /// Called from main thread.
    fn update_pipeline(&mut self, pipeline: Pipeline) {
        let task_graph = TaskGraph::from(pipeline.as_graph());
        self.update_task_graph(task_graph)
    }

    /// Called from main thread.
    fn update_task_graph(&mut self, task_graph: TaskGraph);

    /// Called from worker threads.
    fn next_task(&self, worker: WorkerId) -> Option<Task>;
}
