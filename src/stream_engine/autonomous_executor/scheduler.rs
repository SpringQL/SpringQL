pub(super) mod scheduler_read;
pub(super) mod scheduler_write;

mod flow_efficient_scheduler;

use std::sync::Arc;

pub(crate) use flow_efficient_scheduler::FlowEfficientScheduler;

use crate::error::Result;
use crate::stream_engine::pipeline::{pipeline_version::PipelineVersion, Pipeline};

use super::task::{task_graph::TaskGraph, Task};

pub(in crate::stream_engine) trait WorkerState {}

pub(in crate::stream_engine) trait Scheduler {
    type W: WorkerState + Clone + Default;

    /// Called from main thread.
    fn update_pipeline(&mut self, pipeline: Pipeline) {
        let task_graph = TaskGraph::from(pipeline.as_graph());
        self._update_pipeline_version(pipeline.version());
        self._update_task_graph(task_graph)
    }

    /// Called from main thread.
    fn _update_pipeline_version(&mut self, v: PipelineVersion);

    /// Called from main thread.
    fn _update_task_graph(&mut self, task_graph: TaskGraph);

    /// Called from worker threads.
    fn next_task(&self, worker_state: Self::W) -> Option<(Arc<Task>, Self::W)>;
}
