// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod scheduler_read;
pub(super) mod scheduler_write;

mod flow_efficient_scheduler;

pub(crate) use flow_efficient_scheduler::FlowEfficientScheduler;
use std::fmt::Debug;
use std::sync::Arc;

use crate::error::Result;
use crate::pipeline::{pipeline_version::PipelineVersion, Pipeline};
use crate::stream_engine::autonomous_executor::task::task_graph::TaskGraph;
use crate::stream_engine::autonomous_executor::task::Task;

pub(crate) trait WorkerState {}

pub(crate) trait Scheduler: Debug + Default + Sync + Send + 'static {
    type W: WorkerState + Clone + Default;

    /// Called from main thread.
    fn notify_pipeline_update(&mut self, pipeline: Pipeline) -> Result<()> {
        let task_graph = TaskGraph::from(pipeline.as_graph());
        self._notify_pipeline_version(pipeline.version());
        self._notify_task_graph_update(task_graph)
    }

    /// Called from main thread.
    fn _notify_pipeline_version(&mut self, v: PipelineVersion);

    /// Called from main thread.
    fn _notify_task_graph_update(&mut self, task_graph: TaskGraph) -> Result<()>;

    /// Called from worker threads.
    fn next_task(&self, worker_state: Self::W) -> Option<(Arc<Task>, Self::W)>;

    /// Called from worker threads.
    fn task_graph(&self) -> Arc<TaskGraph>;
}
