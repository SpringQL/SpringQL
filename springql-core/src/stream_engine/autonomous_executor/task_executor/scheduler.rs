// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod flow_efficient_scheduler;

use std::fmt::Debug;

use crate::error::Result;
use crate::pipeline::pipeline_version::PipelineVersion;
use crate::stream_engine::autonomous_executor::current_pipeline::CurrentPipeline;
use crate::stream_engine::autonomous_executor::task_graph::task_id::TaskId;
use crate::stream_engine::autonomous_executor::task_graph::TaskGraph;

pub(crate) trait WorkerState {}

pub(in crate::stream_engine::autonomous_executor) trait Scheduler:
    Debug + Default + Sync + Send + 'static
{
    type W: WorkerState + Clone + Default;

    /// Called from main thread.
    fn notify_pipeline_update(&mut self, current_pipeline: &CurrentPipeline) -> Result<()> {
        let pipeline = current_pipeline.pipeline();
        let task_graph = current_pipeline.task_graph();
        self._notify_pipeline_version(pipeline.version());
        self._notify_task_graph_update(task_graph)
    }

    /// Called from main thread.
    fn _notify_pipeline_version(&mut self, v: PipelineVersion);

    /// Called from main thread.
    fn _notify_task_graph_update(&mut self, task_graph: &TaskGraph) -> Result<()>;

    /// Called from worker threads.
    fn next_task(&self, worker_state: Self::W) -> Option<(TaskId, Self::W)>;
}
