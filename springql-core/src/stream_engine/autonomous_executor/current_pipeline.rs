// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::pipeline::Pipeline;

use super::task::task_graph::TaskGraph;

/// Current pipeline and corresponding task graph.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct CurrentPipeline {
    pipeline: Pipeline,
    task_graph: TaskGraph,
}

impl CurrentPipeline {
    pub(in crate::stream_engine::autonomous_executor) fn new(pipeline: Pipeline) -> Self {
        let task_graph = TaskGraph::from(pipeline.as_graph());
        Self {
            pipeline,
            task_graph,
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }
    pub(in crate::stream_engine::autonomous_executor) fn task_graph(&self) -> &TaskGraph {
        &self.task_graph
    }
}
