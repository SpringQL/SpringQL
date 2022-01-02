// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    borrow::BorrowMut,
    sync::{RwLock, RwLockReadGuard},
};

use crate::pipeline::Pipeline;

use super::task::task_graph::TaskGraph;

/// Current pipeline and corresponding task graph.
///
/// This instance works as a reader-writer lock for autonomous executor (AutonomousExecutor updates pipeline and children read it).
#[derive(Debug, Default)]
pub(crate) struct CurrentPipeline(RwLock<CurrentPipelineInner>);

impl CurrentPipeline {
    /// Blocks until it gets internal write lock.
    pub(in crate::stream_engine::autonomous_executor) fn update(&self, pipeline: Pipeline) {
        let mut inner = self
            .0
            .write()
            .expect("another thread sharing the same LatestPipeline must not panic");
        inner.borrow_mut().update(pipeline);
    }

    /// Blocks until it gets internal read lock.
    pub(in crate::stream_engine::autonomous_executor) fn read(
        &self,
    ) -> RwLockReadGuard<'_, CurrentPipelineInner> {
        self.0
            .read()
            .expect("another thread sharing the same LatestPipeline must not panic")
    }
}

#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct CurrentPipelineInner {
    pipeline: Pipeline,
    task_graph: TaskGraph,
}

impl CurrentPipelineInner {
    fn update(&mut self, pipeline: Pipeline) {
        let task_graph = TaskGraph::from(pipeline.as_graph());
        self.pipeline = pipeline;
        self.task_graph = task_graph;
    }

    pub(in crate::stream_engine::autonomous_executor) fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }
    pub(in crate::stream_engine::autonomous_executor) fn task_graph(&self) -> &TaskGraph {
        &self.task_graph
    }
}
