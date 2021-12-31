use std::{
    borrow::BorrowMut,
    sync::{RwLock, RwLockReadGuard},
};

use crate::pipeline::Pipeline;

use super::task::task_graph::TaskGraph;

// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

/// Latest pipeline and corresponding task graph.
///
/// This instance works as a reader-writer lock for autonomous executor (writer updates pipeline).
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct LatestPipeline(
    RwLock<LatestPipelineInner>,
);

impl LatestPipeline {
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
    ) -> RwLockReadGuard<'_, LatestPipelineInner> {
        self.0
            .read()
            .expect("another thread sharing the same LatestPipeline must not panic")
    }
}

#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct LatestPipelineInner {
    pipeline: Pipeline,
    task_graph: TaskGraph,
}

impl LatestPipelineInner {
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
