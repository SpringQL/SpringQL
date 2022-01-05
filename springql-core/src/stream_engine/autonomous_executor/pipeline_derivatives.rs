// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod task_repository;

use std::sync::Arc;

use self::task_repository::TaskRepository;
use crate::error::Result;
use crate::pipeline::Pipeline;

use super::{
    task::Task,
    task_graph::{task_id::TaskId, TaskGraph},
};

/// Pipeline, and task graph and tasks generated from the pipeline.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct PipelineDerivatives {
    pipeline: Pipeline,
    task_graph: TaskGraph,
    task_repo: TaskRepository,
}

impl PipelineDerivatives {
    pub(in crate::stream_engine::autonomous_executor) fn new(pipeline: Pipeline) -> Self {
        let task_graph = TaskGraph::from(pipeline.as_graph());
        let task_repo = TaskRepository::from(pipeline.as_graph());
        Self {
            pipeline,
            task_graph,
            task_repo,
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    pub(in crate::stream_engine::autonomous_executor) fn task_graph(&self) -> &TaskGraph {
        &self.task_graph
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - TaskId is not found in task graph.
    pub(in crate::stream_engine::autonomous_executor) fn get_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Arc<Task>> {
        self.task_repo.get(task_id)
    }
}
