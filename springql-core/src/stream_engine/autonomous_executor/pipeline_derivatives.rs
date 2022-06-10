// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod task_repository;

use std::sync::Arc;

use crate::{
    api::error::Result,
    pipeline::{Pipeline, PipelineVersion},
    stream_engine::autonomous_executor::{
        pipeline_derivatives::task_repository::TaskRepository,
        task::Task,
        task_graph::{task_id::TaskId, TaskGraph},
    },
};

/// Pipeline, and task graph and tasks generated from the pipeline.
#[derive(Debug)]
pub struct PipelineDerivatives {
    pipeline: Pipeline,
    task_graph: TaskGraph,
    task_repo: TaskRepository,
}

impl PipelineDerivatives {
    pub fn new(pipeline: Pipeline) -> Self {
        let task_graph = TaskGraph::from(&pipeline);
        let task_repo = TaskRepository::from(pipeline.as_graph());
        Self {
            pipeline,
            task_graph,
            task_repo,
        }
    }

    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    pub fn pipeline_version(&self) -> PipelineVersion {
        self.pipeline.version()
    }

    pub fn task_graph(&self) -> &TaskGraph {
        &self.task_graph
    }

    pub fn task_repo(&self) -> &TaskRepository {
        &self.task_repo
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - TaskId is not found in task graph.
    pub fn get_task(&self, task_id: &TaskId) -> Result<Arc<Task>> {
        self.task_repo.get(task_id)
    }
}
