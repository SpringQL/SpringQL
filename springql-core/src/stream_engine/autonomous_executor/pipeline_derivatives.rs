// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod task_repository;

use std::sync::Arc;

use self::task_repository::TaskRepository;
use crate::pipeline::Pipeline;
use crate::{api::error::Result, pipeline::pipeline_version::PipelineVersion};

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
        let task_graph = TaskGraph::from(&pipeline);
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

    pub(in crate::stream_engine::autonomous_executor) fn pipeline_version(
        &self,
    ) -> PipelineVersion {
        self.pipeline.version()
    }

    pub(in crate::stream_engine::autonomous_executor) fn task_graph(&self) -> &TaskGraph {
        &self.task_graph
    }

    pub(in crate::stream_engine::autonomous_executor) fn task_repo(&self) -> &TaskRepository {
        &self.task_repo
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
