// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    pipeline_derivatives::PipelineDerivatives,
    repositories::Repositories,
    task_graph::{QueueId, TaskId},
};

/// Holds everything needed for a task execution.
#[derive(Debug)]
pub struct TaskContext {
    task: TaskId,

    // why a task need to know pipeline? -> source tasks need to know source stream's shape.
    pipeline_derivatives: Arc<PipelineDerivatives>,

    repos: Arc<Repositories>,
}

impl TaskContext {
    pub fn new(
        task: TaskId,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        repos: Arc<Repositories>,
    ) -> Self {
        Self {
            task,
            pipeline_derivatives,
            repos,
        }
    }

    pub fn task(&self) -> TaskId {
        self.task.clone()
    }

    pub fn pipeline_derivatives(&self) -> Arc<PipelineDerivatives> {
        self.pipeline_derivatives.clone()
    }

    pub fn output_queues(&self) -> Vec<QueueId> {
        let task_graph = self.pipeline_derivatives.task_graph();
        task_graph.output_queues(&self.task)
    }

    pub fn repos(&self) -> Arc<Repositories> {
        self.repos.clone()
    }
}
