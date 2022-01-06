// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    pipeline_derivatives::PipelineDerivatives, repositories::Repositories,
    task_graph::task_id::TaskId,
};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct TaskContext {
    task: TaskId,

    // why a task need to know pipeline? -> source tasks need to know source stream's shape.
    pipeline_derivatives: Arc<PipelineDerivatives>,

    repos: Arc<Repositories>,
}

impl TaskContext {
    pub(in crate::stream_engine::autonomous_executor) fn new(
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

    pub(in crate::stream_engine) fn task(&self) -> TaskId {
        self.task.clone()
    }

    pub(in crate::stream_engine) fn pipeline_derivatives(&self) -> Arc<PipelineDerivatives> {
        self.pipeline_derivatives.clone()
    }

    pub(in crate::stream_engine) fn downstream_tasks(&self) -> Vec<TaskId> {
        let task_graph = self.pipeline_derivatives.task_graph();
        task_graph.downstream_tasks(&self.task)
    }

    pub(in crate::stream_engine) fn repos(&self) -> Arc<Repositories> {
        self.repos.clone()
    }
}
