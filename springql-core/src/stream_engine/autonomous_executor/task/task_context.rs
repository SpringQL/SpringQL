// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    pipeline_derivatives::PipelineDerivatives,
    repositories::Repositories,
    task_graph::{queue_id::QueueId, task_id::TaskId},
};

/// Holds everything needed for a task execution.
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

    /// Even a task with multiple input queues (e.g. JOIN) gets a row from a single queue for 1 task execution.
    ///
    /// # Returns
    ///
    /// None if task does not have input queue.
    /// Not only source tasks but also pump tasks without upstream `CREATE SOURCE READER` yet may return None.
    pub(in crate::stream_engine) fn input_queue(&self) -> Option<QueueId> {
        // TODO decide 1 input queue probabilistically from num rows in each queue.

        let task_graph = self.pipeline_derivatives.task_graph();
        task_graph.input_queues(&self.task).first().cloned()
    }
    pub(in crate::stream_engine) fn output_queues(&self) -> Vec<QueueId> {
        let task_graph = self.pipeline_derivatives.task_graph();
        task_graph.output_queues(&self.task)
    }

    pub(in crate::stream_engine) fn repos(&self) -> Arc<Repositories> {
        self.repos.clone()
    }
}
