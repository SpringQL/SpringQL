// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;

use crate::{
    error::{Result, SpringError},
    pipeline::pipeline_graph::{edge::Edge, PipelineGraph},
    stream_engine::autonomous_executor::{
        task::{pump_task::PumpTask, Task},
        task_graph::task_id::TaskId,
    },
};

#[derive(Debug, Default)]
pub(super) struct TaskRepository {
    repo: HashMap<TaskId, Arc<Task>>,
}

impl TaskRepository {
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - TaskId is not found in task repo.
    pub(super) fn get(&self, task_id: &TaskId) -> Result<Arc<Task>> {
        self.repo
            .get(task_id)
            .ok_or_else(|| {
                SpringError::Sql(anyhow!("task id {} is not in TaskRepository", task_id))
            })
            .map(|t| t.clone())
    }
}

impl From<&PipelineGraph> for TaskRepository {
    fn from(pipeline_graph: &PipelineGraph) -> Self {
        let pipeline_petgraph = pipeline_graph.as_petgraph();
        let repo = pipeline_petgraph
            .edge_weights()
            .map(|edge| {
                let task = Task::from(edge);
                (task.id(), Arc::new(task))
            })
            .collect();
        Self { repo }
    }
}
