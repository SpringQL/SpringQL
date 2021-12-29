// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::stream_engine::dependency_injection::DependencyInjection;

use super::{
    source_task::{
        sink_subtask::sink_subtask_repository::SinkSubtaskRepository,
        source_subtask::source_subtask_repository::SourceSubtaskRepository,
    },
    task_graph::TaskGraph,
    task_id::TaskId,
};

#[derive(Debug)]
pub(in crate::stream_engine) struct TaskContext<DI: DependencyInjection> {
    task: TaskId,
    downstream_tasks: Vec<TaskId>,

    row_repo: Arc<DI::RowRepositoryType>,

    source_subtask_repo: Arc<SourceSubtaskRepository>,
    sink_subtask_repo: Arc<SinkSubtaskRepository>,
}

impl<DI: DependencyInjection> TaskContext<DI> {
    pub(in crate::stream_engine) fn new(
        task_graph: &TaskGraph,
        task: TaskId,
        row_repo: Arc<DI::RowRepositoryType>,
        source_subtask_repo: Arc<SourceSubtaskRepository>,
        sink_subtask_repo: Arc<SinkSubtaskRepository>,
    ) -> Self {
        let downstream_tasks = task_graph.downstream_tasks(task.clone());
        Self {
            task,
            downstream_tasks,
            row_repo,
            source_subtask_repo,
            sink_subtask_repo,
        }
    }

    pub(in crate::stream_engine) fn task(&self) -> TaskId {
        self.task.clone()
    }

    pub(in crate::stream_engine) fn downstream_tasks(&self) -> Vec<TaskId> {
        self.downstream_tasks.clone()
    }

    pub(in crate::stream_engine) fn row_repository(&self) -> Arc<DI::RowRepositoryType> {
        self.row_repo.clone()
    }

    pub(in crate::stream_engine) fn source_subtask_repository(
        &self,
    ) -> Arc<SourceSubtaskRepository> {
        self.source_subtask_repo.clone()
    }
    pub(in crate::stream_engine) fn sink_subtask_repository(&self) -> Arc<SinkSubtaskRepository> {
        self.sink_subtask_repo.clone()
    }
}

#[cfg(test)]
impl<DI: DependencyInjection> TaskContext<DI> {
    pub(in crate::stream_engine) fn _test_factory(
        task: TaskId,
        downstream_tasks: Vec<TaskId>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_subtask_repo: Arc<SourceSubtaskRepository>,
        sink_subtask_repo: Arc<SinkSubtaskRepository>,
    ) -> Self {
        Self {
            task,
            downstream_tasks,
            row_repo,
            source_subtask_repo,
            sink_subtask_repo,
        }
    }
}
