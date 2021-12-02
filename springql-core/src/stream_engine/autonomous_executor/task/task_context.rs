// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::stream_engine::{
    autonomous_executor::server_instance::server_repository::ServerRepository,
    dependency_injection::DependencyInjection,
};

use super::{task_graph::TaskGraph, task_id::TaskId};

#[derive(Debug)]
pub(in crate::stream_engine) struct TaskContext<DI: DependencyInjection> {
    task: TaskId,
    downstream_tasks: Vec<TaskId>,

    row_repo: Arc<DI::RowRepositoryType>,
    server_repo: Arc<ServerRepository>,
}

impl<DI: DependencyInjection> TaskContext<DI> {
    pub(in crate::stream_engine) fn new(
        task_graph: &TaskGraph,
        task: TaskId,
        row_repo: Arc<DI::RowRepositoryType>,
        server_repo: Arc<ServerRepository>,
    ) -> Self {
        let downstream_tasks = task_graph.downstream_tasks(task.clone());
        Self {
            task,
            downstream_tasks,
            row_repo,
            server_repo,
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

    pub(in crate::stream_engine) fn server_repository(&self) -> Arc<ServerRepository> {
        self.server_repo.clone()
    }
}

#[cfg(test)]
impl<DI: DependencyInjection> TaskContext<DI> {
    pub(in crate::stream_engine) fn _test_factory(
        task: TaskId,
        downstream_tasks: Vec<TaskId>,
        row_repo: Arc<DI::RowRepositoryType>,
        server_repo: Arc<ServerRepository>,
    ) -> Self {
        Self {
            task,
            downstream_tasks,
            row_repo,
            server_repo,
        }
    }
}
