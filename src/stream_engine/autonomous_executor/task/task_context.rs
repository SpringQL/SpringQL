use std::sync::Arc;

use crate::stream_engine::dependency_injection::DependencyInjection;

use super::{task_graph::TaskGraph, task_id::TaskId};

#[derive(Debug)]
pub(in crate::stream_engine) struct TaskContext<DI: DependencyInjection> {
    task: TaskId,
    downstream_tasks: Vec<TaskId>,

    row_repo: Arc<DI::RowRepositoryType>,
}

impl<DI: DependencyInjection> TaskContext<DI> {
    pub(in crate::stream_engine) fn new(
        task_graph: &TaskGraph,
        task: TaskId,
        row_repo: Arc<DI::RowRepositoryType>,
    ) -> Self {
        let downstream_tasks = task_graph.downstream_tasks(task.clone());
        Self {
            task,
            downstream_tasks,
            row_repo,
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
}

#[cfg(test)]
impl<DI: DependencyInjection> TaskContext<DI> {
    pub(in crate::stream_engine) fn _test_factory(
        task: TaskId,
        downstream_tasks: Vec<TaskId>,
        row_repo: Arc<DI::RowRepositoryType>,
    ) -> Self {
        Self {
            task,
            downstream_tasks,
            row_repo,
        }
    }
}
