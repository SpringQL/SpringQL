use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::Context;

use crate::error::{Result, SpringError};
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::task_id::TaskId;

use super::RowRepository;

/// Has similar structure as RowRepository's concept diagram.
#[derive(Debug, Default)]
pub(crate) struct NaiveRowRepository {
    tasks_buf: Mutex<HashMap<TaskId, VecDeque<Arc<Row>>>>,
}

impl RowRepository for NaiveRowRepository {
    fn _collect_next(&self, task: &TaskId) -> Result<Arc<Row>> {
        let row_ref = self
            .tasks_buf
            .lock()
            .expect("another thread sharing the same RowRepository internal got panic")
            .get_mut(task)
            .with_context(|| {
                format!(
                    "task ({:?}) has not yet registered to the RowRepository internal",
                    task
                )
            })
            .and_then(|rows| rows.pop_back().context("next row not available"))
            .map_err(|e| SpringError::InputTimeout {
                source: e,
                task_name: task.to_string(),
            })?;

        Ok(row_ref)
    }

    fn _emit(&self, row_ref: Arc<Row>, downstream_tasks: &[TaskId]) -> Result<()> {
        let mut pumps_buf = self
            .tasks_buf
            .lock()
            .expect("another thread sharing the same RowRepository internal got panic");
        for task in downstream_tasks {
            pumps_buf
                .entry(task.clone())
                .and_modify(|v| v.push_front(row_ref.clone()));
        }

        Ok(())
    }

    fn _emit_owned(&self, row: Row, downstream_tasks: &[TaskId]) -> Result<()> {
        let row_ref = Arc::new(row);
        self.emit(row_ref, downstream_tasks)
    }

    fn _reset(&self, tasks: Vec<TaskId>) {
        let new_tasks_buf = tasks
            .into_iter()
            .map(|t| (t, VecDeque::new()))
            .collect::<HashMap<TaskId, VecDeque<Arc<Row>>>>();

        *self
            .tasks_buf
            .lock()
            .expect("another thread sharing the same RowRepository internal got panic") =
            new_tasks_buf;
    }
}
