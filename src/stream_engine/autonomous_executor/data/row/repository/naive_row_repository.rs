use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use anyhow::Context;

use crate::error::{Result, SpringError};
use crate::stream_engine::autonomous_executor::data::row::Row;
use crate::stream_engine::autonomous_executor::task::task_id::TaskId;

use super::RowRepository;

/// Has similar structure as RowRepository's concept diagram.
#[derive(Debug, Default)]
pub(crate) struct NaiveRowRepository {
    tasks_buf: RefCell<HashMap<TaskId, VecDeque<Rc<Row>>>>,
}

impl RowRepository for NaiveRowRepository {
    fn collect_next(&self, task: &TaskId) -> Result<Rc<Row>> {
        let row_ref = self
            .tasks_buf
            .borrow_mut()
            .get_mut(task)
            .unwrap()
            .pop_back()
            .context("next row not available")
            .map_err(|e| SpringError::InputTimeout {
                source: e,
                task_name: task.to_string(),
            })?;

        Ok(row_ref)
    }

    fn emit(&self, row_ref: Rc<Row>, downstream_tasks: &[TaskId]) -> Result<()> {
        let mut pumps_buf = self.tasks_buf.borrow_mut();
        for pump in downstream_tasks {
            // <https://github.com/rust-lang/rust-clippy/issues/5549>
            #[allow(clippy::redundant_closure)]
            pumps_buf
                .entry(pump.clone())
                .or_insert_with(|| VecDeque::new())
                .push_front(row_ref.clone());
        }

        Ok(())
    }

    fn emit_owned(&self, row: Row, downstream_tasks: &[TaskId]) -> Result<()> {
        let row_ref = Rc::new(row);
        self.emit(row_ref, downstream_tasks)
    }
}
