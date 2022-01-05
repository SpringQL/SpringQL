// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use anyhow::Context;

use crate::error::{Result, SpringError};
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::task_id::TaskId;

/// # Concept diagram
///
/// `[1]` represents a stream. `--a-->` represents a task.
///
/// ```text
/// ---a---->[1]---b------------>[2]---d-------------->[4]---f---------->
///           |    in buf: []          in buf: []       ^    in buf: []
///           |                                         |
///           +----c------------>[3]---e----------------+
///                in buf: []          in buf: []
/// ```
///
/// ```text
/// emit(r1, vec!["b", "c"]);
///
/// ---a---->[1]---b------------>[2]---d-------------->[4]---f---------->
///           |    in buf: [r1]        in buf: []       ^    in buf: []
///           |                                         |
///           +----c------------>[3]---e----------------+
///                in buf: [r1]        in buf: []
/// ```
///
/// ```text
/// collect_next("b");  // -> r1
///
/// ---a---->[1]---b------------>[2]---d-------------->[4]---f---------->
///           |    in buf: []          in buf: []       ^    in buf: []
///           |                                         |
///           +----c------------>[3]---e----------------+
///                in buf: [r1]        in buf: []
/// ```
///
/// ```text
/// emit(r2, vec!["b", "c"]);
///
/// ---a---->[1]---b------------>[2]---d-------------->[4]---f---------->
///           |    in buf: [r2]        in buf: []       ^    in buf: []
///           |                                         |
///           +----c------------>[3]---e----------------+
///                in buf: [r2,r1]     in buf: []
/// ```
///
/// ```text
/// collect_next("c");  // -> r1
///
/// ---a---->[1]---b------------>[2]---d-------------->[4]---f---------->
///           |    in buf: [r2]        in buf: []       ^    in buf: []
///           |                                         |
///           +----c------------>[3]---e----------------+
///                in buf: [r2]        in buf: []
/// ```
///
/// ```text
/// emit(r3, "f");
///
/// ---a---->[1]---b------------>[2]---d-------------->[4]---f---------->
///           |    in buf: [r2]        in buf: []       ^    in buf: [r3]
///           |                                         |
///           +----c------------>[3]---e----------------+
///                in buf: [r2]        in buf: []
/// ```
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct RowRepository {
    tasks_buf: Mutex<HashMap<TaskId, VecDeque<Row>>>,
}

impl RowRepository {
    pub(in crate::stream_engine::autonomous_executor) fn collect_next(
        &self,
        task: &TaskId,
    ) -> Result<Row> {
        log::debug!("[RowRepository] collect_next({:?})", task);

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

    pub(in crate::stream_engine::autonomous_executor) fn emit(
        &self,
        row: Row,
        downstream_tasks: &[TaskId],
    ) -> Result<()> {
        log::debug!(
            "[RowRepository] emit_owned({:?}, {:?})",
            row,
            downstream_tasks
        );

        let mut pumps_buf = self
            .tasks_buf
            .lock()
            .expect("another thread sharing the same RowRepository internal got panic");

        if downstream_tasks.len() == 1 {
            // no row clone
            let task = downstream_tasks.first().expect("1 len").clone();
            pumps_buf.entry(task).and_modify(|v| v.push_front(row));
        } else {
            for task in downstream_tasks {
                pumps_buf
                    .entry(task.clone())
                    .and_modify(|v| v.push_front(row.clone()));
            }
        }
        Ok(())
    }

    pub(in crate::stream_engine::autonomous_executor) fn reset(&self, tasks: Vec<TaskId>) {
        log::debug!("[RowRepository] reset({:?})", tasks);

        let new_tasks_buf = tasks
            .into_iter()
            .map(|t| (t, VecDeque::new()))
            .collect::<HashMap<TaskId, VecDeque<Row>>>();

        *self
            .tasks_buf
            .lock()
            .expect("another thread sharing the same RowRepository internal got panic") =
            new_tasks_buf;
    }
}
