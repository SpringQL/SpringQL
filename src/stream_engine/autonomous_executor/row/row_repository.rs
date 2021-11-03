pub(crate) mod naive_row_repository;

use std::sync::Arc;

pub(crate) use naive_row_repository::NaiveRowRepository;

use super::Row;
use crate::{error::Result, stream_engine::autonomous_executor::task::task_id::TaskId};
use std::fmt::Debug;

/// Rows in SpringQL are highly re-used around tasks in order to reduce working memory.
///
/// This nature is particularly effective for `WHERE` pumps and WINDOW pumps, who do not change either of rows' columns and values.
///
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
pub(crate) trait RowRepository: Debug + Default + Sync + Send {
    /// Get the next RowRef as `task`'s input.
    ///
    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - next row is not available within `timeout`
    fn collect_next(&self, task: &TaskId) -> Result<Arc<Row>> {
        log::debug!("[RowRepository] collect_next({:?})", task);
        self._collect_next(task)
    }
    fn _collect_next(&self, task: &TaskId) -> Result<Arc<Row>>;

    /// Gives `row_ref` to downstream tasks.
    fn emit(&self, row_ref: Arc<Row>, downstream_tasks: &[TaskId]) -> Result<()> {
        debug_assert!(!downstream_tasks.is_empty());
        log::debug!(
            "[RowRepository] emit({:?}, {:?})",
            row_ref,
            downstream_tasks
        );
        self._emit(row_ref, downstream_tasks)
    }
    fn _emit(&self, row_ref: Arc<Row>, downstream_tasks: &[TaskId]) -> Result<()>;

    /// Move newly created `row` to downstream tasks.
    fn emit_owned(&self, row: Row, downstream_tasks: &[TaskId]) -> Result<()> {
        debug_assert!(!downstream_tasks.is_empty());
        log::debug!(
            "[RowRepository] emit_owned({:?}, {:?})",
            row,
            downstream_tasks
        );
        self._emit_owned(row, downstream_tasks)
    }
    fn _emit_owned(&self, row: Row, downstream_tasks: &[TaskId]) -> Result<()>;

    /// Reset repository with new tasks.
    fn reset(&self, tasks: Vec<TaskId>) {
        log::debug!("[RowRepository] reset({:?})", tasks);
        self._reset(tasks)
    }
    fn _reset(&self, tasks: Vec<TaskId>);
}
