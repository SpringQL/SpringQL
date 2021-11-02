pub(crate) mod naive_row_repository;

use std::rc::Rc;

pub(crate) use naive_row_repository::NaiveRowRepository;

use crate::{
    error::Result, model::name::PumpName, stream_engine::autonomous_executor::task::task_id::TaskId,
};

use super::Row;

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
pub(crate) trait RowRepository: Default {
    /// Get the next RowRef as `task`'s input.
    ///
    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - next row is not available within `timeout`
    fn collect_next(&self, task: &TaskId) -> Result<Rc<Row>>;

    /// Gives `row_ref` to downstream tasks.
    fn emit(&self, row_ref: Rc<Row>, downstream_tasks: &[TaskId]) -> Result<()>;

    /// Move newly created `row` to downstream tasks.
    fn emit_owned(&self, row: Row, downstream_tasks: &[TaskId]) -> Result<()>;
}
