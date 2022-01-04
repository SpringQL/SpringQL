use std::fmt::Display;

use self::{row_task_id::RowTaskId, window_task_id::WindowTaskId};
use serde::{Deserialize, Serialize};

pub(in crate::stream_engine::autonomous_executor) mod row_task_id;
pub(in crate::stream_engine::autonomous_executor) mod window_task_id;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(in crate::stream_engine::autonomous_executor) enum TaskId {
    Row(RowTaskId),
    Window(WindowTaskId),
}

impl From<RowTaskId> for TaskId {
    fn from(row_task_id: RowTaskId) -> Self {
        Self::Row(row_task_id)
    }
}
impl From<WindowTaskId> for TaskId {
    fn from(window_task_id: WindowTaskId) -> Self {
        Self::Window(window_task_id)
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskId::Row(t) => write!(f, "{}", t),
            TaskId::Window(t) => write!(f, "{}", t),
        }
    }
}
