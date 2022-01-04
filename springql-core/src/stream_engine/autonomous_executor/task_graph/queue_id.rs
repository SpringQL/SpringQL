pub(in crate::stream_engine::autonomous_executor) mod row_queue_id;
pub(in crate::stream_engine::autonomous_executor) mod window_queue_id;

use serde::{Deserialize, Serialize};

use self::{row_queue_id::RowQueueId, window_queue_id::WindowQueueId};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(in crate::stream_engine::autonomous_executor) enum QueueId {
    Row(RowQueueId),
    Window(WindowQueueId),
}

impl From<RowQueueId> for QueueId {
    fn from(row_queue_id: RowQueueId) -> Self {
        Self::Row(row_queue_id)
    }
}
impl From<WindowQueueId> for QueueId {
    fn from(window_queue_id: WindowQueueId) -> Self {
        Self::Window(window_queue_id)
    }
}
