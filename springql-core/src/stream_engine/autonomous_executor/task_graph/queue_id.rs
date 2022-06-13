// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod row_queue_id;
mod window_queue_id;

pub use row_queue_id::RowQueueId;
pub use window_queue_id::WindowQueueId;

use crate::pipeline::{PumpInputType, PumpModel, SinkWriterModel, StreamName};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub enum QueueId {
    /// Sink tasks also have row queue.
    Row(RowQueueId),
    Window(WindowQueueId),
}

impl QueueId {
    pub fn from_pump(pump: &PumpModel, upstream: &StreamName) -> Self {
        let name = format!("{}-{}", pump.name(), upstream);
        match pump.input_type() {
            PumpInputType::Row => Self::Row(RowQueueId::new(name)),
            PumpInputType::Window => Self::Window(WindowQueueId::new(name)),
        }
    }

    pub fn from_sink(sink: &SinkWriterModel) -> Self {
        let name = sink.name().to_string();
        Self::Row(RowQueueId::new(name))
    }
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
