// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(in crate::stream_engine::autonomous_executor) mod row_queue_id;
pub(in crate::stream_engine::autonomous_executor) mod window_queue_id;

use crate::{
    pipeline::{PumpInputType, PumpModel, SinkWriterModel, StreamName},
    stream_engine::autonomous_executor::task_graph::queue_id::{
        row_queue_id::RowQueueId, window_queue_id::WindowQueueId,
    },
};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub enum QueueId {
    /// Sink tasks also have row queue.
    Row(RowQueueId),
    Window(WindowQueueId),
}

impl QueueId {
    pub(in crate::stream_engine::autonomous_executor) fn from_pump(
        pump: &PumpModel,
        upstream: &StreamName,
    ) -> Self {
        let name = format!("{}-{}", pump.name(), upstream);
        match pump.input_type() {
            PumpInputType::Row => Self::Row(RowQueueId::new(name)),
            PumpInputType::Window => Self::Window(WindowQueueId::new(name)),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn from_sink(sink: &SinkWriterModel) -> Self {
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
