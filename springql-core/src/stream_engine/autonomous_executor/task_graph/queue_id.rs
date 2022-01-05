pub(in crate::stream_engine::autonomous_executor) mod row_queue_id;
pub(in crate::stream_engine::autonomous_executor) mod window_queue_id;

use serde::{Deserialize, Serialize};

use crate::pipeline::{pump_model::PumpModel, sink_writer_model::SinkWriterModel};

use self::{row_queue_id::RowQueueId, window_queue_id::WindowQueueId};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(in crate::stream_engine::autonomous_executor) enum QueueId {
    /// Sink tasks also have row queue.
    Row(RowQueueId),
    Window(WindowQueueId),
}

impl QueueId {
    pub(in crate::stream_engine::autonomous_executor) fn from_pump(pump: &PumpModel) -> Self {
        let name = pump.name().to_string();
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
