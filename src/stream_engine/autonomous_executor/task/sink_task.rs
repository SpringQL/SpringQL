use crate::stream_engine::pipeline::server_model::ServerModel;

use super::task_id::TaskId;

#[derive(Debug)]
pub(crate) struct SinkTask {
    id: TaskId,
}

impl From<&ServerModel> for SinkTask {
    fn from(server: &ServerModel) -> Self {
        let id = TaskId::from_sink_server(server.serving_foreign_stream().name().clone());
        Self { id }
    }
}

impl SinkTask {
    pub(in crate::stream_engine) fn new(id: TaskId) -> Self {
        Self { id }
    }

    pub(in crate::stream_engine) fn id(&self) -> &TaskId {
        &self.id
    }
}
