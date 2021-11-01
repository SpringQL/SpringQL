use crate::stream_engine::pipeline::server_model::ServerModel;

#[derive(Debug, new)]
pub(in crate::stream_engine) struct SinkTask {}

impl From<&ServerModel> for SinkTask {
    fn from(_: &ServerModel) -> Self {
        todo!()
    }
}

