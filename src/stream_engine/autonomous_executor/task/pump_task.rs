use crate::stream_engine::pipeline::pump_model::PumpModel;

#[derive(Debug)]
pub(in crate::stream_engine) struct PumpTask {}

impl From<&PumpModel> for PumpTask {
    fn from(_: &PumpModel) -> Self {
        todo!()
    }
}
