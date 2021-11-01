use crate::stream_engine::pipeline::pump_model::PumpModel;

use super::task_id::TaskId;

#[derive(Debug)]
pub(crate) struct PumpTask {
    id: TaskId,
}

impl From<&PumpModel> for PumpTask {
    fn from(pump: &PumpModel) -> Self {
        let id = TaskId::from_pump(pump.name().clone());
        Self { id }
    }
}

impl PumpTask {
    pub(in crate::stream_engine) fn id(&self) -> &TaskId {
        &self.id
    }
}
