use crate::model::name::{PumpName, StreamName};

#[derive(Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine) struct PumpModel {
    name: PumpName,
    upstream: StreamName,
    downstream: StreamName,
}

impl PumpModel {
    pub(in crate::stream_engine) fn name(&self) -> &PumpName {
        &self.name
    }
}
