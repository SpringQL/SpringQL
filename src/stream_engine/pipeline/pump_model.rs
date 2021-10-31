use crate::model::name::{PumpName, StreamName};
use serde::{Deserialize, Serialize};

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(in crate::stream_engine) struct PumpModel {
    name: PumpName,
    upstream: StreamName,
    downstream: StreamName,
}

impl PumpModel {
    pub(in crate::stream_engine) fn name(&self) -> &PumpName {
        &self.name
    }

    pub(in crate::stream_engine) fn upstream(&self) -> &StreamName {
        &self.upstream
    }

    pub(in crate::stream_engine) fn downstream(&self) -> &StreamName {
        &self.downstream
    }
}
