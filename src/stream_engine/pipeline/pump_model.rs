use crate::{
    model::name::{PumpName, StreamName},
    stream_engine::autonomous_executor::task::Task,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct PumpModel {
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
