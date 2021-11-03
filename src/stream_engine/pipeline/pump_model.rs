pub(crate) mod pump_state;

use crate::{
    model::name::{PumpName, StreamName},
    stream_engine::autonomous_executor::task::Task,
};
use serde::{Deserialize, Serialize};

use self::pump_state::PumpState;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct PumpModel {
    name: PumpName,
    state: PumpState,

    upstream: StreamName,
    downstream: StreamName,
}

impl PumpModel {
    pub(in crate::stream_engine) fn name(&self) -> &PumpName {
        &self.name
    }

    pub(in crate::stream_engine) fn state(&self) -> &PumpState {
        &self.state
    }

    pub(in crate::stream_engine) fn upstream(&self) -> &StreamName {
        &self.upstream
    }

    pub(in crate::stream_engine) fn downstream(&self) -> &StreamName {
        &self.downstream
    }

    pub(in crate::stream_engine) fn started(&self) -> Self {
        Self {
            state: PumpState::Started,
            ..self.clone()
        }
    }
}
