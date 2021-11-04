pub(crate) mod pump_state;

use crate::model::name::{PumpName, StreamName};
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
    pub(crate) fn name(&self) -> &PumpName {
        &self.name
    }

    pub(crate) fn state(&self) -> &PumpState {
        &self.state
    }

    pub(crate) fn upstream(&self) -> &StreamName {
        &self.upstream
    }

    pub(crate) fn downstream(&self) -> &StreamName {
        &self.downstream
    }

    pub(crate) fn started(&self) -> Self {
        Self {
            state: PumpState::Started,
            ..self.clone()
        }
    }
}
