pub(crate) mod pump_state;

use serde::{Deserialize, Serialize};

use crate::stream_engine::command::{insert_as_plan::InsertAsPlan, query_plan::QueryPlan};

use self::pump_state::PumpState;

use super::name::{PumpName, StreamName};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct PumpModel {
    name: PumpName,
    state: PumpState,

    upstreams: Vec<StreamName>,
    downstream: StreamName,

    query_plan: QueryPlan,
    insert_as_plan: InsertAsPlan,
}

impl PumpModel {
    pub(crate) fn name(&self) -> &PumpName {
        &self.name
    }

    pub(crate) fn state(&self) -> &PumpState {
        &self.state
    }

    /// A pump can have 2 or more upstreams (on JOIN, for example).
    pub(crate) fn upstreams(&self) -> &[StreamName] {
        &self.upstreams
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
