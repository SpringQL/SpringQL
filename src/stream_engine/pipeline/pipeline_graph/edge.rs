use serde::{Deserialize, Serialize};

use crate::stream_engine::{
    autonomous_executor::task::Task,
    pipeline::{
        pump_model::{pump_state::PumpState, PumpModel},
        server_model::ServerModel,
    },
};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(in crate::stream_engine) enum Edge {
    Pump(PumpModel),
    Source(ServerModel),
    Sink(ServerModel),
}
