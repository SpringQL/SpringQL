use serde::{Deserialize, Serialize};

use crate::stream_engine::pipeline::{pump_model::PumpModel, server_model::ServerModel};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(in crate::stream_engine) enum Edge {
    Pump(PumpModel),
    Source(ServerModel),
    Sink(ServerModel),
}
