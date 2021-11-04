use serde::{Deserialize, Serialize};

use crate::pipeline::{pump_model::PumpModel, server_model::ServerModel};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum Edge {
    Pump(PumpModel),
    Source(ServerModel),
    Sink(ServerModel),
}
