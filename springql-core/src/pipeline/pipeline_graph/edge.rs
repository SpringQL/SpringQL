// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::pipeline::{pump_model::PumpModel, server_model::ServerModel};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum Edge {
    /// A pump can have 2 or more upstreams (on JOIN, for example). Then, graph edges share the same PumpModel.
    Pump(Arc<PumpModel>),
    Source(ServerModel),
    Sink(ServerModel),
}
