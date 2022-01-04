// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use super::{name::StreamName, stream_model::StreamModel};
use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct SinkStreamModel(StreamModel);

impl SinkStreamModel {
    pub(crate) fn name(&self) -> &StreamName {
        self.0.name()
    }
}
