// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use super::{
    name::StreamName,
    stream_model::{stream_shape::StreamShape, StreamModel},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct SourceStreamModel(StreamModel);

impl SourceStreamModel {
    pub(crate) fn name(&self) -> &StreamName {
        self.0.name()
    }

    pub(crate) fn shape(&self) -> &StreamShape {
        self.0.shape()
    }
}
