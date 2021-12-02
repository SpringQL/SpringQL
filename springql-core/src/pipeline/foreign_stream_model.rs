// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use super::{
    name::StreamName,
    stream_model::{stream_shape::StreamShape, StreamModel},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct ForeignStreamModel(StreamModel);

impl ForeignStreamModel {
    pub(crate) fn name(&self) -> &StreamName {
        self.0.name()
    }

    pub(crate) fn shape(&self) -> Arc<StreamShape> {
        self.0.shape()
    }
}
