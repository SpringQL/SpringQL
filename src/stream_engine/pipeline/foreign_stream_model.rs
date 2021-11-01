use crate::model::name::StreamName;

use super::stream_model::{stream_shape::StreamShape, StreamModel};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(in crate::stream_engine) struct ForeignStreamModel(StreamModel);

impl ForeignStreamModel {
    pub(in crate::stream_engine) fn name(&self) -> &StreamName {
        self.0.name()
    }

    pub(in crate::stream_engine) fn shape(&self) -> Arc<StreamShape> {
        self.0.shape()
    }
}
