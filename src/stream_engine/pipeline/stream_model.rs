pub(crate) mod stream_shape;

use crate::model::{name::StreamName, option::Options};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use self::stream_shape::StreamShape;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct StreamModel {
    name: StreamName,
    shape: Arc<StreamShape>,
    options: Options,
}

impl StreamModel {
    pub(crate) fn name(&self) -> &StreamName {
        &self.name
    }

    pub(crate) fn shape(&self) -> Arc<StreamShape> {
        self.shape.clone()
    }
}
