pub(crate) mod stream_shape;

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use self::stream_shape::StreamShape;

use super::{name::StreamName, option::Options};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
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
