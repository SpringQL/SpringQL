// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::SpringError,
    pipeline::{stream_model::stream_shape::CANSourceStreamShape, StreamModel, StreamName},
};

use anyhow::anyhow;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct CANSourceStreamModel(StreamModel);

impl CANSourceStreamModel {
    fn new(name: StreamName) -> Self {
        let stream_model =
            StreamModel::new(name, CANSourceStreamShape::default().into_stream_shape());
        Self(stream_model)
    }

    pub fn as_stream_model(&self) -> &StreamModel {
        &self.0
    }
}

impl TryFrom<&StreamModel> for CANSourceStreamModel {
    type Error = SpringError;

    /// # Failure
    ///
    /// - `SpringError::InvalidFormat` when:
    ///   - This stream model is not compatible to CAN source stream.
    fn try_from(stream_model: &StreamModel) -> Result<Self, Self::Error> {
        if stream_model.shape() == &CANSourceStreamShape::default().into_stream_shape() {
            Ok(Self::new(stream_model.name().clone()))
        } else {
            Err(SpringError::InvalidFormat {
                s: "CAN source stream format".to_string(),
                source: anyhow!(
                    "`{}` does not conform to CAN source stream's shape: {:?}",
                    stream_model.name(),
                    stream_model
                ),
            })
        }
    }
}
