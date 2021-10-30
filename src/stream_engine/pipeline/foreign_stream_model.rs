use std::rc::Rc;

use super::stream_model::{stream_shape::StreamShape, StreamModel};

#[derive(Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine) struct ForeignStreamModel(StreamModel);

impl ForeignStreamModel {
    pub(in crate::stream_engine) fn shape(&self) -> Rc<StreamShape> {
        self.0.shape()
    }
}
