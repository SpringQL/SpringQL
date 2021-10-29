pub(crate) mod stream_shape;

use std::rc::Rc;

use crate::model::{name::StreamName, option::Options};

use self::stream_shape::StreamShape;

#[derive(Eq, PartialEq, Debug, new)]
pub(crate) struct StreamModel {
    name: StreamName,
    shape: Rc<StreamShape>,
    options: Options,
}

impl StreamModel {
    pub(crate) fn shape(&self) -> Rc<StreamShape> {
        self.shape.clone()
    }
}
