pub(crate) mod stream_shape;

use std::rc::Rc;

use self::stream_shape::StreamShape;

use super::{name::StreamName, option::Options};

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
