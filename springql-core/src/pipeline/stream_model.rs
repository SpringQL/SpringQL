// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod stream_shape;
use crate::pipeline::{field::ColumnReference, name::StreamName};
pub(crate) use stream_shape::StreamShape;

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(crate) struct StreamModel {
    name: StreamName,
    shape: StreamShape,
}

impl StreamModel {
    pub(crate) fn name(&self) -> &StreamName {
        &self.name
    }

    pub(crate) fn shape(&self) -> &StreamShape {
        &self.shape
    }

    pub(crate) fn column_references(&self) -> Vec<ColumnReference> {
        self.shape
            .column_names()
            .iter()
            .map(|c| ColumnReference::new(self.name.clone(), c.clone()))
            .collect()
    }
}
