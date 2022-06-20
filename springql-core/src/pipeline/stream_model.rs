// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod stream_shape;

pub use stream_shape::StreamShape;

use crate::pipeline::{field::ColumnReference, name::StreamName};

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub struct StreamModel {
    name: StreamName,
    shape: StreamShape,
}

impl StreamModel {
    pub fn name(&self) -> &StreamName {
        &self.name
    }

    pub fn shape(&self) -> &StreamShape {
        &self.shape
    }

    pub fn column_references(&self) -> Vec<ColumnReference> {
        self.shape
            .column_names()
            .iter()
            .map(|c| ColumnReference::Column {
                stream_name: self.name.clone(),
                column_name: c.clone(),
            })
            .collect()
    }
}
