// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod stream_shape;

use serde::{Deserialize, Serialize};

use self::stream_shape::StreamShape;

use super::{field::field_name::ColumnReference, name::StreamName};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
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
