mod repository;

use serde::{Deserialize, Serialize};

use super::{column::column_definition::ColumnDefinition, name::StreamName};

/// Small enough to be held by each row.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StreamModelRef;

impl StreamModelRef {
    fn get(&self) -> &StreamModel {
        unimplemented!()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StreamModel {
    name: StreamName,
    cols: Vec<ColumnDefinition>,
}
