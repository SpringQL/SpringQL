use serde::{Deserialize, Serialize};

use super::{
    column::column_definition::ColumnDefinition,
    name::{ColumnName, StreamName},
};

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct StreamModel {
    name: StreamName,
    cols: Vec<ColumnDefinition>,
    rowtime: Option<ColumnName>,
}

impl StreamModel {
    pub(crate) fn rowtime(&self) -> Option<&ColumnName> {
        self.rowtime.as_ref()
    }

    pub(crate) fn columns(&self) -> &[ColumnDefinition] {
        &self.cols
    }
}
