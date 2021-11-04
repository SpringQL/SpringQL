use serde::{Deserialize, Serialize};

use super::{column_constraint::ColumnConstraint, column_data_type::ColumnDataType};

/// Column definition used in DDL.
#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct ColumnDefinition {
    column_data_type: ColumnDataType,
    column_constraints: Vec<ColumnConstraint>,
}

impl ColumnDefinition {
    pub(crate) fn column_data_type(&self) -> &ColumnDataType {
        &self.column_data_type
    }

    pub(crate) fn column_constraints(&self) -> &[ColumnConstraint] {
        &self.column_constraints
    }
}
