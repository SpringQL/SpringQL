// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::pipeline::name::ColumnName;

use super::{column_constraint::ColumnConstraint, column_data_type::ColumnDataType};

/// Column definition used in DDL.
#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(crate) struct ColumnDefinition {
    column_data_type: ColumnDataType,
    column_constraints: Vec<ColumnConstraint>,
}

impl ColumnDefinition {
    pub(crate) fn column_data_type(&self) -> &ColumnDataType {
        &self.column_data_type
    }

    pub(crate) fn column_name(&self) -> &ColumnName {
        self.column_data_type.column_name()
    }

    pub(crate) fn column_constraints(&self) -> &[ColumnConstraint] {
        &self.column_constraints
    }
}
