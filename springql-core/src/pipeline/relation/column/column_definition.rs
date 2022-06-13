// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::pipeline::name::ColumnName;

use crate::pipeline::relation::column::{
    column_constraint::ColumnConstraint, column_data_type::ColumnDataType,
};

/// Column definition used in DDL.
#[derive(Clone, Eq, PartialEq, Debug, new)]
pub struct ColumnDefinition {
    column_data_type: ColumnDataType,
    column_constraints: Vec<ColumnConstraint>,
}

impl ColumnDefinition {
    pub fn column_data_type(&self) -> &ColumnDataType {
        &self.column_data_type
    }

    pub fn column_name(&self) -> &ColumnName {
        self.column_data_type.column_name()
    }

    pub fn column_constraints(&self) -> &[ColumnConstraint] {
        &self.column_constraints
    }
}
