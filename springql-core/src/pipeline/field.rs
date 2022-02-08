//! A field is a part of a tuple.
//! Types of fields are:
//!
//! - attribute in correlation (denoted as `correlation.attribute`).
//! - constant (e.g. `777`, `"abc").
//! - other expressions (e.g. `c1 + 1 AS c1p`).

pub(crate) mod field_name;

use crate::stream_engine::SqlValue;

use self::field_name::ColumnReference;

/// Field == SqlValue + FieldName
#[derive(Clone, PartialEq, Debug, new)]
pub(crate) struct Field {
    name: ColumnReference, // TODO use ExprLabel instead
    value: SqlValue,
}

impl Field {
    pub(crate) fn name(&self) -> &ColumnReference {
        &self.name
    }

    pub(crate) fn sql_value(&self) -> &SqlValue {
        &self.value
    }

    pub(crate) fn into_sql_value(self) -> SqlValue {
        self.value
    }
}
