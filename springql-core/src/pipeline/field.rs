//! A field is a part of a tuple.
//! Types of fields are:
//!
//! - attribute in correlation (denoted as `correlation.attribute`).
//! - constant (e.g. `777`, `"abc").
//! - other expressions (e.g. `c1 + 1 AS c1p`).

pub(crate) mod aliased_field_name;
pub(crate) mod field_name;
pub(crate) mod field_pointer;

use crate::stream_engine::SqlValue;

use self::aliased_field_name::AliasedFieldName;

/// Field == SqlValue + AliasedFieldName
#[derive(Clone, PartialEq, Debug, new)]
pub(crate) struct Field {
    name: AliasedFieldName,
    value: SqlValue,
}

impl Field {
    pub(crate) fn name(&self) -> &AliasedFieldName {
        &self.name
    }

    pub(crate) fn sql_value(&self) -> &SqlValue {
        &self.value
    }

    pub(crate) fn into_sql_value(self) -> SqlValue {
        self.value
    }
}
