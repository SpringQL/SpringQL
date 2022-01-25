use crate::pipeline::{
    correlation::aliased_correlation_name::AliasedCorrelationName,
    name::{AttributeName, ColumnName, CorrelationName, FieldAlias, StreamName},
};

use super::{field_name::FieldName, field_pointer::FieldPointer};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(crate) struct AliasedFieldName {
    pub(crate) field_name: FieldName,
    pub(crate) field_alias: Option<FieldAlias>,
}

impl AliasedFieldName {
    pub(crate) fn from_stream_column(stream_name: StreamName, column_name: ColumnName) -> Self {
        let correlation_name = CorrelationName::new(stream_name.to_string());
        let aliased_correlation_name = AliasedCorrelationName::new(correlation_name, None);

        let attribute_name = AttributeName::new(column_name.to_string());

        let field_name = FieldName::new(aliased_correlation_name, attribute_name);

        Self::new(field_name, None)
    }

    /// Whether the index matches to this name.
    pub(crate) fn matches(&self, pointer: &FieldPointer) -> bool {
        match pointer.prefix() {
            Some(prefix) => self._prefix_attr_match(prefix, pointer.attr()),
            None => self._attr_matches(pointer.attr()),
        }
    }

    /// Whether the attr part of index matches to this name.
    fn _attr_matches(&self, attr: &str) -> bool {
        self.field_name.attribute_name.as_ref() == attr
            || self
                .field_alias
                .as_ref()
                .map_or_else(|| false, |alias| alias.as_ref() == attr)
    }

    /// Whether the prefix part of index matches to this name.
    fn _prefix_matches(&self, prefix: &str) -> bool {
        let aliased_correlation_name = &self.field_name.aliased_correlation_name;
        aliased_correlation_name.correlation_name.as_ref() == prefix
            || aliased_correlation_name
                .correlation_alias
                .as_ref()
                .map_or_else(|| false, |alias| alias.as_ref() == prefix)
    }

    /// Whether both the attr part and prefix part of index match to this name.
    fn _prefix_attr_match(&self, prefix: &str, attr: &str) -> bool {
        self._prefix_matches(prefix) && self._attr_matches(attr)
    }

    pub(crate) fn to_stream_name(&self) -> StreamName {
        let correlantion_name = self
            .field_name
            .aliased_correlation_name
            .correlation_name
            .to_string();
        StreamName::new(correlantion_name)
    }

    pub(crate) fn to_column_name(&self) -> ColumnName {
        let attribute_name = self.field_name.attribute_name.to_string();
        ColumnName::new(attribute_name)
    }
}

impl From<&AliasedFieldName> for FieldPointer {
    fn from(n: &AliasedFieldName) -> Self {
        Self::from(&n.field_name)
    }
}
