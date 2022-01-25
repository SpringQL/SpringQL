use crate::pipeline::{
    correlation::aliased_correlation_name::AliasedCorrelationName, name::AttributeName,
};

use super::field_pointer::FieldPointer;

/// Name of a field.
/// Although correlation name is sometimes omitted in SQL, it must be supplied from context to create this struct.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(crate) struct FieldName {
    pub(crate) aliased_correlation_name: AliasedCorrelationName,
    pub(crate) attribute_name: AttributeName,
}

impl From<&FieldName> for FieldPointer {
    fn from(n: &FieldName) -> Self {
        let s = format!(
            "{}.{}",
            n.aliased_correlation_name.correlation_name, n.attribute_name
        );
        Self::from(s.as_str())
    }
}
