use crate::pipeline::{
    field::aliased_field_name::AliasedFieldName,
    name::{ColumnName, StreamName},
};

/// Aliaser is a glue of names between streams (rows) tuples.
///
/// Streams use ColumnName as a full column name, while tuples use AliasedFieldName as a fully-aliased field name.
/// Rows do not contain aliases but tuples used everywhere should hold them in order for FieldPointer to correctly pick aliased field names.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub(crate) struct Aliaser(Vec<AliasedFieldName>);

impl Aliaser {
    pub(crate) fn alias(
        &self,
        stream_name: &StreamName,
        column_name: &ColumnName,
    ) -> AliasedFieldName {
        self.0
            .iter()
            .find_map(|afn| {
                if &afn.to_stream_name() == stream_name && &afn.to_column_name() == column_name {
                    Some(afn.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                AliasedFieldName::from_stream_column(stream_name.clone(), column_name.clone())
            })
    }
}

impl From<Vec<AliasedFieldName>> for Aliaser {
    fn from(aliased_field_names: Vec<AliasedFieldName>) -> Self {
        Self(aliased_field_names)
    }
}
