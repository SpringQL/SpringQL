use crate::pipeline::name::{ColumnName, StreamName};

/// Reference to a column in a row.
///
/// Note that this never point to other expressions like `1 + 1 AS a`.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(crate) struct ColumnReference {
    pub(crate) stream_name: StreamName,
    pub(crate) column_name: ColumnName,
}
