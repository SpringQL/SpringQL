use crate::pipeline::name::{ColumnName, StreamName};

use super::field_pointer::FieldPointer;

/// Reference to a column in a row.
///
/// Note that this never point to other expressions like `1 + 1 AS a`.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(crate) struct ColumnReference {
    pub(crate) stream_name: StreamName,
    pub(crate) column_name: ColumnName,
}

impl ColumnReference {
    /// Whether the index matches to this name.
    pub(crate) fn matches(&self, pointer: &FieldPointer) -> bool {
        match pointer.prefix() {
            Some(prefix) => self._prefix_attr_match(prefix, pointer.attr()),
            None => self._attr_matches(pointer.attr()),
        }
    }

    /// Whether the attr part of index matches to this name.
    fn _attr_matches(&self, attr: &str) -> bool {
        self.column_name.as_ref() == attr
    }

    /// Whether the prefix part of index matches to this name.
    fn _prefix_matches(&self, _prefix: &str) -> bool {
        true // FIXME label match
    }

    /// Whether both the attr part and prefix part of index match to this name.
    fn _prefix_attr_match(&self, prefix: &str, attr: &str) -> bool {
        self._prefix_matches(prefix) && self._attr_matches(attr)
    }
}

impl From<&ColumnReference> for FieldPointer {
    fn from(n: &ColumnReference) -> Self {
        let s = format!("{}.{}", n.stream_name, n.column_name);
        Self::from(s.as_str())
    }
}
