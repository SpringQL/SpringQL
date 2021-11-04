use serde::{Deserialize, Serialize};

/// Column with data type.
#[derive(Eq, PartialEq, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) enum ColumnConstraint {
    Rowtime,
}
