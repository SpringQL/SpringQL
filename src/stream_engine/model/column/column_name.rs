use serde::{Deserialize, Serialize};

/// Column name.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub(in crate::stream_engine) struct ColumnName(String);
