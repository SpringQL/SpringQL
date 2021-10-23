use serde::{Deserialize, Serialize};

/// Column name.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct ColumnName(String);
